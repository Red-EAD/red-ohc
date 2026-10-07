package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.CacheMaintenanceException;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.SizeClasses;
import com.red.ohc.storage.WriterArena;

/** Failure after value preparation must release both the private value and any writer claim. */
public final class PreparedPutFailureTest {
  private static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] value = new byte[buffer.remaining()];
          buffer.get(value);
          return value;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @DataProvider
  public Object[][] preparedValues() {
    return new Object[][] {
      {false, 5_120, false},
      {true, 5_120, false},
      {false, 40_000, false},
      {true, 40_000, false},
      {false, 5_120, true},
      {true, 5_120, true}
    };
  }

  @Test(dataProvider = "preparedValues", timeOut = 20_000L)
  public void lookupFailureReleasesPreparedValueAndPreclaimedWriter(
      boolean present, int length, boolean cleanupFails)
      throws Exception {
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    ConcurrentHashMap<Entry, Entry> original = cache.data;
    MaintenanceEventLoop worker = (MaintenanceEventLoop) field(cache, "worker");
    NativeMemory.Memory memory = (NativeMemory.Memory) field(cache, "memory");
    Field dataField = OffHeapCache.class.getDeclaredField("data");
    dataField.setAccessible(true);
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch resumeActor = new CountDownLatch(1);
    Method retirementHook =
        WriterArena.class.getDeclaredMethod("setRetirementHookForTest", Runnable.class);
    retirementHook.setAccessible(true);
    WriterArena arena = null;
    Entry existing = null;
    Throwable primaryFailure = null;
    try {
      byte[] key = new byte[] {1};
      byte[] oldValue = new byte[cleanupFails ? 1 : length];
      Arrays.fill(oldValue, (byte) 7);
      // Warm the writer resource so raw resource allocation cannot hide a direct-value leak.
      cache.put(new byte[] {2}, new byte[] {3});
      cache.remove(new byte[] {2});
      if (present) {
        cache.put(key, oldValue);
      }
      cache.flushAsync().get(5L, TimeUnit.SECONDS);
      existing = original.isEmpty() ? null : original.values().iterator().next();
      assertTrue(
          worker.submitActorTaskForTest(
              () -> {
                actorPaused.countDown();
                try {
                  assertTrue(resumeActor.await(10L, TimeUnit.SECONDS));
                } catch (InterruptedException failure) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(failure);
                }
              },
              failure -> {
                throw new AssertionError(failure);
              }));
      assertTrue(actorPaused.await(5L, TimeUnit.SECONDS));
      long slotsBefore = liveSlots(memory);
      long bytesBefore = cache.totalAllocatedBytes();
      IllegalStateException injected = new IllegalStateException("failure during put lookup");
      IllegalArgumentException cleanupFailure = new IllegalArgumentException("cleanup failure");
      @SuppressWarnings("unchecked")
      ThreadLocal<ThreadContext> contexts = (ThreadLocal<ThreadContext>) field(cache, "contexts");
      arena = contexts.get().writer();
      if (cleanupFails) {
        retirementHook.invoke(
            arena,
            (Runnable)
                () -> {
                  throw cleanupFailure;
                });
      }
      ConcurrentHashMap<Entry, Entry> failingLookup =
          new ConcurrentHashMap<Entry, Entry>() {
            @Override
            public com.red.ohc.index.Entry get(Object lookup) {
              com.red.ohc.index.Entry entry = original.get(lookup);
              worker.recordTerminalFailure(injected);
              return entry;
            }
          };
      dataField.set(cache, failingLookup);
      CacheMaintenanceException failure =
          expectThrows(CacheMaintenanceException.class, () -> cache.put(key, new byte[length]));
      dataField.set(cache, original);

      assertSame(failure.getCause(), injected, "cleanup must retain the original failure");
      if (cleanupFails) {
        assertEquals(failure.getSuppressed().length, 1);
        assertSame(failure.getSuppressed()[0], cleanupFailure);
      }
      if (existing != null) {
        assertFalse(existing.isWriterLocked(), "failed put retained its preclaimed writer");
        assertEquals(cache.get(key), oldValue, "failed replacement changed the live value");
      } else {
        assertTrue(original.isEmpty(), "failed insertion published an entry");
      }
      assertEquals(liveSlots(memory), slotsBefore, "failed put retained a private pooled slot");
      if (length == 40_000) {
        assertEquals(cache.totalAllocatedBytes(), bytesBefore, "failed put leaked a direct value");
      }
    } catch (Throwable failure) {
      primaryFailure = failure;
      throw failure;
    } finally {
      try {
        dataField.set(cache, original);
        if (arena != null) {
          retirementHook.invoke(arena, (Object) null);
        }
        // The actor is still paused and no other caller exists: a leaked claim belongs to this put.
        // Release it on RED so an assertion failure cannot strand the fixture's shutdown.
        if (actorPaused.getCount() == 0L && existing != null && existing.isWriterLocked()) {
          existing.finishWriter();
        }
      } finally {
        resumeActor.countDown();
        CacheTestSupport.stop(cache, primaryFailure);
      }
    }
    assertEquals(cache.totalAllocatedBytes(), 0L);
  }

  private static long liveSlots(NativeMemory.Memory memory) {
    long[] pages = new long[SizeClasses.count()];
    long[] allocated = new long[SizeClasses.count()];
    long[] freed = new long[SizeClasses.count()];
    long cursor = 0L;
    do {
      cursor = memory.auditPageUsage(cursor, 1_024, pages, allocated, freed);
    } while (cursor >= 0L);
    long live = 0L;
    for (int index = 0; index < allocated.length; index++) {
      live += allocated[index] - freed[index];
    }
    return live;
  }

  private static Object field(Object owner, String name) throws Exception {
    Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }
}
