package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Ticker;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.runtime.ThreadContext;

public final class ExpiredReaderLaneTest {
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
  public Object[][] readPaths() {
    return new Object[][] {{"get"}, {"getDirect"}, {"getAll"}, {"getDirectAll"}};
  }

  @Test(dataProvider = "readPaths", timeOut = 30_000L)
  public void expiryLazilyCreatesAndReusesTheReaderLane(String path) throws Exception {
    AtomicLong nanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          public long nanos() {
            return nanos.get();
          }

          public long currentTimeMillis() {
            return 1_000L + nanos.get() / 1_000_000L;
          }
        };
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .defaultTTLmillis(1L)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Throwable failure = null;
    try {
      byte[] key = new byte[] {1};
      cache.put(key, new byte[] {2});
      cache.flushAsync().join();
      Field workerField = OffHeapCache.class.getDeclaredField("worker");
      workerField.setAccessible(true);
      MaintenanceEventLoop worker = (MaintenanceEventLoop) workerField.get(cache);
      assertTrue(
          worker.submitActorTaskForTest(
              () -> {
                paused.countDown();
                try {
                  release.await();
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(interrupted);
                }
              },
              error -> {
                throw new AssertionError(error);
              }));
      assertTrue(paused.await(2L, TimeUnit.SECONDS));
      Field contextsField = OffHeapCache.class.getDeclaredField("contexts");
      contextsField.setAccessible(true);
      ThreadLocal<?> contexts = (ThreadLocal<?>) contextsField.get(cache);
      caller
          .submit(
              () -> {
                ThreadContext context = (ThreadContext) contexts.get();
                assertEquals(probe(cache, key, path), 1);
                assertFalse(
                    context.hasWriterResources(),
                    "ordinary reads must not obtain writer resources");
                nanos.set(2_000_000L);
                assertEquals(probe(cache, key, path), 0);
                assertTrue(
                    context.hasWriterResources(), "first expiry must acquire its publishing lane");
                WriterLifecycleLane lane = context.lifecycleLane();
                assertEquals(lane.publishedRecordsTotal(), 1L);
                assertEquals(probe(cache, key, path), 0);
                assertTrue(context.lifecycleLane() == lane);
                assertEquals(lane.publishedRecordsTotal(), 1L, "duplicate hints must coalesce");
                assertEquals(context.readerDepth(), 0);
              })
          .get(2L, TimeUnit.SECONDS);
    } catch (Throwable operationFailure) {
      failure = operationFailure;
      throw operationFailure;
    } finally {
      release.countDown();
      CacheTestSupport.stop(cache, failure, caller);
    }
  }

  @Test(timeOut = 30_000L)
  public void delayedFirstHintDoesNotTouchReusedNativeMetadata() throws Exception {
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    com.red.ohc.storage.NativeMemory.Memory fixture =
        new com.red.ohc.storage.NativeMemory.Memory();
    com.red.ohc.index.Entry stale =
        com.red.ohc.index.EntryTestSupport.entry(fixture, 0, 17, 0L);
    java.lang.reflect.Method publish =
        OffHeapCache.class.getDeclaredMethod(
            "publishReaderMutation", com.red.ohc.index.Entry.class);
    publish.setAccessible(true);
    Field resourcesField = OffHeapCache.class.getDeclaredField("writerResources");
    resourcesField.setAccessible(true);
    Object resources = resourcesField.get(cache);
    Field lockField = resources.getClass().getDeclaredField("registrationLock");
    lockField.setAccessible(true);
    java.util.concurrent.atomic.AtomicReference<Throwable> failure =
        new java.util.concurrent.atomic.AtomicReference<>();
    Thread caller =
        new Thread(
            () -> {
              try {
                cache.get(new byte[] {9});
                publish.invoke(cache, stale);
              } catch (Throwable error) {
                failure.set(error);
              }
            },
            "delayed-expiry-hint");
    com.red.ohc.index.Entry reused = null;
    Throwable primaryFailure = null;
    try {
      synchronized (lockField.get(resources)) {
        caller.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (caller.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
          Thread.yield();
        }
        assertTrue(
            caller.getState() == Thread.State.BLOCKED, "first resource acquisition must pause");
        stale.markDead();
        reused = new com.red.ohc.index.Entry(stale.nativeKeyAddress, stale.keyLength, 0L);
        reused.initializeNativeMetadata();
      }
      CacheTestSupport.awaitCaller(caller);
      assertFalse(caller.isAlive());
      assertEquals(failure.get(), null);
      assertEquals(
          reused.mutationVersion(), 0L, "stale hint must not mutate the reused native block");
      assertEquals(reused.pendingFlags(), 0);
    } catch (Throwable error) {
      primaryFailure = error;
      throw error;
    } finally {
      CacheTestSupport.stopAfterCallers(cache, primaryFailure, caller);
      if (!caller.isAlive()) {
        fixture.closeArenas();
      }
    }
  }

  private static int probe(OffHeapCache<byte[], byte[]> cache, byte[] key, String path) {
    switch (path) {
      case "get":
        return cache.get(key) == null ? 0 : 1;
      case "getDirect":
        return cache.getDirect(key, value -> {}) ? 1 : 0;
      case "getAll":
        return cache.getAll(Collections.singletonList(key)).size();
      case "getDirectAll":
        return cache.getDirectAll(Collections.singletonList(key), (ignored, value) -> {});
      default:
        throw new AssertionError(path);
    }
  }
}
