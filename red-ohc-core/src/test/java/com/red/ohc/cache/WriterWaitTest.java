package com.red.ohc.cache;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;

/** Regression tests for event-driven Entry writer handoff. */
public final class WriterWaitTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
      };

  @Test(timeOut = 5_000L)
  public void contendedWriterWaitsForReleaseNotification() throws Exception {
    Method awaitWriter;
    Method releaseWriter;
    try {
      awaitWriter = OffHeapCache.class.getDeclaredMethod("awaitWriter", Entry.class);
      releaseWriter = OffHeapCache.class.getDeclaredMethod("releaseWriter", Entry.class);
    } catch (NoSuchMethodException missingProtocol) {
      fail(
          "cache must expose one internal event-driven writer wait/release protocol",
          missingProtocol);
      return;
    }
    awaitWriter.setAccessible(true);
    releaseWriter.setAccessible(true);

    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean acquired = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread waiter =
        new Thread(
            () -> {
              started.countDown();
              try {
                acquired.set((Long) awaitWriter.invoke(cache, entry) != 0L);
                if (acquired.get()) {
                  releaseWriter.invoke(cache, entry);
                }
              } catch (Throwable error) {
                failure.set(error);
              }
            },
            "ohc-writer-waiter");
    Throwable primaryFailure = null;
    boolean ownerReleased = false;
    try {
      assertTrue(entry.claimWriter(), "the test owner must hold the native writer first");
      waiter.start();
      assertTrue(started.await(1L, TimeUnit.SECONDS));

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
      while (waiter.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertFalse(waiter.getState() == Thread.State.TERMINATED, "waiter returned before release");
      assertTrue(
          waiter.getState() == Thread.State.WAITING,
          "a native writer conflict must wait for an event, not complete via a timer retry");

      releaseWriter.invoke(cache, entry);
      ownerReleased = true;
      waiter.join(1_000L);
      assertFalse(waiter.isAlive(), "writer waiter was not notified after release");
      if (failure.get() != null) {
        throw new AssertionError("writer wait protocol failed", failure.get());
      }
      assertTrue(acquired.get(), "waiter must acquire after the owner releases the native writer");

    } catch (Throwable operationFailure) {
      primaryFailure = operationFailure;
      throw operationFailure;
    } finally {
      if (!ownerReleased && entry.isWriterLocked()) {
        releaseWriter.invoke(cache, entry);
      }
      CacheTestSupport.stopAfterCallers(cache, primaryFailure, waiter);
    }
  }
}
