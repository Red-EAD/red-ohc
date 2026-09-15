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
      fail("cache must expose one internal event-driven writer wait/release protocol", missingProtocol);
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
    assertTrue(entry.claimWriter(), "the test owner must hold the native writer first");

    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean acquired = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread waiter =
        new Thread(
            () -> {
              started.countDown();
              try {
                acquired.set((Boolean) awaitWriter.invoke(cache, entry));
                if (acquired.get()) {
                  releaseWriter.invoke(cache, entry);
                }
              } catch (Throwable error) {
                failure.set(error);
              }
            },
            "ohc-writer-waiter");
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
    waiter.join(1_000L);
    assertFalse(waiter.isAlive(), "writer waiter was not notified after release");
    if (failure.get() != null) {
      throw new AssertionError("writer wait protocol failed", failure.get());
    }
    assertTrue(acquired.get(), "waiter must acquire after the owner releases the native writer");
    cache.close();
  }

  @Test(timeOut = 5_000L)
  public void closingWriterWaitersAreAllReleased() throws Exception {
    Method awaitWriter = OffHeapCache.class.getDeclaredMethod("awaitWriter", Entry.class);
    Method releaseWriter = OffHeapCache.class.getDeclaredMethod("releaseWriter", Entry.class);
    awaitWriter.setAccessible(true);
    releaseWriter.setAccessible(true);

    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    Entry entry = EntryTestSupport.entry(1, 8, 0L);
    assertTrue(entry.claimWriter(), "the test owner must hold the native writer first");

    CountDownLatch started = new CountDownLatch(2);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread first = closingWaiter(cache, awaitWriter, entry, started, failure, "ohc-closing-waiter-1");
    Thread second = closingWaiter(cache, awaitWriter, entry, started, failure, "ohc-closing-waiter-2");
    try {
      first.start();
      second.start();
      assertTrue(started.await(1L, TimeUnit.SECONDS));

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
      while ((first.getState() != Thread.State.WAITING
              || second.getState() != Thread.State.WAITING)
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(first.getState() == Thread.State.WAITING);
      assertTrue(second.getState() == Thread.State.WAITING);

      cache.close();
      releaseWriter.invoke(cache, entry);
      first.join(1_000L);
      second.join(1_000L);
      assertFalse(first.isAlive(), "first waiter was not released during close");
      assertFalse(second.isAlive(), "second waiter was not released during close");
      if (failure.get() != null) {
        throw new AssertionError("closing writer wait protocol failed", failure.get());
      }
    } finally {
      if (first.isAlive() || second.isAlive()) {
        synchronized (entry) {
          entry.notifyAll();
        }
      }
      if (entry.isWriterLocked()) {
        releaseWriter.invoke(cache, entry);
      }
      if (cache != null) {
        cache.close();
      }
    }
  }

  private static Thread closingWaiter(
      OffHeapCache<String, String> cache,
      Method awaitWriter,
      Entry entry,
      CountDownLatch started,
      AtomicReference<Throwable> failure,
      String name) {
    return new Thread(
        () -> {
          started.countDown();
          try {
            awaitWriter.invoke(cache, entry);
          } catch (Throwable error) {
            failure.compareAndSet(null, error);
          }
        },
        name);
  }
}
