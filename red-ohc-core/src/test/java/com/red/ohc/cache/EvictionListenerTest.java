package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.EvictionListener;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;

public final class EvictionListenerTest {
  @Test(timeOut = 5_000L)
  public void unusedLazyKeyAndValueAreNotDeserialized() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<RemovalCause> cause = new AtomicReference<>();

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(256L)
            .keySerializer(countingSerializer(keyDeserializations))
            .valueSerializer(countingSerializer(valueDeserializations))
            .evictionListener(
                (Supplier<String> key, Supplier<String> value, RemovalCause removalCause) -> {
                  cause.set(removalCause);
                  callback.countDown();
                })
            .buildTyped()) {
      assertTrue(cache.put("one", "value-one"));
      cache.flushAsync().join();
      assertTrue(cache.put("two", "value-two"));
      cache.flushAsync().join();

      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertEquals(cause.get(), RemovalCause.SIZE);
      assertEquals(keyDeserializations.get(), 0);
      assertEquals(valueDeserializations.get(), 0);
    }
  }

  @Test(timeOut = 5_000L)
  public void onlyAccessedSupplierIsDeserializedAndMemoized() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<String> observedKey = new AtomicReference<>();

    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(keyDeserializations),
            countingSerializer(valueDeserializations),
            (key, value, cause) -> {
              assertEquals(cause, RemovalCause.SIZE);
              observedKey.set(key.get());
              assertEquals(key.get(), observedKey.get());
              callback.countDown();
            })) {
      evictOne(cache);
      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertEquals(observedKey.get(), "one");
      assertEquals(keyDeserializations.get(), 1);
      assertEquals(valueDeserializations.get(), 0);
    }
  }

  @Test(timeOut = 5_000L)
  public void valueSupplierIsIndependentFromKeySupplier() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<String> observedValue = new AtomicReference<>();

    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(keyDeserializations),
            countingSerializer(valueDeserializations),
            (key, value, cause) -> {
              assertEquals(cause, RemovalCause.SIZE);
              observedValue.set(value.get());
              assertEquals(value.get(), observedValue.get());
              callback.countDown();
            })) {
      evictOne(cache);
      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertEquals(observedValue.get(), "value-one");
      assertEquals(keyDeserializations.get(), 0);
      assertEquals(valueDeserializations.get(), 1);
    }
  }

  @Test(timeOut = 5_000L)
  public void bothSuppliersDeserializeExactlyOnce() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    CountDownLatch callback = new CountDownLatch(1);

    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(keyDeserializations),
            countingSerializer(valueDeserializations),
            (key, value, cause) -> {
              assertEquals(key.get(), "one");
              assertEquals(value.get(), "value-one");
              assertEquals(key.get(), "one");
              assertEquals(value.get(), "value-one");
              callback.countDown();
            })) {
      evictOne(cache);
      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertEquals(keyDeserializations.get(), 1);
      assertEquals(valueDeserializations.get(), 1);
    }
  }

  @Test(timeOut = 5_000L)
  public void expiredEvictionUsesExpiredCause() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    AtomicReference<Long> nowMillis = new AtomicReference<>(1_000L);
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<RemovalCause> cause = new AtomicReference<>();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowMillis.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return nowMillis.get();
          }
        };

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(countingSerializer(keyDeserializations))
            .valueSerializer(countingSerializer(valueDeserializations))
            .evictionListener(
                (key, value, removalCause) -> {
                  cause.set(removalCause);
                  callback.countDown();
                })
            .buildTyped()) {
      assertTrue(cache.put("expired", "value", 2_000L));
      cache.flushAsync().join();
      nowMillis.set(3_000L);
      assertTrue(cache.put("trigger", "value", 4_000L));
      cache.flushAsync().join();

      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertEquals(cause.get(), RemovalCause.EXPIRED);
      assertEquals(keyDeserializations.get(), 0);
      assertEquals(valueDeserializations.get(), 0);
    }
  }

  @Test(timeOut = 5_000L)
  public void writerSideExpiredRemovalNotifiesListener() throws Exception {
    AtomicReference<Long> nowMillis = new AtomicReference<>(1_000L);
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<RemovalCause> cause = new AtomicReference<>();
    AtomicReference<String> observedKey = new AtomicReference<>();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowMillis.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return nowMillis.get();
          }
        };

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(countingSerializer(new AtomicInteger()))
            .valueSerializer(countingSerializer(new AtomicInteger()))
            .evictionListener(
                (key, value, removalCause) -> {
                  observedKey.set(key.get());
                  cause.set(removalCause);
                  callback.countDown();
                })
            .buildTyped()) {
      assertTrue(cache.put("expired", "old", 2_000L));
      cache.flushAsync().join();
      nowMillis.set(3_000L);

      assertTrue(cache.putIfAbsentAsync("expired", "new", 4_000L).join());
      assertTrue(callback.await(500L, TimeUnit.MILLISECONDS), "eviction listener was not called");
      assertEquals(observedKey.get(), "expired");
      assertEquals(cause.get(), RemovalCause.EXPIRED);
      assertEquals(cache.get("expired"), "new");
    }
  }

  @Test(timeOut = 5_000L)
  public void explicitRemovalDoesNotNotifyListener() throws Exception {
    CountDownLatch callback = new CountDownLatch(1);
    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> callback.countDown())) {
      assertTrue(cache.put("one", "value-one"));
      cache.flushAsync().join();
      assertTrue(cache.put("one", "replacement"));
      cache.flushAsync().join();
      assertTrue(cache.remove("one"));
      cache.flushAsync().join();
      assertTrue(callback.getCount() == 1L, "explicit removal unexpectedly notified listener");
    }
  }

  @Test(timeOut = 5_000L)
  public void closeDoesNotNotifyListener() throws Exception {
    CountDownLatch callback = new CountDownLatch(1);
    OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> callback.countDown());
    assertTrue(cache.put("one", "value-one"));
    cache.flushAsync().join();
    cache.close();
    assertEquals(callback.getCount(), 1L);
  }

  @Test(timeOut = 5_000L)
  public void lazyDeserializationFailureDoesNotBlockRetirement() throws Exception {
    AtomicInteger valueDeserializations = new AtomicInteger();
    CacheSerializer<String> failingValue =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            valueDeserializations.incrementAndGet();
            throw new IllegalStateException("synthetic eviction deserialization failure");
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
          }
        };

    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            failingValue,
            (key, value, cause) -> {
              value.get();
            })) {
      evictOne(cache);
      cache.flushAsync().join();
      assertEquals(valueDeserializations.get(), 1);
      assertEquals(cache.stats().evictionCount, 1L);
    }
  }

  @Test(timeOut = 5_000L)
  public void listenerExceptionDoesNotBlockRetirement() throws Exception {
    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> {
              throw new IllegalStateException("synthetic listener failure");
            })) {
      evictOne(cache);
      cache.flushAsync().join();
      assertEquals(cache.stats().evictionCount, 1L);
    }
  }

  @Test(timeOut = 5_000L)
  public void suppliersAreInvalidAfterTheCallbackReturns() throws Exception {
    AtomicReference<Supplier<String>> keySupplier = new AtomicReference<>();
    CountDownLatch callback = new CountDownLatch(1);
    try (OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> {
              keySupplier.set(key);
              callback.countDown();
            })) {
      evictOne(cache);
      assertTrue(callback.await(2L, TimeUnit.SECONDS), "eviction listener was not called");
      assertTrue(
          expectThrows(IllegalStateException.class, () -> keySupplier.get().get())
              instanceof IllegalStateException);
    }
  }

  @Test(timeOut = 5_000L)
  public void closeWaitsForAnInFlightEvictionCallback() throws Exception {
    CountDownLatch callbackEntered = new CountDownLatch(1);
    CountDownLatch releaseCallback = new CountDownLatch(1);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> {
              callbackEntered.countDown();
              try {
                assertTrue(
                    releaseCallback.await(2L, TimeUnit.SECONDS),
                    "test did not release eviction callback");
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
              }
            });
    try {
      assertTrue(cache.put("one", "value-one"));
      cache.flushAsync().join();
      Future<?> eviction =
          callers.submit(
              () -> {
                assertTrue(cache.put("two", "value-two"));
                cache.flushAsync().join();
              });
      assertTrue(callbackEntered.await(2L, TimeUnit.SECONDS), "eviction callback did not start");

      Future<?> close = callers.submit(cache::close);
      Thread.sleep(50L);
      assertTrue(!close.isDone(), "close released native state during the callback");

      releaseCallback.countDown();
      eviction.get(2L, TimeUnit.SECONDS);
      close.get(2L, TimeUnit.SECONDS);
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      releaseCallback.countDown();
      callers.shutdownNow();
      cache.close();
    }
  }

  private static void evictOne(OffHeapCache<String, String> cache) {
    assertTrue(cache.put("one", "value-one"));
    cache.flushAsync().join();
    assertTrue(cache.put("two", "value-two"));
    cache.flushAsync().join();
  }

  private static OffHeapCache<String, String> newSmallCache(
      CacheSerializer<String> keySerializer,
      CacheSerializer<String> valueSerializer,
      EvictionListener<String, String> listener) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(256L)
        .keySerializer(keySerializer)
        .valueSerializer(valueSerializer)
        .evictionListener(listener)
        .buildTyped();
  }

  private static CacheSerializer<String> countingSerializer(AtomicInteger deserializations) {
    return new CacheSerializer<String>() {
      @Override
      public void serialize(String value, ByteBuffer buffer) {
        buffer.put(value.getBytes(StandardCharsets.UTF_8));
      }

      @Override
      public String deserialize(ByteBuffer buffer) {
        deserializations.incrementAndGet();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public int serializedSize(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
      }
    };
  }
}
