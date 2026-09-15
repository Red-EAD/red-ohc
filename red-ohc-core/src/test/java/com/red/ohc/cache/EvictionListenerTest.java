package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.EvictionListener;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;

public final class EvictionListenerTest {
  @Test(timeOut = 5_000L)
  public void unusedLazyKeyAndValueAreNotDeserialized() throws Exception {
    AtomicInteger keyDeserializations = new AtomicInteger();
    AtomicInteger valueDeserializations = new AtomicInteger();
    CountDownLatch callback = new CountDownLatch(1);
    AtomicReference<RemovalCause> cause = new AtomicReference<>();

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1_000L)
            .keySerializer(countingSerializer(keyDeserializations))
            .valueSerializer(countingSerializer(valueDeserializations))
            .evictionListener(
                (Supplier<String> key, Supplier<String> value, RemovalCause removalCause) -> {
                  cause.set(removalCause);
                  callback.countDown();
                })
            .buildTyped()) {
      cache.put("one", "value-one");
      cache.flushAsync().join();
      evictOne(cache);

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
      cache.put("expired", "value", 2_000L);
      cache.flushAsync().join();
      nowMillis.set(3_000L);
      cache.put("trigger", "value", 4_000L);
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
      cache.put("expired", "old", 2_000L);
      cache.flushAsync().join();
      nowMillis.set(3_000L);

      assertEquals(cache.putIfAbsent("expired", "new", 4_000L), null);
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
      cache.put("one", "value-one");
      cache.flushAsync().join();
      cache.put("one", "replacement");
      cache.flushAsync().join();
      cache.remove("one");
      assertTrue(!cache.containsKey("one"));
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
    cache.put("one", "value-one");
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
      assertEquals(cache.stats().evictionCount(), 1L);
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
      assertEquals(cache.stats().evictionCount(), 1L);
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

  @Test(timeOut = 10_000L)
  public void closedListenerCacheCanBeCollected() throws Exception {
    WeakReference<OffHeapCache<String, String>> reference = createClosedListenerCache();
    for (int attempt = 0; attempt < 40 && reference.get() != null; attempt++) {
      System.gc();
      Thread.sleep(25L);
    }
    assertEquals(reference.get(), null);
  }

  private static WeakReference<OffHeapCache<String, String>> createClosedListenerCache() {
    OffHeapCache<String, String> cache =
        newSmallCache(
            countingSerializer(new AtomicInteger()),
            countingSerializer(new AtomicInteger()),
            (key, value, cause) -> {});
    WeakReference<OffHeapCache<String, String>> reference = new WeakReference<>(cache);
    cache.close();
    return reference;
  }

  private static void evictOne(OffHeapCache<String, String> cache) {
    cache.put("one", "value-one");
    cache.flushAsync().join();
    Entry entry = cache.dataForTest().values().iterator().next();
    try {
      Field workerField = OffHeapCache.class.getDeclaredField("worker");
      workerField.setAccessible(true);
      Object worker = workerField.get(cache);
      Method remove =
          worker
              .getClass()
              .getDeclaredMethod(
                  "removeFromMap",
                  com.red.ohc.index.Entry.class,
                  boolean.class,
                  long.class,
                  long.class,
                  RemovalCause.class);
      remove.setAccessible(true);
      assertTrue(
          (Boolean)
              remove.invoke(worker, entry, true, entry.generation(), entry.valueAddress, RemovalCause.SIZE));
    } catch (ReflectiveOperationException failure) {
      throw new AssertionError("failed to trigger actor eviction", failure);
    }
  }

  private static OffHeapCache<String, String> newSmallCache(
      CacheSerializer<String> keySerializer,
      CacheSerializer<String> valueSerializer,
      EvictionListener<String, String> listener) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1_000L)
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
