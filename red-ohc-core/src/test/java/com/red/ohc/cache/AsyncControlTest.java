package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.Ticker;

public class AsyncControlTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(StandardCharsets.UTF_8).length;
        }
      };

  @Test
  public void maintenanceControlRequestsReturnExactSerializedStateResults() {
    try (OHCache<String, String> cache = newCache(Runnable::run)) {
      assertTrue(cache.putIfAbsentAsync("k", "one", 0L).join());
      assertFalse(cache.putIfAbsentAsync("k", "two", 0L).join());
      assertFalse(cache.replaceAsync("k", "other", "two", 0L).join());
      assertTrue(cache.replaceAsync("k", "one", "two", 0L).join());
      assertTrue(cache.removeAsync("k").join());
      assertFalse(cache.removeAsync("k").join());
    }
  }

  @Test
  public void asyncMutationSerializesOnTheMaintenanceEventLoop() {
    AtomicReference<Thread> serializerThread = new AtomicReference<>();
    CacheSerializer<String> tracking =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            serializerThread.set(Thread.currentThread());
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    Thread caller = Thread.currentThread();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(tracking)
            .build()) {
      CompletableFuture<Boolean> result = cache.putIfAbsentAsync("async", "value", 0L);
      assertTrue(result.join());
      assertNotSame(serializerThread.get(), caller);
      assertTrue(serializerThread.get().getName().contains("red-ohc-maintenance-event-loop"));
    }
  }

  @Test(timeOut = 5_000L)
  public void laterFlushExtendsAnExistingBarrierToCoverEarlierSubmissions() throws Exception {
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch releaseSecond = new CountDownLatch(1);
    CacheSerializer<String> blocking =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("first".equals(value)) {
              firstStarted.countDown();
              await(releaseFirst);
            } else if ("second".equals(value)) {
              secondStarted.countDown();
              await(releaseSecond);
            }
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(blocking)
            .build()) {
      CompletableFuture<Boolean> first =
          cache.putIfAbsentAsync("first-key", "first", 0L);
      assertTrue(firstStarted.await(2L, TimeUnit.SECONDS));
      CompletableFuture<Void> firstFlush = cache.flushAsync();
      CompletableFuture<Boolean> second =
          cache.putIfAbsentAsync("second-key", "second", 0L);
      CompletableFuture<Void> secondFlush = cache.flushAsync();
      assertNotSame(secondFlush, firstFlush);
      assertTrue(firstFlush.cancel(false));
      assertFalse(secondFlush.isCancelled());

      releaseFirst.countDown();
      assertTrue(secondStarted.await(2L, TimeUnit.SECONDS));
      assertFalse(secondFlush.isDone(), "flush completed while its covered task was still running");

      releaseSecond.countDown();
      secondFlush.get(2L, TimeUnit.SECONDS);
      assertTrue(first.join());
      assertTrue(second.join());
    } finally {
      releaseFirst.countDown();
      releaseSecond.countDown();
    }
  }

  @Test
  public void replaceRejectsAnEntryLargerThanTheByteCapacityBeforePublishing() {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(256)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
      assertTrue(cache.put("k", "old"));
      try {
        cache.replaceAsync("k", "old", repeat('x', 1_000), 0L).join();
        throw new AssertionError("oversized replacement must fail");
      } catch (CompletionException expected) {
        assertTrue(expected.getCause() instanceof IllegalArgumentException);
      }
      assertEquals(cache.get("k"), "old");
    }
  }

  @Test
  public void replacementRejectsKeyAndValueThatTogetherExceedCapacity() {
    String key = repeat('k', 400);
    String oldValue = repeat('o', 100);
    String newValue = repeat('n', 600);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1_024)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put(key, oldValue));
      try {
        cache.put(key, newValue);
        throw new AssertionError("replacement must reject key plus new value over capacity");
      } catch (IllegalArgumentException expected) {
        assertEquals(cache.get(key), oldValue);
      }
    }
  }

  @Test
  public void conditionalReplacementRejectsKeyAndValueThatTogetherExceedCapacity() {
    String key = repeat('k', 400);
    String oldValue = repeat('o', 100);
    String newValue = repeat('n', 600);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1_024)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put(key, oldValue));
      try {
        cache.replaceAsync(key, oldValue, newValue, 0L).join();
        throw new AssertionError(
            "conditional replacement must reject key plus new value over capacity");
      } catch (CompletionException expected) {
        assertTrue(expected.getCause() instanceof IllegalArgumentException);
      }
      assertEquals(cache.get(key), oldValue);
    }
  }

  @Test
  public void loaderRunsOnConfiguredExecutorAndNeverOnTheMaintenanceEventLoop() {
    AtomicInteger executions = new AtomicInteger();
    Executor executor =
        command -> {
          executions.incrementAndGet();
          command.run();
        };
    try (OHCache<String, String> cache = newCache(executor)) {
      assertEquals(cache.getOrLoadAsync("load", key -> "value", 0L).join(), "value");
      assertEquals(executions.get(), 1);
      OHCacheStats stats = cache.stats();
      assertEquals(stats.loadSuccessCount(), 1L);
      assertEquals(stats.loadFailureCount(), 0L);
      assertEquals(stats.loadCount(), 1L);
      assertTrue(stats.totalLoadTime() >= 0L);
    }
  }

  @Test
  public void loaderInternalLookupsDoNotDuplicateRequestStats() {
    try (OHCache<String, String> cache = newCache(Runnable::run)) {
      assertEquals(cache.getOrLoadAsync("stats", key -> "value", 0L).join(), "value");
      cache.flushAsync().join();

      OHCacheStats stats = cache.stats();
      assertEquals(stats.hitCount(), 0L);
      assertEquals(stats.missCount(), 1L);
      assertEquals(stats.requestCount(), 1L);
    }
  }

  @Test
  public void loaderFailureIsCountedOnlyAtTheActualLoaderBoundary() {
    try (OHCache<String, String> cache = newCache(Runnable::run)) {
      try {
        cache
            .getOrLoadAsync(
                "failed",
                key -> {
                  throw new IllegalStateException("boom");
                },
                0L)
            .join();
        throw new AssertionError("loader failure must complete exceptionally");
      } catch (CompletionException expected) {
        assertTrue(expected.getCause() instanceof IllegalStateException);
      }

      OHCacheStats stats = cache.stats();
      assertEquals(stats.loadSuccessCount(), 0L);
      assertEquals(stats.loadFailureCount(), 1L);
      assertEquals(stats.loadCount(), 1L);
      assertEquals(stats.loadFailureRate(), 1.0d, 0.0d);
    }
  }

  @Test
  public void loaderReturnsComputedValueWhenBestEffortAdmissionFails() {
    MutableTicker ticker = new MutableTicker(0L);
    AtomicReference<OHCache<String, String>> cacheRef = new AtomicReference<>();
    AtomicReference<String> loadedResult = new AtomicReference<>();
    AtomicReference<String> expiredResult = new AtomicReference<>();
    CacheSerializer<String> nestedLoadSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("outer-value".equals(value)) {
              loadedResult.set(
                  cacheRef
                      .get()
                      .getOrLoadAsync("loaded-key", ignored -> "loaded-value", 0L)
                      .join());
              expiredResult.set(
                  cacheRef
                      .get()
                      .getOrLoadAsync(
                          "expired-key",
                          ignored -> {
                            ticker.now = 100L;
                            return "expired-value";
                          },
                          100L)
                      .join());
            }
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(nestedLoadSerializer)
            .loaderExecutor(Runnable::run)
            .build()) {
      cacheRef.set(cache);

      assertTrue(cache.put("outer-key", "outer-value"));

      assertEquals(loadedResult.get(), "loaded-value");
      assertEquals(expiredResult.get(), null);
      assertEquals(cache.get("loaded-key"), null, "cache admission remains best effort");
      assertEquals(cache.get("expired-key"), null);
    }
  }

  @Test
  public void concurrentMissesUseOneLoaderAndReturnOneSharedResult() throws Exception {
    ExecutorService loaderExecutor = Executors.newFixedThreadPool(2);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    CountDownLatch loaderStarted = new CountDownLatch(1);
    CountDownLatch releaseLoader = new CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();
    try (OHCache<String, String> cache = newCache(loaderExecutor)) {
      java.util.concurrent.Callable<String> call =
          () ->
              cache
                  .getOrLoadAsync(
                      "single",
                      key -> {
                        loads.incrementAndGet();
                        loaderStarted.countDown();
                        releaseLoader.await(5, TimeUnit.SECONDS);
                        return "loaded";
                      },
                      0L)
                  .join();
      java.util.concurrent.Future<String> first = callers.submit(call);
      java.util.concurrent.Future<String> second = callers.submit(call);
      assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
      releaseLoader.countDown();
      assertEquals(first.get(5, TimeUnit.SECONDS), "loaded");
      assertEquals(second.get(5, TimeUnit.SECONDS), "loaded");
      assertEquals(loads.get(), 1);
      assertEquals(cache.stats().loadSuccessCount(), 1L);
      assertEquals(cache.stats().loadCount(), 1L);
    } finally {
      callers.shutdownNow();
      loaderExecutor.shutdownNow();
    }
  }

  @Test(timeOut = 10_000L)
  public void loaderWaitersAreIndependentAndCancellationIsLocal() throws Exception {
    ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch loaderStarted = new CountDownLatch(1);
    CountDownLatch releaseLoader = new CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();
    try (OHCache<String, String> cache = newCache(loaderExecutor)) {
      CompletableFuture<String> first =
          cache.getOrLoadAsync(
              "cancel-local",
              key -> {
                loads.incrementAndGet();
                loaderStarted.countDown();
                await(releaseLoader);
                return "loaded";
              },
              0L);
      CompletableFuture<String> second =
          cache.getOrLoadAsync("cancel-local", key -> "must-not-run", 0L);

      assertNotSame(first, second);
      assertTrue(loaderStarted.await(5L, TimeUnit.SECONDS));
      assertTrue(first.cancel(true));
      assertTrue(first.isCancelled());
      assertFalse(second.isCancelled());
      assertEquals(loads.get(), 1);

      releaseLoader.countDown();
      assertEquals(second.get(5L, TimeUnit.SECONDS), "loaded");
      assertEquals(loads.get(), 1);
    } finally {
      releaseLoader.countDown();
      loaderExecutor.shutdownNow();
    }
  }

  @Test
  public void loaderReturnsTheValueThatWonPutIfAbsent() throws Exception {
    ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch loaderStarted = new CountDownLatch(1);
    CountDownLatch releaseLoader = new CountDownLatch(1);
    try (OHCache<String, String> cache = newCache(loaderExecutor)) {
      java.util.concurrent.Future<String> loaded =
          cache
              .getOrLoadAsync(
                  "race",
                  key -> {
                    loaderStarted.countDown();
                    releaseLoader.await(5, TimeUnit.SECONDS);
                    return "loader";
                  },
                  0L)
              .thenApply(value -> value)
              .toCompletableFuture();
      assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
      assertTrue(cache.put("race", "external"));
      releaseLoader.countDown();
      assertEquals(loaded.get(5, TimeUnit.SECONDS), "external");
    } finally {
      loaderExecutor.shutdownNow();
    }
  }

  @Test(timeOut = 5_000L)
  public void asyncMutationCompletesAfterWriterExitForSynchronousCallbacks() {
    AtomicReference<OffHeapCache<String, String>> cacheRef = new AtomicReference<>();
    AtomicReference<Throwable> closeFailure = new AtomicReference<>();
    try (OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newCache(Runnable::run)) {
      cacheRef.set(cache);
      cache
          .putIfAbsentAsync("callback-key", "callback-value", 0L)
          .thenApply(
              accepted -> {
                assertTrue(accepted);
                assertTrue(cacheRef.get().put("nested-key", "nested-value"));
                try {
                  cacheRef.get().close();
                } catch (Throwable failure) {
                  closeFailure.set(failure);
                }
                return accepted;
              })
          .join();

      assertEquals(cache.get("nested-key"), "nested-value");
      assertTrue(closeFailure.get() instanceof IllegalStateException);
    }
  }

  @Test
  public void loaderThatExpiresBeforePublicationReturnsNull() {
    MutableTicker ticker = new MutableTicker(0L);
    Executor direct = Runnable::run;
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .loaderExecutor(direct)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertEquals(
          cache
              .getOrLoadAsync(
                  "expires",
                  key -> {
                    ticker.now = 100L;
                    return "expired";
                  },
                  100L)
              .join(),
          null);
      assertEquals(cache.get("expires"), null);
    }
  }

  @Test(timeOut = 10_000L)
  public void getOrLoadMissReusesOneEncodedKeyAndReturnsLoadedObject() throws Exception {
    CountingSerializer keySerializer = new CountingSerializer();
    CountingSerializer valueSerializer = new CountingSerializer();
    ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
    byte[] key = bytes(24, 7);
    byte[] loaded = bytes(5 * 1024, 11);
    try (OHCache<byte[], byte[]> cache =
        newCache(keySerializer, valueSerializer, loaderExecutor)) {
      byte[] result = cache.getOrLoadAsync(key, ignored -> loaded, 0L).get();

      assertSame(result, loaded);
      assertEquals(keySerializer.sizeCalls.get(), 1);
      assertEquals(keySerializer.serializeCalls.get(), 1);
      assertEquals(valueSerializer.deserializeCalls.get(), 0);
    } finally {
      loaderExecutor.shutdownNow();
    }
  }

  @Test
  public void replaceMismatchDoesNotSerializeReplacementValue() {
    byte[] key = bytes(24, 1);
    byte[] oldValue = bytes(5 * 1024, 2);
    byte[] wrongExpected = bytes(5 * 1024, 3);
    byte[] replacement = bytes(5 * 1024, 4);
    TrackingValueSerializer values = new TrackingValueSerializer(wrongExpected, replacement);

    try (OHCache<byte[], byte[]> cache =
        newCache(new CountingSerializer(), values, Runnable::run)) {
      assertTrue(cache.put(key, oldValue));
      values.reset();

      assertFalse(cache.replaceAsync(key, wrongExpected, replacement, 0L).join());
      assertEquals(values.expectedSizeCalls.get(), 1);
      assertEquals(values.expectedSerializeCalls.get(), 1);
      assertEquals(values.replacementSizeCalls.get(), 0);
      assertEquals(values.replacementSerializeCalls.get(), 0);
      assertTrue(Arrays.equals(oldValue, cache.get(key)));
    }
  }

  @Test
  public void replaceMissingKeyDoesNotSerializeExpectedOrReplacement() {
    byte[] key = bytes(24, 5);
    byte[] expected = bytes(5 * 1024, 6);
    byte[] replacement = bytes(5 * 1024, 7);
    TrackingValueSerializer values = new TrackingValueSerializer(expected, replacement);

    try (OHCache<byte[], byte[]> cache =
        newCache(new CountingSerializer(), values, Runnable::run)) {
      assertFalse(cache.replaceAsync(key, expected, replacement, 0L).join());
      assertEquals(values.expectedSizeCalls.get(), 0);
      assertEquals(values.expectedSerializeCalls.get(), 0);
      assertEquals(values.replacementSizeCalls.get(), 0);
      assertEquals(values.replacementSerializeCalls.get(), 0);
    }
  }

  @Test
  public void replaceExpiredKeyDoesNotSerializeExpectedOrReplacement() {
    AtomicInteger now = new AtomicInteger();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return now.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return now.get();
          }
        };
    byte[] key = bytes(24, 12);
    byte[] oldValue = bytes(5 * 1024, 13);
    byte[] expected = bytes(5 * 1024, 14);
    byte[] replacement = bytes(5 * 1024, 15);
    TrackingValueSerializer values = new TrackingValueSerializer(expected, replacement);

    try (OffHeapCache<byte[], byte[]> cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(16L << 20)
                .ticker(ticker)
                .keySerializer(new CountingSerializer())
                .valueSerializer(values)
                .build()) {
      assertTrue(cache.put(key, oldValue, 10L));
      values.reset();
      now.set(10);

      assertFalse(cache.replaceAsync(key, expected, replacement, 0L).join());
      assertEquals(values.expectedSizeCalls.get(), 0);
      assertEquals(values.expectedSerializeCalls.get(), 0);
      assertEquals(values.replacementSizeCalls.get(), 0);
      assertEquals(values.replacementSerializeCalls.get(), 0);
    }
  }

  @Test
  public void replaceMatchSerializesReplacementOnceAndPublishesIt() {
    byte[] key = bytes(24, 8);
    byte[] oldValue = bytes(5 * 1024, 9);
    byte[] replacement = bytes(5 * 1024, 10);
    TrackingValueSerializer values = new TrackingValueSerializer(oldValue, replacement);

    try (OHCache<byte[], byte[]> cache =
        newCache(new CountingSerializer(), values, Runnable::run)) {
      assertTrue(cache.put(key, oldValue));
      values.reset();

      assertTrue(cache.replaceAsync(key, oldValue, replacement, 0L).join());
      assertEquals(values.expectedSizeCalls.get(), 1);
      assertEquals(values.expectedSerializeCalls.get(), 1);
      assertEquals(values.replacementSizeCalls.get(), 1);
      assertEquals(values.replacementSerializeCalls.get(), 1);
      assertTrue(Arrays.equals(replacement, cache.get(key)));
    }
  }

  @Test
  public void replacementSerializationFailureLeavesOldValueAndReleasesNativeMemory() {
    byte[] key = bytes(24, 16);
    byte[] oldValue = bytes(5 * 1024, 17);
    byte[] replacement = bytes(5 * 1024, 18);
    ThrowingReplacementSerializer values = new ThrowingReplacementSerializer(replacement);
    OffHeapCache<byte[], byte[]> cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(16L << 20)
                .keySerializer(new CountingSerializer())
                .valueSerializer(values)
                .build();
    try {
      assertTrue(cache.put(key, oldValue));
      try {
        cache.replaceAsync(key, oldValue, replacement, 0L).join();
        throw new AssertionError("replacement serializer failure must be propagated");
      } catch (CompletionException expected) {
        assertTrue(expected.getCause() instanceof IllegalStateException);
        assertTrue(expected.getCause().getMessage().contains("replacement serialization failed"));
      }
      assertTrue(Arrays.equals(oldValue, cache.get(key)));
    } finally {
      cache.close();
    }
    assertEquals(cache.totalAllocatedBytes(), 0L);
  }

  private static String repeat(char value, int length) {
    char[] chars = new char[length];
    java.util.Arrays.fill(chars, value);
    return new String(chars);
  }

  private static byte[] bytes(int length, int seed) {
    byte[] result = new byte[length];
    Arrays.fill(result, (byte) seed);
    return result;
  }

  private static final class MutableTicker implements Ticker {
    volatile long now;

    MutableTicker(long now) {
      this.now = now;
    }

    @Override
    public long nanos() {
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return now;
    }
  }

  private static OHCache<String, String> newCache(Executor executor) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .loaderExecutor(executor)
        .build();
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(2L, TimeUnit.SECONDS)) {
        throw new AssertionError("test latch was not released");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static OHCache<byte[], byte[]> newCache(
      CacheSerializer<byte[]> keySerializer,
      CacheSerializer<byte[]> valueSerializer,
      Executor loaderExecutor) {
    return OHCacheBuilder.<byte[], byte[]>newBuilder()
        .capacity(16L << 20)
        .keySerializer(keySerializer)
        .valueSerializer(valueSerializer)
        .loaderExecutor(loaderExecutor)
        .build();
  }

  private static class CountingSerializer implements CacheSerializer<byte[]> {
    final AtomicInteger sizeCalls = new AtomicInteger();
    final AtomicInteger serializeCalls = new AtomicInteger();
    final AtomicInteger deserializeCalls = new AtomicInteger();

    @Override
    public void serialize(byte[] value, ByteBuffer buffer) {
      serializeCalls.incrementAndGet();
      buffer.put(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
      deserializeCalls.incrementAndGet();
      byte[] result = new byte[buffer.remaining()];
      buffer.get(result);
      return result;
    }

    @Override
    public int serializedSize(byte[] value) {
      sizeCalls.incrementAndGet();
      return value.length;
    }
  }

  private static final class ThrowingReplacementSerializer extends CountingSerializer {
    private final byte[] replacement;

    ThrowingReplacementSerializer(byte[] replacement) {
      this.replacement = replacement;
    }

    @Override
    public void serialize(byte[] value, ByteBuffer buffer) {
      if (value == replacement) {
        throw new IllegalStateException("replacement serialization failed");
      }
      super.serialize(value, buffer);
    }
  }

  private static final class TrackingValueSerializer extends CountingSerializer {
    private final byte[] expected;
    private final byte[] replacement;
    final AtomicInteger expectedSizeCalls = new AtomicInteger();
    final AtomicInteger expectedSerializeCalls = new AtomicInteger();
    final AtomicInteger replacementSizeCalls = new AtomicInteger();
    final AtomicInteger replacementSerializeCalls = new AtomicInteger();

    TrackingValueSerializer(byte[] expected, byte[] replacement) {
      this.expected = expected;
      this.replacement = replacement;
    }

    void reset() {
      expectedSizeCalls.set(0);
      expectedSerializeCalls.set(0);
      replacementSizeCalls.set(0);
      replacementSerializeCalls.set(0);
    }

    @Override
    public void serialize(byte[] value, ByteBuffer buffer) {
      super.serialize(value, buffer);
      if (value == expected) {
        expectedSerializeCalls.incrementAndGet();
      } else if (value == replacement) {
        replacementSerializeCalls.incrementAndGet();
      }
    }

    @Override
    public int serializedSize(byte[] value) {
      int length = super.serializedSize(value);
      if (value == expected) {
        expectedSizeCalls.incrementAndGet();
      } else if (value == replacement) {
        replacementSizeCalls.incrementAndGet();
      }
      return length;
    }
  }
}
