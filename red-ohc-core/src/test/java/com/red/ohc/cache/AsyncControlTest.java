package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
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
  public void replaceRejectsAnEntryLargerThanTheConfiguredMaximumBeforePublishing() {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .maxEntrySize(8)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
      assertTrue(cache.put("k", "old"));
      try {
        cache.replaceAsync("k", "old", "too-large", 0L);
        throw new AssertionError("oversized replacement must fail");
      } catch (IllegalArgumentException expected) {
        assertEquals(cache.get("k"), "old");
      }
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
            .maxEntrySize(2_048)
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
            .maxEntrySize(2_048)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put(key, oldValue));
      try {
        cache.replaceAsync(key, oldValue, newValue, 0L);
        throw new AssertionError(
            "conditional replacement must reject key plus new value over capacity");
      } catch (IllegalArgumentException expected) {
        assertEquals(cache.get(key), oldValue);
      }
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
    } finally {
      callers.shutdownNow();
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

  private static String repeat(char value, int length) {
    char[] chars = new char[length];
    java.util.Arrays.fill(chars, value);
    return new String(chars);
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
}
