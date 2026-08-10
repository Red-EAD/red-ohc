package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;

public final class MaxSizeTest {
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
  public void maxSizeAndCapacityAreMutuallyExclusive() {
    expectThrows(
        IllegalStateException.class,
        () ->
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .maxSize(2));
    expectThrows(
        IllegalStateException.class,
        () ->
            OHCacheBuilder.<String, String>newBuilder()
                .maxSize(2)
                .capacity(1 << 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> OHCacheBuilder.<String, String>newBuilder().maxSize(0));
  }

  @Test(dataProvider = "evictionStrategies", timeOut = 2_000L)
  public void maxSizeEvictsByEntryCountAfterMaintenanceFlush(Eviction eviction) {
    try (OHCache<String, String> cache = newMaxSizeCache(2, eviction)) {
      assertEquals(cache.capacity(), -1L);
      assertTrue(cache.put("one", "short"));
      assertTrue(cache.put("two", "a value with a different size"));
      assertTrue(cache.put("three", "third"));

      cache.flushAsync().join();

      assertEquals(cache.size(), 2L);
      assertTrue(cache.stats().getLiveWeight() > 0L);
    }
  }

  @org.testng.annotations.DataProvider(name = "evictionStrategies")
  public Object[][] evictionStrategies() {
    return new Object[][] {{Eviction.LRU}, {Eviction.W_TINY_LFU}, {Eviction.S3_FIFO}};
  }

  @Test
  public void replacingAnEntryDoesNotConsumeAnotherMaxSizeSlot() {
    try (OHCache<String, String> cache = newMaxSizeCache(2)) {
      assertTrue(cache.put("one", "one"));
      assertTrue(cache.put("two", "two"));
      assertTrue(cache.put("one", "a replacement with a larger value"));

      cache.flushAsync().join();

      assertEquals(cache.size(), 2L);
      assertEquals(cache.get("one"), "a replacement with a larger value");
    }
  }

  @Test
  public void maxSizeDoesNotEvictBasedOnValueBytes() {
    String small = "s";
    String medium = repeat('m', 8_192);
    String large = repeat('l', 16_384);
    try (OHCache<String, String> cache = newMaxSizeCache(3)) {
      assertTrue(cache.put("small", small));
      assertTrue(cache.put("medium", medium));
      assertTrue(cache.put("large", large));

      cache.flushAsync().join();

      assertEquals(cache.size(), 3L);
      assertEquals(cache.get("small"), small);
      assertEquals(cache.get("medium"), medium);
      assertEquals(cache.get("large"), large);
      assertTrue(cache.stats().getLiveWeight() > medium.length());
    }
  }

  @Test
  public void ttlAndRemoveUpdateCountAndLiveBytesAfterFlush() {
    MutableTicker ticker = new MutableTicker();
    try (OHCache<String, String> cache = newMaxSizeCache(4, ticker)) {
      assertTrue(cache.put("expires", "value", 100L));
      assertTrue(cache.put("remove", "another value"));
      cache.flushAsync().join();
      long liveBeforeRemove = cache.stats().getLiveWeight();

      assertTrue(cache.remove("remove"));
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.stats().getLiveWeight() < liveBeforeRemove);

      ticker.now = 200L;
      cache.flushAsync().join();
      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().getLiveWeight(), 0L);
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentWritesEventuallyConvergeToMaxSize() throws Exception {
    ExecutorService writers = Executors.newFixedThreadPool(4);
    try (OHCache<String, String> cache = newMaxSizeCache(8)) {
      List<Future<?>> tasks = new ArrayList<>();
      for (int worker = 0; worker < 4; worker++) {
        final int workerId = worker;
        tasks.add(
            writers.submit(
                () -> {
                  for (int index = 0; index < 100; index++) {
                    cache.put(
                        "key-" + workerId + '-' + index,
                        repeat((char) ('a' + workerId), index + 1));
                  }
                }));
      }
      writers.shutdown();
      assertTrue(writers.awaitTermination(5L, TimeUnit.SECONDS));
      for (Future<?> task : tasks) {
        task.get();
      }
      cache.flushAsync().join();
      assertTrue(cache.size() <= 8L);
    } finally {
      writers.shutdownNow();
    }
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize) {
    return newMaxSizeCache(maxSize, Eviction.S3_FIFO);
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize, Eviction eviction) {
    return OHCacheBuilder.<String, String>newBuilder()
        .maxSize(maxSize)
        .eviction(eviction)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize, Ticker ticker) {
    return OHCacheBuilder.<String, String>newBuilder()
        .maxSize(maxSize)
        .ticker(ticker)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static String repeat(char value, int count) {
    char[] result = new char[count];
    java.util.Arrays.fill(result, value);
    return new String(result);
  }

  private static final class MutableTicker implements Ticker {
    volatile long now;

    @Override
    public long nanos() {
      return now * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return now;
    }
  }
}
