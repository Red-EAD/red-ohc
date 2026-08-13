package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.Ticker;

public class MaintenanceAccountingTest {
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
  public void physicalExpiryRemovesTheLiveWeight() {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value", 64L));
      cache.flushAsync().join();
      assertTrue(cache.stats().liveWeight() > 0L);
      assertEquals(
          cache.stats().liveWeight(),
          256L,
          "live weight must include two 128-byte allocator slots, not only the 32-byte raw blocks");

      ticker.millis = 1_000L;
      cache.flushAsync().join();

      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().liveWeight(), 0L, "TTL removal must refund live weight");
    }
  }

  @Test
  public void liveWeightIsPublishedByTheMaintenanceActorNotByAWriterHotCounter() {
    for (Field field : OffHeapCache.class.getDeclaredFields()) {
      assertTrue(
          !(field.getType() == AtomicLong.class && field.getName().equals("liveEntryBytes")),
          "successful writes must not contend on a cache-wide live-entry AtomicLong");
    }
  }

  @Test(timeOut = 2_000L)
  public void scheduledTtlIsPhysicallyCleanedOnTheMaintenanceCadence() throws Exception {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value", 64L));
      cache.flushAsync().join();

      ticker.millis = 128L;
      Thread.sleep(130L);

      assertEquals(
          cache.get("key"), null, "strict reads must reject an idle cache's logically expired value");
      assertEquals(cache.size(), 0L, "scheduled TTL cleanup must converge without a later producer event");
    }
  }

  @Test(timeOut = 5_000L)
  public void residenceTimeCountsEveryValueVersionAtSecondResolutionWithoutFlush()
      throws InterruptedException {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "old"));

      ticker.millis = 7_000L;
      assertTrue(cache.put("key", "new"));

      ticker.millis = 12_000L;
      assertTrue(cache.remove("key"));

      OHCacheStats stats = awaitResidenceStats(cache, 2L, 10_000L);

      assertEquals(stats.entryResidenceCount(), 2L);
      assertTrue(stats.totalEntryResidenceTimeMillis() >= 10_000L);
      assertTrue(stats.averageEntryResidenceTimeMillis() >= 5_000.0d);
    }
  }

  private static OHCacheStats awaitResidenceStats(
      OHCache<String, String> cache, long expectedCount, long expectedMillis)
      throws InterruptedException {
    long deadline = System.nanoTime() + 4_000_000_000L;
    OHCacheStats stats;
    do {
      stats = cache.stats();
      if (stats.entryResidenceCount() >= expectedCount
          && stats.totalEntryResidenceTimeMillis() >= expectedMillis) {
        return stats;
      }
      Thread.sleep(10L);
    } while (System.nanoTime() < deadline);
    return stats;
  }

  @Test
  public void evictionPublishesThePolicyWeight() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(256L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("two", "value-two"));
      cache.flushAsync().join();

      OHCacheStats stats = cache.stats();
      assertTrue(stats.evictionCount() > 0L);
      assertTrue(stats.evictionWeight() > 0L);
    }
  }

  private static final class MutableTicker implements Ticker {
    volatile long millis;

    MutableTicker(long millis) {
      this.millis = millis;
    }

    @Override
    public long nanos() {
      return millis * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return millis;
    }
  }
}
