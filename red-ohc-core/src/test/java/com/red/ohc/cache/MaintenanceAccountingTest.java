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
    MutableTicker ticker = new MutableTicker();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value", 64L));
      cache.flushAsync().join();
      assertTrue(cache.stats().getLiveWeight() > 0L);
      assertEquals(
          cache.stats().getLiveWeight(),
          256L,
          "live weight must include two 128-byte allocator slots, not only the 32-byte raw blocks");

      ticker.millis = 1_000L;
      cache.flushAsync().join();

      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().getLiveWeight(), 0L, "TTL removal must refund live weight");
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
    MutableTicker ticker = new MutableTicker();
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

  private static final class MutableTicker implements Ticker {
    volatile long millis;

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
