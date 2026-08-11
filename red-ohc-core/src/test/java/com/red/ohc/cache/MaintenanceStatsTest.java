package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;

public final class MaintenanceStatsTest {
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
  public void statsSeparateLiveResidentRetiredAndActorPressureWithoutSentinelValues() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value"));
      cache.flushAsync().join();

      OHCacheStats stats = cache.stats();
      assertEquals(stats.getSize(), 1L);
      assertTrue(stats.getLiveWeight() > 0L);
      assertTrue(stats.getResidentWeight() >= stats.getLiveWeight());
      assertTrue(stats.getRetiredWeight() >= 0L);
      assertTrue(stats.getNativeAllocatedBytes() >= stats.getResidentWeight());
      assertTrue(stats.getTtlBacklog() >= 0L);
      assertEquals(stats.getMaintenanceQueueDepth(), 0L);
      assertEquals(stats.getRetirementQueueDepth(), 0L);
    }
  }
}
