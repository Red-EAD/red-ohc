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
  public void statsExposeCapacityAndMaintenanceHealthWithoutSentinelValues() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("key", "value");
      cache.flushAsync().join();

      OHCacheStats stats = cache.stats();
      assertEquals(stats.size(), 1L);
      assertTrue(stats.liveWeight() > 0L);
      assertTrue(stats.nativeAllocatedBytes() >= stats.liveWeight());
      assertTrue(stats.ttlBacklog() >= 0L);
      assertEquals(stats.maintenanceQueueDepth(), 0L);
      assertTrue(stats.maintenancePassWorkNanos() >= 0L);
      assertTrue(stats.maintenanceActiveNanosTotal() >= 0L);
      assertTrue(stats.maintenanceParkNanosTotal() >= 0L);
      assertTrue(stats.maintenanceImmediateContinuationCount() >= 0L);
      assertTrue(stats.retirementQueueDepth() >= 0L);
      assertTrue(stats.retirementSealRecordsTotal() >= 0L);
      assertTrue(stats.retirementReclaimRecordsTotal() >= 0L);
      assertTrue(stats.retirementSealScannedLanesTotal() >= 0L);
      assertTrue(stats.retirementSealHeadOfLineStops() >= 0L);
      assertTrue(stats.retirementReclaimBlockedCount() >= 0L);
      assertTrue(stats.retirementReclaimBlockedNanos() >= 0L);
      assertTrue(stats.retirementRetryWakeCount() >= 0L);
      assertTrue(stats.activeReaderCount() >= 0L);
      assertTrue(stats.asyncMutationQueueDepth() >= 0L);
      assertTrue(stats.nativeDebtBudgetBytes() > 0L);
      assertTrue(stats.nativeDebtHeadroomBytes() >= 0L);
    }
  }

  @Test(timeOut = 10_000L)
  public void nativeDebtBudgetIsDiagnosticOnlyAndDoesNotRejectWrites() {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .nativeMemoryBudgetBytes(8L << 10)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      cache.put("key", "initial");
      for (int index = 0; index < 32; index++) {
        cache.put("key", "value-" + index);
      }

      OHCacheStats stats = cache.stats();
      assertEquals(stats.nativeDebtBudgetBytes(), 8L << 10);
      assertTrue(stats.nativeDebtHeadroomBytes() >= 0L);
      assertEquals(cache.get("key"), "value-31");
    }
  }
}
