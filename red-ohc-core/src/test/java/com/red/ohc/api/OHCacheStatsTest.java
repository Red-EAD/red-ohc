package com.red.ohc.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.testng.annotations.Test;

public final class OHCacheStatsTest {
  @Test
  public void gettersExposeAllStatsFields() {
    OHCacheStats stats =
        new OHCacheStats(
            1L,
            2L,
            3L,
            4L,
            true,
            5L,
            6L,
            7L,
            8L,
            9L,
            10L,
            11L,
            12L,
            13L,
            14L,
            15L,
            16L,
            17L,
            18L,
            19L,
            20L,
            21L,
            22L,
            23L,
            24L,
            25L,
            26L,
            27L,
            28L,
            29L,
            30L,
            31L);

    assertEquals(stats.getReadHits(), 1L);
    assertEquals(stats.getReadMisses(), 2L);
    assertEquals(stats.getMaintenanceQueueDepth(), 3L);
    assertEquals(stats.getMaintenanceQueueCapacity(), 4L);
    assertTrue(stats.getMaintenanceUnhealthy());
    assertEquals(stats.getPhysicalExpired(), 5L);
    assertEquals(stats.getTtlLagMillis(), 6L);
    assertEquals(stats.getTtlBacklog(), 7L);
    assertEquals(stats.getEvictionCount(), 8L);
    assertEquals(stats.getRetiredEntries(), 9L);
    assertEquals(stats.getSize(), 10L);
    assertEquals(stats.getLiveWeight(), 11L);
    assertEquals(stats.getResidentWeight(), 12L);
    assertEquals(stats.getRetiredWeight(), 13L);
    assertEquals(stats.getNativeAllocatedBytes(), 14L);
    assertEquals(stats.getTimerHeapBytes(), 15L);
    assertEquals(stats.getSketchHeapBytes(), 16L);
    assertEquals(stats.getGhostHeapBytes(), 17L);
    assertEquals(stats.getRetirementQueueNativeBytes(), 18L);
    assertEquals(stats.getRetirementQueueDepth(), 19L);
    assertEquals(stats.getRetirementQueueCapacity(), 20L);
    assertEquals(stats.getNonBlockingPutFailureCount(), 21L);
    assertEquals(stats.getNonBlockingReplaceFailureCount(), 22L);
    assertEquals(stats.getNonBlockingRemoveFailureCount(), 23L);
    assertEquals(stats.getWriterContentionFailureCount(), 24L);
    assertEquals(stats.getRetirementAdmissionFailureCount(), 25L);
    assertEquals(stats.getReliableRemovalAdmissionFailureCount(), 26L);
    assertEquals(stats.getNativeAllocationFailureCount(), 27L);
    assertEquals(stats.getRepairQueueDepth(), 28L);
    assertEquals(stats.getAsyncMutationQueueDepth(), 29L);
    assertEquals(stats.getAsyncMutationFailedCount(), 30L);
    assertEquals(stats.getAsyncMutationRejectedCount(), 31L);
  }

  @Test
  public void statsFieldsArePrivate() {
    for (Field field : OHCacheStats.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        assertTrue(
            Modifier.isPrivate(field.getModifiers()),
            "stats field must be private: " + field.getName());
      }
    }
  }

}
