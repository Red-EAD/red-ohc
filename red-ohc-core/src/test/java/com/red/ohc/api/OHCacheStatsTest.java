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
            5L,
            6L,
            7L,
            8L,
            true,
            10L,
            11L,
            12L,
            13L,
            14L,
            15L,
            16L,
            17L,
            18L,
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
            31L,
            32L,
            33L,
            34L,
            35L,
            36L,
            37L,
            38L,
            39L,
            40L,
            41L);

    assertEquals(stats.getReadHits(), 1L);
    assertEquals(stats.getReadMisses(), 2L);
    assertEquals(stats.getReadAccessDropped(), 3L);
    assertEquals(stats.getMutationAccepted(), 4L);
    assertEquals(stats.getMutationApplied(), 5L);
    assertEquals(stats.getMaintenanceQueueDepth(), 6L);
    assertEquals(stats.getMaintenanceQueueCapacity(), 7L);
    assertEquals(stats.getMaintenanceLoopNanos(), 8L);
    assertTrue(stats.getMaintenanceUnhealthy());
    assertEquals(stats.getLogicalExpired(), 10L);
    assertEquals(stats.getPhysicalExpired(), 11L);
    assertEquals(stats.getTtlLagMillis(), 12L);
    assertEquals(stats.getTtlBacklog(), 13L);
    assertEquals(stats.getEvictionCount(), 14L);
    assertEquals(stats.getEvictionScanCount(), 15L);
    assertEquals(stats.getEvictionLockedSkips(), 16L);
    assertEquals(stats.getRetiredEntries(), 17L);
    assertEquals(stats.getSize(), 18L);
    assertEquals(stats.getLiveWeight(), 18L);
    assertEquals(stats.getResidentWeight(), 19L);
    assertEquals(stats.getRetiredWeight(), 20L);
    assertEquals(stats.getNativeAllocatedBytes(), 21L);
    assertEquals(stats.getTimerHeapBytes(), 22L);
    assertEquals(stats.getSketchHeapBytes(), 23L);
    assertEquals(stats.getGhostHeapBytes(), 24L);
    assertEquals(stats.getRetirementQueueNativeBytes(), 25L);
    assertEquals(stats.getRetirementQueueDepth(), 26L);
    assertEquals(stats.getRetirementQueueCapacity(), 27L);
    assertEquals(stats.getWakeSignals(), 28L);
    assertEquals(stats.getMergedWakeSignals(), 29L);
    assertEquals(stats.getNonBlockingPutFailureCount(), 30L);
    assertEquals(stats.getNonBlockingReplaceFailureCount(), 31L);
    assertEquals(stats.getNonBlockingRemoveFailureCount(), 32L);
    assertEquals(stats.getWriterContentionFailureCount(), 33L);
    assertEquals(stats.getRetirementAdmissionFailureCount(), 34L);
    assertEquals(stats.getReliableRemovalAdmissionFailureCount(), 35L);
    assertEquals(stats.getNativeAllocationFailureCount(), 36L);
    assertEquals(stats.getRepairQueueDepth(), 37L);
    assertEquals(stats.getAsyncMutationQueueDepth(), 38L);
    assertEquals(stats.getAsyncMutationCompletedCount(), 39L);
    assertEquals(stats.getAsyncMutationFailedCount(), 40L);
    assertEquals(stats.getAsyncMutationRejectedCount(), 41L);
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
