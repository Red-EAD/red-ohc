package com.red.ohc.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.testng.annotations.Test;

public final class OHCacheStatsTest {
  @Test
  public void exposesStandardCacheMetricsAndDerivedRates() {
    OHCacheStats stats =
        new OHCacheStats(
            10L,
            5L,
            3L,
            1L,
            40L,
            7L,
            9L,
            2L,
            4L,
            0.25d,
            25.0d,
            11L,
            12L,
            13L,
            false,
            14L,
            15L,
            16L,
            17L,
            18L,
            19L,
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
            39L,
            40L,
            42L,
            43L,
            44L,
            59L,
            60L,
            61L,
            62L,
            63L,
            64L,
            65L,
            66L,
            67L,
            68L,
            70L,
            71L,
            72L,
            45L,
            48L,
            49L,
            53L,
            50L,
            51L,
            52L,
            true,
            56L,
            57L,
            58L,
            59L,
            60L,
            61L,
            62L,
            63L,
            64L,
            65L,
            66L,
            67L,
            73L,
            74L,
            78.0d,
            79.0d,
            80L,
            81L,
            82L,
            83L,
            84L,
            85L,
            new long[] {86L},
            new long[] {87L},
            new double[] {88.0d},
            89L,
            90L,
            91L);

    assertEquals(stats.hitCount(), 10L);
    assertEquals(stats.missCount(), 5L);
    assertEquals(stats.requestCount(), 15L);
    assertEquals(stats.hitRate(), 10.0 / 15.0, 0.000001);
    assertEquals(stats.missRate(), 5.0 / 15.0, 0.000001);
    assertEquals(stats.loadSuccessCount(), 3L);
    assertEquals(stats.loadFailureCount(), 1L);
    assertEquals(stats.loadCount(), 4L);
    assertEquals(stats.loadFailureRate(), 0.25, 0.000001);
    assertEquals(stats.totalLoadTime(), 40L);
    assertEquals(stats.averageLoadPenalty(), 10.0, 0.000001);
    assertEquals(stats.evictionCount(), 7L);
    assertEquals(stats.evictionWeight(), 9L);
    assertEquals(stats.expirationCount(), 2L);
    assertEquals(stats.residenceSampleCount(), 4L);
    assertEquals(stats.residenceSampleRate(), 0.25, 0.000001);
    assertEquals(stats.sampledAverageResidenceTimeMillis(), 25.0, 0.000001);
    assertEquals(stats.size(), 11L);
    assertEquals(stats.liveWeight(), 12L);
    assertEquals(stats.nativeAllocatedBytes(), 13L);
    assertTrue(!stats.maintenanceUnhealthy());
    assertEquals(stats.maintenanceQueueDepth(), 14L);
    assertEquals(stats.ttlLagMillis(), 15L);
    assertEquals(stats.ttlBacklog(), 16L);
    assertEquals(stats.nativeAllocationFailureCount(), 17L);
    assertEquals(stats.smallAllocationFallbackCount(), 18L);
    assertEquals(stats.directEntryAllocationCount(), 19L);
    assertEquals(stats.maintenancePassWorkNanos(), 25L);
    assertEquals(stats.maintenanceActiveNanosTotal(), 26L);
    assertEquals(stats.maintenanceParkNanosTotal(), 27L);
    assertEquals(stats.maintenanceImmediateContinuationCount(), 28L);
    assertEquals(stats.retirementSealScannedLanes(), 29L);
    assertEquals(stats.retirementSealSealedLanes(), 30L);
    assertEquals(stats.retirementSealRecords(), 31L);
    assertEquals(stats.retirementSealRecordsTotal(), 32L);
    assertEquals(stats.retirementReclaimRecordsTotal(), 33L);
    assertEquals(stats.retirementSealScannedLanesTotal(), 34L);
    assertEquals(stats.retirementSealHeadOfLineStops(), 35L);
    assertEquals(stats.retirementReclaimBlockedCount(), 36L);
    assertEquals(stats.retirementReclaimBlockedNanos(), 39L);
    assertEquals(stats.retirementRetryWakeCount(), 40L);
    assertEquals(stats.activeReaderCount(), 42L);
    assertEquals(stats.accessRingDroppedCount(), 43L);
    assertEquals(stats.retirementQueueDepth(), 44L);
    assertEquals(stats.retirementPublishedRecordsTotal(), 59L);
    assertEquals(stats.retirementCompletedRecordsTotal(), 60L);
    assertEquals(stats.retirementLagRecords(), 61L);
    assertEquals(stats.retirementUnsafeRecords(), 62L);
    assertEquals(stats.retirementUnsafeBytes(), 63L);
    assertEquals(stats.retirementSafeRecords(), 64L);
    assertEquals(stats.retirementSafeBytes(), 65L);
    assertEquals(stats.retirementClaimedRecords(), 66L);
    assertEquals(stats.retirementClaimedBytes(), 67L);
    assertEquals(stats.retirementActorReclaimedRecords(), 68L);
    assertEquals(stats.retirementAllocatedSegments(), 70L);
    assertEquals(stats.retirementReusedSegments(), 71L);
    assertEquals(stats.retirementTrimmedSegments(), 72L);
    assertEquals(stats.asyncMutationQueueDepth(), 45L);
    assertEquals(stats.asyncMutationPublishedRecords(), 48L);
    assertEquals(stats.asyncMutationCompletedRecords(), 49L);
    assertEquals(stats.asyncMutationLagRecords(), 53L);
    assertEquals(stats.ghostNativeBytes(), 50L);
    assertEquals(stats.ghostAllocationTrimCount(), 51L);
    assertEquals(stats.ghostAllocationDropCount(), 52L);
    assertTrue(stats.ghostRehashPending());
    assertEquals(stats.lifecycleJournalPublishedRecords(), 56L);
    assertEquals(stats.lifecycleJournalCompletedRecords(), 57L);
    assertEquals(stats.lifecycleJournalLagRecords(), 58L);
    assertEquals(stats.lifecycleJournalAllocatedSegments(), 59L);
    assertEquals(stats.lifecycleJournalHeadOfLineStopCount(), 60L);
    assertEquals(stats.allocatorPageAllocatedCount(), 61L);
    assertEquals(stats.allocatorPageReusedCount(), 62L);
    assertEquals(stats.allocatorPageReadyCount(), 63L);
    assertEquals(stats.allocatorPageTrimmedCount(), 64L);
    assertEquals(stats.writerResourceActiveCount(), 65L);
    assertEquals(stats.writerResourceRetiringCount(), 66L);
    assertEquals(stats.writerResourcePooledCount(), 67L);
    assertEquals(stats.retirementGeneratedBytesTotal(), 73L);
    assertEquals(stats.retirementCompletedBytesTotal(), 74L);
    assertEquals(stats.retirementGeneratedBytesPerSecond(), 78.0d, 0.0d);
    assertEquals(stats.retirementCompletedBytesPerSecond(), 79.0d, 0.0d);
    assertEquals(stats.nativeDebtBudgetBytes(), 80L);
    assertEquals(stats.nativeDebtHeadroomBytes(), 81L);
    assertEquals(stats.retirementSafeSegmentCount(), 82L);
    assertEquals(stats.retirementReclaimBatchCount(), 83L);
    assertEquals(stats.retirementOldestSafeWaitNanos(), 84L);
    assertEquals(stats.mailboxHeadUnpublishedCount(), 85L);
    assertEquals(stats.allocatorReadyPagesByClass(), new long[] {86L});
    assertEquals(stats.allocatorPagesInUseByClass(), new long[] {87L});
    assertEquals(stats.allocatorPageOccupancyByClass()[0], 88.0d, 0.0d);
    assertEquals(stats.allocatorRetainedPagesCurrent(), 89L);
    assertEquals(stats.allocatorPooledPageCount(), 90L);
    assertEquals(stats.allocatorTrimmedBytesTotal(), 91L);
  }

  @Test
  public void derivedRatesUseEmptyDefaults() {
    OHCacheStats stats =
        new OHCacheStats(
            0L, // hitCount
            0L, // missCount
            0L, // loadSuccessCount
            0L, // loadFailureCount
            0L, // totalLoadTime
            0L, // evictionCount
            0L, // evictionWeight
            0L, // expirationCount
            0L, // residenceSampleCount
            0.0d, // residenceSampleRate
            0.0d, // sampledAverageResidenceTimeMillis
            0L, // size
            0L, // liveWeight
            0L, // nativeAllocatedBytes
            false, // maintenanceUnhealthy
            0L, // maintenanceQueueDepth
            0L, // ttlLagMillis
            0L, // ttlBacklog
            0L, // nativeAllocationFailureCount
            0L, // smallAllocationFallbackCount
            0L, // directEntryAllocationCount
            0L, // maintenancePassWorkNanos
            0L, // maintenanceActiveNanosTotal
            0L, // maintenanceParkNanosTotal
            0L, // maintenanceImmediateContinuationCount
            0L, // retirementSealScannedLanes
            0L, // retirementSealSealedLanes
            0L, // retirementSealRecords
            0L, // retirementSealRecordsTotal
            0L, // retirementReclaimRecordsTotal
            0L, // retirementSealScannedLanesTotal
            0L, // retirementSealHeadOfLineStops
            0L, // retirementReclaimBlockedCount
            0L, // retirementReclaimBlockedNanos
            0L, // retirementRetryWakeCount
            0L, // activeReaderCount
            0L, // accessRingDroppedCount
            0L, // retirementQueueDepth
            0L, // retirementPublishedRecordsTotal
            0L, // retirementCompletedRecordsTotal
            0L, // retirementLagRecords
            0L, // retirementUnsafeRecords
            0L, // retirementUnsafeBytes
            0L, // retirementSafeRecords
            0L, // retirementSafeBytes
            0L, // retirementClaimedRecords
            0L, // retirementClaimedBytes
            0L, // retirementActorReclaimedRecords
            0L, // retirementAllocatedSegments
            0L, // retirementReusedSegments
            0L, // retirementTrimmedSegments
            0L, // asyncMutationQueueDepth
            0L, // asyncMutationPublishedRecords
            0L, // asyncMutationCompletedRecords
            0L, // asyncMutationLagRecords
            0L, // ghostNativeBytes
            0L, // ghostAllocationTrimCount
            0L, // ghostAllocationDropCount
            false, // ghostRehashPending
            0L, // lifecycleJournalPublishedRecords
            0L, // lifecycleJournalCompletedRecords
            0L, // lifecycleJournalLagRecords
            0L, // lifecycleJournalAllocatedSegments
            0L, // lifecycleJournalHeadOfLineStopCount
            0L, // allocatorPageAllocatedCount
            0L, // allocatorPageReusedCount
            0L, // allocatorPageReadyCount
            0L, // allocatorPageTrimmedCount
            0L, // writerResourceActiveCount
            0L, // writerResourceRetiringCount
            0L, // writerResourcePooledCount
            0L, // retirementGeneratedBytesTotal
            0L, // retirementCompletedBytesTotal
            0.0d, // retirementGeneratedBytesPerSecond
            0.0d, // retirementCompletedBytesPerSecond
            0L, // nativeDebtBudgetBytes
            0L, // nativeDebtHeadroomBytes
            0L, // retirementSafeSegmentCount
            0L, // retirementReclaimBatchCount
            0L, // retirementOldestSafeWaitNanos
            0L, // mailboxHeadUnpublishedCount
            new long[0], // allocatorReadyPagesByClass
            new long[0], // allocatorPagesInUseByClass
            new double[0], // allocatorPageOccupancyByClass
            0L, // allocatorRetainedPagesCurrent
            0L, // allocatorPooledPageCount
            0L); // allocatorTrimmedBytesTotal

    assertEquals(stats.requestCount(), 0L);
    assertEquals(stats.hitRate(), 1.0, 0.0);
    assertEquals(stats.missRate(), 0.0, 0.0);
    assertEquals(stats.loadCount(), 0L);
    assertEquals(stats.loadFailureRate(), 0.0, 0.0);
    assertEquals(stats.averageLoadPenalty(), 0.0, 0.0);
    assertEquals(stats.sampledAverageResidenceTimeMillis(), 0.0, 0.0);
  }

  @Test
  public void statsFieldsArePrivate() {
    String[] expectedFields = {
      "hitCount",
      "missCount",
      "loadSuccessCount",
      "loadFailureCount",
      "totalLoadTime",
      "evictionCount",
      "evictionWeight",
      "expirationCount",
      "residenceSampleCount",
      "residenceSampleRate",
      "sampledAverageResidenceTimeMillis",
      "size",
      "liveWeight",
      "nativeAllocatedBytes",
      "maintenanceUnhealthy",
      "maintenanceQueueDepth",
      "ttlLagMillis",
      "ttlBacklog",
      "nativeAllocationFailureCount",
      "smallAllocationFallbackCount",
      "directEntryAllocationCount",
      "maintenancePassWorkNanos",
      "maintenanceActiveNanosTotal",
      "maintenanceParkNanosTotal",
      "maintenanceImmediateContinuationCount",
      "retirementSealScannedLanes",
      "retirementSealSealedLanes",
      "retirementSealRecords",
      "retirementSealRecordsTotal",
      "retirementReclaimRecordsTotal",
      "retirementSealScannedLanesTotal",
      "retirementSealHeadOfLineStops",
      "retirementReclaimBlockedCount",
      "retirementReclaimBlockedNanos",
      "retirementRetryWakeCount",
      "activeReaderCount",
      "accessRingDroppedCount",
      "retirementQueueDepth",
      "retirementPublishedRecordsTotal",
      "retirementCompletedRecordsTotal",
      "retirementLagRecords",
      "retirementUnsafeRecords",
      "retirementUnsafeBytes",
      "retirementSafeRecords",
      "retirementSafeBytes",
      "retirementClaimedRecords",
      "retirementClaimedBytes",
      "retirementActorReclaimedRecords",
      "retirementAllocatedSegments",
      "retirementReusedSegments",
      "retirementTrimmedSegments",
      "asyncMutationQueueDepth",
      "asyncMutationPublishedRecords",
      "asyncMutationCompletedRecords",
      "asyncMutationLagRecords",
      "ghostNativeBytes",
      "ghostAllocationTrimCount",
      "ghostAllocationDropCount",
      "ghostRehashPending",
      "lifecycleJournalPublishedRecords",
      "lifecycleJournalCompletedRecords",
      "lifecycleJournalLagRecords",
      "lifecycleJournalAllocatedSegments",
      "lifecycleJournalHeadOfLineStopCount",
      "allocatorPageAllocatedCount",
      "allocatorPageReusedCount",
      "allocatorPageReadyCount",
      "allocatorPageTrimmedCount",
      "writerResourceActiveCount",
      "writerResourceRetiringCount",
      "writerResourcePooledCount",
      "retirementGeneratedBytesTotal",
      "retirementCompletedBytesTotal",
      "retirementGeneratedBytesPerSecond",
      "retirementCompletedBytesPerSecond",
      "nativeDebtBudgetBytes",
      "nativeDebtHeadroomBytes",
      "retirementSafeSegmentCount",
      "retirementReclaimBatchCount",
      "retirementOldestSafeWaitNanos",
      "mailboxHeadUnpublishedCount",
      "allocatorReadyPagesByClass",
      "allocatorPagesInUseByClass",
      "allocatorPageOccupancyByClass",
      "allocatorRetainedPagesCurrent",
      "allocatorPooledPageCount",
      "allocatorTrimmedBytesTotal"
    };
    assertEquals(OHCacheStats.class.getDeclaredFields().length, expectedFields.length);
    for (String expectedField : expectedFields) {
      assertTrue(
          Modifier.isPrivate(getField(expectedField).getModifiers()),
          "stats field must be private: " + expectedField);
    }
    for (Field field : OHCacheStats.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        assertTrue(
            Modifier.isPrivate(field.getModifiers()),
            "stats field must be private: " + field.getName());
      }
    }
  }

  private static boolean hasPublicLongGetter(String name) {
    try {
      return OHCacheStats.class.getMethod(name).getReturnType() == long.class;
    } catch (NoSuchMethodException missing) {
      return false;
    }
  }

  private static Field getField(String name) {
    try {
      return OHCacheStats.class.getDeclaredField(name);
    } catch (NoSuchFieldException missing) {
      throw new AssertionError("missing stats field: " + name, missing);
    }
  }
}
