package com.red.ohc.api;

/** Weakly consistent cache and maintenance metrics. */
public final class OHCacheStats {
  private final long hitCount;
  private final long missCount;
  private final long loadSuccessCount;
  private final long loadFailureCount;
  private final long totalLoadTime;
  private final long evictionCount;
  private final long evictionWeight;
  private final long expirationCount;
  private final long residenceSampleCount;
  private final double residenceSampleRate;
  private final double sampledAverageResidenceTimeMillis;
  private final long size;
  private final long liveWeight;
  private final long nativeAllocatedBytes;
  private final boolean maintenanceUnhealthy;
  private final long maintenanceQueueDepth;
  private final long ttlLagMillis;
  private final long ttlBacklog;
  private final long nativeAllocationFailureCount;
  private final long smallAllocationFallbackCount;
  private final long directEntryAllocationCount;
  private final long maintenancePassWorkNanos;
  private final long maintenanceActiveNanosTotal;
  private final long maintenanceParkNanosTotal;
  private final long maintenanceImmediateContinuationCount;
  private final long retirementSealScannedLanes;
  private final long retirementSealSealedLanes;
  private final long retirementSealRecords;
  private final long retirementSealRecordsTotal;
  private final long retirementReclaimRecordsTotal;
  private final long retirementSealScannedLanesTotal;
  private final long retirementSealHeadOfLineStops;
  private final long retirementReclaimBlockedCount;
  private final long retirementReclaimBlockedNanos;
  private final long retirementRetryWakeCount;
  private final long activeReaderCount;
  private final long accessRingDroppedCount;
  private final long retirementQueueDepth;
  private final long retirementPublishedRecordsTotal;
  private final long retirementCompletedRecordsTotal;
  private final long retirementLagRecords;
  private final long retirementUnsafeRecords;
  private final long retirementUnsafeBytes;
  private final long retirementSafeRecords;
  private final long retirementSafeBytes;
  private final long retirementClaimedRecords;
  private final long retirementClaimedBytes;
  private final long retirementActorReclaimedRecords;
  private final long retirementAllocatedSegments;
  private final long retirementReusedSegments;
  private final long retirementTrimmedSegments;
  private final long asyncMutationQueueDepth;
  private final long asyncMutationPublishedRecords;
  private final long asyncMutationCompletedRecords;
  private final long asyncMutationLagRecords;
  private final long ghostNativeBytes;
  private final long ghostAllocationTrimCount;
  private final long ghostAllocationDropCount;
  private final boolean ghostRehashPending;
  private final long lifecycleJournalPublishedRecords;
  private final long lifecycleJournalCompletedRecords;
  private final long lifecycleJournalLagRecords;
  private final long lifecycleJournalAllocatedSegments;
  private final long lifecycleJournalHeadOfLineStopCount;
  private final long allocatorPageAllocatedCount;
  private final long allocatorPageReusedCount;
  private final long allocatorPageReadyCount;
  private final long allocatorPageTrimmedCount;
  private final long writerResourceActiveCount;
  private final long writerResourceRetiringCount;
  private final long writerResourcePooledCount;
  private final long retirementGeneratedBytesTotal;
  private final long retirementCompletedBytesTotal;
  private final double retirementGeneratedBytesPerSecond;
  private final double retirementCompletedBytesPerSecond;
  private final long nativeDebtBudgetBytes;
  private final long nativeDebtHeadroomBytes;
  private final long retirementSafeSegmentCount;
  private final long retirementReclaimBatchCount;
  private final long retirementOldestSafeWaitNanos;
  private final long mailboxHeadUnpublishedCount;

  public OHCacheStats(
      long hitCount,
      long missCount,
      long loadSuccessCount,
      long loadFailureCount,
      long totalLoadTime,
      long evictionCount,
      long evictionWeight,
      long expirationCount,
      long residenceSampleCount,
      double residenceSampleRate,
      double sampledAverageResidenceTimeMillis,
      long size,
      long liveWeight,
      long nativeAllocatedBytes,
      boolean maintenanceUnhealthy,
      long maintenanceQueueDepth,
      long ttlLagMillis,
      long ttlBacklog,
      long nativeAllocationFailureCount,
      long smallAllocationFallbackCount,
      long directEntryAllocationCount,
      long maintenancePassWorkNanos,
      long maintenanceActiveNanosTotal,
      long maintenanceParkNanosTotal,
      long maintenanceImmediateContinuationCount,
      long retirementSealScannedLanes,
      long retirementSealSealedLanes,
      long retirementSealRecords,
      long retirementSealRecordsTotal,
      long retirementReclaimRecordsTotal,
      long retirementSealScannedLanesTotal,
      long retirementSealHeadOfLineStops,
      long retirementReclaimBlockedCount,
      long retirementReclaimBlockedNanos,
      long retirementRetryWakeCount,
      long activeReaderCount,
      long accessRingDroppedCount,
      long retirementQueueDepth,
      long retirementPublishedRecordsTotal,
      long retirementCompletedRecordsTotal,
      long retirementLagRecords,
      long retirementUnsafeRecords,
      long retirementUnsafeBytes,
      long retirementSafeRecords,
      long retirementSafeBytes,
      long retirementClaimedRecords,
      long retirementClaimedBytes,
      long retirementActorReclaimedRecords,
      long retirementAllocatedSegments,
      long retirementReusedSegments,
      long retirementTrimmedSegments,
      long asyncMutationQueueDepth,
      long asyncMutationPublishedRecords,
      long asyncMutationCompletedRecords,
      long asyncMutationLagRecords,
      long ghostNativeBytes,
      long ghostAllocationTrimCount,
      long ghostAllocationDropCount,
      boolean ghostRehashPending,
      long lifecycleJournalPublishedRecords,
      long lifecycleJournalCompletedRecords,
      long lifecycleJournalLagRecords,
      long lifecycleJournalAllocatedSegments,
      long lifecycleJournalHeadOfLineStopCount,
      long allocatorPageAllocatedCount,
      long allocatorPageReusedCount,
      long allocatorPageReadyCount,
      long allocatorPageTrimmedCount,
      long writerResourceActiveCount,
      long writerResourceRetiringCount,
      long writerResourcePooledCount,
      long retirementGeneratedBytesTotal,
      long retirementCompletedBytesTotal,
      double retirementGeneratedBytesPerSecond,
      double retirementCompletedBytesPerSecond,
      long nativeDebtBudgetBytes,
      long nativeDebtHeadroomBytes,
      long retirementSafeSegmentCount,
      long retirementReclaimBatchCount,
      long retirementOldestSafeWaitNanos,
      long mailboxHeadUnpublishedCount) {
    this.hitCount = hitCount;
    this.missCount = missCount;
    this.loadSuccessCount = loadSuccessCount;
    this.loadFailureCount = loadFailureCount;
    this.totalLoadTime = totalLoadTime;
    this.evictionCount = evictionCount;
    this.evictionWeight = evictionWeight;
    this.expirationCount = expirationCount;
    this.residenceSampleCount = residenceSampleCount;
    this.residenceSampleRate = residenceSampleRate;
    this.sampledAverageResidenceTimeMillis = sampledAverageResidenceTimeMillis;
    this.size = size;
    this.liveWeight = liveWeight;
    this.nativeAllocatedBytes = nativeAllocatedBytes;
    this.maintenanceUnhealthy = maintenanceUnhealthy;
    this.maintenanceQueueDepth = maintenanceQueueDepth;
    this.ttlLagMillis = ttlLagMillis;
    this.ttlBacklog = ttlBacklog;
    this.nativeAllocationFailureCount = nativeAllocationFailureCount;
    this.smallAllocationFallbackCount = smallAllocationFallbackCount;
    this.directEntryAllocationCount = directEntryAllocationCount;
    this.maintenancePassWorkNanos = maintenancePassWorkNanos;
    this.maintenanceActiveNanosTotal = maintenanceActiveNanosTotal;
    this.maintenanceParkNanosTotal = maintenanceParkNanosTotal;
    this.maintenanceImmediateContinuationCount = maintenanceImmediateContinuationCount;
    this.retirementSealScannedLanes = retirementSealScannedLanes;
    this.retirementSealSealedLanes = retirementSealSealedLanes;
    this.retirementSealRecords = retirementSealRecords;
    this.retirementSealRecordsTotal = retirementSealRecordsTotal;
    this.retirementReclaimRecordsTotal = retirementReclaimRecordsTotal;
    this.retirementSealScannedLanesTotal = retirementSealScannedLanesTotal;
    this.retirementSealHeadOfLineStops = retirementSealHeadOfLineStops;
    this.retirementReclaimBlockedCount = retirementReclaimBlockedCount;
    this.retirementReclaimBlockedNanos = retirementReclaimBlockedNanos;
    this.retirementRetryWakeCount = retirementRetryWakeCount;
    this.activeReaderCount = activeReaderCount;
    this.accessRingDroppedCount = accessRingDroppedCount;
    this.retirementQueueDepth = retirementQueueDepth;
    this.retirementPublishedRecordsTotal = retirementPublishedRecordsTotal;
    this.retirementCompletedRecordsTotal = retirementCompletedRecordsTotal;
    this.retirementLagRecords = retirementLagRecords;
    this.retirementUnsafeRecords = retirementUnsafeRecords;
    this.retirementUnsafeBytes = retirementUnsafeBytes;
    this.retirementSafeRecords = retirementSafeRecords;
    this.retirementSafeBytes = retirementSafeBytes;
    this.retirementClaimedRecords = retirementClaimedRecords;
    this.retirementClaimedBytes = retirementClaimedBytes;
    this.retirementActorReclaimedRecords = retirementActorReclaimedRecords;
    this.retirementAllocatedSegments = retirementAllocatedSegments;
    this.retirementReusedSegments = retirementReusedSegments;
    this.retirementTrimmedSegments = retirementTrimmedSegments;
    this.asyncMutationQueueDepth = asyncMutationQueueDepth;
    this.asyncMutationPublishedRecords = asyncMutationPublishedRecords;
    this.asyncMutationCompletedRecords = asyncMutationCompletedRecords;
    this.asyncMutationLagRecords = asyncMutationLagRecords;
    this.ghostNativeBytes = ghostNativeBytes;
    this.ghostAllocationTrimCount = ghostAllocationTrimCount;
    this.ghostAllocationDropCount = ghostAllocationDropCount;
    this.ghostRehashPending = ghostRehashPending;
    this.lifecycleJournalPublishedRecords = lifecycleJournalPublishedRecords;
    this.lifecycleJournalCompletedRecords = lifecycleJournalCompletedRecords;
    this.lifecycleJournalLagRecords = lifecycleJournalLagRecords;
    this.lifecycleJournalAllocatedSegments = lifecycleJournalAllocatedSegments;
    this.lifecycleJournalHeadOfLineStopCount = lifecycleJournalHeadOfLineStopCount;
    this.allocatorPageAllocatedCount = allocatorPageAllocatedCount;
    this.allocatorPageReusedCount = allocatorPageReusedCount;
    this.allocatorPageReadyCount = allocatorPageReadyCount;
    this.allocatorPageTrimmedCount = allocatorPageTrimmedCount;
    this.writerResourceActiveCount = writerResourceActiveCount;
    this.writerResourceRetiringCount = writerResourceRetiringCount;
    this.writerResourcePooledCount = writerResourcePooledCount;
    this.retirementGeneratedBytesTotal = retirementGeneratedBytesTotal;
    this.retirementCompletedBytesTotal = retirementCompletedBytesTotal;
    this.retirementGeneratedBytesPerSecond = retirementGeneratedBytesPerSecond;
    this.retirementCompletedBytesPerSecond = retirementCompletedBytesPerSecond;
    this.nativeDebtBudgetBytes = nativeDebtBudgetBytes;
    this.nativeDebtHeadroomBytes = nativeDebtHeadroomBytes;
    this.retirementSafeSegmentCount = retirementSafeSegmentCount;
    this.retirementReclaimBatchCount = retirementReclaimBatchCount;
    this.retirementOldestSafeWaitNanos = retirementOldestSafeWaitNanos;
    this.mailboxHeadUnpublishedCount = mailboxHeadUnpublishedCount;
  }

  public long hitCount() {
    return hitCount;
  }

  public long missCount() {
    return missCount;
  }

  public long requestCount() {
    return saturatedAdd(hitCount, missCount);
  }

  public double hitRate() {
    long requests = requestCount();
    return requests == 0L ? 1.0d : (double) hitCount / requests;
  }

  public double missRate() {
    long requests = requestCount();
    return requests == 0L ? 0.0d : (double) missCount / requests;
  }

  public long loadSuccessCount() {
    return loadSuccessCount;
  }

  public long loadFailureCount() {
    return loadFailureCount;
  }

  public long loadCount() {
    return saturatedAdd(loadSuccessCount, loadFailureCount);
  }

  public double loadFailureRate() {
    long loads = loadCount();
    return loads == 0L ? 0.0d : (double) loadFailureCount / loads;
  }

  /** Total loader execution time in nanoseconds. */
  public long totalLoadTime() {
    return totalLoadTime;
  }

  public double averageLoadPenalty() {
    long loads = loadCount();
    return loads == 0L ? 0.0d : (double) totalLoadTime / loads;
  }

  public long evictionCount() {
    return evictionCount;
  }

  /** Aggregate logical serialized-entry bytes removed by eviction. */
  public long evictionWeight() {
    return evictionWeight;
  }

  public long expirationCount() {
    return expirationCount;
  }

  public long residenceSampleCount() {
    return residenceSampleCount;
  }

  public double residenceSampleRate() {
    return residenceSampleRate;
  }

  public double sampledAverageResidenceTimeMillis() {
    return sampledAverageResidenceTimeMillis;
  }

  public long size() {
    return size;
  }

  /** Aggregate logical serialized-entry bytes currently resident in the maintenance policy. */
  public long liveWeight() {
    return liveWeight;
  }

  /** Current physical native allocation, including allocator and shared structures. */
  public long nativeAllocatedBytes() {
    return nativeAllocatedBytes;
  }

  public boolean maintenanceUnhealthy() {
    return maintenanceUnhealthy;
  }

  public long maintenanceQueueDepth() {
    return maintenanceQueueDepth;
  }

  public long ttlLagMillis() {
    return ttlLagMillis;
  }

  public long ttlBacklog() {
    return ttlBacklog;
  }

  public long nativeAllocationFailureCount() {
    return nativeAllocationFailureCount;
  }

  public long smallAllocationFallbackCount() {
    return smallAllocationFallbackCount;
  }

  public long directEntryAllocationCount() {
    return directEntryAllocationCount;
  }

  public long maintenancePassWorkNanos() {
    return maintenancePassWorkNanos;
  }

  public long maintenanceActiveNanosTotal() {
    return maintenanceActiveNanosTotal;
  }

  public long maintenanceParkNanosTotal() {
    return maintenanceParkNanosTotal;
  }

  public long maintenanceImmediateContinuationCount() {
    return maintenanceImmediateContinuationCount;
  }

  public long retirementSealScannedLanes() {
    return retirementSealScannedLanes;
  }

  public long retirementSealSealedLanes() {
    return retirementSealSealedLanes;
  }

  public long retirementSealRecords() {
    return retirementSealRecords;
  }

  public long retirementSealRecordsTotal() {
    return retirementSealRecordsTotal;
  }

  public long retirementReclaimRecordsTotal() {
    return retirementReclaimRecordsTotal;
  }

  public long retirementSealScannedLanesTotal() {
    return retirementSealScannedLanesTotal;
  }

  public long retirementSealHeadOfLineStops() {
    return retirementSealHeadOfLineStops;
  }

  public long retirementReclaimBlockedCount() {
    return retirementReclaimBlockedCount;
  }

  public long retirementReclaimBlockedNanos() {
    return retirementReclaimBlockedNanos;
  }

  public long retirementRetryWakeCount() {
    return retirementRetryWakeCount;
  }

  public long activeReaderCount() {
    return activeReaderCount;
  }

  public long accessRingDroppedCount() {
    return accessRingDroppedCount;
  }

  public long retirementQueueDepth() {
    return retirementQueueDepth;
  }

  public long retirementPublishedRecordsTotal() {
    return retirementPublishedRecordsTotal;
  }

  public long retirementCompletedRecordsTotal() {
    return retirementCompletedRecordsTotal;
  }

  public long retirementLagRecords() {
    return retirementLagRecords;
  }

  public long retirementUnsafeRecords() {
    return retirementUnsafeRecords;
  }

  public long retirementUnsafeBytes() {
    return retirementUnsafeBytes;
  }

  public long retirementSafeRecords() {
    return retirementSafeRecords;
  }

  public long retirementSafeBytes() {
    return retirementSafeBytes;
  }

  public long retirementClaimedRecords() {
    return retirementClaimedRecords;
  }

  public long retirementClaimedBytes() {
    return retirementClaimedBytes;
  }

  public long retirementActorReclaimedRecords() {
    return retirementActorReclaimedRecords;
  }

  public long retirementAllocatedSegments() {
    return retirementAllocatedSegments;
  }

  public long retirementReusedSegments() {
    return retirementReusedSegments;
  }

  public long retirementTrimmedSegments() {
    return retirementTrimmedSegments;
  }

  public long asyncMutationQueueDepth() {
    return asyncMutationQueueDepth;
  }

  public long asyncMutationPublishedRecords() {
    return asyncMutationPublishedRecords;
  }

  public long asyncMutationCompletedRecords() {
    return asyncMutationCompletedRecords;
  }

  public long asyncMutationLagRecords() {
    return asyncMutationLagRecords;
  }

  /** Native bytes reserved by the S3-FIFO ghost history. */
  public long ghostNativeBytes() {
    return ghostNativeBytes;
  }

  /** Ghost records trimmed to make room under the shared native hard limit. */
  public long ghostAllocationTrimCount() {
    return ghostAllocationTrimCount;
  }

  /** Ghost records dropped after native allocation remained unavailable. */
  public long ghostAllocationDropCount() {
    return ghostAllocationDropCount;
  }

  /** Whether the actor still has an incremental S3 ghost rehash to complete. */
  public boolean ghostRehashPending() {
    return ghostRehashPending;
  }

  public long lifecycleJournalPublishedRecords() {
    return lifecycleJournalPublishedRecords;
  }

  public long lifecycleJournalCompletedRecords() {
    return lifecycleJournalCompletedRecords;
  }

  public long lifecycleJournalLagRecords() {
    return lifecycleJournalLagRecords;
  }

  public long lifecycleJournalAllocatedSegments() {
    return lifecycleJournalAllocatedSegments;
  }

  public long lifecycleJournalHeadOfLineStopCount() {
    return lifecycleJournalHeadOfLineStopCount;
  }

  public long allocatorPageAllocatedCount() {
    return allocatorPageAllocatedCount;
  }

  public long allocatorPageReusedCount() {
    return allocatorPageReusedCount;
  }

  public long allocatorPageReadyCount() {
    return allocatorPageReadyCount;
  }

  public long allocatorPageTrimmedCount() {
    return allocatorPageTrimmedCount;
  }

  public long writerResourceActiveCount() {
    return writerResourceActiveCount;
  }

  public long writerResourceRetiringCount() {
    return writerResourceRetiringCount;
  }

  public long writerResourcePooledCount() {
    return writerResourcePooledCount;
  }

  public long retirementGeneratedBytesTotal() {
    return retirementGeneratedBytesTotal;
  }

  public long retirementCompletedBytesTotal() {
    return retirementCompletedBytesTotal;
  }

  public double retirementGeneratedBytesPerSecond() {
    return retirementGeneratedBytesPerSecond;
  }

  public double retirementCompletedBytesPerSecond() {
    return retirementCompletedBytesPerSecond;
  }

  public long nativeDebtBudgetBytes() {
    return nativeDebtBudgetBytes;
  }

  public long nativeDebtHeadroomBytes() {
    return nativeDebtHeadroomBytes;
  }

  public long retirementSafeSegmentCount() {
    return retirementSafeSegmentCount;
  }

  public long retirementReclaimBatchCount() {
    return retirementReclaimBatchCount;
  }

  public long retirementOldestSafeWaitNanos() {
    return retirementOldestSafeWaitNanos;
  }

  public long mailboxHeadUnpublishedCount() {
    return mailboxHeadUnpublishedCount;
  }

  private static long saturatedAdd(long left, long right) {
    return right > 0L && left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }
}
