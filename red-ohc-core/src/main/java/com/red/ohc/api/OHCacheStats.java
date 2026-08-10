package com.red.ohc.api;

/** Eventually-consistent cache and maintenance metrics. */
public final class OHCacheStats {
  private long readHits;
  private long readMisses;
  private long readAccessDropped;
  private long mutationAccepted;
  private long mutationApplied;
  private long maintenanceQueueDepth;
  private long maintenanceQueueCapacity;
  private long maintenanceLoopNanos;
  private boolean maintenanceUnhealthy;
  private long logicalExpired;
  private long physicalExpired;
  private long ttlLagMillis;
  private long ttlBacklog;
  private long evictionCount;
  private long evictionScanCount;
  private long evictionLockedSkips;
  private long retiredEntries;
  private long size;
  private long liveWeight;
  private long residentWeight;
  private long retiredWeight;
  private long nativeAllocatedBytes;
  private long timerHeapBytes;
  private long sketchHeapBytes;
  private long ghostHeapBytes;
  private long retirementQueueNativeBytes;
  private long retirementQueueDepth;
  private long retirementQueueCapacity;
  private long wakeSignals;
  private long mergedWakeSignals;
  private long nonBlockingPutFailureCount;
  private long nonBlockingReplaceFailureCount;
  private long nonBlockingRemoveFailureCount;
  private long writerContentionFailureCount;
  private long retirementAdmissionFailureCount;
  private long reliableRemovalAdmissionFailureCount;
  private long nativeAllocationFailureCount;
  private long repairQueueDepth;
  private long asyncMutationQueueDepth;
  private long asyncMutationCompletedCount;
  private long asyncMutationFailedCount;
  private long asyncMutationRejectedCount;

  public OHCacheStats(
      long readHits,
      long readMisses,
      long readAccessDropped,
      long mutationAccepted,
      long mutationApplied,
      long maintenanceQueueDepth,
      long maintenanceQueueCapacity,
      long maintenanceLoopNanos,
      boolean maintenanceUnhealthy,
      long logicalExpired,
      long physicalExpired,
      long ttlLagMillis,
      long ttlBacklog,
      long evictionCount,
      long evictionScanCount,
      long evictionLockedSkips,
      long retiredEntries,
      long size,
      long liveWeight,
      long residentWeight,
      long retiredWeight,
      long nativeAllocatedBytes,
      long timerHeapBytes,
      long sketchHeapBytes,
      long ghostHeapBytes,
      long retirementQueueNativeBytes,
      long retirementQueueDepth,
      long retirementQueueCapacity,
      long wakeSignals,
      long mergedWakeSignals,
      long nonBlockingPutFailureCount,
      long nonBlockingReplaceFailureCount,
      long nonBlockingRemoveFailureCount,
      long writerContentionFailureCount,
      long retirementAdmissionFailureCount,
      long reliableRemovalAdmissionFailureCount,
      long nativeAllocationFailureCount,
      long repairQueueDepth,
      long asyncMutationQueueDepth,
      long asyncMutationCompletedCount,
      long asyncMutationFailedCount,
      long asyncMutationRejectedCount) {
    this.readHits = readHits;
    this.readMisses = readMisses;
    this.readAccessDropped = readAccessDropped;
    this.mutationAccepted = mutationAccepted;
    this.mutationApplied = mutationApplied;
    this.maintenanceQueueDepth = maintenanceQueueDepth;
    this.maintenanceQueueCapacity = maintenanceQueueCapacity;
    this.maintenanceLoopNanos = maintenanceLoopNanos;
    this.maintenanceUnhealthy = maintenanceUnhealthy;
    this.logicalExpired = logicalExpired;
    this.physicalExpired = physicalExpired;
    this.ttlLagMillis = ttlLagMillis;
    this.ttlBacklog = ttlBacklog;
    this.evictionCount = evictionCount;
    this.evictionScanCount = evictionScanCount;
    this.evictionLockedSkips = evictionLockedSkips;
    this.retiredEntries = retiredEntries;
    this.size = size;
    this.liveWeight = liveWeight;
    this.residentWeight = residentWeight;
    this.retiredWeight = retiredWeight;
    this.nativeAllocatedBytes = nativeAllocatedBytes;
    this.timerHeapBytes = timerHeapBytes;
    this.sketchHeapBytes = sketchHeapBytes;
    this.ghostHeapBytes = ghostHeapBytes;
    this.retirementQueueNativeBytes = retirementQueueNativeBytes;
    this.retirementQueueDepth = retirementQueueDepth;
    this.retirementQueueCapacity = retirementQueueCapacity;
    this.wakeSignals = wakeSignals;
    this.mergedWakeSignals = mergedWakeSignals;
    this.nonBlockingPutFailureCount = nonBlockingPutFailureCount;
    this.nonBlockingReplaceFailureCount = nonBlockingReplaceFailureCount;
    this.nonBlockingRemoveFailureCount = nonBlockingRemoveFailureCount;
    this.writerContentionFailureCount = writerContentionFailureCount;
    this.retirementAdmissionFailureCount = retirementAdmissionFailureCount;
    this.reliableRemovalAdmissionFailureCount = reliableRemovalAdmissionFailureCount;
    this.nativeAllocationFailureCount = nativeAllocationFailureCount;
    this.repairQueueDepth = repairQueueDepth;
    this.asyncMutationQueueDepth = asyncMutationQueueDepth;
    this.asyncMutationCompletedCount = asyncMutationCompletedCount;
    this.asyncMutationFailedCount = asyncMutationFailedCount;
    this.asyncMutationRejectedCount = asyncMutationRejectedCount;
  }

  public long getReadHits() {
    return readHits;
  }

  public long getReadMisses() {
    return readMisses;
  }

  public long getReadAccessDropped() {
    return readAccessDropped;
  }

  public long getMutationAccepted() {
    return mutationAccepted;
  }

  public long getMutationApplied() {
    return mutationApplied;
  }

  public long getMaintenanceQueueDepth() {
    return maintenanceQueueDepth;
  }

  public long getMaintenanceQueueCapacity() {
    return maintenanceQueueCapacity;
  }

  public long getMaintenanceLoopNanos() {
    return maintenanceLoopNanos;
  }

  public boolean getMaintenanceUnhealthy() {
    return maintenanceUnhealthy;
  }

  public long getLogicalExpired() {
    return logicalExpired;
  }

  public long getPhysicalExpired() {
    return physicalExpired;
  }

  public long getTtlLagMillis() {
    return ttlLagMillis;
  }

  public long getTtlBacklog() {
    return ttlBacklog;
  }

  public long getEvictionCount() {
    return evictionCount;
  }

  public long getEvictionScanCount() {
    return evictionScanCount;
  }

  public long getEvictionLockedSkips() {
    return evictionLockedSkips;
  }

  public long getRetiredEntries() {
    return retiredEntries;
  }

  public long getSize() {
    return size;
  }

  public long getLiveWeight() {
    return liveWeight;
  }

  public long getResidentWeight() {
    return residentWeight;
  }

  public long getRetiredWeight() {
    return retiredWeight;
  }

  public long getNativeAllocatedBytes() {
    return nativeAllocatedBytes;
  }

  public long getTimerHeapBytes() {
    return timerHeapBytes;
  }

  public long getSketchHeapBytes() {
    return sketchHeapBytes;
  }

  public long getGhostHeapBytes() {
    return ghostHeapBytes;
  }

  public long getRetirementQueueNativeBytes() {
    return retirementQueueNativeBytes;
  }

  public long getRetirementQueueDepth() {
    return retirementQueueDepth;
  }

  public long getRetirementQueueCapacity() {
    return retirementQueueCapacity;
  }

  public long getWakeSignals() {
    return wakeSignals;
  }

  public long getMergedWakeSignals() {
    return mergedWakeSignals;
  }

  public long getNonBlockingPutFailureCount() {
    return nonBlockingPutFailureCount;
  }

  public long getNonBlockingReplaceFailureCount() {
    return nonBlockingReplaceFailureCount;
  }

  public long getNonBlockingRemoveFailureCount() {
    return nonBlockingRemoveFailureCount;
  }

  public long getWriterContentionFailureCount() {
    return writerContentionFailureCount;
  }

  public long getRetirementAdmissionFailureCount() {
    return retirementAdmissionFailureCount;
  }

  public long getReliableRemovalAdmissionFailureCount() {
    return reliableRemovalAdmissionFailureCount;
  }

  public long getNativeAllocationFailureCount() {
    return nativeAllocationFailureCount;
  }

  public long getRepairQueueDepth() {
    return repairQueueDepth;
  }

  public long getAsyncMutationQueueDepth() {
    return asyncMutationQueueDepth;
  }

  public long getAsyncMutationCompletedCount() {
    return asyncMutationCompletedCount;
  }

  public long getAsyncMutationFailedCount() {
    return asyncMutationFailedCount;
  }

  public long getAsyncMutationRejectedCount() {
    return asyncMutationRejectedCount;
  }
}
