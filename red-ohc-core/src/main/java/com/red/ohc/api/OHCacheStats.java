package com.red.ohc.api;

/** Eventually-consistent cache and maintenance metrics. */
public final class OHCacheStats {
  private final long readHits;
  private final long readMisses;
  private final long maintenanceQueueDepth;
  private final long maintenanceQueueCapacity;
  private final boolean maintenanceUnhealthy;
  private final long physicalExpired;
  private final long ttlLagMillis;
  private final long ttlBacklog;
  private final long evictionCount;
  private final long retiredEntries;
  private final long size;
  private final long liveWeight;
  private final long residentWeight;
  private final long retiredWeight;
  private final long nativeAllocatedBytes;
  private final long timerHeapBytes;
  private final long sketchHeapBytes;
  private final long ghostHeapBytes;
  private final long retirementQueueNativeBytes;
  private final long retirementQueueDepth;
  private final long retirementQueueCapacity;
  private final long nonBlockingPutFailureCount;
  private final long nonBlockingReplaceFailureCount;
  private final long nonBlockingRemoveFailureCount;
  private final long writerContentionFailureCount;
  private final long retirementAdmissionFailureCount;
  private final long reliableRemovalAdmissionFailureCount;
  private final long nativeAllocationFailureCount;
  private final long repairQueueDepth;
  private final long asyncMutationQueueDepth;
  private final long asyncMutationFailedCount;
  private final long asyncMutationRejectedCount;

  public OHCacheStats(
      long readHits,
      long readMisses,
      long maintenanceQueueDepth,
      long maintenanceQueueCapacity,
      boolean maintenanceUnhealthy,
      long physicalExpired,
      long ttlLagMillis,
      long ttlBacklog,
      long evictionCount,
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
      long nonBlockingPutFailureCount,
      long nonBlockingReplaceFailureCount,
      long nonBlockingRemoveFailureCount,
      long writerContentionFailureCount,
      long retirementAdmissionFailureCount,
      long reliableRemovalAdmissionFailureCount,
      long nativeAllocationFailureCount,
      long repairQueueDepth,
      long asyncMutationQueueDepth,
      long asyncMutationFailedCount,
      long asyncMutationRejectedCount) {
    this.readHits = readHits;
    this.readMisses = readMisses;
    this.maintenanceQueueDepth = maintenanceQueueDepth;
    this.maintenanceQueueCapacity = maintenanceQueueCapacity;
    this.maintenanceUnhealthy = maintenanceUnhealthy;
    this.physicalExpired = physicalExpired;
    this.ttlLagMillis = ttlLagMillis;
    this.ttlBacklog = ttlBacklog;
    this.evictionCount = evictionCount;
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
    this.nonBlockingPutFailureCount = nonBlockingPutFailureCount;
    this.nonBlockingReplaceFailureCount = nonBlockingReplaceFailureCount;
    this.nonBlockingRemoveFailureCount = nonBlockingRemoveFailureCount;
    this.writerContentionFailureCount = writerContentionFailureCount;
    this.retirementAdmissionFailureCount = retirementAdmissionFailureCount;
    this.reliableRemovalAdmissionFailureCount = reliableRemovalAdmissionFailureCount;
    this.nativeAllocationFailureCount = nativeAllocationFailureCount;
    this.repairQueueDepth = repairQueueDepth;
    this.asyncMutationQueueDepth = asyncMutationQueueDepth;
    this.asyncMutationFailedCount = asyncMutationFailedCount;
    this.asyncMutationRejectedCount = asyncMutationRejectedCount;
  }

  public long getReadHits() {
    return readHits;
  }

  public long getReadMisses() {
    return readMisses;
  }

  public long getMaintenanceQueueDepth() {
    return maintenanceQueueDepth;
  }

  public long getMaintenanceQueueCapacity() {
    return maintenanceQueueCapacity;
  }

  public boolean getMaintenanceUnhealthy() {
    return maintenanceUnhealthy;
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

  public long getAsyncMutationFailedCount() {
    return asyncMutationFailedCount;
  }

  public long getAsyncMutationRejectedCount() {
    return asyncMutationRejectedCount;
  }
}
