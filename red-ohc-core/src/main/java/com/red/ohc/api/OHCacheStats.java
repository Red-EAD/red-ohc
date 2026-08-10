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
  private long maintenanceAssistCount;
  private long maintenanceAssistWork;
  private long maintenanceWaitCount;
  private long maintenanceWaitNanos;
  private long maintenanceProgressVersion;

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
      long maintenanceAssistCount,
      long maintenanceAssistWork,
      long maintenanceWaitCount,
      long maintenanceWaitNanos,
      long maintenanceProgressVersion) {
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
    this.maintenanceAssistCount = maintenanceAssistCount;
    this.maintenanceAssistWork = maintenanceAssistWork;
    this.maintenanceWaitCount = maintenanceWaitCount;
    this.maintenanceWaitNanos = maintenanceWaitNanos;
    this.maintenanceProgressVersion = maintenanceProgressVersion;
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

  public long getMaintenanceAssistCount() {
    return maintenanceAssistCount;
  }

  public long getMaintenanceAssistWork() {
    return maintenanceAssistWork;
  }

  public long getMaintenanceWaitCount() {
    return maintenanceWaitCount;
  }

  public long getMaintenanceWaitNanos() {
    return maintenanceWaitNanos;
  }

  public long getMaintenanceProgressVersion() {
    return maintenanceProgressVersion;
  }

  public void setReadHits(long readHits) {
    this.readHits = readHits;
  }

  public void setReadMisses(long readMisses) {
    this.readMisses = readMisses;
  }

  public void setReadAccessDropped(long readAccessDropped) {
    this.readAccessDropped = readAccessDropped;
  }

  public void setMutationAccepted(long mutationAccepted) {
    this.mutationAccepted = mutationAccepted;
  }

  public void setMutationApplied(long mutationApplied) {
    this.mutationApplied = mutationApplied;
  }

  public void setMaintenanceQueueDepth(long maintenanceQueueDepth) {
    this.maintenanceQueueDepth = maintenanceQueueDepth;
  }

  public void setMaintenanceQueueCapacity(long maintenanceQueueCapacity) {
    this.maintenanceQueueCapacity = maintenanceQueueCapacity;
  }

  public void setMaintenanceLoopNanos(long maintenanceLoopNanos) {
    this.maintenanceLoopNanos = maintenanceLoopNanos;
  }

  public void setMaintenanceUnhealthy(boolean maintenanceUnhealthy) {
    this.maintenanceUnhealthy = maintenanceUnhealthy;
  }

  public void setLogicalExpired(long logicalExpired) {
    this.logicalExpired = logicalExpired;
  }

  public void setPhysicalExpired(long physicalExpired) {
    this.physicalExpired = physicalExpired;
  }

  public void setTtlLagMillis(long ttlLagMillis) {
    this.ttlLagMillis = ttlLagMillis;
  }

  public void setTtlBacklog(long ttlBacklog) {
    this.ttlBacklog = ttlBacklog;
  }

  public void setEvictionCount(long evictionCount) {
    this.evictionCount = evictionCount;
  }

  public void setEvictionScanCount(long evictionScanCount) {
    this.evictionScanCount = evictionScanCount;
  }

  public void setEvictionLockedSkips(long evictionLockedSkips) {
    this.evictionLockedSkips = evictionLockedSkips;
  }

  public void setRetiredEntries(long retiredEntries) {
    this.retiredEntries = retiredEntries;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public void setLiveWeight(long liveWeight) {
    this.liveWeight = liveWeight;
  }

  public void setResidentWeight(long residentWeight) {
    this.residentWeight = residentWeight;
  }

  public void setRetiredWeight(long retiredWeight) {
    this.retiredWeight = retiredWeight;
  }

  public void setNativeAllocatedBytes(long nativeAllocatedBytes) {
    this.nativeAllocatedBytes = nativeAllocatedBytes;
  }

  public void setTimerHeapBytes(long timerHeapBytes) {
    this.timerHeapBytes = timerHeapBytes;
  }

  public void setSketchHeapBytes(long sketchHeapBytes) {
    this.sketchHeapBytes = sketchHeapBytes;
  }

  public void setGhostHeapBytes(long ghostHeapBytes) {
    this.ghostHeapBytes = ghostHeapBytes;
  }

  public void setRetirementQueueNativeBytes(long retirementQueueNativeBytes) {
    this.retirementQueueNativeBytes = retirementQueueNativeBytes;
  }

  public void setRetirementQueueDepth(long retirementQueueDepth) {
    this.retirementQueueDepth = retirementQueueDepth;
  }

  public void setRetirementQueueCapacity(long retirementQueueCapacity) {
    this.retirementQueueCapacity = retirementQueueCapacity;
  }

  public void setWakeSignals(long wakeSignals) {
    this.wakeSignals = wakeSignals;
  }

  public void setMergedWakeSignals(long mergedWakeSignals) {
    this.mergedWakeSignals = mergedWakeSignals;
  }

  public void setMaintenanceAssistCount(long maintenanceAssistCount) {
    this.maintenanceAssistCount = maintenanceAssistCount;
  }

  public void setMaintenanceAssistWork(long maintenanceAssistWork) {
    this.maintenanceAssistWork = maintenanceAssistWork;
  }

  public void setMaintenanceWaitCount(long maintenanceWaitCount) {
    this.maintenanceWaitCount = maintenanceWaitCount;
  }

  public void setMaintenanceWaitNanos(long maintenanceWaitNanos) {
    this.maintenanceWaitNanos = maintenanceWaitNanos;
  }

  public void setMaintenanceProgressVersion(long maintenanceProgressVersion) {
    this.maintenanceProgressVersion = maintenanceProgressVersion;
  }
}
