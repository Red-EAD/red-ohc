package com.red.ohc;

/** Immutable, eventually-consistent cache and maintenance metrics. */
public final class OHCacheStats {
    public final long readHits;
    public final long readMisses;
    public final long readAccessDropped;
    public final long mutationAccepted;
    public final long mutationRejectedQueue;
    public final long mutationRejectedBudget;
    public final long mutationApplied;
    public final long maintenanceQueueDepth;
    public final long maintenanceQueueCapacity;
    public final long maintenanceLoopNanos;
    public final boolean maintenanceUnhealthy;
    public final long logicalExpired;
    public final long physicalExpired;
    public final long ttlLagMillis;
    public final long ttlBacklog;
    public final long evictionCount;
    public final long evictionScanCount;
    public final long evictionLockedSkips;
    public final long retiredEntries;
    public final long liveWeight;
    public final long residentWeight;
    public final long retiredWeight;
    public final long nativeAllocatedBytes;
    public final long timerHeapBytes;
    public final long sketchHeapBytes;
    public final long ghostHeapBytes;
    public final long retirementQueueNativeBytes;
    public final long wakeSignals;
    public final long mergedWakeSignals;

    public OHCacheStats(long readHits, long readMisses, long readAccessDropped,
                        long mutationAccepted, long mutationRejectedQueue, long mutationRejectedBudget,
                        long mutationApplied, long maintenanceQueueDepth, long maintenanceQueueCapacity,
                        long maintenanceLoopNanos, boolean maintenanceUnhealthy,
                        long logicalExpired, long physicalExpired, long ttlLagMillis, long ttlBacklog,
                        long evictionCount, long evictionScanCount, long evictionLockedSkips,
                        long retiredEntries, long liveWeight, long residentWeight, long retiredWeight,
                        long nativeAllocatedBytes, long timerHeapBytes, long sketchHeapBytes,
                        long ghostHeapBytes, long retirementQueueNativeBytes,
                        long wakeSignals, long mergedWakeSignals) {
        this.readHits = readHits;
        this.readMisses = readMisses;
        this.readAccessDropped = readAccessDropped;
        this.mutationAccepted = mutationAccepted;
        this.mutationRejectedQueue = mutationRejectedQueue;
        this.mutationRejectedBudget = mutationRejectedBudget;
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
        this.liveWeight = liveWeight;
        this.residentWeight = residentWeight;
        this.retiredWeight = retiredWeight;
        this.nativeAllocatedBytes = nativeAllocatedBytes;
        this.timerHeapBytes = timerHeapBytes;
        this.sketchHeapBytes = sketchHeapBytes;
        this.ghostHeapBytes = ghostHeapBytes;
        this.retirementQueueNativeBytes = retirementQueueNativeBytes;
        this.wakeSignals = wakeSignals;
        this.mergedWakeSignals = mergedWakeSignals;
    }
}
