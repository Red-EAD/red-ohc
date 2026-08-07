package com.red.ohc;

/** Immutable, eventually-consistent aggregation of cache and maintenance metrics. */
public final class OHCacheStats {
    public final long readHits;
    public final long readMisses;
    public final long readAccessDropped;
    public final long mutationAccepted;
    public final long mutationRejectedQueue;
    public final long mutationRejectedBudget;
    public final long mutationApplied;
    public final long maintenanceDropped;
    public final long maintenanceQueueDepth;
    public final long maintenanceQueueLagNanos;
    public final long pendingBytes;
    public final long maintenanceLoopNanos;
    public final boolean maintenanceUnhealthy;
    public final long indexSlotCapacity;
    public final double indexLoad;
    public final int indexMaxProbe;
    public final boolean indexResizeInProgress;
    public final long indexHeapBytes;
    public final long indexOverflowEntries;
    public final long indexFallbackHeapBytes;
    public final long logicalExpired;
    public final long physicalExpired;
    public final long ttlLagMillis;
    public final long evictionCount;
    public final long lruEvictions;
    public final long wTinyLfuRejections;
    public final long s3SmallEvictions;
    public final long s3MainEvictions;
    public final long s3GhostHits;
    public final long retiredEntries;
    public final long retiredBytes;
    public final long oldestRetireEpoch;
    public final long entryBytes;
    public final long maintenanceQueueBytes;
    public final long timerBytes;
    public final long sketchBytes;
    public final long ghostHeapBytes;
    public final long totalAllocatedBytes;

    public OHCacheStats(long readHits, long readMisses, long readAccessDropped,
                 long mutationAccepted, long mutationRejectedQueue, long mutationRejectedBudget,
                 long mutationApplied, long maintenanceDropped, long maintenanceQueueDepth,
                 long maintenanceQueueLagNanos, long pendingBytes, long maintenanceLoopNanos, boolean maintenanceUnhealthy,
                 long indexSlotCapacity, double indexLoad, int indexMaxProbe, boolean indexResizeInProgress,
                 long indexHeapBytes, long indexOverflowEntries, long indexFallbackHeapBytes,
                 long logicalExpired, long physicalExpired, long ttlLagMillis, long evictionCount,
                 long lruEvictions, long wTinyLfuRejections, long s3SmallEvictions,
                 long s3MainEvictions, long s3GhostHits, long retiredEntries, long retiredBytes,
                 long oldestRetireEpoch, long entryBytes, long maintenanceQueueBytes,
                 long timerBytes, long sketchBytes, long ghostHeapBytes, long totalAllocatedBytes) {
        this.readHits = readHits;
        this.readMisses = readMisses;
        this.readAccessDropped = readAccessDropped;
        this.mutationAccepted = mutationAccepted;
        this.mutationRejectedQueue = mutationRejectedQueue;
        this.mutationRejectedBudget = mutationRejectedBudget;
        this.mutationApplied = mutationApplied;
        this.maintenanceDropped = maintenanceDropped;
        this.maintenanceQueueDepth = maintenanceQueueDepth;
        this.maintenanceQueueLagNanos = maintenanceQueueLagNanos;
        this.pendingBytes = pendingBytes;
        this.maintenanceLoopNanos = maintenanceLoopNanos;
        this.maintenanceUnhealthy = maintenanceUnhealthy;
        this.indexSlotCapacity = indexSlotCapacity;
        this.indexLoad = indexLoad;
        this.indexMaxProbe = indexMaxProbe;
        this.indexResizeInProgress = indexResizeInProgress;
        this.indexHeapBytes = indexHeapBytes;
        this.indexOverflowEntries = indexOverflowEntries;
        this.indexFallbackHeapBytes = indexFallbackHeapBytes;
        this.logicalExpired = logicalExpired;
        this.physicalExpired = physicalExpired;
        this.ttlLagMillis = ttlLagMillis;
        this.evictionCount = evictionCount;
        this.lruEvictions = lruEvictions;
        this.wTinyLfuRejections = wTinyLfuRejections;
        this.s3SmallEvictions = s3SmallEvictions;
        this.s3MainEvictions = s3MainEvictions;
        this.s3GhostHits = s3GhostHits;
        this.retiredEntries = retiredEntries;
        this.retiredBytes = retiredBytes;
        this.oldestRetireEpoch = oldestRetireEpoch;
        this.entryBytes = entryBytes;
        this.maintenanceQueueBytes = maintenanceQueueBytes;
        this.timerBytes = timerBytes;
        this.sketchBytes = sketchBytes;
        this.ghostHeapBytes = ghostHeapBytes;
        this.totalAllocatedBytes = totalAllocatedBytes;
    }
}
