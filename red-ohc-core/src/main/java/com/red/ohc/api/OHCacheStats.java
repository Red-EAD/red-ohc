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
  private final long entryResidenceCount;
  private final long totalEntryResidenceTimeMillis;
  private final long size;
  private final long liveWeight;
  private final long nativeAllocatedBytes;
  private final boolean maintenanceUnhealthy;
  private final long maintenanceQueueDepth;
  private final long ttlLagMillis;
  private final long ttlBacklog;
  private final long nativeAllocationFailureCount;

  public OHCacheStats(
      long hitCount,
      long missCount,
      long loadSuccessCount,
      long loadFailureCount,
      long totalLoadTime,
      long evictionCount,
      long evictionWeight,
      long expirationCount,
      long entryResidenceCount,
      long totalEntryResidenceTimeMillis,
      long size,
      long liveWeight,
      long nativeAllocatedBytes,
      boolean maintenanceUnhealthy,
      long maintenanceQueueDepth,
      long ttlLagMillis,
      long ttlBacklog,
      long nativeAllocationFailureCount) {
    this.hitCount = hitCount;
    this.missCount = missCount;
    this.loadSuccessCount = loadSuccessCount;
    this.loadFailureCount = loadFailureCount;
    this.totalLoadTime = totalLoadTime;
    this.evictionCount = evictionCount;
    this.evictionWeight = evictionWeight;
    this.expirationCount = expirationCount;
    this.entryResidenceCount = entryResidenceCount;
    this.totalEntryResidenceTimeMillis = totalEntryResidenceTimeMillis;
    this.size = size;
    this.liveWeight = liveWeight;
    this.nativeAllocatedBytes = nativeAllocatedBytes;
    this.maintenanceUnhealthy = maintenanceUnhealthy;
    this.maintenanceQueueDepth = maintenanceQueueDepth;
    this.ttlLagMillis = ttlLagMillis;
    this.ttlBacklog = ttlBacklog;
    this.nativeAllocationFailureCount = nativeAllocationFailureCount;
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

  public long evictionWeight() {
    return evictionWeight;
  }

  public long expirationCount() {
    return expirationCount;
  }

  public long entryResidenceCount() {
    return entryResidenceCount;
  }

  public long totalEntryResidenceTimeMillis() {
    return totalEntryResidenceTimeMillis;
  }

  public double averageEntryResidenceTimeMillis() {
    return entryResidenceCount == 0L
        ? 0.0d
        : (double) totalEntryResidenceTimeMillis / entryResidenceCount;
  }

  public long size() {
    return size;
  }

  public long liveWeight() {
    return liveWeight;
  }

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

  private static long saturatedAdd(long left, long right) {
    return right > 0L && left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }
}
