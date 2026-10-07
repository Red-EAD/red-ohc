package com.red.ohc.jmh;

import java.lang.reflect.Field;

import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OffHeapCache;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;

/** Backend-specific support keeps unrelated libraries out of isolated runtime graphs. */
public final class RedOhcBenchmarkSupport {
  private RedOhcBenchmarkSupport() {}

  public static void stopOHC(OHCache<?, ?> cache) {
    stopOHC(cache, null);
  }

  public static void stopOHC(OHCache<?, ?> cache, Throwable primaryFailure) {
    if (cache == null) {
      return;
    }
    try {
      Field workerField = OffHeapCache.class.getDeclaredField("worker");
      workerField.setAccessible(true);
      MaintenanceEventLoop worker = (MaintenanceEventLoop) workerField.get(cache);
      worker.stop();
      boolean interrupted = false;
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30L);
      try {
        while (worker.isAlive()) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0L) {
            break;
          }
          try {
            worker.join(
                Math.max(1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining)));
          } catch (InterruptedException interruption) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (worker.isAlive()) {
        throw new IllegalStateException("benchmark cache actor did not stop");
      }
      Field admissionField = OffHeapCache.class.getDeclaredField("logicalAdmission");
      admissionField.setAccessible(true);
      worker.assertLogicalAdmissionStable((LogicalAdmission) admissionField.get(cache));
      if (cache.totalAllocatedBytes() != 0L) {
        throw new IllegalStateException("benchmark cache retained native memory");
      }
    } catch (Throwable failure) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(failure);
      } else {
        throw new IllegalStateException("benchmark cache teardown failed", failure);
      }
    }
  }

  public static long ohcCapacityBytes(int keyBytes, int valueBytes) {
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyBytes);
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    return CacheMath.logicalEntryBytes(keyAllocation, valueAllocation) * SerializedBenchmarkSupport.CAPACITY_ENTRIES;
  }
}
