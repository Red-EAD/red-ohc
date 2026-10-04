package com.red.ohc.jmh;

import java.lang.reflect.Field;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.red.ohc.cache.OffHeapCache;
import com.red.ohc.maintenance.MaintenanceEventLoop;

public final class OHCInvocationLifecycleTest {
  @Test(timeOut = 30_000L)
  public void timeoutInvocationsReleaseTheirActorsAndNativeMemory() throws Exception {
    OHCTimeoutDrainBenchmark benchmark = new OHCTimeoutDrainBenchmark();
    benchmark.entries = 16;
    benchmark.keyBytes = 16;
    benchmark.valueBytes = 256;
    benchmark.createPayloads();
    Field cacheField = OHCTimeoutDrainBenchmark.class.getDeclaredField("cache");
    cacheField.setAccessible(true);
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    for (int invocation = 0; invocation < 10; invocation++) {
      benchmark.scheduleExpiryStorm();
      OffHeapCache<?, ?> cache = (OffHeapCache<?, ?>) cacheField.get(benchmark);
      MaintenanceEventLoop worker = (MaintenanceEventLoop) workerField.get(cache);
      Throwable failure = null;
      try {
        Assert.assertEquals(benchmark.drainExpiredBucket(), 16L);
      } catch (Throwable operationFailure) {
        failure = operationFailure;
        throw operationFailure;
      } finally {
        try {
          benchmark.stopCache();
        } catch (Throwable cleanupFailure) {
          if (failure == null) {
            throw cleanupFailure;
          }
          failure.addSuppressed(cleanupFailure);
        }
      }
      Assert.assertFalse(worker.isAlive());
      Assert.assertEquals(cache.totalAllocatedBytes(), 0L);
      Assert.assertNull(cacheField.get(benchmark));
    }
  }
}
