package com.red.ohc.cache;

import java.lang.reflect.Field;

import com.red.ohc.api.OHCache;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;

/** Quiesced teardown for cache tests. */
public final class CacheTestSupport {
  private CacheTestSupport() {}

  public static void awaitCallers(
      java.util.concurrent.ExecutorService caller, Throwable primaryFailure) {
    try {
      awaitCallers(caller);
    } catch (Throwable failure) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(failure);
      } else {
        throw failure;
      }
    }
  }

  public static void awaitCallers(java.util.concurrent.ExecutorService caller) {
    if (caller == null) {
      return;
    }
    caller.shutdownNow();
    boolean interrupted = false;
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30L);
    try {
      while (!caller.isTerminated()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0L) {
          throw new AssertionError("concurrent cache callers did not terminate");
        }
        try {
          caller.awaitTermination(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
        } catch (InterruptedException interruption) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void awaitCaller(Thread caller) {
    if (caller == null) {
      return;
    }
    boolean interrupted = false;
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30L);
    try {
      while (caller.isAlive()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0L) {
          throw new AssertionError(
              "concurrent cache caller did not terminate: " + caller.getName());
        }
        try {
          caller.join(Math.max(1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining)));
        } catch (InterruptedException interruption) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void stopAfterCallers(
      OHCache<?, ?> cache, Throwable primaryFailure, Thread... callers) {
    try {
      for (Thread caller : callers) {
        if (caller != null) {
          caller.interrupt();
        }
      }
      for (Thread caller : callers) {
        awaitCaller(caller);
      }
      stop(cache, primaryFailure);
    } catch (Throwable failure) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(failure);
      } else {
        throw new AssertionError("cache caller teardown failed", failure);
      }
    }
  }

  public static void stop(OHCache<?, ?> cache) {
    stop(cache, null);
  }

  public static void stop(
      OHCache<?, ?> cache,
      Throwable primaryFailure,
      java.util.concurrent.ExecutorService... callers) {
    if (cache == null) {
      return;
    }
    try {
      for (java.util.concurrent.ExecutorService caller : callers) {
        awaitCallers(caller);
      }
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
        throw new AssertionError("cache actor did not stop; quiesce callers first");
      }
      Field admissionField = OffHeapCache.class.getDeclaredField("logicalAdmission");
      admissionField.setAccessible(true);
      worker.assertLogicalAdmissionStable((LogicalAdmission) admissionField.get(cache));
      if (cache.totalAllocatedBytes() != 0L) {
        throw new AssertionError("cache retained native memory after stopping");
      }
    } catch (Throwable failure) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(failure);
      } else {
        throw new AssertionError("cache teardown failed", failure);
      }
    }
  }
}
