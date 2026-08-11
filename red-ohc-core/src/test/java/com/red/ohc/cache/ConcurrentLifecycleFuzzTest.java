package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;

/**
 * Exercises the public cache with a full logical-CPU mix of mapping changes and expiry. This is
 * deliberately a liveness/allocator regression: writes use the cache's non-blocking admission,
 * while remove/get are allowed to observe the cache's weakly-consistent state.
 */
public final class ConcurrentLifecycleFuzzTest {
  private static final int KEYS = 8_192;
  private static final int OPERATIONS_PER_THREAD = 1_000;
  /* Both native blocks round to the same 288B class, so every CPU writer needs only one page. */
  private static final CacheSerializer<Integer> KEY = fixedInteger(248);
  private static final CacheSerializer<Integer> VALUE = fixedInteger(240);

  @DataProvider(name = "allocators")
  public Object[][] allocators() {
    return new Object[][] {{AllocatorType.JNA}, {AllocatorType.UNSAFE}};
  }

  @Test(dataProvider = "allocators", timeOut = 30_000L)
  public void fullCpuPutRemoveAndTtlFuzzLeavesNoUnhealthyActorOrNativeLeak(AllocatorType allocator)
      throws Exception {
    OffHeapCache<Integer, Integer> cache =
        OHCacheBuilder.<Integer, Integer>newBuilder()
            // Full-CPU writers each keep partial 64KiB pages for the key and value classes.
            // This is deliberately large enough to exercise those real pages alongside the
            // fixed retirement ledger. Native hard-limit rejection is intentionally a
            // best-effort write result and is covered by the allocator-specific tests.
            .capacity(1L << 20)
            .expectedEntries(KEYS)
            .keySerializer(KEY)
            .valueSerializer(VALUE)
            .allocator(allocator)
            .eviction(Eviction.S3_FIFO)
            .buildTyped();
    int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
    ExecutorService callers = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<?>[] tasks = new Future<?>[threads];
      for (int thread = 0; thread < threads; thread++) {
        final int worker = thread;
        tasks[thread] =
            callers.submit(
                () -> {
                  await(start);
                  SplittableRandom random = new SplittableRandom(0x5eedL + worker);
                  for (int operation = 0; operation < OPERATIONS_PER_THREAD; operation++) {
                    int key = random.nextInt(KEYS);
                    switch (random.nextInt(4)) {
                      case 0:
                        putBestEffort(cache, key, random.nextInt(), 0L);
                        break;
                      case 1:
                        putBestEffort(
                            cache, key, random.nextInt(), System.currentTimeMillis() + 64L);
                        break;
                      case 2:
                        cache.remove(key);
                        break;
                      default:
                        cache.get(key);
                        break;
                    }
                  }
                });
      }
      start.countDown();
      for (Future<?> task : tasks) {
        task.get(20L, TimeUnit.SECONDS);
      }

      awaitQuiescence(cache);
      OHCacheStats stats = cache.stats();
      assertFalse(stats.getMaintenanceUnhealthy());
      assertEquals(stats.getMaintenanceQueueDepth(), 0L);
      assertEquals(stats.getRetirementQueueDepth(), 0L);
      assertTrue(
          stats.getLiveWeight() <= cache.capacity(),
          "liveWeight="
              + stats.getLiveWeight()
              + ", capacity="
              + cache.capacity()
              + ", evictions="
              + stats.getEvictionCount()
              + ", size="
              + cache.size()
              + ", queue="
              + stats.getMaintenanceQueueDepth()
              + ", retirement="
              + stats.getRetirementQueueDepth());
    } finally {
      callers.shutdownNow();
      cache.close();
      assertEquals(cache.totalAllocatedBytes(), 0L);
    }
  }

  @Test(dataProvider = "allocators", timeOut = 30_000L)
  public void fullCpuMixedReadAndConditionalWriteFuzzConverges(AllocatorType allocator)
      throws Exception {
    OffHeapCache<Integer, Integer> cache =
        OHCacheBuilder.<Integer, Integer>newBuilder()
            .capacity(1L << 20)
            .expectedEntries(KEYS)
            .keySerializer(KEY)
            .valueSerializer(VALUE)
            .allocator(allocator)
            .eviction(Eviction.S3_FIFO)
            .buildTyped();
    int threads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
    int operationsPerThread = 1_000;
    ExecutorService callers = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<?>[] tasks = new Future<?>[threads];
      for (int thread = 0; thread < threads; thread++) {
        final int worker = thread;
        tasks[thread] =
            callers.submit(
                () -> {
                  await(start);
                  SplittableRandom random = new SplittableRandom(0x6eedL + worker);
                  for (int operation = 0; operation < operationsPerThread; operation++) {
                    int key = random.nextInt(KEYS);
                    switch (random.nextInt(8)) {
                      case 0:
                        putBestEffort(cache, key, random.nextInt(), 0L);
                        break;
                      case 1:
                        putBestEffort(
                            cache, key, random.nextInt(), System.currentTimeMillis() + 64L);
                        break;
                      case 2:
                        cache.putIfAbsentAsync(key, random.nextInt(), 0L).join();
                        break;
                      case 3:
                        cache.replaceAsync(key, random.nextInt(), random.nextInt(), 0L).join();
                        break;
                      case 4:
                        cache.remove(key);
                        break;
                      case 5:
                        cache.get(key);
                        break;
                      case 6:
                        cache.containsKey(key);
                        break;
                      default:
                        cache.getDirect(key, value -> value.getLong(0));
                        break;
                    }
                  }
                });
      }
      start.countDown();
      for (Future<?> task : tasks) {
        task.get(20L, TimeUnit.SECONDS);
      }

      awaitQuiescence(cache);
      OHCacheStats stats = cache.stats();
      assertFalse(stats.getMaintenanceUnhealthy());
      assertEquals(stats.getMaintenanceQueueDepth(), 0L);
      assertEquals(stats.getRetirementQueueDepth(), 0L);
    } finally {
      callers.shutdownNow();
      cache.close();
      assertEquals(cache.totalAllocatedBytes(), 0L);
    }
  }

  private static void putBestEffort(
      OffHeapCache<Integer, Integer> cache, int key, int value, long expireAtMillis) {
    if (expireAtMillis == 0L) {
      cache.put(key, value);
    } else {
      cache.put(key, value, expireAtMillis);
    }
    // Concurrent stats snapshots must never traverse the actor-owned retirement compact set.
    OHCacheStats stats = cache.stats();
    if (stats.getMaintenanceUnhealthy()) {
      throw new AssertionError("maintenance actor became unhealthy during concurrent write");
    }
  }

  private static void awaitQuiescence(OffHeapCache<?, ?> cache) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
    while (System.nanoTime() < deadline) {
      cache.flushAsync().join();
      OHCacheStats stats = cache.stats();
      if (stats.getMaintenanceQueueDepth() == 0L && stats.getRetirementQueueDepth() == 0L) {
        return;
      }
      Thread.yield();
    }
    throw new AssertionError(
        "maintenance did not quiesce: queue="
            + cache.stats().getMaintenanceQueueDepth()
            + ", retirement="
            + cache.stats().getRetirementQueueDepth());
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static CacheSerializer<Integer> fixedInteger(int bytes) {
    return new CacheSerializer<Integer>() {
      @Override
      public void serialize(Integer value, ByteBuffer buffer) {
        buffer.putInt(value);
        for (int index = Integer.BYTES; index < bytes; index++) {
          buffer.put((byte) 0);
        }
      }

      @Override
      public Integer deserialize(ByteBuffer buffer) {
        return buffer.getInt();
      }

      @Override
      public int serializedSize(Integer value) {
        return bytes;
      }
    };
  }
}
