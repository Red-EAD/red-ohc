package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;

public final class MaxSizeTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(StandardCharsets.UTF_8).length;
        }
      };

  @Test
  public void maxSizeAndCapacityAreMutuallyExclusive() {
    expectThrows(
        IllegalStateException.class,
        () ->
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .maxSize(2));
    expectThrows(
        IllegalStateException.class,
        () ->
            OHCacheBuilder.<String, String>newBuilder()
                .maxSize(2)
                .capacity(1 << 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> OHCacheBuilder.<String, String>newBuilder().maxSize(0));
  }

  @Test(dataProvider = "evictionStrategies", timeOut = 2_000L)
  public void maxSizeEvictsByEntryCountAfterMaintenanceFlush(Eviction eviction) {
    try (OHCache<String, String> cache = newMaxSizeCache(2, eviction)) {
      assertEquals(cache.capacity(), -1L);
      assertTrue(cache.put("one", "short"));
      assertTrue(cache.put("two", "a value with a different size"));
      assertTrue(cache.put("three", "third"));

      cache.flushAsync().join();

      assertEquals(cache.size(), 2L);
      assertTrue(cache.stats().liveWeight() > 0L);
    }
  }

  @org.testng.annotations.DataProvider(name = "evictionStrategies")
  public Object[][] evictionStrategies() {
    return new Object[][] {{Eviction.LRU}, {Eviction.W_TINY_LFU}, {Eviction.S3_FIFO}};
  }

  @Test
  public void replacingAnEntryDoesNotConsumeAnotherMaxSizeSlot() {
    try (OHCache<String, String> cache = newMaxSizeCache(2)) {
      assertTrue(cache.put("one", "one"));
      assertTrue(cache.put("two", "two"));
      assertTrue(cache.put("one", "a replacement with a larger value"));

      cache.flushAsync().join();

      assertEquals(cache.size(), 2L);
      assertEquals(cache.get("one"), "a replacement with a larger value");
    }
  }

  @Test
  public void maxSizeDoesNotEvictBasedOnValueBytes() {
    String small = "s";
    String medium = repeat('m', 8_192);
    String large = repeat('l', 16_384);
    try (OHCache<String, String> cache = newMaxSizeCache(3)) {
      assertPutEventually(cache, "small", small);
      assertPutEventually(cache, "medium", medium);
      assertPutEventually(cache, "large", large);

      cache.flushAsync().join();

      assertEquals(cache.size(), 3L);
      assertEquals(cache.get("small"), small);
      assertEquals(cache.get("medium"), medium);
      assertEquals(cache.get("large"), large);
      assertTrue(cache.stats().liveWeight() > medium.length());
    }
  }

  @Test
  public void ttlAndRemoveUpdateCountAndLiveBytesAfterFlush() {
    MutableTicker ticker = new MutableTicker();
    try (OHCache<String, String> cache = newMaxSizeCache(4, ticker)) {
      assertTrue(cache.put("expires", "value", 100L));
      assertTrue(cache.put("remove", "another value"));
      cache.flushAsync().join();
      long liveBeforeRemove = cache.stats().liveWeight();

      assertTrue(cache.remove("remove"));
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.stats().liveWeight() < liveBeforeRemove);

      ticker.now = 200L;
      cache.flushAsync().join();
      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().liveWeight(), 0L);
    }
  }

  @Test(timeOut = 10_000L)
  public void flushDrainsEveryDueTtlContinuationBeforeCompleting() {
    MutableTicker ticker = new MutableTicker();
    try (OHCache<String, String> cache = newMaxSizeCache(2_048, ticker)) {
      for (int index = 0; index < 1_025; index++) {
        assertTrue(cache.put("expires-" + index, "value", 64L));
      }
      cache.flushAsync().join();

      ticker.now = 128L;
      cache.flushAsync().join();

      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().expirationCount(), 1_025L);
    }
  }

  @Test(timeOut = 10_000L)
  public void resourceMaintenanceDrainsEveryDueTtlContinuation() throws Exception {
    MutableTicker ticker = new MutableTicker();
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>) newMaxSizeCache(2_048, ticker)) {
      for (int index = 0; index < 1_025; index++) {
        assertTrue(cache.put("expires-" + index, "value", 64L));
      }
      cache.flushAsync().join();

      ticker.now = 128L;
      worker(cache).requestMaintenance();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
      while (cache.stats().expirationCount() < 1_025L && System.nanoTime() < deadline) {
        Thread.yield();
      }

      assertEquals(cache.stats().expirationCount(), 1_025L);
      assertEquals(cache.size(), 0L);
    }
  }

  @Test(timeOut = 10_000L)
  public void resourceMaintenanceRetainsFutureTtl() throws Exception {
    MutableTicker ticker = new MutableTicker();
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>) newMaxSizeCache(2_048, ticker)) {
      assertTrue(cache.put("future", "value", 10_000L));
      cache.flushAsync().join();

      ticker.now = 128L;
      worker(cache).requestMaintenance();
      cache.flushAsync().join();

      assertEquals(cache.get("future"), "value");
      assertEquals(cache.size(), 1L);
      assertEquals(cache.stats().expirationCount(), 0L);
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentWritesEventuallyConvergeToMaxSize() throws Exception {
    ExecutorService writers = Executors.newFixedThreadPool(4);
    try (OHCache<String, String> cache = newMaxSizeCache(8)) {
      List<Future<?>> tasks = new ArrayList<>();
      for (int worker = 0; worker < 4; worker++) {
        final int workerId = worker;
        tasks.add(
            writers.submit(
                () -> {
                  for (int index = 0; index < 100; index++) {
                    cache.put(
                        "key-" + workerId + '-' + index,
                        repeat((char) ('a' + workerId), index + 1));
                  }
                }));
      }
      writers.shutdown();
      assertTrue(writers.awaitTermination(5L, TimeUnit.SECONDS));
      for (Future<?> task : tasks) {
        task.get();
      }
      cache.flushAsync().join();
      OHCacheStats stats = cache.stats();
      assertTrue(
          cache.size() <= 8L,
          "size="
              + cache.size()
              + ", queue="
              + stats.maintenanceQueueDepth()
              + ", liveWeight="
              + stats.liveWeight());
    } finally {
      writers.shutdownNow();
    }
  }

  @Test(timeOut = 10_000L)
  public void maxSizeRejectsNewKeysAtTheApproximateHighWatermark() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(1)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    pauseMaintenance(cache, paused, release);
    try {
      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("two", "value-two"));
      assertFalse(cache.put("three", "value-three"));
      assertEquals(cache.size(), 2L);
      assertTrue(cache.put("one", "replacement"));
      assertEquals(cache.get("one"), "replacement");
    } finally {
      release.countDown();
      try {
        cache.flushAsync().join();
        assertEquals(cache.size(), 1L);
      } finally {
        cache.close();
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void asyncAndLoaderAdmissionShareTheSynchronousHighWatermark() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(1)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .loaderExecutor(Runnable::run)
            .buildTyped();
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    pauseMaintenance(cache, paused, release);
    try {
      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("two", "value-two"));
      long nativeBytes = cache.totalAllocatedBytes();

      CompletableFuture<Boolean> asyncPut =
          cache.putIfAbsentAsync("three", "value-three", 0L);
      CompletableFuture<String> asyncLoad =
          cache.getOrLoadAsync("four", ignored -> "value-four", 0L);

      assertFalse(asyncPut.isDone());
      assertEquals(asyncLoad.get(2L, TimeUnit.SECONDS), "value-four");
      assertEquals(cache.size(), 2L);
      assertEquals(cache.totalAllocatedBytes(), nativeBytes);

      release.countDown();
      assertFalse(asyncPut.get(2L, TimeUnit.SECONDS));
      assertEquals(cache.size(), 2L);
      assertEquals(cache.totalAllocatedBytes(), nativeBytes);
    } finally {
      release.countDown();
      cache.close();
    }
  }

  @Test(timeOut = 2_000L)
  public void highWaterRejectionWakesTheIdleActorForAsyncRecovery() throws Exception {
    BlockingPassTicker ticker = new BlockingPassTicker();
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(1)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    MaintenanceEventLoop worker = worker(cache);
    Entry first = new Entry(0L, 0, 101, 0L);
    Entry second = new Entry(0L, 0, 102, 0L);
    try {
      waitUntilParked(worker);
      cache.dataForTest().put(first, first);
      cache.dataForTest().put(second, second);
      ticker.blockPass = true;
      assertFalse(cache.put("rejected", "value"));

      assertTrue(
          ticker.passStarted.await(1L, TimeUnit.SECONDS),
          "a rejected write must enter the next strictly scheduled maintenance pass");
    } finally {
      ticker.releasePass.countDown();
      cache.dataForTest().remove(first, first);
      cache.dataForTest().remove(second, second);
      cache.close();
    }
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize) {
    return newMaxSizeCache(maxSize, Eviction.S3_FIFO);
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize, Eviction eviction) {
    return OHCacheBuilder.<String, String>newBuilder()
        .maxSize(maxSize)
        .eviction(eviction)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static OHCache<String, String> newMaxSizeCache(long maxSize, Ticker ticker) {
    return OHCacheBuilder.<String, String>newBuilder()
        .maxSize(maxSize)
        .ticker(ticker)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static String repeat(char value, int count) {
    char[] result = new char[count];
    java.util.Arrays.fill(result, value);
    return new String(result);
  }

  private static void assertPutEventually(
      OHCache<String, String> cache, String key, String value) {
    for (int attempt = 0; attempt < 1_000; attempt++) {
      if (cache.put(key, value)) {
        return;
      }
      Thread.yield();
    }
    assertTrue(false, "put admission did not succeed for " + key);
  }

  private static void pauseMaintenance(
      OffHeapCache<?, ?> cache, CountDownLatch paused, CountDownLatch release) throws Exception {
    MaintenanceEventLoop worker = worker(cache);
    assertTrue(
        worker.submitAsyncMutation(
            () -> {
              paused.countDown();
              await(release);
            },
            failure -> {
              throw new AssertionError(failure);
            }));
    assertTrue(paused.await(2L, TimeUnit.SECONDS), "maintenance actor did not pause");
  }

  private static MaintenanceEventLoop worker(OffHeapCache<?, ?> cache) throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    return (MaintenanceEventLoop) workerField.get(cache);
  }

  private static void waitUntilParked(MaintenanceEventLoop worker) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
    while (!worker.isParked() && System.nanoTime() < deadline) {
      Thread.sleep(1L);
    }
    assertTrue(worker.isParked(), "maintenance actor did not park");
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static final class MutableTicker implements Ticker {
    volatile long now;

    @Override
    public long nanos() {
      return now * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return now;
    }
  }

  private static final class BlockingPassTicker implements Ticker {
    final CountDownLatch passStarted = new CountDownLatch(1);
    final CountDownLatch releasePass = new CountDownLatch(1);
    volatile boolean blockPass;

    @Override
    public long nanos() {
      if (blockPass) {
        passStarted.countDown();
        await(releasePass);
      }
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  }
}
