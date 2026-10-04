package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        () -> OHCacheBuilder.<String, String>newBuilder().capacity(1 << 20).maxSize(2));
    expectThrows(
        IllegalStateException.class,
        () -> OHCacheBuilder.<String, String>newBuilder().maxSize(2).capacity(1 << 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> OHCacheBuilder.<String, String>newBuilder().maxSize(0));
  }

  @Test
  public void sizingModeMustBeConfiguredBeforeBuild() {
    expectThrows(
        IllegalStateException.class,
        () ->
            OHCacheBuilder.<String, String>newBuilder()
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build());
  }

  @Test(dataProvider = "evictionStrategies", timeOut = 5_000L)
  public void maxSizeEvictsAtTheEntryCountBoundary(Eviction eviction) {
    {
      OHCache<String, String> cache = newMaxSizeCache(2, eviction);
      Throwable cacheFailure9 = null;
      try {
        assertEquals(cache.capacity(), -1L);
        cache.put("one", "short");
        cache.put("two", "a value with a different size");
        cache.put("three", "third");

        cache.flushAsync().join();

        assertEquals(cache.size(), 2L);
        assertTrue(cache.stats().liveWeight() > 0L);
        assertEquals(cache.get("three"), "third");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure9 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure9);
      }
    }
  }

  @org.testng.annotations.DataProvider(name = "evictionStrategies")
  public Object[][] evictionStrategies() {
    return new Object[][] {{Eviction.LRU}, {Eviction.W_TINY_LFU}, {Eviction.S3_FIFO}};
  }

  @Test
  public void replacingAnEntryDoesNotConsumeAnotherMaxSizeSlot() {
    {
      OHCache<String, String> cache = newMaxSizeCache(2);
      Throwable cacheFailure8 = null;
      try {
        cache.put("one", "one");
        cache.put("two", "two");
        cache.put("one", "a replacement with a larger value");

        cache.flushAsync().join();

        assertEquals(cache.size(), 2L);
        assertEquals(cache.get("one"), "a replacement with a larger value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure8 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure8);
      }
    }
  }

  @Test
  public void maxSizeDoesNotEvictBasedOnValueBytes() {
    String small = "s";
    String medium = repeat('m', 8_192);
    String large = repeat('l', 16_384);
    {
      OHCache<String, String> cache = newMaxSizeCache(3);
      Throwable cacheFailure7 = null;
      try {
        assertPutEventually(cache, "small", small);
        assertPutEventually(cache, "medium", medium);
        assertPutEventually(cache, "large", large);

        cache.flushAsync().join();

        assertEquals(cache.size(), 3L);
        assertEquals(cache.get("small"), small);
        assertEquals(cache.get("medium"), medium);
        assertEquals(cache.get("large"), large);
        assertTrue(cache.stats().liveWeight() > medium.length());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure7 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure7);
      }
    }
  }

  @Test
  public void maxSizeUsesTheSharedNativePoolWithoutByteAdmission() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(8)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    {
      Throwable explicitCacheFailure7 = null;
      try {

        cache.put("one", "value");
        Map<String, String> batch = new LinkedHashMap<>();
        batch.put("batch", "batch-value");
        cache.putAll(batch);
        assertTrue(cache.putIfAbsent("async", "async-value", 0L) == null);
        assertTrue(cache.replace("one", "value", "replacement", 0L));

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure7 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        CacheTestSupport.stop(cache, explicitCacheFailure7);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void synchronousReplaceUsesTheSharedNativePoolPath() throws Exception {
    OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(1);
    {
      Throwable explicitCacheFailure6 = null;
      try {

        cache.put("one", "old");
        assertTrue(cache.replace("one", "old", "new", 0L));
        assertEquals(cache.get("one"), "new");

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure6 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        CacheTestSupport.stop(cache, explicitCacheFailure6);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void replacementDoesNotWaitForTheMaintenanceActor() throws Exception {
    OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(1);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    {
      Throwable explicitCacheFailure5 = null;
      try {
        pauseMaintenance(cache, paused, release);

        cache.put("one", "value-one");

        Future<?> replacement = writer.submit(() -> cache.put("one", "replacement"));
        replacement.get(1L, TimeUnit.SECONDS);
        assertEquals(cache.get("one"), "replacement");

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure5 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure5, writer);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void newKeyPutDoesNotWaitForTheMaintenanceActorWhenTargetIsFull() throws Exception {
    OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(1);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    {
      Throwable explicitCacheFailure4 = null;
      try {
        pauseMaintenance(cache, paused, release);

        cache.put("one", "value-one");
        Future<?> second = writer.submit(() -> cache.put("two", "value-two"));

        second.get(1L, TimeUnit.SECONDS);
        assertEquals(cache.size(), 2L);

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure4 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure4, writer);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void putAllDoesNotWaitForTheMaintenanceActor() throws Exception {
    OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(1);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    {
      Throwable explicitCacheFailure3 = null;
      try {
        pauseMaintenance(cache, paused, release);

        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("one", "value-one");
        entries.put("two", "value-two");
        entries.put("three", "value-three");

        Future<?> putAll = writer.submit(() -> cache.putAll(entries));
        putAll.get(1L, TimeUnit.SECONDS);
        assertEquals(cache.size(), 3L);
        release.countDown();
        cache.flushAsync().join();

        assertEquals(cache.size(), 1L);
        assertEquals(cache.get("three"), "value-three");

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure3 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure3, writer);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void newKeyPutDoesNotWaitForTheMaintenanceActor() throws Exception {
    OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(1);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    {
      Throwable explicitCacheFailure2 = null;
      try {
        pauseMaintenance(cache, paused, release);

        cache.put("first", "value");
        Future<?> second = writer.submit(() -> cache.put("second", "value"));
        second.get(1L, TimeUnit.SECONDS);
        assertEquals(cache.size(), 2L);
        release.countDown();
        cache.flushAsync().join();
        assertEquals(cache.get("second"), "value");
        assertEquals(cache.size(), 1L);

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure2 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure2, writer);
      }
    }
  }

  @Test
  public void ttlAndRemoveUpdateCountAndLiveBytesAfterFlush() {
    MutableTicker ticker = new MutableTicker();
    {
      OHCache<String, String> cache = newMaxSizeCache(4, ticker);
      Throwable cacheFailure6 = null;
      try {
        cache.put("expires", "value", 100L);
        cache.put("remove", "another value");
        cache.flushAsync().join();
        long liveBeforeRemove = cache.stats().liveWeight();

        cache.remove("remove");
        assertTrue(!cache.containsKey("remove"));
        cache.flushAsync().join();
        assertEquals(cache.size(), 1L);
        assertTrue(cache.stats().liveWeight() < liveBeforeRemove);

        ticker.now = 200L;
        cache.flushAsync().join();
        assertEquals(
            cache.size(),
            0L,
            "ttl flush did not remove the expired entry: expirationCount="
                + cache.stats().expirationCount()
                + ", ttlBacklog="
                + cache.stats().ttlBacklog()
                + ", queueDepth="
                + cache.stats().lifecycleJournalLagRecords()
                + ", liveWeight="
                + cache.stats().liveWeight());
        assertEquals(cache.stats().liveWeight(), 0L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure6 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure6);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void flushDrainsEveryDueTtlContinuationBeforeCompleting() {
    MutableTicker ticker = new MutableTicker();
    {
      OHCache<String, String> cache = newMaxSizeCache(2_048, ticker);
      Throwable cacheFailure5 = null;
      try {
        for (int index = 0; index < 1_025; index++) {
          cache.put("expires-" + index, "value", 64L);
        }
        cache.flushAsync().join();

        ticker.now = 128L;
        cache.flushAsync().join();

        assertEquals(cache.size(), 0L);
        assertEquals(cache.stats().expirationCount(), 1_025L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure5 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure5);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void resourceMaintenanceDrainsEveryDueTtlContinuation() throws Exception {
    MutableTicker ticker = new MutableTicker();
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>) newMaxSizeCache(2_048, ticker);
      Throwable cacheFailure4 = null;
      try {
        for (int index = 0; index < 1_025; index++) {
          cache.put("expires-" + index, "value", 64L);
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure4 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure4);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void resourceMaintenanceRetainsFutureTtl() throws Exception {
    MutableTicker ticker = new MutableTicker();
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>) newMaxSizeCache(2_048, ticker);
      Throwable cacheFailure3 = null;
      try {
        cache.put("future", "value", 10_000L);
        cache.flushAsync().join();

        ticker.now = 128L;
        worker(cache).requestMaintenance();
        cache.flushAsync().join();

        assertEquals(cache.get("future"), "value");
        assertEquals(cache.size(), 1L);
        assertEquals(cache.stats().expirationCount(), 0L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
    }
  }

  @Test(timeOut = 20_000L)
  public void concurrentWritesEventuallyConvergeToMaxSize() throws Exception {
    ExecutorService writers = Executors.newFixedThreadPool(4);
    try {
      OHCache<String, String> cache = newMaxSizeCache(8);
      Throwable cacheFailure2 = null;
      try {
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
        assertTrue(writers.awaitTermination(15L, TimeUnit.SECONDS));
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
                + stats.lifecycleJournalLagRecords()
                + ", liveWeight="
                + stats.liveWeight());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2, writers);
      }
    } finally {
      CacheTestSupport.awaitCallers(writers);
    }
  }

  @Test(timeOut = 20_000L)
  public void concurrentInsertChurnDoesNotRejectWhenVictimsAreAvailable() throws Exception {
    int writerCount = 8;
    int operationsPerWriter = 256;
    ExecutorService writers = Executors.newFixedThreadPool(writerCount);
    CountDownLatch start = new CountDownLatch(1);
    try {
      OffHeapCache<String, String> cache = (OffHeapCache<String, String>) newMaxSizeCache(64);
      Throwable cacheFailure1 = null;
      try {
        List<Future<?>> tasks = new ArrayList<>();
        for (int worker = 0; worker < writerCount; worker++) {
          final int workerId = worker;
          tasks.add(
              writers.submit(
                  () -> {
                    start.await();
                    for (int index = 0; index < operationsPerWriter; index++) {
                      cache.put("churn-" + workerId + '-' + index, "value");
                    }
                    return null;
                  }));
        }
        start.countDown();
        for (Future<?> task : tasks) {
          task.get();
        }
        cache.flushAsync().join();
        assertTrue(cache.size() <= 64L, "size=" + cache.size());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1, writers);
      }
    } finally {
      CacheTestSupport.awaitCallers(writers);
    }
  }

  @Test(timeOut = 10_000L)
  public void maxSizeWritesDoNotWaitForLogicalEvictionOrPhysicalReclaim() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(1)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    {
      Throwable explicitCacheFailure1 = null;
      try {
        pauseMaintenance(cache, paused, release);

        cache.put("one", "value-one");

        Future<?> second = writer.submit(() -> cache.put("two", "value-two"));
        second.get(1L, TimeUnit.SECONDS);
        assertEquals(cache.size(), 2L);
        release.countDown();

        cache.flushAsync().join();
        assertEquals(cache.size(), 1L);
        assertEquals(cache.get("two"), "value-two");

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure1 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure1, writer);
      }
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

  private static void assertPutEventually(OHCache<String, String> cache, String key, String value) {
    for (int attempt = 0; attempt < 1_000; attempt++) {
      cache.put(key, value);
      return;
    }
    assertTrue(false, "put admission did not succeed for " + key);
  }

  private static void pauseMaintenance(
      OffHeapCache<?, ?> cache, CountDownLatch paused, CountDownLatch release) throws Exception {
    MaintenanceEventLoop worker = worker(cache);
    assertTrue(
        worker.submitActorTaskForTest(
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
}
