package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.storage.ValueBlock;

public class MaintenanceAccountingTest {
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
  public void logicalExpiryRemovesTheLiveWeight() {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      cache.put("key", "value", 64L);
      cache.flushAsync().join();
      assertTrue(cache.stats().liveWeight() > 0L);
      long logicalEntryBytes =
          Entry.keyAllocationLengthForKeyLength(3) + ValueBlock.allocationLength(5);
      assertEquals(cache.stats().liveWeight(), logicalEntryBytes);
      assertTrue(cache.stats().nativeAllocatedBytes() > logicalEntryBytes);

      ticker.millis = 1_000L;
      cache.flushAsync().join();

      assertEquals(cache.size(), 0L);
      assertEquals(cache.stats().liveWeight(), 0L, "TTL removal must refund live weight");
      assertEquals(cache.stats().evictionCount(), 0L, "TTL removal is not a SIZE eviction");
      assertEquals(cache.stats().evictionWeight(), 0L, "TTL removal has no eviction weight");

      cache.put("manual", "value");
      cache.flushAsync().join();
      cache.remove("manual");
      cache.flushAsync().join();
      assertEquals(cache.stats().evictionCount(), 0L, "manual removal is not a SIZE eviction");
      assertEquals(cache.stats().evictionWeight(), 0L, "manual removal has no eviction weight");
    }
  }

  @Test
  public void liveWeightIsPublishedByTheMaintenanceActorNotByAWriterHotCounter() {
    for (Field field : OffHeapCache.class.getDeclaredFields()) {
      assertTrue(
          !(field.getType() == AtomicLong.class && field.getName().equals("liveEntryBytes")),
          "successful writes must not contend on a cache-wide live-entry AtomicLong");
    }
  }

  @Test(timeOut = 2_000L)
  public void scheduledTtlIsPhysicallyCleanedOnTheMaintenanceCadence() throws Exception {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("key", "value", 64L);
      cache.flushAsync().join();

      ticker.millis = 128L;
      Thread.sleep(130L);

      assertEquals(
          cache.get("key"), null, "strict reads must reject an idle cache's logically expired value");
      assertEquals(cache.size(), 0L, "scheduled TTL cleanup must converge without a later producer event");
    }
  }

  @Test(timeOut = 5_000L)
  public void residenceTimeCountsEveryValueVersionAtSecondResolutionWithoutFlush()
      throws InterruptedException {
    MutableTicker ticker = new MutableTicker(0L);
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("key", "old");

      ticker.millis = 7_000L;
      for (int index = 0; index < 1_024; index++) {
        cache.put("key", "new-" + index);
      }

      ticker.millis = 12_000L;
      cache.remove("key");
      assertTrue(!cache.containsKey("key"));

      OHCacheStats stats = awaitResidenceStats(cache, 1L, 0.0d);

      assertEquals(stats.residenceSampleCount(), 1L);
      assertEquals(stats.residenceSampleRate(), 1.0d / 1_024.0d, 0.000001);
      assertTrue(stats.sampledAverageResidenceTimeMillis() >= 0.0d);
    }
  }

  private static OHCacheStats awaitResidenceStats(
      OHCache<String, String> cache, long expectedCount, double expectedMillis)
      throws InterruptedException {
    long deadline = System.nanoTime() + 4_000_000_000L;
    OHCacheStats stats;
    do {
      stats = cache.stats();
      if (stats.residenceSampleCount() >= expectedCount
          && stats.sampledAverageResidenceTimeMillis() >= expectedMillis) {
        return stats;
      }
      Thread.sleep(10L);
    } while (System.nanoTime() < deadline);
    return stats;
  }

  @Test
  public void evictionPublishesThePolicyWeight() throws Exception {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(8_000L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      String value = "x".repeat(5 * 1024);
      cache.put("one", value);
      cache.flushAsync().join();
      Entry entry = cache.dataForTest().values().iterator().next();
      try {
        Field workerField = OffHeapCache.class.getDeclaredField("worker");
        workerField.setAccessible(true);
        Object worker = workerField.get(cache);
        java.lang.reflect.Method remove =
            worker
                .getClass()
                .getDeclaredMethod(
                    "removeFromMap", Entry.class, boolean.class, long.class, long.class, RemovalCause.class);
        remove.setAccessible(true);
        assertTrue(
            (Boolean)
                remove.invoke(worker, entry, true, entry.generation(), entry.valueAddress, RemovalCause.SIZE));
      } catch (ReflectiveOperationException failure) {
        throw new AssertionError("failed to trigger actor eviction", failure);
      }

      OHCacheStats stats = cache.stats();
      long expectedWeight =
          Entry.keyAllocationLengthForKeyLength(3) + ValueBlock.allocationLength(5 * 1024);
      assertEquals(stats.evictionCount(), 1L);
      assertEquals(
          stats.evictionWeight(), expectedWeight);
      assertEquals(logicalAdmission(cache).logicalCharge(), 0L);
    }
  }

  @Test(timeOut = 5_000L)
  public void capacityEvictionIsCountedAfterTheActorDrainsTheOverage() throws Exception {
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch releaseActor = new CountDownLatch(1);
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .maxSize(1)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      pauseMaintenance(cache, actorPaused, releaseActor);

      cache.put("first", "value");
      cache.put("second", "value");

      OHCacheStats stats = cache.stats();
      assertEquals(cache.size(), 2L, "writers may temporarily exceed the eviction target");
      assertEquals(stats.evictionCount(), 0L, "the paused actor cannot evict synchronously");

      releaseActor.countDown();
      cache.flushAsync().join();
      long expectedWeight =
          Entry.keyAllocationLengthForKeyLength("first".length())
              + ValueBlock.allocationLength("value".length());
      assertEquals(cache.size(), 1L);
      assertEquals(cache.stats().evictionCount(), 1L);
      assertEquals(cache.stats().evictionWeight(), expectedWeight);
    } finally {
      releaseActor.countDown();
      cache.close();
    }
  }

  private static void pauseMaintenance(
      OffHeapCache<?, ?> cache, CountDownLatch paused, CountDownLatch release) throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    MaintenanceEventLoop worker = (MaintenanceEventLoop) workerField.get(cache);
    assertTrue(
        worker.submitAsyncMutation(
            () -> {
              paused.countDown();
              try {
                release.await();
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
              }
            },
            failure -> {
              throw new AssertionError(failure);
            }));
    assertTrue(paused.await(2L, TimeUnit.SECONDS), "maintenance actor did not pause");
  }

  private static LogicalAdmission logicalAdmission(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("logicalAdmission");
    field.setAccessible(true);
    return (LogicalAdmission) field.get(cache);
  }

  private static final class MutableTicker implements Ticker {
    volatile long millis;

    MutableTicker(long millis) {
      this.millis = millis;
    }

    @Override
    public long nanos() {
      return millis * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return millis;
    }
  }

}
