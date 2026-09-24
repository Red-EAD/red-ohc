package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class CapacityAccountingTest {
  private static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] value = new byte[buffer.remaining()];
          buffer.get(value);
          return value;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Test
  public void publicCapacityAndLiveWeightUseLogicalSerializedEntryBytes() {
    int keyLength = 32;
    int valueLength = 5 * 1024;
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long valueAllocation = ValueBlock.allocationLength(valueLength);
    long logicalEntryBytes = CacheMath.logicalEntryBytes(keyAllocation, valueAllocation);
    long physicalWeight =
        WriterArena.allocationWeight(keyAllocation) + WriterArena.allocationWeight(valueAllocation);

    assertEquals(logicalEntryBytes, 5_232L);
    assertEquals(physicalWeight, 5_824L);

    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(logicalEntryBytes)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    try {
      cache.put(new byte[keyLength], new byte[valueLength]);
      cache.flushAsync().join();

      assertEquals(cache.stats().liveWeight(), logicalEntryBytes);
      assertTrue(cache.stats().nativeAllocatedBytes() > logicalEntryBytes);
    } finally {
      cache.close();
    }
    assertEquals(cache.totalAllocatedBytes(), 0L);
  }

  @Test
  public void writerRemovalRefundsThePublishedLogicalCharge() throws Exception {
    int keyLength = 32;
    int valueLength = 128;
    byte[] key = new byte[keyLength];
    long logicalEntryBytes =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyLength),
            ValueBlock.allocationLength(valueLength));
    try (OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(logicalEntryBytes * 2L)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped()) {
      cache.put(key, new byte[valueLength]);
      cache.flushAsync().join();
      assertEquals(logicalAdmission(cache).logicalCharge(), logicalEntryBytes);

      assertTrue(cache.removeIfPresent(key));
      cache.flushAsync().join();

      assertEquals(logicalAdmission(cache).logicalCharge(), 0L);
      assertEquals(cache.size(), 0L);
    }
  }

  @Test
  public void capacityEvictsTheOldestEntryWhenLogicalBytesAreFull() {
    int keyLength = 32;
    int valueLength = 5 * 1024;
    long entryBytes =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyLength),
            ValueBlock.allocationLength(valueLength));
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(entryBytes)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(new byte[keyLength], new byte[valueLength]);

      byte[] secondKey = new byte[keyLength];
      secondKey[0] = 1;
      cache.put(secondKey, new byte[valueLength]);
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(secondKey) != null);
    }
  }

  @Test
  public void capacityEvictionIsAsynchronousTargetMaintenance() {
    int keyLength = 32;
    int valueLength = 5 * 1024;
    long entryBytes =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyLength),
            ValueBlock.allocationLength(valueLength));
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(entryBytes)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      byte[] firstKey = new byte[keyLength];
      byte[] secondKey = new byte[keyLength];
      secondKey[0] = 1;

      cache.put(firstKey, new byte[valueLength]);
      cache.put(secondKey, new byte[valueLength]);

      assertTrue(cache.size() >= 1L);
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(firstKey) == null);
      assertTrue(cache.get(secondKey) != null);
    }
  }

  @Test(timeOut = 10_000L)
  public void byteCapacityPutDoesNotWaitForTheMaintenanceActor() throws Exception {
    int keyLength = 32;
    int valueLength = 128;
    long entryBytes =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyLength),
            ValueBlock.allocationLength(valueLength));
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(entryBytes)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch releaseActor = new CountDownLatch(1);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      pauseMaintenance(cache, actorPaused, releaseActor);
      cache.put(new byte[keyLength], new byte[valueLength]);

      byte[] secondKey = new byte[keyLength];
      secondKey[0] = 1;
      Future<?> second = writer.submit(() -> cache.put(secondKey, new byte[valueLength]));

      second.get(1L, TimeUnit.SECONDS);
      assertEquals(cache.size(), 2L);
    } finally {
      releaseActor.countDown();
      writer.shutdownNow();
      cache.close();
    }
  }

  @Test
  public void oversizedEntryIsPublishedAndEvictedAfterFlush() {
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(80L)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(new byte[32], new byte[128]);
      cache.flushAsync().join();
      assertEquals(cache.size(), 0L);
    }
  }

  @Test
  public void logicalCapacityPressureEvictsASecondResidentEntry() {
    int keyLength = 32;
    int valueLength = 5 * 1024;
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long valueAllocation = ValueBlock.allocationLength(valueLength);
    long capacity = CacheMath.logicalEntryBytes(keyAllocation, valueAllocation);
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .eviction(com.red.ohc.api.Eviction.LRU)
            .build()) {
      cache.put(new byte[keyLength], new byte[valueLength]);

      byte[] secondKey = new byte[keyLength];
      secondKey[0] = 1;
      cache.put(secondKey, new byte[valueLength]);

      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(secondKey) != null);
    }
  }

  @Test
  public void largePutConvergesAfterMultipleResidentsExceedCapacity() {
    int keyLength = 32;
    int smallValueLength = 5 * 1024;
    int largeValueLength = 8 * 1024;
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long smallWeight =
        CacheMath.logicalEntryBytes(keyAllocation, ValueBlock.allocationLength(smallValueLength));
    long largeWeight =
        CacheMath.logicalEntryBytes(keyAllocation, ValueBlock.allocationLength(largeValueLength));
    long capacity = smallWeight * 2L;
    assertTrue(largeWeight < capacity);

    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .eviction(com.red.ohc.api.Eviction.LRU)
            .build()) {
      byte[] firstKey = new byte[keyLength];
      byte[] secondKey = new byte[keyLength];
      secondKey[0] = 1;
      byte[] thirdKey = new byte[keyLength];
      thirdKey[0] = 2;
      cache.put(firstKey, new byte[smallValueLength]);
      cache.put(secondKey, new byte[smallValueLength]);
      cache.put(thirdKey, new byte[largeValueLength]);

      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(thirdKey) != null);
    }
  }

  @Test(timeOut = 30_000L)
  public void largePutPublishesBeforeAsynchronousLogicalEviction() throws Exception {
    int keyLength = 32;
    int smallValueLength = 1;
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long smallWeight =
        CacheMath.logicalEntryBytes(keyAllocation, ValueBlock.allocationLength(smallValueLength));
    int residentCount = 3_073;
    long capacity = smallWeight * residentCount;
    int largeValueLength = (int) (capacity - keyAllocation - 16L);

    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .eviction(com.red.ohc.api.Eviction.LRU)
            .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    pauseMaintenance(cache, actorPaused, release);
    try {
      for (int index = 0; index < residentCount; index++) {
        byte[] key = new byte[keyLength];
        key[0] = (byte) (index >>> 8);
        key[1] = (byte) index;
        cache.put(key, new byte[smallValueLength]);
      }

      byte[] largeKey = new byte[keyLength];
      largeKey[0] = 13;
      release.countDown();
      cache.put(largeKey, new byte[largeValueLength]);

      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(largeKey) != null);
    } finally {
      release.countDown();
      cache.close();
    }
  }

  @Test(timeOut = 30_000L)
  public void largePutIfAbsentPublishesBeforeAsynchronousLogicalEviction() throws Exception {
    int keyLength = 32;
    int smallValueLength = 1;
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long smallWeight =
        CacheMath.logicalEntryBytes(keyAllocation, ValueBlock.allocationLength(smallValueLength));
    int residentCount = 3_073;
    long capacity = smallWeight * residentCount;
    int largeValueLength = (int) (capacity - keyAllocation - 16L);

    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .eviction(com.red.ohc.api.Eviction.LRU)
            .build()) {
      for (int index = 0; index < residentCount; index++) {
        byte[] key = new byte[keyLength];
        key[0] = (byte) (index >>> 8);
        key[1] = (byte) index;
        cache.put(key, new byte[smallValueLength]);
      }

      byte[] largeKey = new byte[keyLength];
      largeKey[0] = 100;
      assertTrue(cache.putIfAbsent(largeKey, new byte[largeValueLength], 0L) == null);
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertTrue(cache.get(largeKey) != null);
    }
  }

  @Test
  public void oversizedReplacementIsPublishedAndEvictedAfterFlush() {
    byte[] key = new byte[32];
    byte[] oldValue = new byte[1];
    byte[] largeValue = new byte[5 * 1024];
    long oldLogicalBytes =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(key.length), ValueBlock.allocationLength(1));
    long capacity = 256L;

    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(key, oldValue);
      cache.flushAsync().join();
      assertTrue(oldLogicalBytes < capacity);

      cache.put(key, largeValue);
      cache.flushAsync().join();
      assertEquals(cache.size(), 0L);
    }
  }

  @Test
  public void replacementRefreshesLogicalLiveWeightWithinOneAllocatorClass() {
    byte[] key = new byte[32];
    byte[] oldValue = new byte[1];
    byte[] newValue = new byte[32];
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(key.length);
    long oldAllocation = ValueBlock.allocationLength(oldValue.length);
    long newAllocation = ValueBlock.allocationLength(newValue.length);

    assertEquals(WriterArena.allocationWeight(oldAllocation), WriterArena.allocationWeight(newAllocation));

    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(512L)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(key, oldValue);
      cache.flushAsync().join();
      assertEquals(
          cache.stats().liveWeight(), CacheMath.logicalEntryBytes(keyAllocation, oldAllocation));

      cache.put(key, newValue);
      cache.flushAsync().join();
      assertEquals(
          cache.stats().liveWeight(), CacheMath.logicalEntryBytes(keyAllocation, newAllocation));
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
    assertTrue(
        paused.await(2L, java.util.concurrent.TimeUnit.SECONDS),
        "maintenance actor did not pause");
  }

  private static LogicalAdmission logicalAdmission(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("logicalAdmission");
    field.setAccessible(true);
    return (LogicalAdmission) field.get(cache);
  }
}
