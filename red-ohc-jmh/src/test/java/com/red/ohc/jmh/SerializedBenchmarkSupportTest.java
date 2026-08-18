package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.ehcache.Cache;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class SerializedBenchmarkSupportTest {
  @Test
  public void defaultPolicyUsesTheTargetBenchmarkShape() {
    Assert.assertEquals(SerializedBenchmarkSupport.DEFAULT_KEY_BYTES, 32);
    Assert.assertEquals(SerializedBenchmarkSupport.DEFAULT_VALUE_BYTES, 5 * 1024);
    Assert.assertEquals(
        SerializedBenchmarkSupport.CAPACITY_ENTRIES,
        SerializedBenchmarkSupport.WORKING_SET * 4 / 5);
    Assert.assertEquals(
        SerializedBenchmarkSupport.TTL_MILLIS, TimeUnit.MINUTES.toMillis(10));
  }

  @Test
  public void writeScheduleSupportsNinetyTen() {
    int writes = 0;
    for (long operation = 1; operation <= 1_000; operation++) {
      if (SerializedBenchmarkSupport.isWrite("READ_90_WRITE_10", operation)) {
        writes++;
      }
    }

    Assert.assertEquals(writes, 100);
  }

  @Test
  public void writeScheduleSupportsEveryWrite() {
    for (long operation = 1; operation <= 1_000; operation++) {
      Assert.assertTrue(
          SerializedBenchmarkSupport.isWrite("WRITE_100", operation),
          "WRITE_100 must write operation " + operation);
    }
  }

  @Test
  public void readScheduleNeverWrites() {
    for (long operation = 1; operation <= 1_000; operation++) {
      Assert.assertFalse(SerializedBenchmarkSupport.isWrite("READ_100", operation));
    }
  }

  @Test
  public void threadOffsetsAvoidSameKeyStart() {
    int first = SerializedBenchmarkSupport.threadStartOffset(0, 4, 1 << 16);
    int second = SerializedBenchmarkSupport.threadStartOffset(1, 4, 1 << 16);

    Assert.assertNotEquals(first, second);
  }

  @Test
  public void ownedCopyHasIndependentStorage() {
    byte[] source = new byte[] {1, 2, 3, 4};
    byte[] copy = SerializedBenchmarkSupport.ownedCopy(source);

    Assert.assertEquals(copy, source);
    Assert.assertNotSame(copy, source);
    copy[0] = 9;
    Assert.assertEquals(source[0], 1);
  }

  @Test
  public void ownershipHelpersHandleNull() {
    Assert.assertNull(SerializedBenchmarkSupport.ownedCopy(null));
  }

  @Test
  public void caffeineAdapterCopiesOnWriteAndRead() {
    com.github.benmanes.caffeine.cache.Cache<SerializedBenchmarkSupport.RawKey, byte[]> cache =
        Caffeine.newBuilder().build();
    SerializedBenchmarkSupport.RawKey key =
        SerializedBenchmarkSupport.RawKey.copyOf(new byte[] {7, 8, 9});
    byte[] source = new byte[] {10, 11, 12, 13};
    byte[] expected = source.clone();

    CaffeineReadBenchmark.putOwned(cache, key, source);
    source[0] = 99;

    byte[] stored = cache.getIfPresent(key);
    Assert.assertEquals(stored, expected);
    Assert.assertNotSame(stored, source);

    byte[] firstRead = CaffeineReadBenchmark.getOwned(cache, key);
    Assert.assertEquals(firstRead, expected);
    Assert.assertNotSame(firstRead, stored);
    firstRead[0] = 88;

    byte[] secondRead = CaffeineReadBenchmark.getOwned(cache, key);
    Assert.assertEquals(secondRead, expected);
    Assert.assertNotSame(secondRead, firstRead);
  }

  @Test
  public void rawKeyAdapterUsesContentEqualityAndOwnsItsBytes() {
    byte[] source = new byte[] {7, 8, 9};
    SerializedBenchmarkSupport.RawKey first =
        SerializedBenchmarkSupport.RawKey.copyOf(source);
    SerializedBenchmarkSupport.RawKey second =
        SerializedBenchmarkSupport.RawKey.copyOf(new byte[] {7, 8, 9});

    Assert.assertEquals(first, second);
    Assert.assertEquals(first.hashCode(), second.hashCode());
    source[0] = 99;
    Assert.assertEquals(first, second);
  }

  @Test
  public void offHeapCacheRoundTripsTheRawByteCodecWithoutPersistence() {
    SerializedBenchmarkSupport.EhcacheStore store =
        SerializedBenchmarkSupport.newEhcache(
            1 << 20, SerializedBenchmarkSupport.TTL_MILLIS);
    try {
      byte[] key = new byte[] {1, 2, 3, 4};
      byte[] value = new byte[] {5, 6, 7, 8};
      Cache<byte[], byte[]> cache = store.cache();

      cache.put(key, value);
      byte[] result = cache.get(key);

      Assert.assertEquals(result, value);
      Assert.assertNotSame(result, value, "reads must materialize the serializer output");
    } finally {
      store.close();
    }
  }

  @Test
  public void ehcacheCapacityFitsTheTargetResidentSet() {
    SerializedBenchmarkSupport.Dataset dataset =
        SerializedBenchmarkSupport.dataset(32, 5120, "UNIFORM");
    SerializedBenchmarkSupport.EhcacheStore store =
        SerializedBenchmarkSupport.newEhcache(
            SerializedBenchmarkSupport.ehcacheCapacityBytes(32, 5120),
            SerializedBenchmarkSupport.TTL_MILLIS);
    try {
      Cache<byte[], byte[]> cache = store.cache();
      for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
        cache.put(dataset.keys[i], dataset.values[i]);
      }

      int resident = 0;
      for (Cache.Entry<byte[], byte[]> ignored : cache) {
        resident++;
      }
      Assert.assertEquals(resident, SerializedBenchmarkSupport.CAPACITY_ENTRIES);
    } finally {
      store.close();
    }
  }

  @Test
  public void mapDbDirectStoreRoundTripsTheRawByteCodecWithoutPersistence() {
    SerializedBenchmarkSupport.MapDbStore store =
        SerializedBenchmarkSupport.newMapDb(
            2, SerializedBenchmarkSupport.TTL_MILLIS);
    try {
      byte[] key = new byte[] {11, 12, 13, 14};
      byte[] value = new byte[] {15, 16, 17, 18};

      store.map().put(key, value);

      Assert.assertEquals(store.map().get(key), value);
      Assert.assertEquals(store.segmentCount(), SerializedBenchmarkSupport.MAPDB_SEGMENTS);
    } finally {
      store.close();
    }
  }

  @Test
  public void chronicleMapInMemoryStoreRoundTripsTheRawByteCodecWithoutPersistence() {
    if (Runtime.version().feature() >= 21) {
      throw new SkipException("Chronicle Map 3.27ea1 uses inaccessible JDK internals on JDK 21+");
    }
    SerializedBenchmarkSupport.ChronicleMapStore store =
        SerializedBenchmarkSupport.newChronicleMap(64, 128, 1_024);
    try {
      byte[] key = new byte[] {21, 22, 23, 24};
      byte[] value = new byte[] {25, 26, 27, 28};

      store.map().put(key, value);

      Assert.assertEquals(store.map().get(key), value);
    } finally {
      store.close();
    }
  }
}
