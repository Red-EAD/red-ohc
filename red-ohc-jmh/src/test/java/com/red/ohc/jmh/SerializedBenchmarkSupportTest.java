package com.red.ohc.jmh;

import org.ehcache.Cache;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializedBenchmarkSupportTest {
    @Test
    public void offHeapCacheRoundTripsTheRawByteCodecWithoutPersistence() {
        SerializedBenchmarkSupport.EhcacheStore store = SerializedBenchmarkSupport.newEhcache(1 << 20);
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
    public void mapDbDirectStoreRoundTripsTheRawByteCodecWithoutPersistence() {
        SerializedBenchmarkSupport.MapDbStore store = SerializedBenchmarkSupport.newMapDb();
        try {
            byte[] key = new byte[] {11, 12, 13, 14};
            byte[] value = new byte[] {15, 16, 17, 18};

            store.map().put(key, value);

            Assert.assertEquals(store.map().get(key), value);
        } finally {
            store.close();
        }
    }

    @Test
    public void chronicleMapInMemoryStoreRoundTripsTheRawByteCodecWithoutPersistence() {
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
