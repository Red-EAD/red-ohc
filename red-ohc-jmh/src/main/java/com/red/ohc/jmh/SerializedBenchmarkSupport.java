package com.red.ohc.jmh;

import java.nio.ByteBuffer;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.Serializer;

/** Shared raw-byte serialization helpers for the OHC and Ehcache JMH states. */
public final class SerializedBenchmarkSupport {
    public static final int WORKING_SET = 24_576;

    private SerializedBenchmarkSupport() {
    }

    public static EhcacheStore newEhcache(long offHeapBytes) {
        CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
                .withCache("serialized", CacheConfigurationBuilder
                        .newCacheConfigurationBuilder(byte[].class, byte[].class,
                                ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(offHeapBytes, MemoryUnit.B))
                        .withKeySerializer(new RawByteArraySerializer())
                        .withValueSerializer(new RawByteArraySerializer()))
                .build(true);
        return new EhcacheStore(manager, manager.getCache("serialized", byte[].class, byte[].class));
    }

    public static MapDbStore newMapDb() {
        DB db = DBMaker.memoryDirectDB().make();
        HTreeMap<byte[], byte[]> map = db.hashMap("serialized", org.mapdb.Serializer.BYTE_ARRAY,
                org.mapdb.Serializer.BYTE_ARRAY)
                .createOrOpen();
        return new MapDbStore(db, map);
    }

    public static ChronicleMapStore newChronicleMap(int keyBytes, int valueBytes, long entries) {
        ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(entries)
                .averageKeySize(keyBytes)
                .averageValueSize(valueBytes)
                .create();
        return new ChronicleMapStore(map);
    }

    public static Dataset dataset(int keyBytes, int valueBytes, String distribution) {
        byte[][] keys = new byte[WORKING_SET][];
        byte[][] values = new byte[WORKING_SET][];
        for (int i = 0; i < WORKING_SET; i++) {
            keys[i] = bytes(keyBytes, i);
            values[i] = bytes(valueBytes, i * 31 + 7);
        }
        return new Dataset(keys, values, "ZIPF_099".equals(distribution) ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET));
    }

    public static boolean isWrite(String workload, int operation) {
        return "READ_95_WRITE_5".equals(workload) && operation % 20 == 0;
    }

    public static long firstLong(byte[] value) {
        if (value == null) return 0L;
        return ((long) value[0] & 0xffL)
             | (((long) value[1] & 0xffL) << 8)
             | (((long) value[2] & 0xffL) << 16)
             | (((long) value[3] & 0xffL) << 24)
             | (((long) value[4] & 0xffL) << 32)
             | (((long) value[5] & 0xffL) << 40)
             | (((long) value[6] & 0xffL) << 48)
             | (((long) value[7] & 0xffL) << 56);
    }

    private static byte[] bytes(int length, int seed) {
        byte[] bytes = new byte[length];
        long value = seed * 0x9e3779b97f4a7c15L;
        for (int i = 0; i < length; i++) {
            value ^= value >>> 12;
            value ^= value << 25;
            value ^= value >>> 27;
            bytes[i] = (byte) value;
        }
        return bytes;
    }

    private static int[] uniformSequence(int bound) {
        int[] sequence = new int[1 << 16];
        long seed = 1L;
        for (int i = 0; i < sequence.length; i++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            sequence[i] = (int) Long.remainderUnsigned(seed, bound);
        }
        return sequence;
    }

    private static int[] zipfSequence(int bound) {
        double[] cdf = new double[bound];
        double sum = 0d;
        for (int rank = 1; rank <= bound; rank++) sum += 1d / Math.pow(rank, .99d);
        double running = 0d;
        for (int rank = 1; rank <= bound; rank++) {
            running += 1d / Math.pow(rank, .99d) / sum;
            cdf[rank - 1] = running;
        }
        int[] sequence = new int[1 << 16];
        long seed = 7L;
        for (int i = 0; i < sequence.length; i++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            double sample = (seed >>> 11) * 0x1.0p-53d;
            int low = 0;
            int high = cdf.length - 1;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (cdf[middle] < sample) low = middle + 1; else high = middle;
            }
            sequence[i] = low;
        }
        return sequence;
    }

    public static final class Dataset {
        public final byte[][] keys;
        public final byte[][] values;
        public final int[] accessSequence;

        private Dataset(byte[][] keys, byte[][] values, int[] accessSequence) {
            this.keys = keys;
            this.values = values;
            this.accessSequence = accessSequence;
        }
    }

    public static final class EhcacheStore implements AutoCloseable {
        private final CacheManager manager;
        private final Cache<byte[], byte[]> cache;

        private EhcacheStore(CacheManager manager, Cache<byte[], byte[]> cache) {
            this.manager = manager;
            this.cache = cache;
        }

        public Cache<byte[], byte[]> cache() {
            return cache;
        }

        @Override
        public void close() {
            manager.close();
        }
    }

    public static final class MapDbStore implements AutoCloseable {
        private final DB db;
        private final HTreeMap<byte[], byte[]> map;

        private MapDbStore(DB db, HTreeMap<byte[], byte[]> map) {
            this.db = db;
            this.map = map;
        }

        public HTreeMap<byte[], byte[]> map() {
            return map;
        }

        @Override
        public void close() {
            db.close();
        }
    }

    public static final class ChronicleMapStore implements AutoCloseable {
        private final ChronicleMap<byte[], byte[]> map;

        private ChronicleMapStore(ChronicleMap<byte[], byte[]> map) {
            this.map = map;
        }

        public ChronicleMap<byte[], byte[]> map() {
            return map;
        }

        @Override
        public void close() {
            map.close();
        }
    }

    public static final class RawByteArraySerializer implements Serializer<byte[]> {
        @Override
        public ByteBuffer serialize(byte[] value) {
            return ByteBuffer.wrap(value);
        }

        @Override
        public byte[] read(ByteBuffer binary) {
            ByteBuffer source = binary.duplicate();
            byte[] value = new byte[source.remaining()];
            source.get(value);
            return value;
        }

        @Override
        public boolean equals(byte[] value, ByteBuffer binary) {
            if (value.length != binary.remaining()) return false;
            int offset = binary.position();
            for (int i = 0; i < value.length; i++) {
                if (value[i] != binary.get(offset + i)) return false;
            }
            return true;
        }
    }
}
