package com.red.ohc;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.red.ohc.CacheSerializer;
import com.red.ohc.EncodedKey;
import com.red.ohc.OHCacheBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class EncodedWriteTest {
    private static CacheSerializer<byte[]> trackingSerializer(AtomicInteger serializeCalls) {
        return new CacheSerializer<byte[]>() {
            @Override public void serialize(byte[] value, ByteBuffer buffer) {
                serializeCalls.incrementAndGet();
                buffer.put(value);
            }

            @Override public byte[] deserialize(ByteBuffer buffer) {
                byte[] copy = new byte[buffer.remaining()];
                buffer.get(copy);
                return copy;
            }

            @Override public int serializedSize(byte[] value) { return value.length; }
        };
    }

    private static final CacheSerializer<byte[]> FORBIDDEN_SERIALIZER = new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
            throw new AssertionError("encoded writes must not invoke a serializer");
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
            throw new AssertionError("not used by this test");
        }

        @Override
        public int serializedSize(byte[] value) {
            throw new AssertionError("encoded writes must not query a serializer");
        }
    };

    @Test
    public void encodedPutCopiesPreencodedValueWithoutInvokingSerializers() {
        AtomicInteger keySerializations = new AtomicInteger();
        AtomicInteger valueSerializations = new AtomicInteger();
        try (OffHeapCache<byte[], byte[]> cache = (OffHeapCache<byte[], byte[]>) OHCacheBuilder
                .<byte[], byte[]>newBuilder()
                .capacity(1 << 20)
                .keySerializer(trackingSerializer(keySerializations))
                .valueSerializer(trackingSerializer(valueSerializations))
                .build()) {
            EncodedKey key = EncodedKey.copyOf(new byte[] {1, 2, 3, 4});
            byte[] value = new byte[] {7, 8, 9, 10};

            assertTrue(cache.putEncoded(key, value));
            assertEquals(keySerializations.get(), 0);
            assertEquals(valueSerializations.get(), 0);
            value[0] = 99;
            cache.flushAsync().join();

            assertTrue(cache.getDirect(new byte[] {1, 2, 3, 4},
                    view -> assertEquals(view.getByte(0), (byte) 7)));
        }
    }

    @Test
    public void encodedSmallWritesReuseTheWriterArenaPage() {
        try (OffHeapCache<byte[], byte[]> cache = (OffHeapCache<byte[], byte[]>) OHCacheBuilder
                .<byte[], byte[]>newBuilder()
                .capacity(1 << 20)
                .keySerializer(FORBIDDEN_SERIALIZER)
                .valueSerializer(FORBIDDEN_SERIALIZER)
                .build()) {
            EncodedKey key = EncodedKey.copyOf(new byte[] {11, 12, 13, 14});
            long before = cache.entryNativeAllocations();

            assertTrue(cache.putEncoded(key, new byte[] {1, 2, 3, 4}));
            cache.flushAsync().join();
            assertTrue(cache.putEncoded(key, new byte[] {5, 6, 7, 8}));
            cache.flushAsync().join();

            assertEquals(cache.entryNativeAllocations() - before, 1L);
        }
    }

    @Test
    public void sameWeightPermanentReplacementDoesNotCreateAMaintenanceMutation() {
        AtomicInteger serializations = new AtomicInteger();
        try (OffHeapCache<byte[], byte[]> cache = (OffHeapCache<byte[], byte[]>) OHCacheBuilder
                .<byte[], byte[]>newBuilder()
                .capacity(1 << 20)
                .keySerializer(trackingSerializer(serializations))
                .valueSerializer(trackingSerializer(serializations))
                .build()) {
            EncodedKey key = EncodedKey.copyOf(new byte[] {21, 22, 23, 24});
            assertTrue(cache.putEncoded(key, new byte[] {1, 2, 3, 4}));
            cache.flushAsync().join();
            long applied = cache.stats().mutationApplied;

            assertTrue(cache.putEncoded(key, new byte[] {5, 6, 7, 8}));
            cache.flushAsync().join();

            assertEquals(cache.stats().mutationApplied, applied,
                    "same allocation weight and deadline leave actor-owned policy/timer state unchanged");
            assertTrue(cache.getDirect(new byte[] {21, 22, 23, 24},
                    view -> assertEquals(view.getByte(0), (byte) 5)));
        }
    }

    @Test(timeOut = 10_000L)
    public void pendingByteWatermarkKeepsAContinuousEncodedWriteStreamAccepted() {
        final int keys = 4_096;
        try (OffHeapCache<byte[], byte[]> cache = (OffHeapCache<byte[], byte[]>) OHCacheBuilder
                .<byte[], byte[]>newBuilder()
                .capacity((long) keys * (16 + 256) * 2L)
                .keySerializer(FORBIDDEN_SERIALIZER)
                .valueSerializer(FORBIDDEN_SERIALIZER)
                .build()) {
            EncodedKey[] encoded = new EncodedKey[keys];
            byte[] value = new byte[256];
            for (int i = 0; i < keys; i++) {
                byte[] key = new byte[16];
                key[0] = (byte) i;
                key[1] = (byte) (i >>> 8);
                encoded[i] = EncodedKey.copyOf(key);
            }
            for (int i = 0; i < 20_000; i += 64) {
                while (cache.mutationBacklogExceeds()) Thread.yield();
                for (int offset = 0; offset < 64; offset++) {
                    int write = i + offset;
                    if (!cache.putEncoded(encoded[write & (keys - 1)], value)) {
                        OHCacheStats stats = cache.stats();
                        fail("write " + write + " rejected: unhealthy=" + stats.maintenanceUnhealthy
                                + ", resident=" + stats.residentWeight + ", retired=" + stats.retiredWeight
                                + ", native=" + stats.nativeAllocatedBytes
                                + ", nativeLimit=" + cache.nativeHardLimitForTest());
                    }
                }
            }
            cache.flushAsync().join();
            OHCacheStats stats = cache.stats();
            assertTrue(!stats.maintenanceUnhealthy);
            assertTrue(stats.maintenanceAssistCount + stats.maintenanceWaitCount >= 0L);
        }
    }

}
