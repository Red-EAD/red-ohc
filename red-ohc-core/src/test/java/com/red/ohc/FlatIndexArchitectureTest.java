package com.red.ohc;

import com.red.ohc.index.FlatConcurrentMap;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class FlatIndexArchitectureTest {
    private static final CacheSerializer<String> STRING = new CacheSerializer<String>() {
        @Override public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override public String deserialize(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override public int serializedSize(String value) {
            return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    };

    @Test
    public void cacheUsesTheFlatIndexAndPublishesPutAndRemoveSynchronously() {
        try (OffHeapCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .expectedEntries(64)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
            assertTrue(cache.indexForTest() instanceof FlatConcurrentMap);
            assertTrue(cache.put("key", "one"));
            assertEquals(cache.get("key"), "one");
            assertTrue(cache.put("key", "two"));
            assertEquals(cache.get("key"), "two");
            assertTrue(cache.remove("key"));
            assertFalse(cache.containsKey("key"));
        }
    }

    @Test(timeOut = 15_000L)
    public void cacheRemoveUsesTheCurrentEntryLocationAfterSeveralIndexResizes() {
        int entries = 192;
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .expectedEntries(16)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
            for (int index = 0; index < entries; index++) {
                assertTrue(cache.put("key-" + index, "value-" + index));
            }
            cache.flushAsync().join();
            assertTrue(cache.stats().indexSlotCapacity > 32L);

            for (int index = 0; index < entries; index += 2) {
                assertTrue(cache.remove("key-" + index));
            }
            cache.flushAsync().join();

            for (int index = 0; index < entries; index++) {
                if ((index & 1) == 0) assertNull(cache.get("key-" + index));
                else assertEquals(cache.get("key-" + index), "value-" + index);
            }
        }
    }
}
