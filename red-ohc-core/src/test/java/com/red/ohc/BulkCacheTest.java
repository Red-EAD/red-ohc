package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

public class BulkCacheTest {
    private static final CacheSerializer<String> STRING = new CacheSerializer<String>() {
        @Override public void serialize(String value, ByteBuffer buffer) { buffer.put(value.getBytes(StandardCharsets.UTF_8)); }
        @Override public String deserialize(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        @Override public int serializedSize(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }
    };

    @Test
    public void bulkCommandsApplyWithoutCrossKeyAtomicity() {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .capacity(1 << 20)
                                                           .keySerializer(STRING)
                                                           .valueSerializer(STRING)
                                                           .build()) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < 64; i++) values.put("key-" + i, "value-" + i);

            assertEquals(cache.putAll(values), values.size());
            cache.flushAsync().join();
            Map<String, String> fetched = getAllEventually(cache, values.keySet());
            assertTrue(fetched instanceof HashMap);
            assertEquals(fetched, values);

            assertEquals(cache.removeAll(Arrays.asList("key-0", "key-7", "key-63")), 3);
            cache.flushAsync().join();
            assertEquals(getEventually(cache, "key-0"), null);
            assertEquals(getEventually(cache, "key-7"), null);
            assertEquals(getEventually(cache, "key-63"), null);
        }
    }

    @Test
    public void bulkReadUsesAStandardHashMapAcrossMultipleReaderEpochBatches() {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 22)
                .expectedEntries(2_048)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int index = 0; index < 1_025; index++) values.put("bulk-" + index, "value-" + index);
            assertEquals(cache.putAll(values), values.size());
            cache.flushAsync().join();
            ArrayList<String> duplicateInput = new ArrayList<>(values.keySet());
            duplicateInput.addAll(values.keySet());
            Map<String, String> fetched = cache.getAll(duplicateInput);
            assertTrue(fetched instanceof HashMap);
            assertEquals(fetched, values);
        }
    }

    @Test
    public void bulkDeserializationRunsAfterTheReaderEpochIsReleased() throws Exception {
        AtomicReference<com.red.ohc.runtime.ThreadContext> context = new AtomicReference<>();
        AtomicBoolean deserializedOutsideEpoch = new AtomicBoolean();
        CacheSerializer<String> checkingValueSerializer = new CacheSerializer<String>() {
            @Override public void serialize(String value, ByteBuffer buffer) { STRING.serialize(value, buffer); }
            @Override public String deserialize(ByteBuffer buffer) {
                deserializedOutsideEpoch.set(context.get().slot.epoch == 0L);
                return STRING.deserialize(buffer);
            }
            @Override public int serializedSize(String value) { return STRING.serializedSize(value); }
        };
        try (OffHeapCache<String, String> cache = (OffHeapCache<String, String>) OHCacheBuilder
                .<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(checkingValueSerializer)
                .build()) {
            assertTrue(cache.put("key", "value"));
            cache.flushAsync().join();
            context.set(threadContext(cache));

            assertEquals(cache.getAll(Arrays.asList("key")).get("key"), "value");
            assertTrue(deserializedOutsideEpoch.get(),
                    "getAll must deserialize only after it releases its QSBR reader epoch");
        }
    }

    @SuppressWarnings("unchecked")
    private static com.red.ohc.runtime.ThreadContext threadContext(OffHeapCache<?, ?> cache) throws Exception {
        Field field = OffHeapCache.class.getDeclaredField("contexts");
        field.setAccessible(true);
        return ((ThreadLocal<com.red.ohc.runtime.ThreadContext>) field.get(cache)).get();
    }

    private static Map<String, String> getAllEventually(OHCache<String, String> cache, java.util.Collection<String> keys) {
        for (int i = 0; i < 200; i++) {
            Map<String, String> values = cache.getAll(keys);
            if (values.size() == keys.size()) return values;
            Thread.yield();
        }
        return cache.getAll(keys);
    }

    private static String getEventually(OHCache<String, String> cache, String key) {
        for (int i = 0; i < 200; i++) {
            String value = cache.get(key);
            if (value != null) return value;
            Thread.yield();
        }
        return null;
    }
}
