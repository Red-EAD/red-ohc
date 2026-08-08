package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void putAllKeepsWriterAdmissionAcrossEach512EntryBatch() throws Exception {
        try (OffHeapCache<String, String> cache = (OffHeapCache<String, String>) OHCacheBuilder
                .<String, String>newBuilder()
                .capacity(1 << 22)
                .expectedEntries(1_024)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int index = 0; index < 513; index++) {
                values.put("writer-key-" + index, "writer-value-" + index);
            }
            com.red.ohc.runtime.ThreadContext context = threadContext(cache);
            ArrayList<Boolean> writerStates = new ArrayList<>();
            Map<String, String> observed = new AbstractMap<String, String>() {
                @Override
                public Set<Entry<String, String>> entrySet() {
                    return new AbstractSet<Entry<String, String>>() {
                        @Override
                        public Iterator<Entry<String, String>> iterator() {
                            Iterator<Entry<String, String>> delegate = values.entrySet().iterator();
                            return new Iterator<Entry<String, String>>() {
                                @Override public boolean hasNext() { return delegate.hasNext(); }
                                @Override public Entry<String, String> next() {
                                    writerStates.add(context.slot.writerActive);
                                    return delegate.next();
                                }
                                @Override public void remove() { delegate.remove(); }
                            };
                        }

                        @Override public int size() { return values.size(); }
                    };
                }
            };

            assertEquals(cache.putAll(observed), values.size());
            assertEquals(writerStates.size(), values.size());
            for (Boolean writerActive : writerStates) {
                assertTrue(writerActive, "putAll must keep writer admission for the active 512-entry batch");
            }
        }
    }

    @Test
    public void putAllKeepsCompletedWritesAndReleasesWriterAfterSerializerFailure() {
        CacheSerializer<String> failingValueSerializer = new CacheSerializer<String>() {
            @Override public void serialize(String value, ByteBuffer buffer) {
                if ("boom".equals(value)) throw new IllegalStateException("serializer failure");
                STRING.serialize(value, buffer);
            }
            @Override public String deserialize(ByteBuffer buffer) { return STRING.deserialize(buffer); }
            @Override public int serializedSize(String value) { return STRING.serializedSize(value); }
        };
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(failingValueSerializer)
                .build()) {
            Map<String, String> values = new LinkedHashMap<>();
            values.put("before", "kept");
            values.put("failure", "boom");
            values.put("after", "not-written");

            try {
                cache.putAll(values);
                throw new AssertionError("putAll must propagate serializer failure");
            } catch (IllegalStateException expected) {
                // The completed prefix remains visible and the writer admission is released below.
            }
            assertEquals(cache.get("before"), "kept");
            assertTrue(cache.put("after-failure", "writer-released"));
            assertEquals(cache.get("after-failure"), "writer-released");
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
    public void bulkReadStartsDeserializingAfterTheFirst512KeyBatch() {
        AtomicInteger encodedKeys = new AtomicInteger();
        AtomicInteger deserializations = new AtomicInteger();
        AtomicInteger firstDeserializeAfterKeys = new AtomicInteger();
        CacheSerializer<String> countingKeySerializer = new CacheSerializer<String>() {
            @Override public void serialize(String value, ByteBuffer buffer) {
                encodedKeys.incrementAndGet();
                STRING.serialize(value, buffer);
            }
            @Override public String deserialize(ByteBuffer buffer) { return STRING.deserialize(buffer); }
            @Override public int serializedSize(String value) { return STRING.serializedSize(value); }
        };
        CacheSerializer<String> observingValueSerializer = new CacheSerializer<String>() {
            @Override public void serialize(String value, ByteBuffer buffer) { STRING.serialize(value, buffer); }
            @Override public String deserialize(ByteBuffer buffer) {
                if (deserializations.getAndIncrement() == 0) {
                    firstDeserializeAfterKeys.set(encodedKeys.get());
                }
                return STRING.deserialize(buffer);
            }
            @Override public int serializedSize(String value) { return STRING.serializedSize(value); }
        };
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 23)
                .expectedEntries(1_024)
                .keySerializer(countingKeySerializer)
                .valueSerializer(observingValueSerializer)
                .build()) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int index = 0; index < 513; index++) {
                values.put("read-key-" + index, "read-value-" + index);
            }
            assertEquals(cache.putAll(values), values.size());
            cache.flushAsync().join();
            encodedKeys.set(0);

            assertEquals(cache.getAll(values.keySet()).size(), values.size());
            assertEquals(firstDeserializeAfterKeys.get(), 512,
                    "getAll must deserialize a completed reader batch before scanning the next batch");
        }
    }

    @Test
    public void bulkReadSkipsTheTemporaryDedupSetForSetInput() {
        AtomicInteger hashCalls = new AtomicInteger();
        CacheSerializer<CountingKey> keySerializer = new CacheSerializer<CountingKey>() {
            @Override public void serialize(CountingKey value, ByteBuffer buffer) { buffer.put(value.bytes); }
            @Override public CountingKey deserialize(ByteBuffer buffer) { throw new UnsupportedOperationException(); }
            @Override public int serializedSize(CountingKey value) { return value.bytes.length; }
        };
        CountingKey first = new CountingKey("set-key-1", hashCalls);
        CountingKey second = new CountingKey("set-key-2", hashCalls);
        Set<CountingKey> keys = new java.util.LinkedHashSet<>(Arrays.asList(first, second));
        hashCalls.set(0);
        try (OHCache<CountingKey, String> cache = OHCacheBuilder.<CountingKey, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(keySerializer)
                .valueSerializer(STRING)
                .build()) {
            assertTrue(cache.put(first, "set-value-1"));
            assertTrue(cache.put(second, "set-value-2"));
            assertEquals(cache.getAll(keys).size(), keys.size());
            assertEquals(hashCalls.get(), keys.size(),
                    "only the final result HashMap should hash Set keys during getAll");
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

    private static final class CountingKey {
        private final byte[] bytes;
        private final AtomicInteger hashCalls;

        private CountingKey(String value, AtomicInteger hashCalls) {
            this.bytes = value.getBytes(StandardCharsets.UTF_8);
            this.hashCalls = hashCalls;
        }

        @Override
        public int hashCode() {
            hashCalls.incrementAndGet();
            return Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CountingKey && Arrays.equals(bytes, ((CountingKey) other).bytes);
        }
    }
}
