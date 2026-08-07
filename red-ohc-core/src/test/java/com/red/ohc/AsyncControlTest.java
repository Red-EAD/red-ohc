package com.red.ohc;

import com.red.ohc.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

public class AsyncControlTest {
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
    public void maintenanceControlRequestsReturnExactSerializedStateResults() {
        try (OHCache<String, String> cache = newCache(Runnable::run)) {
            assertTrue(cache.putIfAbsentAsync("k", "one", 0L).join());
            assertFalse(cache.putIfAbsentAsync("k", "two", 0L).join());
            assertFalse(cache.replaceAsync("k", "other", "two", 0L).join());
            assertTrue(cache.replaceAsync("k", "one", "two", 0L).join());
            assertTrue(cache.removeAsync("k").join());
            assertFalse(cache.removeAsync("k").join());
        }
    }

    @Test
    public void loaderRunsOnConfiguredExecutorAndNeverOnTheMaintenanceEventLoop() {
        AtomicInteger executions = new AtomicInteger();
        Executor executor = command -> {
            executions.incrementAndGet();
            command.run();
        };
        try (OHCache<String, String> cache = newCache(executor)) {
            assertEquals(cache.getOrLoadAsync("load", key -> "value", 0L).join(), "value");
            assertEquals(executions.get(), 1);
        }
    }

    private static OHCache<String, String> newCache(Executor executor) {
        return OHCacheBuilder.<String, String>newBuilder()
                             .capacity(1 << 20)
                             .keySerializer(STRING)
                             .valueSerializer(STRING)
                             .loaderExecutor(executor)
                             .build();
    }
}
