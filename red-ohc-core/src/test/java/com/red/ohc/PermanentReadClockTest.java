package com.red.ohc;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class PermanentReadClockTest {
    private static final CacheSerializer<byte[]> BYTES = new CacheSerializer<byte[]>() {
        @Override public void serialize(byte[] value, ByteBuffer buffer) { buffer.put(value); }
        @Override public byte[] deserialize(ByteBuffer buffer) { byte[] copy = new byte[buffer.remaining()]; buffer.get(copy); return copy; }
        @Override public int serializedSize(byte[] value) { return value.length; }
    };

    @Test
    public void permanentDirectHitDoesNotReadTheBusinessThreadClock() {
        Thread reader = Thread.currentThread();
        AtomicInteger readerClockCalls = new AtomicInteger();
        Ticker ticker = new Ticker() {
            @Override public long nanos() { return System.nanoTime(); }
            @Override public long currentTimeMillis() {
                if (Thread.currentThread() == reader) readerClockCalls.incrementAndGet();
                return System.currentTimeMillis();
            }
        };
        byte[] key = {1, 2, 3, 4};
        try (OHCache<byte[], byte[]> cache = OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(1 << 20)
                .ticker(ticker)
                .keySerializer(BYTES)
                .valueSerializer(BYTES)
                .build()) {
            assertTrue(cache.put(key, new byte[16]));
            cache.flushAsync().join();
            EncodedKey encoded = EncodedKey.copyOf(key);
            readerClockCalls.set(0);
            assertTrue(cache.withDirectValue(encoded, value -> value.getLong(0)));
            assertEquals(readerClockCalls.get(), 0);
        }
    }

    @Test
    public void ttlReadUsesTheCurrentTickerEvenBeforeTheWorkerPhysicallyRemovesTheEntry() {
        AtomicInteger now = new AtomicInteger();
        Ticker ticker = new Ticker() {
            @Override public long nanos() { return now.get() * 1_000_000L; }
            @Override public long currentTimeMillis() { return now.get(); }
        };
        byte[] key = {9, 8, 7, 6};
        try (OHCache<byte[], byte[]> cache = OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(1 << 20)
                .ticker(ticker)
                .keySerializer(BYTES)
                .valueSerializer(BYTES)
                .build()) {
            assertTrue(cache.put(key, new byte[16], 64L));
            cache.flushAsync().join();
            now.set(64);
            assertTrue(!cache.withDirectValue(EncodedKey.copyOf(key), value -> value.getLong(0)));
        }
    }
}
