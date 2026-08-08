package com.red.ohc;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

public class WriteAdmissionTest {
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
    public void closedPutThrowsInsteadOfReturningResourceRejection() {
        OffHeapCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
        cache.close();
        expectThrows(IllegalStateException.class, () -> cache.put("key", "value"));
    }

    @Test
    public void oversizePutThrowsInsteadOfReturningFalse() {
        try (OffHeapCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .maxEntrySize(16)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
            expectThrows(IllegalArgumentException.class,
                    () -> cache.put("this-key-is-too-large", "value"));
        }
    }

    @Test(timeOut = 10_000L)
    public void writerAssistEventuallyAdmitsReplacementAfterReaderQuiesces() throws Exception {
        OffHeapCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                .capacity(8L << 20)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.ExecutorService readers = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<Boolean> direct = null;
        try {
            assertTrue(cache.put("key", "initial"));
            cache.flushAsync().join();
            direct = readers.submit(() -> cache.withDirectValue("key", view -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                view.getByte(0);
            }));
            assertTrue(entered.await(2L, java.util.concurrent.TimeUnit.SECONDS));

            java.util.concurrent.Future<Boolean> writer = readers.submit(() -> {
                boolean accepted = true;
                for (int i = 0; i < 20_000; i++) {
                    accepted &= cache.put("key", "value-" + i);
                }
                return accepted;
            });
            Thread.sleep(100L);
            release.countDown();
            assertTrue(writer.get(8L, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(direct.get(2L, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            release.countDown();
            if (direct != null) direct.cancel(true);
            readers.shutdownNow();
            cache.close();
        }
    }
}
