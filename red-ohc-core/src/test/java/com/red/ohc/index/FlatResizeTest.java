package com.red.ohc.index;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public final class FlatResizeTest {
    @Test
    public void growsIncrementallyWithoutLosingExistingMappings() {
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(16L, 0L);
            Entry[] entries = entries(support, 256);
            for (Entry entry : entries) assertNull(map.putIfAbsent(entry, entry));
            drainResize(map);
            assertEquals(map.size(), entries.length);
            assertFalse(map.snapshot().resizeInProgress);
            for (int index = 0; index < entries.length; index++) assertSame(map.get(lookup(index)), entries[index]);
        }
    }

    @Test(timeOut = 15_000L)
    public void concurrentWritersCompleteResizeWithoutLosingMappings() throws Exception {
        try (IndexTestSupport support = new IndexTestSupport()) {
            Entry[] entries = entries(support, 1024);
            FlatConcurrentMap map = new FlatConcurrentMap(16L, 0L);
            int writers = 8;
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch ready = new CountDownLatch(writers);
            CountDownLatch start = new CountDownLatch(1);
            try {
                java.util.concurrent.Future<?>[] futures = new java.util.concurrent.Future<?>[writers];
                for (int writer = 0; writer < writers; writer++) {
                    final int offset = writer;
                    futures[writer] = pool.submit(() -> {
                        ready.countDown();
                        start.await();
                        for (int index = offset; index < entries.length; index += writers) {
                            if (map.putIfAbsent(entries[index], entries[index]) != null) throw new AssertionError(index);
                        }
                        return null;
                    });
                }
                assertTrue(ready.await(2L, TimeUnit.SECONDS));
                start.countDown();
                for (java.util.concurrent.Future<?> future : futures) future.get(10L, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
            drainResize(map);
            assertEquals(map.size(), entries.length);
            for (int index = 0; index < entries.length; index++) assertSame(map.get(lookup(index)), entries[index]);
        }
    }

    private static Entry[] entries(IndexTestSupport support, int count) {
        Entry[] entries = new Entry[count];
        for (int index = 0; index < count; index++) entries[index] = support.entry(bytes(index));
        return entries;
    }

    private static com.red.ohc.codec.LookupKey lookup(int value) {
        byte[] bytes = bytes(value);
        com.red.ohc.codec.LookupKey lookup = new com.red.ohc.codec.LookupKey();
        lookup.set(bytes, bytes.length);
        return lookup;
    }

    private static byte[] bytes(int value) { return Integer.toString(value).getBytes(StandardCharsets.US_ASCII); }

    private static void drainResize(FlatConcurrentMap map) {
        int attempts = 0;
        while (map.snapshot().resizeInProgress) {
            map.helpResize(64);
            if (++attempts > 1_024 && map.snapshot().resizeInProgress) {
                FlatIndexStats stats = map.snapshot();
                throw new AssertionError("resize did not converge: cursor=" + stats.migrationCursor
                        + ", migrated=" + stats.migratedGroups + ", pending=" + stats.pendingMigrationGroups
                        + ", size=" + stats.size + ", slots=" + stats.slotCapacity);
            }
        }
    }
}
