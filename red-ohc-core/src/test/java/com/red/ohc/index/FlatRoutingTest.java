package com.red.ohc.index;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public final class FlatRoutingTest {
    @Test
    public void routesHighAndLowHashBitsWithoutAdditionalMixing() {
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(48L, 0L);
            Entry[] entries = new Entry[33];
            for (int index = 0; index < entries.length; index++) {
                long hash = hashForGroupsZeroAndOne(index);
                byte[] key = Integer.toString(index).getBytes(StandardCharsets.US_ASCII);
                entries[index] = support.entry(key, hash);
                assertNull(map.putIfAbsent(entries[index], entries[index]));
            }

            assertEquals(map.snapshot().slotCapacity, 128);
            for (Entry entry : entries) assertSame(map.get(entry), entry);
        }
    }

    private static long hashForGroupsZeroAndOne(int value) {
        long low = (long) value << 2;
        long high = 1L | ((long) value << 2);
        return (high << 32) | low;
    }
}
