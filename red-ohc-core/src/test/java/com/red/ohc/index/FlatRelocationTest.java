package com.red.ohc.index;

import java.nio.charset.StandardCharsets;

import com.red.ohc.codec.LookupKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public final class FlatRelocationTest {
    @Test
    public void movesOneResidentToItsAlternateGroupBeforeGrowingTheTable() {
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(48L, 0L);
            Entry[] residents = new Entry[32];
            for (int index = 0; index < 16; index++) {
                residents[index] = entryForGroups(support, index, 0, 2);
                assertNull(map.putIfAbsent(residents[index], residents[index]));
            }
            for (int index = 0; index < 16; index++) {
                residents[16 + index] = entryForGroups(support, 16 + index, 1, 3);
                assertNull(map.putIfAbsent(residents[16 + index], residents[16 + index]));
            }
            Entry candidate = entryForGroups(support, 100, 0, 1);
            assertNull(map.putIfAbsent(candidate, candidate));
            assertEquals(map.snapshot().slotCapacity, 64);
            for (Entry resident : residents) assertSame(map.get(resident), resident);
            assertSame(map.get(candidate), candidate);
        }
    }

    private static Entry entryForGroups(IndexTestSupport support, int id, int first, int second) {
        long hash = hashForGroups(first, second, id * 10_000L + 1L);
        return support.entry(Integer.toString(id).getBytes(StandardCharsets.US_ASCII), hash);
    }

    private static long hashForGroups(int first, int second, long start) {
        long low = (start << 2) | first;
        long high = (start << 2) | second;
        return (high << 32) | (low & 0xffffffffL);
    }
}
