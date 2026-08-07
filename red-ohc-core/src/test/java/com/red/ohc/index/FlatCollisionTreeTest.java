package com.red.ohc.index;

import java.nio.charset.StandardCharsets;

import com.red.ohc.codec.LookupKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public final class FlatCollisionTreeTest {
    @Test
    public void controlled64BitHashFloodKeepsEveryKeyReachable() {
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(16L, 0L);
            Entry[] entries = new Entry[96];
            for (int index = 0; index < entries.length; index++) {
                entries[index] = support.entry(bytes(index), 7L);
                assertNull(map.putIfAbsent(entries[index], entries[index]));
            }
            assertTrue(map.snapshot().overflowSize > 0);
            assertEquals(map.size(), entries.length);
            for (int index = 0; index < entries.length; index++) assertSame(map.get(lookup(index)), entries[index]);
            for (int index = 0; index < entries.length; index += 3) assertTrue(map.remove(lookup(index), entries[index]));
            for (int index = 0; index < entries.length; index++) {
                if (index % 3 == 0) assertNull(map.get(lookup(index)));
                else assertSame(map.get(lookup(index)), entries[index]);
            }
        }
    }

    private static byte[] bytes(int value) { return Integer.toString(value).getBytes(StandardCharsets.US_ASCII); }
    private static LookupKey lookup(int value) {
        byte[] bytes = bytes(value);
        LookupKey lookup = new LookupKey();
        lookup.setPrecomputed(bytes, bytes.length, 7L);
        return lookup;
    }
}
