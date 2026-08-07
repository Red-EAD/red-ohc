package com.red.ohc.index;

import java.nio.charset.StandardCharsets;

import com.red.ohc.EncodedKey;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public final class FlatConcurrentMapTest {
    @Test
    public void directLookupAndConditionalRemoveKeepOneWinner() {
        byte[] bytes = "first".getBytes(StandardCharsets.US_ASCII);
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(32L, 0L);
            Entry first = support.entry(bytes);
            Entry duplicate = support.entry(bytes);
            LookupKey lookup = support.lookup(bytes);

            assertNull(map.get(lookup));
            assertNull(map.putIfAbsent(first, first));
            assertSame(map.get(lookup), first);
            assertSame(map.putIfAbsent(duplicate, duplicate), first);
            assertFalse(map.remove(lookup, duplicate));
            assertTrue(map.remove(lookup, first));
            assertNull(map.get(lookup));
        }
    }

    @Test
    public void sameHashAndFingerprintStillRequireTheExactNativeKey() {
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(32L, 0L);
            Entry first = support.entry("one".getBytes(StandardCharsets.US_ASCII), 7L);
            Entry second = support.entry("two".getBytes(StandardCharsets.US_ASCII), 7L);
            assertNull(map.putIfAbsent(first, first));
            assertNull(map.putIfAbsent(second, second));

            LookupKey one = new LookupKey();
            byte[] oneBytes = "one".getBytes(StandardCharsets.US_ASCII);
            one.setPrecomputed(oneBytes, oneBytes.length, 7L);
            LookupKey two = new LookupKey();
            byte[] twoBytes = "two".getBytes(StandardCharsets.US_ASCII);
            two.setPrecomputed(twoBytes, twoBytes.length, 7L);
            assertSame(map.get(one), first);
            assertSame(map.get(two), second);
        }
    }

    @Test
    public void preencodedLookupDoesNotNeedAReusableLookupKey() {
        byte[] bytes = "encoded".getBytes(StandardCharsets.US_ASCII);
        try (IndexTestSupport support = new IndexTestSupport()) {
            FlatConcurrentMap map = new FlatConcurrentMap(32L, 0L);
            Entry entry = support.entry(bytes);
            EncodedKey encoded = EncodedKey.copyOf(bytes);
            assertNull(map.putIfAbsent(entry, entry));
            assertSame(map.get(encoded), entry);
            assertTrue(encoded.matches(entry));
        }
    }

    @Test
    public void controlByteMaskMatchesOnlyTheRequestedControlByte() {
        long controls = controlBytes(0x2a, 0x80, 0x2a, 0xfe, 0x00, 0x2a, 0x7f, 0x80);
        assertEquals(FlatConcurrentMap.matchingByteMask(controls, 0x2a), highBits(0, 2, 5));
        assertEquals(FlatConcurrentMap.matchingByteMask(controls, 0x80), highBits(1, 7));
        assertEquals(FlatConcurrentMap.matchingByteMask(controls, 0xfe), highBits(3));
    }

    @Test
    public void heapLedgerCountsOnlyFlatTableArrayPayload() {
        FlatConcurrentMap map = new FlatConcurrentMap(16L, 0L);
        FlatIndexStats stats = map.snapshot();
        long expected = 32L * NativeMemory.objectReferenceSize() + 4L * Long.BYTES + 2L * Integer.BYTES;
        assertEquals(stats.slotCapacity, 32);
        assertEquals(stats.heapPayloadBytes, expected);
    }

    private static long controlBytes(int... bytes) {
        long controls = 0L;
        for (int index = 0; index < bytes.length; index++) controls |= (long) (bytes[index] & 0xff) << (index << 3);
        return controls;
    }

    private static long highBits(int... offsets) {
        long bits = 0L;
        for (int offset : offsets) bits |= 0x80L << (offset << 3);
        return bits;
    }
}
