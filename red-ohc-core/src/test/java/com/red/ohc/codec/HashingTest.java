package com.red.ohc.codec;

import com.red.ohc.EncodedKey;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public final class HashingTest {
    @Test
    public void lookupAndPreencodedKeyKeepTheSameFoldedXxHash64() {
        byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
        LookupKey lookup = new LookupKey();

        lookup.set(bytes, bytes.length);

        assertEquals(lookup.hash(), (int) 0xae58efdEL);
        assertEquals(lookup.hashComputations(), 1);

        // Re-reading the cached value must not execute the hash again.
        assertEquals(lookup.hash(), (int) 0xae58efdEL);
        assertEquals(lookup.hashComputations(), 1);

        EncodedKey encoded = EncodedKey.copyOf(bytes);
        assertEquals(encoded.hash(), lookup.hash());
    }
}
