package com.red.ohc;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public final class EncodedKeyImmutabilityTest {
    @Test
    public void byteAccessCannotMutateThePrecomputedKey() {
        byte[] source = {1, 2, 3, 4};
        EncodedKey key = EncodedKey.copyOf(source);
        int expectedHash = key.hash();

        source[0] = 9;
        byte[] exposed = key.bytes();
        exposed[1] = 8;

        assertEquals(key.bytes()[0], (byte) 1);
        assertEquals(key.bytes()[1], (byte) 2);
        assertEquals(key.hash(), expectedHash);
    }
}
