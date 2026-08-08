package com.red.ohc.codec;

import com.red.ohc.EncodedKey;
import com.red.ohc.CacheSerializer;
import com.red.ohc.AllocatorType;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public final class HashingTest {
    @Test
    public void lookupAndPreencodedKeyKeepTheSameFoldedXxHash64() {
        byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
        LookupKey lookup = new LookupKey();

        lookup.set(bytes, bytes.length);

        assertEquals(lookup.hash(), (int) 0xae58efdEL);
        assertEquals(lookup.hash64(), 0x26c7827d889f6da3L);

        // Re-reading the cached value must not execute the hash again.
        assertEquals(lookup.hash(), (int) 0xae58efdEL);

        EncodedKey encoded = EncodedKey.copyOf(bytes);
        assertEquals(encoded.hash(), lookup.hash());
        assertEquals(encoded.hash64(), lookup.hash64());
    }

    @Test
    public void lookupDoesNotCarryLazyHashState() {
        for (Field field : LookupKey.class.getDeclaredFields()) {
            if (field.getName().equals("hashed") || field.getName().equals("hashComputations")) {
                throw new AssertionError("LookupKey still carries lazy hash state: " + field.getName());
            }
        }
    }

    @Test
    public void keyEncoderBindsTheSerializedLookupKey() {
        CacheSerializer<byte[]> serializer = new CacheSerializer<byte[]>() {
            @Override public void serialize(byte[] value, ByteBuffer buffer) { buffer.put(value); }
            @Override public byte[] deserialize(ByteBuffer buffer) { throw new UnsupportedOperationException(); }
            @Override public int serializedSize(byte[] value) { return value.length; }
        };
        byte[] bytes = "encoded".getBytes(StandardCharsets.US_ASCII);
        ThreadContext context = new ThreadContext(null, null);

        int length = KeyEncoder.encode(serializer, bytes, context);

        assertEquals(length, bytes.length);
        assertEquals(context.lookupKey.length(), bytes.length);
        assertEquals(context.lookupKey.hash(), Hashing.xxHash64Folded(bytes, 0, bytes.length));
    }

    @Test
    public void equalHashesStillRequireAnExactNativeKeyMatch() {
        byte[] lookupBytes = "hello".getBytes(StandardCharsets.US_ASCII);
        byte[] storedBytes = "world".getBytes(StandardCharsets.US_ASCII);
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        long address = memory.allocate(Long.BYTES + storedBytes.length);
        try {
            NativeMemory.putLong(address, 0x12345678L);
            NativeMemory.copy(storedBytes, 0, address + Long.BYTES, storedBytes.length);
            Entry entry = new Entry(address, storedBytes.length, 0x12345678, 0L);
            LookupKey lookup = new LookupKey();
            lookup.setPrecomputed(lookupBytes, lookupBytes.length, 0x12345678);

            assertFalse(lookup.equals(entry));
        } finally {
            memory.free(address, Long.BYTES + storedBytes.length);
            memory.closeArenas();
        }
    }
}
