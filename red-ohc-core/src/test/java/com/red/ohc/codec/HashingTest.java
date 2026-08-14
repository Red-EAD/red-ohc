package com.red.ohc.codec;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;

public final class HashingTest {
  @Test
  public void lookupAndPreencodedKeyKeepTheSameFoldedFarmHashUo() {
    byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
    LookupKey lookup = new LookupKey();

    lookup.set(bytes, bytes.length);

    assertEquals(lookup.hash(), (int) 0x85b3e941L);
    assertEquals(lookup.hash64(), 0xb48be5a931380ce8L);

    // Re-reading the cached value must not execute the hash again.
    assertEquals(lookup.hash(), (int) 0x85b3e941L);

    EncodedKey encoded = EncodedKey.copyOf(bytes);
    assertEquals(encoded.hash(), lookup.hash());
    assertEquals(encoded.hash64(), lookup.hash64());
  }

  @Test
  public void encodedKeyBindingUsesItsCachedHash() {
    byte[] bytes = "encoded-key".getBytes(StandardCharsets.US_ASCII);
    EncodedKey encoded = EncodedKey.copyOf(bytes);
    LookupKey lookup = new LookupKey();
    byte[] target = new byte[bytes.length];

    lookup.set(target, encoded);

    assertEquals(lookup.length(), bytes.length);
    assertEquals(lookup.hash(), encoded.hash());
    assertEquals(lookup.hash64(), encoded.hash64());
    assertEquals(target, bytes);
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
    CacheSerializer<byte[]> serializer =
        new CacheSerializer<byte[]>() {
          @Override
          public void serialize(byte[] value, ByteBuffer buffer) {
            buffer.put(value);
          }

          @Override
          public byte[] deserialize(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
          }

          @Override
          public int serializedSize(byte[] value) {
            return value.length;
          }
        };
    byte[] bytes = "encoded".getBytes(StandardCharsets.US_ASCII);
    ThreadContext context = new ThreadContext(null);

    int length = KeyEncoder.encode(serializer, bytes, context);

    assertEquals(length, bytes.length);
    assertEquals(context.lookupKey.length(), bytes.length);
    long expectedHash64 = 0x2c9eed22957a7483L;
    assertEquals(context.lookupKey.hash64(), expectedHash64);
    assertEquals(context.lookupKey.hash(), (int) (expectedHash64 ^ (expectedHash64 >>> 32)));
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
      LookupKey lookup = new LookupKey();
      lookup.set(lookupBytes, lookupBytes.length);
      NativeMemory.putLong(address, lookup.hash64());
      Entry entry = new Entry(address, storedBytes.length, lookup.hash(), 0L);

      assertFalse(lookup.equals(entry));
    } finally {
      memory.free(address, Long.BYTES + storedBytes.length);
      memory.closeArenas();
    }
  }
}
