package com.red.ohc.codec;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

  @Test
  public void lookupKeyMatchesExactNativeBytesAcrossComparisonBoundaries() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {0, 7, 8, 63, 64, 127, 128, 129, 257}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 31 + length);
        }
        long allocation = Math.max(8L, Long.BYTES + length);
        long address = memory.allocate(allocation);
        try {
          long hash64 = Hashing.farmHashUo(bytes, 0, length);
          NativeMemory.putLong(address, hash64);
          NativeMemory.copy(bytes, 0, address + Long.BYTES, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, lookup.hash(), hash64, 0L);

          assertTrue(lookup.equals(entry), "length=" + length);
          if (length != 0) {
            NativeMemory.putByte(
                address + Long.BYTES + length - 1,
                (byte) (NativeMemory.getByte(address + Long.BYTES + length - 1) ^ 1));
            assertFalse(lookup.equals(entry), "mismatch length=" + length);
          }
        } finally {
          memory.free(address, allocation);
        }
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void lookupKeyRejectsFirstWordAndComparisonBoundaryMismatches() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {1, 7, 8, 63, 64, 127, 128, 129, 257}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 17 + length);
        }
        long allocation = Math.max(8L, Long.BYTES + length);
        long address = memory.allocate(allocation);
        try {
          long hash64 = Hashing.farmHashUo(bytes, 0, length);
          NativeMemory.putLong(address, hash64);
          NativeMemory.copy(bytes, 0, address + Long.BYTES, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, lookup.hash(), hash64, 0L);

          for (int mismatch :
              new int[] {
                0,
                Math.min(7, length - 1),
                Math.min(63, length - 1),
                Math.min(64, length - 1),
                Math.min(127, length - 1),
                length - 1
              }) {
            long mismatchAddress = address + Long.BYTES + mismatch;
            byte original = NativeMemory.getByte(mismatchAddress);
            try {
              NativeMemory.putByte(mismatchAddress, (byte) (original ^ 1));
              assertFalse(
                  lookup.equals(entry),
                  "mismatch=" + mismatch + ", length=" + length);
            } finally {
              NativeMemory.putByte(mismatchAddress, original);
            }
          }
        } finally {
          memory.free(address, allocation);
        }
      }
    } finally {
      memory.closeArenas();
    }
  }
}
