package com.red.ohc.codec;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;

public final class KeyHashTest {
  @Test
  public void lookupAndPreencodedKeyKeepTheSamePolynomialHash() {
    byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
    LookupKey lookup = new LookupKey();

    lookup.set(bytes, bytes.length);

    assertEquals(lookup.hash(), 0x079df171);

    // Re-reading the cached value must not execute the hash again.
    assertEquals(lookup.hash(), 0x079df171);

    EncodedKey encoded = EncodedKey.copyOf(bytes);
    assertEquals(encoded.hash(), lookup.hash());
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
    assertEquals(target, bytes);
  }

  @Test
  public void reusesTheSameLookupBackingArrayWhileRefreshingTheHash() {
    byte[] bytes = "first-key".getBytes(StandardCharsets.US_ASCII);
    LookupKey lookup = new LookupKey();

    lookup.set(bytes, bytes.length);
    int firstHash = lookup.hash();
    bytes[0] = 's';
    lookup.set(bytes, bytes.length);

    assertSame(lookup.bytes(), bytes);
    assertTrue(lookup.hash() != firstHash, "the hash must still be refreshed for the new bytes");
    assertEquals(lookup.hash(), Arrays.hashCode(bytes));
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
    assertEquals(context.lookupKey.hash(), 0x0812b1ed);
  }

  @Test
  public void equalHashesStillRequireAnExactNativeKeyMatch() {
    byte[] lookupBytes = "hello".getBytes(StandardCharsets.US_ASCII);
    byte[] storedBytes = "world".getBytes(StandardCharsets.US_ASCII);
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(storedBytes.length);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.putLong(address, Entry.keyIndexWord(0x12345678, storedBytes.length));
      NativeMemory.copy(storedBytes, 0, address + Long.BYTES, storedBytes.length);
      LookupKey lookup = new LookupKey();
      lookup.set(lookupBytes, lookupBytes.length);
      NativeMemory.putLong(address, Entry.keyIndexWord(lookup.hash(), storedBytes.length));
      Entry entry = new Entry(address, storedBytes.length, 0L);

      assertFalse(lookup.equals(entry));
    } finally {
      memory.free(address, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void lookupKeyMatchesExactNativeBytesAcrossComparisonBoundaries() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {0, 7, 8, 63, 64, 127, 128, 129, 257}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 31 + length);
        }
        long allocation = Entry.keyAllocationLengthForKeyLength(length);
        long address = memory.allocate(allocation);
        try {
          NativeMemory.putLong(address, Entry.keyIndexWord(Arrays.hashCode(bytes), length));
          NativeMemory.copy(bytes, 0, address + Long.BYTES, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, 0L);
    
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
    NativeMemory.Memory memory = new NativeMemory.Memory();
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {1, 7, 8, 63, 64, 127, 128, 129, 257}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 17 + length);
        }
        long allocation = Entry.keyAllocationLengthForKeyLength(length);
        long address = memory.allocate(allocation);
        try {
          NativeMemory.putLong(address, Entry.keyIndexWord(Arrays.hashCode(bytes), length));
          NativeMemory.copy(bytes, 0, address + Long.BYTES, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, 0L);
    
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