package com.red.ohc.codec;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;

public final class KeyHashTest {
  @Test
  public void lookupAndPreencodedKeyKeepTheSameHash() {
    byte[] bytes = "hello".getBytes(StandardCharsets.US_ASCII);
    LookupKey lookup = new LookupKey();

    lookup.set(bytes, bytes.length);
    int firstHash = lookup.hash();

    // Re-reading the cached value must not execute the hash again.
    assertEquals(lookup.hash(), firstHash);

    LookupKey fresh = new LookupKey();
    fresh.set(bytes.clone(), bytes.length);
    assertEquals(fresh.hash(), firstHash, "the same bytes must always hash identically");

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
    LookupKey refreshed = new LookupKey();
    refreshed.set(bytes.clone(), bytes.length);
    assertEquals(lookup.hash(), refreshed.hash());
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
    LookupKey expected = new LookupKey();
    expected.set(bytes, bytes.length);
    assertEquals(context.lookupKey.hash(), expected.hash());
  }

  @Test
  public void equalIdentitiesStillRequireAnExactNativeKeyMatch() {
    byte[] lookupBytes = "hello".getBytes(StandardCharsets.US_ASCII);
    byte[] storedBytes = "world".getBytes(StandardCharsets.US_ASCII);
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(storedBytes.length);
    long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
    try {
      NativeMemory.copy(storedBytes, 0, address, storedBytes.length);
      LookupKey lookup = new LookupKey();
      lookup.set(lookupBytes, lookupBytes.length);
      // Deliberately hand the entry the lookup's identity so only the bytes can differ.
      Entry entry = new Entry(address, storedBytes.length, 0L);
      entry.initializeKeyHash(lookup.hash());

      assertFalse(lookup.equals(entry));
    } finally {
      memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void lookupKeyMatchesExactNativeBytesAcrossComparisonBoundaries() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {0, 1, 7, 8, 9, 16, 17, 63, 64, 127, 128, 129, 255, 256, 257, 300}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 31 + length);
        }
        long allocation = Entry.keyAllocationLengthForKeyLength(length);
        long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
        try {
          NativeMemory.copy(bytes, 0, address, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, 0L);
          entry.initializeKeyHash(lookup.hash());

          assertTrue(lookup.equals(entry), "length=" + length);
          if (length != 0) {
            long lastByte = address + length - 1;
            NativeMemory.putByte(lastByte, (byte) (NativeMemory.getByte(lastByte) ^ 1));
            assertFalse(lookup.equals(entry), "mismatch length=" + length);
          }
        } finally {
          memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
        }
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void lookupKeyRejectsHeadTailAndMiddleMismatches() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    LookupKey lookup = new LookupKey();
    try {
      for (int length : new int[] {1, 9, 16, 24, 64, 255, 300}) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
          bytes[index] = (byte) (index * 17 + length);
        }
        long allocation = Entry.keyAllocationLengthForKeyLength(length);
        long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
        try {
          NativeMemory.copy(bytes, 0, address, length);
          lookup.set(bytes, length);
          Entry entry = new Entry(address, length, 0L);
          entry.initializeKeyHash(lookup.hash());

          for (int mismatch :
              new int[] {
                0,
                Math.min(7, length - 1),
                Math.min(15, length - 1),
                Math.min(16, length - 1),
                Math.min(length / 2, length - 1),
                length - 8 < 0 ? 0 : Math.min(length - 8, length - 1),
                length - 1
              }) {
            long mismatchAddress = address + mismatch;
            byte original = NativeMemory.getByte(mismatchAddress);
            try {
              NativeMemory.putByte(mismatchAddress, (byte) (original ^ 1));
              assertFalse(lookup.equals(entry), "mismatch=" + mismatch + ", length=" + length);
            } finally {
              NativeMemory.putByte(mismatchAddress, original);
            }
          }
        } finally {
          memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
        }
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void everyByteOfEveryShortKeyAffectsTheHash() {
    Random random = new Random(0x5eed);
    for (int length = 1; length <= 64; length++) {
      byte[] base = new byte[length];
      random.nextBytes(base);
      java.util.HashSet<Integer> seen = new java.util.HashSet<>();
      assertTrue(seen.add(KeyHash.hash(base, 0, length)));
      for (int index = 0; index < length; index++) {
        byte original = base[index];
        for (int value = 1; value <= 3; value++) {
          base[index] = (byte) (original + value * 37);
          assertTrue(seen.add(KeyHash.hash(base, 0, length)),
              "length=" + length + ", index=" + index + ", value=" + value);
        }
        base[index] = original;
      }
      assertEquals(seen.size(), 1 + length * 3, "length=" + length);
    }
  }

  @Test
  public void singleBitFlipsAvalancheAcrossThe24OutputBits() {
    Random random = new Random(0xa17a1L);
    for (int length : new int[] {8, 16, 32, 255}) {
      long flips = 0;
      long observations = 0;
      for (int trial = 0; trial < 64; trial++) {
        byte[] base = new byte[length];
        random.nextBytes(base);
        int baseline = KeyHash.hash(base, 0, length);
        for (int bit = 0; bit < length * 8; bit++) {
          base[bit >> 3] ^= (byte) (1 << (bit & 7));
          int flipped = KeyHash.hash(base, 0, length);
          base[bit >> 3] ^= (byte) (1 << (bit & 7));
          flips += Integer.bitCount(baseline ^ flipped);
          observations += 32;
        }
      }
      double rate = (double) flips / observations;
      assertTrue(rate > 0.48 && rate < 0.52, "length=" + length + ", rate=" + rate);
    }
  }

  @Test
  public void randomKeyCollisionsStayAtTheBirthdayFloor() {
    Random random = new Random(0x1234);
    int count = 1 << 19;
    int[] hashes = new int[count];
    byte[] key = new byte[16];
    for (int index = 0; index < count; index++) {
      random.nextBytes(key);
      hashes[index] = KeyHash.hash(key, 0, 16);
    }
    Arrays.sort(hashes);
    long pairs = 0;
    for (int index = 1; index < count; index++) {
      if (hashes[index] == hashes[index - 1]) {
        pairs++;
      }
    }
    double birthdayBound = (double) count * (count - 1) / (1L << 33);
    assertTrue(pairs <= birthdayBound * 2, "pairs=" + pairs + ", bound=" + birthdayBound);
  }

  @Test
  public void hashIsStableAcrossOffsetsIntoASharedBuffer() {
    Random random = new Random(0x0ff);
    byte[] buffer = new byte[97];
    random.nextBytes(buffer);
    for (int length = 1; length <= 64; length++) {
      for (int offset = 0; offset + length <= buffer.length; offset += 7) {
        assertEquals(
            KeyHash.hash(buffer, offset, length),
            KeyHash.hash(Arrays.copyOfRange(buffer, offset, offset + length), 0, length),
            "length=" + length + ", offset=" + offset);
      }
    }
  }
}
