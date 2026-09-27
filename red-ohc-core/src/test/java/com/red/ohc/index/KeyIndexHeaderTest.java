package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.red.ohc.codec.KeyHash;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;

/**
 * The native key block is headerless: bytes start at the block address and the immutable 32-bit
 * hash lives in the native prefix slot at +48.
 */
public final class KeyIndexHeaderTest {
  @Test
  public void keyBytesStartAtTheBlockAddressAndHashLivesInThePrefix() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    byte[] bytes = fill(16);
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
    try {
      NativeMemory.copy(bytes, 0, address, 16);
      Entry entry = new Entry(address, 16, 0L);
      entry.initializeKeyHash(0x9b7f4601);
      assertEquals(address, entry.nativeKeyBytesAddress());
      assertTrue(NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, 16));
      assertEquals(NativeMemory.getInt(address - 16L), 0x9b7f4601);
      assertEquals(entry.keyHash(), 0x9b7f4601);
      assertEquals(entry.hashCode(), 0x9b7f4601);
    } finally {
      memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void negativeHashRoundTripsThroughThePrefixSlot() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
    try {
      Entry entry = new Entry(address, 16, 0L);
      entry.initializeKeyHash(0x9b7f4601 | Integer.MIN_VALUE);
      assertEquals(entry.keyHash(), 0x9b7f4601 | Integer.MIN_VALUE);
    } finally {
      memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void entryAndLookupAgreeOnTheHash() {
    byte[] bytes = fill(16);
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation) + Entry.NATIVE_METADATA_BYTES;
    try {
      NativeMemory.copy(bytes, 0, address, 16);
      Entry entry = new Entry(address, 16, 0L);
      entry.initializeKeyHash(KeyHash.hash(bytes, 0, 16));
      LookupKey lookup = new LookupKey();
      lookup.set(bytes, 16);

      assertEquals(lookup.hashCode(), entry.hashCode());
      assertTrue(lookup.equals(entry));

      byte[] mutated = bytes.clone();
      mutated[15]++;
      LookupKey other = new LookupKey();
      other.set(mutated, 16);
      assertFalse(other.equals(entry));
    } finally {
      memory.free(address - Entry.NATIVE_METADATA_BYTES, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void hashDistributesAcrossPowerOfTwoMasks() {
    Random random = new Random(42);
    LookupKey lookup = new LookupKey();
    for (int bits = 13; bits <= 21; bits++) {
      int buckets = 1 << bits;
      int[] counts = new int[buckets];
      int samples = 4 * buckets;
      for (int index = 0; index < samples; index++) {
        byte[] key = new byte[16];
        random.nextBytes(key);
        lookup.set(key, 16);
        counts[lookup.hash() & (buckets - 1)]++;
      }
      double chi2 = 0;
      double mean = (double) samples / buckets;
      for (int count : counts) {
        chi2 += (count - mean) * (count - mean) / mean;
      }
      double z = (chi2 - (buckets - 1)) / Math.sqrt(2.0 * (buckets - 1));
      assertTrue(Math.abs(z) < 5, "bits=" + bits + ", chi2/df=" + (chi2 / (buckets - 1)));
    }
  }

  private static byte[] fill(int length) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) 'h');
    return bytes;
  }
}
