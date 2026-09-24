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

/** The native key block is headerless: bytes start at the block address, identity is packed. */
public final class KeyIndexHeaderTest {
  @Test
  public void keyBytesStartAtTheBlockAddress() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    byte[] bytes = fill(16);
    long allocation = Entry.keyPhysicalAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.copy(bytes, 0, address, 16);
      Entry entry = new Entry(address, packed(bytes), 0L);
      assertEquals(address, entry.nativeKeyBytesAddress());
      assertTrue(NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, 16));
    } finally {
      memory.free(address, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void packedKeyIndexCarriesHashAndLength() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    byte[] bytes = fill(16);
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation);
    try {
      Entry entry = new Entry(address, packed(bytes), 0L);
      assertEquals(entry.keyLength(), 16);
      assertEquals(entry.keyHash(), KeyHash.hash(bytes, 0, 16));
      assertEquals(entry.hashCode(), entry.keyHash());
      assertTrue(entry.keyHash() >= 0);
      assertTrue(entry.keyHash() <= 0xff_ffff);
    } finally {
      memory.free(address, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void entryAndLookupAgreeOnThePackedIdentity() {
    byte[] bytes = fill(16);
    while (KeyHash.hash(bytes, 0, 16) < 0x80_0000) {
      bytes[0]++;
    }
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.copy(bytes, 0, address, 16);
      Entry entry = new Entry(address, packed(bytes), 0L);
      LookupKey lookup = new LookupKey();
      lookup.set(bytes, 16);

      assertEquals(lookup.hashCode(), entry.hashCode());
      assertEquals(lookup.keyIndex(), entry.keyIndex());
      assertTrue(lookup.equals(entry));

      byte[] mutated = bytes.clone();
      mutated[15]++;
      LookupKey other = new LookupKey();
      other.set(mutated, 16);
      assertFalse(other.equals(entry));
    } finally {
      memory.free(address, allocation);
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

  private static int packed(byte[] bytes) {
    return (KeyHash.hash(bytes, 0, bytes.length) << 8) | bytes.length;
  }

  private static byte[] fill(int length) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) 'h');
    return bytes;
  }
}
