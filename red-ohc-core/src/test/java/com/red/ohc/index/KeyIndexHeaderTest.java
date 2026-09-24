package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

import com.red.ohc.codec.KeyHash;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;

public final class KeyIndexHeaderTest {
  @Test
  public void headerPacksHashLowAndKeyLengthHigh() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      assertPackedWord(memory, hashOf("hello".getBytes()), 5);
      assertPackedWord(memory, hashOf(fill(16)), 16);
      assertPackedWord(memory, hashOf(new byte[0]), 0);
    } finally {
      memory.closeArenas();
    }
  }

  private static void assertPackedWord(NativeMemory.Memory memory, int hash, int keyLength) {
    long allocation = Entry.keyPhysicalAllocationLengthForKeyLength(keyLength);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.putLong(address, Entry.keyIndexWord(hash, keyLength));
      assertEquals(NativeMemory.getInt(address), hash);
      assertEquals(NativeMemory.getInt(address + Integer.BYTES), keyLength);
      assertEquals(NativeMemory.getLong(address), Entry.keyIndexWord(hash, keyLength));
      assertEquals((address + Long.BYTES) % 8, 0L);
    } finally {
      memory.free(address, allocation);
    }
  }

  @Test
  public void keyHashAccessorsExtractFromThePackedWord() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.putLong(address, Entry.keyIndexWord(0x9b7f4601, 16));
      Entry entry = new Entry(address, 16, 0L);

      assertEquals(entry.keyHash(), 0x9b7f4601);
      assertEquals(entry.hashCode(), 0x9b7f4601);
    } finally {
      memory.free(address, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void entryAndLookupAgreeForNegativeHash() {
    byte[] bytes = fill(16);
    while (hashOf(bytes) >= 0) {
      bytes[0]++;
    }
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long allocation = Entry.keyAllocationLengthForKeyLength(16);
    long address = memory.allocate(allocation);
    try {
      NativeMemory.putLong(address, Entry.keyIndexWord(hashOf(bytes), 16));
      NativeMemory.copy(bytes, 0, address + Long.BYTES, 16);
      Entry entry = new Entry(address, 16, 0L);
      LookupKey lookup = new LookupKey();
      lookup.set(bytes, 16);

      assertEquals(lookup.hashCode(), entry.hashCode());
      assertTrue(lookup.equals(entry));
    } finally {
      memory.free(address, allocation);
      memory.closeArenas();
    }
  }

  @Test
  public void hashDistributesAcrossPowerOfTwoMasks() {
    Random random = new Random(42);
    LookupKey lookup = new LookupKey();
    for (int bits = 17; bits <= 21; bits++) {
      int buckets = 1 << bits;
      int[] counts = new int[buckets];
      int samples = 4 * buckets;
      for (int index = 0; index < samples; index++) {
        byte[] key = new byte[16];
        random.nextBytes(key);
        lookup.set(key, 16);
        counts[lookup.hash() & (buckets - 1)]++;
      }
      int maxCount = 0;
      for (int count : counts) {
        maxCount = Math.max(maxCount, count);
      }
      assertTrue(maxCount < 6 * (samples / buckets), "bits=" + bits + ", max=" + maxCount);
    }
  }

  private static int hashOf(byte[] bytes) {
    return KeyHash.hash(bytes, 0, bytes.length);
  }

  private static byte[] fill(int length) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) 'h');
    return bytes;
  }
}