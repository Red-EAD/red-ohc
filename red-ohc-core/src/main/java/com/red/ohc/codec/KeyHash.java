package com.red.ohc.codec;

import com.red.ohc.storage.NativeMemory;

/**
 * Canonical 32-bit key hash over serialized bytes. The hash values are process-internal
 * (insert and lookup share this function) and never persisted, so the function may change.
 */
public final class KeyHash {
  private static final int M1 = 0x85ebca6b;
  private static final int M2 = 0xc2b2ae35;
  private static final int M3 = 0x7feb352d;
  private static final int M4 = 0x846ca68b;

  /** Hashes [offset, offset+length) in 8-byte chunks with a Murmur3-style finalizer. */
  public static int hash(byte[] bytes, int offset, int length) {
    int end = offset + length;
    int h = 0x9e3779b9 ^ length;
    int index = offset;
    for (; index + 8 <= end; index += 8) {
      long chunk = NativeMemory.getLong(bytes, index);
      h = (h ^ (int) chunk) * M1;
      h = (h ^ (int) (chunk >>> 32)) * M2;
    }
    long tail = 0L;
    for (int shift = 0; index < end; index++, shift += 8) {
      tail |= (long) (bytes[index] & 0xff) << shift;
    }
    h = (h ^ (int) tail) * M1;
    h = (h ^ (int) (tail >>> 32)) * M2;
    h ^= h >>> 16;
    h *= M3;
    h ^= h >>> 15;
    h *= M4;
    h ^= h >>> 16;
    return h;
  }

  private KeyHash() {}
}
