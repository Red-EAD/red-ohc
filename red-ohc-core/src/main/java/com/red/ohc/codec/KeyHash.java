package com.red.ohc.codec;

import com.red.ohc.storage.NativeMemory;

/**
 * Canonical 32-bit key hash over serialized bytes. The hash values are process-internal
 * (insert and lookup share this function) and never persisted, so the function may change.
 */
public final class KeyHash {
  private static final long P1 = 0xe7037ed1a0b428dbL;
  private static final long P2 = 0x8ebc6af09c88c6e3L;
  private static final long P3 = 0xa0761d6478bd642fL;
  private static final long P4 = 0xc2b2ae3d27d4eb4fL;

  /**
   * Head and tail overlapping reads cover every length; middle bytes fold through a
   * multiplyHigh pair so each chunk diffuses into all 64 bits. Long keys run two independent
   * lanes, halving the serial multiply chain where it dominates. Only bytes inside
   * [offset, offset+length) are read: the caller's buffer is reused.
   */
  public static int hash(byte[] bytes, int offset, int length) {
    return length >= 64
        ? hashLong(bytes, offset, length)
        : hashShort(bytes, offset, length);
  }

  private static int hashShort(byte[] bytes, int offset, int length) {
    long a;
    long b;
    long m = 0L;
    if (length >= 9) {
      a = NativeMemory.getLong(bytes, offset);
      b = NativeMemory.getLong(bytes, offset + length - 8);
      int end = offset + length - 8;
      int index = offset + 8;
      for (; index + 8 <= end; index += 8) {
        long t = m ^ NativeMemory.getLong(bytes, index);
        m = (t * P3) ^ Math.multiplyHigh(t, P3);
      }
      long partial = 0L;
      for (; index < end; index++) {
        partial = (partial << 8) | (bytes[index] & 0xffL);
      }
      if (partial != 0L) {
        m ^= (partial * P3) ^ Math.multiplyHigh(partial, P3);
      }
    } else if (length >= 4) {
      a = NativeMemory.getInt(bytes, offset) & 0xffff_ffffL;
      b = NativeMemory.getInt(bytes, offset + length - 4) & 0xffff_ffffL;
    } else if (length > 0) {
      a = ((bytes[offset] & 0xffL) << 16)
          | ((bytes[offset + (length >> 1)] & 0xffL) << 8)
          | (bytes[offset + length - 1] & 0xffL);
      b = 0L;
    } else {
      a = 0L;
      b = 0L;
    }
    long x = a ^ m ^ P1 ^ length;
    long y = b ^ P2;
    long folded = (x * y) ^ Math.multiplyHigh(x, y);
    return (int) folded;
  }

  private static int hashLong(byte[] bytes, int offset, int length) {
    long a = NativeMemory.getLong(bytes, offset);
    long b = NativeMemory.getLong(bytes, offset + length - 8);
    int end = offset + length - 8;
    int index = offset + 8;
    long m1 = 0L;
    long m2 = 0L;
    while (index + 16 <= end) {
      long t1 = m1 ^ NativeMemory.getLong(bytes, index);
      m1 = (t1 * P3) ^ Math.multiplyHigh(t1, P3);
      long t2 = m2 ^ NativeMemory.getLong(bytes, index + 8);
      m2 = (t2 * P4) ^ Math.multiplyHigh(t2, P4);
      index += 16;
    }
    if (index + 8 <= end) {
      long t = m1 ^ NativeMemory.getLong(bytes, index);
      m1 = (t * P3) ^ Math.multiplyHigh(t, P3);
      index += 8;
    }
    long partial = 0L;
    for (; index < end; index++) {
      partial = (partial << 8) | (bytes[index] & 0xffL);
    }
    if (partial != 0L) {
      m1 ^= (partial * P3) ^ Math.multiplyHigh(partial, P3);
    }
    long x = a ^ m1 ^ P1 ^ length;
    long y = b ^ m2 ^ P2;
    long folded = (x * y) ^ Math.multiplyHigh(x, y);
    return (int) folded;
  }

  private KeyHash() {}
}
