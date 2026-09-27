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

  /**
   * Head and tail overlapping reads cover every length; middle bytes fold through a
   * multiplyHigh pair so each chunk diffuses into all 64 bits. Only bytes inside
   * [offset, offset+length) are read: the caller's buffer is reused.
   */
  public static int hash(byte[] bytes, int offset, int length) {
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

  private KeyHash() {}
}
