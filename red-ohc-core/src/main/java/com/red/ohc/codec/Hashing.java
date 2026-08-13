package com.red.ohc.codec;

import com.dynatrace.hash4j.hashing.Hasher64;

/** Allocation-free FarmHashUo for serialized keys. */
public final class Hashing {
  private static final Hasher64 FARM_HASH_UO =
      com.dynatrace.hash4j.hashing.Hashing.farmHashUo();

  private Hashing() {}

  public static long farmHashUo(byte[] bytes, int offset, int length) {
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException("offset=" + offset + ", length=" + length);
    }
    return FARM_HASH_UO.hashBytesToLong(bytes, offset, length);
  }
}
