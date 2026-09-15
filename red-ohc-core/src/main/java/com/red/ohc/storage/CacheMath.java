package com.red.ohc.storage;

/** Native allocation alignment primitives. */
public final class CacheMath {
  private CacheMath() {}

  public static long roundUpTo8(long value) {
    return (value + 7L) & ~7L;
  }

  /** Serialized entry bytes charged to the public cache capacity. */
  public static long logicalEntryBytes(long keyAllocation, long valueAllocation) {
    return keyAllocation + valueAllocation;
  }
}
