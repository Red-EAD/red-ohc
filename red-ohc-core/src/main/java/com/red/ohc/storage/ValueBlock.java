package com.red.ohc.storage;

/** Native value block. Metadata is kept beside the serialized value, never in Entry. */
public final class ValueBlock {
  static final int EXPIRE_AT = 0;
  static final int LENGTH = 8;
  static final int HEADER = 16;
  private static final long MILLIS_PER_SECOND = 1_000L;

  private ValueBlock() {}

  public static long allocationLength(int valueLength) {
    return HEADER + CacheMath.roundUpTo8(valueLength);
  }

  public static void initialize(
      long address, long expireAtMillis, int valueLength, long createdAtMillis) {
    NativeMemory.putLong(address + EXPIRE_AT, expireAtMillis);
    NativeMemory.putInt(address + LENGTH, valueLength);
    NativeMemory.putInt(
        address + 12L,
        createdAtMillis <= 0L ? 0 : (int) (createdAtMillis / MILLIS_PER_SECOND));
  }

  /** Expiry is immutable after publication; Entry.valueAddress publishes the native block. */
  public static long expireAtMillis(long address) {
    return NativeMemory.getLong(address + EXPIRE_AT);
  }

  public static int length(long address) {
    return NativeMemory.getInt(address + LENGTH);
  }

  /** Returns the second-resolution creation time stored in the reserved header word. */
  public static long createdAtMillis(long address) {
    return Integer.toUnsignedLong(NativeMemory.getInt(address + 12L)) * MILLIS_PER_SECOND;
  }

  public static long payloadAddress(long address) {
    return address + HEADER;
  }

  public static boolean expired(long address, long nowMillis) {
    long expire = expireAtMillis(address);
    return expire > 0L && expire <= nowMillis;
  }
}
