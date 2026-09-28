package com.red.ohc.storage;

/** Native value block. Metadata is kept beside the serialized value, never in Entry. */
public final class ValueBlock {
  static final int DEADLINE_NANOS = 0;
  static final int LENGTH = 8;
  static final int HEADER = 16;
  private static final long NO_DEADLINE = Long.MIN_VALUE;
  private static final long MILLIS_PER_SECOND = 1_000L;

  private ValueBlock() {}

  public static long allocationLength(int valueLength) {
    return HEADER + CacheMath.roundUpTo8(valueLength);
  }

  public static void initialize(
      long address, long deadlineNanos, int valueLength, long createdAtMillis) {
    NativeMemory.putLong(address + DEADLINE_NANOS, deadlineNanos);
    NativeMemory.putInt(address + LENGTH, valueLength);
    NativeMemory.putInt(
        address + 12L,
        createdAtMillis <= 0L ? 0 : (int) (createdAtMillis / MILLIS_PER_SECOND));
  }

  /** The deadline is immutable after publication; Entry.valueAddress publishes the native block. */
  public static long deadlineNanos(long address) {
    return NativeMemory.getLong(address + DEADLINE_NANOS);
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

  /** Expiry test on a deadline read before the liveness checks; keeps the header load overlapped. */
  public static boolean expiredByDeadline(long deadlineNanos, long nowNanos) {
    return deadlineNanos != NO_DEADLINE && deadlineNanos <= nowNanos;
  }

  public static boolean expired(long address, long nowNanos) {
    return expiredByDeadline(deadlineNanos(address), nowNanos);
  }
}
