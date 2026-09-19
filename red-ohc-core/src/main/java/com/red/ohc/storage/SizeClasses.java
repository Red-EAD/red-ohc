package com.red.ohc.storage;

import java.util.Arrays;

/** Fixed small-allocation classes, deliberately shared by all writer arenas. */
public final class SizeClasses {
  public static final int MIN_PAGE_BYTES = 64 << 10;
  private static final int[] SLOT_BYTES = createSlots();
  private static final int MAX_SLOT_BYTES = SLOT_BYTES[SLOT_BYTES.length - 1];
  private static final byte[] CLASS_BY_16_BYTES = createClassLookup();

  private SizeClasses() {}

  public static int count() {
    return SLOT_BYTES.length;
  }

  public static int indexForEntry(long entryBytes) {
    long required = WriterArena.PREFIX_BYTES + entryBytes;
    if (required > MAX_SLOT_BYTES) {
      return -1;
    }
    return CLASS_BY_16_BYTES[(int) ((required + 15L) >>> 4)] & 0xff;
  }

  public static int slotBytes(int index) {
    return SLOT_BYTES[index];
  }

  public static int pageBytes(int index) {
    return pageBytesForSlot(SLOT_BYTES[index]);
  }

  /**
   * Continuous page sizing between the 64KB floor and the 2MB cap; every page holds 256 slots
   * (512 for the floor classes), so finer pages cut fan-out and partial-page slack without
   * any size cliff. Direct mappings tolerate non-power-of-two lengths.
   */
  public static int pageBytesForSlot(int slotBytes) {
    long alignedSlotBytes = (slotBytes + 63L) & ~63L;
    if (alignedSlotBytes <= 0L || alignedSlotBytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("slot size is out of range: " + slotBytes);
    }
    long required = Math.max((long) MIN_PAGE_BYTES, 256L * alignedSlotBytes);
    return (int) Math.min(2L * 1024 * 1024, required);
  }

  public static long directBytes(long entryBytes) {
    long blockBytes = (WriterArena.PREFIX_BYTES + entryBytes + 63L) & ~63L;
    // Native malloc implementations are not required to return 64-byte aligned addresses. Keep
    // enough slack for Memory to align the visible block while retaining the raw base at +56.
    return blockBytes + 128L;
  }

  private static int[] createSlots() {
    int[] slots = new int[87];
    int cursor = 0;
    for (int value = 128; value <= 1024; value += 32) {
      slots[cursor++] = value;
    }
    for (int value = 1152; value <= 4096; value += 128) {
      slots[cursor++] = value;
    }
    slots[cursor++] = 4128;
    for (int value = 4608; value <= 5120; value += 512) {
      slots[cursor++] = value;
    }
    slots[cursor++] = 5152;
    for (int value = 5632; value <= 16384; value += 512) {
      slots[cursor++] = value;
    }
    for (int value = 18432; value <= 32768; value += 2048) {
      slots[cursor++] = value;
    }
    if (cursor != slots.length) {
      throw new AssertionError(cursor);
    }
    int unique = 0;
    int previous = -1;
    for (int index = 0; index < cursor; index++) {
      int aligned = (slots[index] + 63) & ~63;
      if (aligned != previous) {
        slots[unique++] = aligned;
        previous = aligned;
      }
    }
    return Arrays.copyOf(slots, unique);
  }

  private static byte[] createClassLookup() {
    byte[] classes = new byte[(MAX_SLOT_BYTES >>> 4) + 1];
    int firstUnit = 0;
    for (int index = 0; index < SLOT_BYTES.length; index++) {
      int lastUnit = SLOT_BYTES[index] >>> 4;
      Arrays.fill(classes, firstUnit, lastUnit + 1, (byte) index);
      firstUnit = lastUnit + 1;
    }
    return classes;
  }
}
