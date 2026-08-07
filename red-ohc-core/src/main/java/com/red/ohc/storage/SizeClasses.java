package com.red.ohc.storage;

import java.util.Arrays;

/** Fixed small-allocation classes, deliberately shared by all writer arenas. */
public final class SizeClasses {
    public static final int PAGE_BYTES = 64 << 10;
    private static final int[] SLOT_BYTES = createSlots();
    private static final int MAX_SLOT_BYTES = SLOT_BYTES[SLOT_BYTES.length - 1];
    private static final byte[] CLASS_BY_16_BYTES = createClassLookup();

    private SizeClasses() { }

    public static int count() { return SLOT_BYTES.length; }

    public static int indexForEntry(long entryBytes) {
        long required = WriterArena.PREFIX_BYTES + entryBytes;
        if (required > MAX_SLOT_BYTES) return -1;
        return CLASS_BY_16_BYTES[(int) ((required + 15L) >>> 4)] & 0xff;
    }

    public static int slotBytes(int index) { return SLOT_BYTES[index]; }

    public static long directBytes(long entryBytes) {
        return (WriterArena.PREFIX_BYTES + entryBytes + 15L) & ~15L;
    }

    private static int[] createSlots() {
        int[] slots = new int[86];
        int cursor = 0;
        for (int value = 128; value <= 1024; value += 32) slots[cursor++] = value;
        for (int value = 1152; value <= 4096; value += 128) slots[cursor++] = value;
        slots[cursor++] = 4128;
        for (int value = 4608; value <= 16384; value += 512) slots[cursor++] = value;
        for (int value = 18432; value <= 32768; value += 2048) slots[cursor++] = value;
        if (cursor != slots.length) throw new AssertionError(cursor);
        return slots;
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
