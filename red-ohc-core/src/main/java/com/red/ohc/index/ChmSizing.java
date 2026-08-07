package com.red.ohc.index;

/** Computes the CHM constructor and maintenance capacities from cache sizing inputs. */
public final class ChmSizing {
    static final double HEADROOM = 1.125d;
    static final long NATIVE_ENTRY_RESERVE = 128L;

    private ChmSizing() { }

    public static long plannedEntries(long expectedEntries) {
        if (expectedEntries <= 0L) return 64L;
        long quotient = expectedEntries / 8L;
        long remainder = expectedEntries % 8L;
        long extra = (remainder * 9L + 7L) / 8L;
        if (quotient > (Long.MAX_VALUE - extra) / 9L) return Long.MAX_VALUE;
        return Math.max(64L, quotient * 9L + extra);
    }

    public static long initialCapacity(long expectedEntries, long capacity, long maxEntrySize) {
        if (expectedEntries > 0L) return plannedEntries(expectedEntries);
        long divisor = maxEntrySize > Long.MAX_VALUE - NATIVE_ENTRY_RESERVE
                ? Long.MAX_VALUE : Math.max(1L, maxEntrySize + NATIVE_ENTRY_RESERVE);
        long estimate;
        if (capacity <= 0L) {
            estimate = 64L;
        } else if (capacity > Long.MAX_VALUE - divisor + 1L) {
            estimate = Long.MAX_VALUE / divisor + 1L;
        } else {
            estimate = (capacity + divisor - 1L) / divisor;
        }
        return Math.max(64L, estimate);
    }

    public static int constructorCapacity(long expectedEntries, long capacity, long maxEntrySize) {
        long value = initialCapacity(expectedEntries, capacity, maxEntrySize);
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    /** Returns the expected table length after CHM applies its 0.75 load-factor sizing. */
    public static int tableLengthFor(long expectedEntries, long capacity, long maxEntrySize) {
        long requested = initialCapacity(expectedEntries, capacity, maxEntrySize);
        long required = requested >= Long.MAX_VALUE / 4L
                ? Long.MAX_VALUE : (requested * 4L + 2L) / 3L;
        int table = 1;
        while (table < required && table < (1 << 30)) table <<= 1;
        return table;
    }

    public static int maintenanceQueueCapacity(long expectedEntries, long capacity, long maxEntrySize) {
        long planned = initialCapacity(expectedEntries, capacity, maxEntrySize);
        long requested = Math.max(1024L, Math.min(1L << 20, Math.max(1L, planned / 64L)));
        int queue = 1;
        while (queue < requested && queue < (1 << 20)) queue <<= 1;
        return queue;
    }
}
