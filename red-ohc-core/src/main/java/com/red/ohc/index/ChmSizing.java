package com.red.ohc.index;

/** Computes the CHM constructor and maintenance capacities from cache sizing inputs. */
public final class ChmSizing {
  static final double HEADROOM = 1.125d;

  private ChmSizing() {}

  public static long plannedEntries(long expectedEntries) {
    if (expectedEntries <= 0L) {
      return 64L;
    }
    long quotient = expectedEntries / 8L;
    long remainder = expectedEntries % 8L;
    long extra = (remainder * 9L + 7L) / 8L;
    if (quotient > (Long.MAX_VALUE - extra) / 9L) {
      return Long.MAX_VALUE;
    }
    return Math.max(64L, quotient * 9L + extra);
  }

  public static long initialCapacity(long expectedEntries, long capacity) {
    if (expectedEntries > 0L) {
      return plannedEntries(expectedEntries);
    }
    // A byte capacity does not provide a reliable entry-count estimate. Keep the byte-bounded
    // default conservative; count-bounded caches pass maxSize as expectedEntries above.
    return 64L;
  }

  public static int constructorCapacity(long expectedEntries, long capacity) {
    long value = initialCapacity(expectedEntries, capacity);
    return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }

  /** Returns the expected table length after CHM applies its 0.75 load-factor sizing. */
  public static int tableLengthFor(long expectedEntries, long capacity) {
    long requested = initialCapacity(expectedEntries, capacity);
    long required = requested >= Long.MAX_VALUE / 4L ? Long.MAX_VALUE : (requested * 4L + 2L) / 3L;
    int table = 1;
    while (table < required && table < (1 << 30)) {
      table <<= 1;
    }
    return table;
  }

  /** Bounded advisory mutation transport capacity. */
  public static int maintenanceQueueCapacity(long expectedEntries, long capacity) {
    long planned =
        expectedEntries > 0L
            ? plannedEntries(expectedEntries)
            : initialCapacity(expectedEntries, capacity);
    long requested = Math.max(1_024L, planned / 64L);
    long bounded = Math.min(1L << 20, requested);
    int result = 1;
    while (result < bounded) {
      result <<= 1;
    }
    return result;
  }
}
