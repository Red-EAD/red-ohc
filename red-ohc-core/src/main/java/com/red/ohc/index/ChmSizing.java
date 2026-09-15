package com.red.ohc.index;

/** Computes the CHM constructor and maintenance capacities from cache sizing inputs. */
public final class ChmSizing {
  static final double HEADROOM = 1.125d;

  private ChmSizing() {}

  public static long plannedEntries(long entryEstimate) {
    if (entryEstimate <= 0L) {
      return 64L;
    }
    long quotient = entryEstimate / 8L;
    long remainder = entryEstimate % 8L;
    long extra = (remainder * 9L + 7L) / 8L;
    if (quotient > (Long.MAX_VALUE - extra) / 9L) {
      return Long.MAX_VALUE;
    }
    return Math.max(64L, quotient * 9L + extra);
  }

  public static long initialCapacity(long entryEstimate, long capacity) {
    if (entryEstimate > 0L) {
      return plannedEntries(entryEstimate);
    }
    // A byte capacity does not provide a reliable entry-count estimate. Keep the byte-bounded
    // default conservative; count-bounded caches pass maxSize as the estimate.
    return 64L;
  }

  public static int constructorCapacity(long entryEstimate, long capacity) {
    long value = initialCapacity(entryEstimate, capacity);
    return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }

  /** Returns the expected table length after CHM applies its 0.75 load-factor sizing. */
  public static int tableLengthFor(long entryEstimate, long capacity) {
    long requested = initialCapacity(entryEstimate, capacity);
    long required = requested >= Long.MAX_VALUE / 4L ? Long.MAX_VALUE : (requested * 4L + 2L) / 3L;
    int table = 1;
    while (table < required && table < (1 << 30)) {
      table <<= 1;
    }
    return table;
  }

}
