package com.red.ohc.maintenance;

/** Small worker-owned Count-Min sketch for W-TinyLFU admission/selection. */
public final class FrequencySketch {
  private static final int DEFAULT_TABLE_SIZE = 1 << 14;
  private static final long RESET_MASK = 0x7777777777777777L;
  private static final long ONE_MASK = 0x1111111111111111L;
  private static final int[] SEEDS = {0x9e3779b9, 0x85ebca6b, 0xc2b2ae35, 0x27d4eb2d};

  private final long[] table;
  private final int tableMask;
  private final long sampleSize;
  private long size;

  /** Preserves the prior default table size for standalone tests and small caches. */
  public FrequencySketch() {
    this((long) DEFAULT_TABLE_SIZE << 3);
  }

  /**
   * Uses one packed 64-bit counter word per eight planned index slots, rounded to a power of two.
   * The event window remains ten times the planned index size, not ten times heap bytes.
   */
  public FrequencySketch(long indexSlots) {
    long requested = Math.max(1_024L, (Math.max(1L, indexSlots) + 7L) >>> 3);
    int length = powerOfTwo(requested);
    this.table = new long[length];
    this.tableMask = length - 1;
    this.sampleSize = Math.min(Long.MAX_VALUE / 10L, Math.max(1L, indexSlots)) * 10L;
  }

  public void increment(int hash) {
    long mixed = mix64(hash);
    for (int depth = 0; depth < SEEDS.length; depth++) {
      int index = (int) (mix64(mixed + SEEDS[depth]) & tableMask);
      int nibble = (int) ((mixed >>> (depth * 4)) & 15) * 4;
      long mask = 0xfL << nibble;
      long current = table[index];
      if ((current & mask) != mask) {
        table[index] = current + (1L << nibble);
      }
    }
    if (++size >= sampleSize) {
      reset();
    }
  }

  public int frequency(long hash) {
    long mixed = mix64(hash);
    int frequency = 15;
    for (int depth = 0; depth < SEEDS.length; depth++) {
      int index = (int) (mix64(mixed + SEEDS[depth]) & tableMask);
      int nibble = (int) ((mixed >>> (depth * 4)) & 15) * 4;
      frequency = Math.min(frequency, (int) ((table[index] >>> nibble) & 15L));
    }
    return frequency;
  }

  public long bytes() {
    return (long) table.length * Long.BYTES;
  }

  long sampleSize() {
    return sampleSize;
  }

  private void reset() {
    long oddCounters = 0L;
    for (int i = 0; i < table.length; i++) {
      long current = table[i];
      oddCounters += Long.bitCount(current & ONE_MASK);
      table[i] = (current >>> 1) & RESET_MASK;
    }
    size = (size - (oddCounters >>> 2)) >>> 1;
  }

  private static long mix64(long value) {
    value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
    value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
    return value ^ (value >>> 31);
  }

  private static int powerOfTwo(long requested) {
    int result = 1;
    long maximum = 1 << 26;
    long target = Math.min(maximum, requested);
    while (result < target) {
      result <<= 1;
    }
    return result;
  }
}
