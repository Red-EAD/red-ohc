package com.red.ohc.maintenance;

/** Small worker-owned Count-Min sketch for W-TinyLFU admission/selection. */
public final class FrequencySketch {
    private static final int TABLE_BITS = 14;
    private static final int TABLE_SIZE = 1 << TABLE_BITS;
    private static final int TABLE_MASK = TABLE_SIZE - 1;
    private static final long RESET_MASK = 0x7777777777777777L;
    private static final int[] SEEDS = {0x9e3779b9, 0x85ebca6b, 0xc2b2ae35, 0x27d4eb2d};

    private final long[] table = new long[TABLE_SIZE];
    private final long sampleSize = 10L * TABLE_SIZE;
    private long size;

    public void increment(long hash) {
        long mixed = mix64(hash);
        for (int depth = 0; depth < SEEDS.length; depth++) {
            int index = (int) (mix64(mixed + SEEDS[depth]) & TABLE_MASK);
            int nibble = (int) ((mixed >>> (depth * 4)) & 15) * 4;
            long mask = 0xfL << nibble;
            long current = table[index];
            if ((current & mask) != mask) table[index] = current + (1L << nibble);
        }
        if (++size >= sampleSize) reset();
    }

    public int frequency(long hash) {
        long mixed = mix64(hash);
        int frequency = 15;
        for (int depth = 0; depth < SEEDS.length; depth++) {
            int index = (int) (mix64(mixed + SEEDS[depth]) & TABLE_MASK);
            int nibble = (int) ((mixed >>> (depth * 4)) & 15) * 4;
            frequency = Math.min(frequency, (int) ((table[index] >>> nibble) & 15L));
        }
        return frequency;
    }

    public long bytes() { return (long) table.length * Long.BYTES; }

    private void reset() {
        for (int i = 0; i < table.length; i++) {
            long current = table[i];
            table[i] = (current >>> 1) & RESET_MASK;
        }
        size >>>= 1;
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
