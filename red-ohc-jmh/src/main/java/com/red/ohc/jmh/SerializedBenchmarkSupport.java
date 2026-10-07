package com.red.ohc.jmh;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Backend-independent datasets and serialized workload helpers. */
public final class SerializedBenchmarkSupport {
  public static final int WORKING_SET = 24_576;
  public static final int DEFAULT_KEY_BYTES = 32;
  public static final int DEFAULT_VALUE_BYTES = 5 * 1024;
  public static final int CAPACITY_ENTRIES = WORKING_SET * 4 / 5;
  public static final long TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);
  public static final int ACCESS_SEQUENCE_LENGTH = 1 << 16;

  private SerializedBenchmarkSupport() {}

  public static Dataset dataset(int keyBytes, int valueBytes, String distribution) {
    byte[][] keys = new byte[WORKING_SET][];
    byte[][] values = new byte[WORKING_SET][];
    for (int i = 0; i < WORKING_SET; i++) {
      keys[i] = bytes(keyBytes, i);
      values[i] = bytes(valueBytes, i * 31 + 7);
    }
    return new Dataset(keys, values, sequenceForDistribution(WORKING_SET, distribution));
  }

  public static int[] writeSequence(String shape, String distribution) {
    switch (shape) {
      case "REPLACE_ONLY":
        return sequenceForDistribution(CAPACITY_ENTRIES, distribution);
      case "MIXED":
        return sequenceForDistribution(WORKING_SET, distribution);
      case "INSERT_CHURN":
        return insertChurnSequence(distribution);
      case "NEW_KEY":
        // The sequence keeps value selection deterministic; the key itself is generated from the
        // writer thread and its monotonic operation ordinal by newKey().
        return sequenceForDistribution(CAPACITY_ENTRIES, distribution);
      default:
        throw new IllegalArgumentException("unsupported write shape: " + shape);
    }
  }

  public static boolean isNewKeyShape(String shape) {
    return "NEW_KEY".equals(shape);
  }

  public static boolean isWrite(String workload, long operation) {
    if ("READ_100".equals(workload)) {
      return false;
    }
    if ("WRITE_100".equals(workload)) {
      return true;
    }
    if ("READ_90_WRITE_10".equals(workload)) {
      return Math.floorMod(operation, 10L) == 0L;
    }
    throw new IllegalArgumentException("unsupported workload: " + workload);
  }

  public static int threadStartOffset(int threadIndex, int threadCount, int sequenceLength) {
    if (threadIndex < 0 || threadCount <= 0 || sequenceLength <= 0) {
      throw new IllegalArgumentException("invalid thread sequence parameters");
    }
    return (int) ((long) sequenceLength * threadIndex / threadCount);
  }

  public static int benchmarkThreadCount() {
    int configured =
        Integer.getInteger("redohc.benchmark.threads", Runtime.getRuntime().availableProcessors());
    if (configured <= 0) {
      throw new IllegalArgumentException("redohc.benchmark.threads must be positive");
    }
    return configured;
  }

  public static byte[] ownedCopy(byte[] value) {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  /**
   * Creates a deterministic key for a sustained-new-key workload. The high 32 bits identify the JMH
   * writer thread and the low 32 bits identify that thread's write ordinal, so benchmark threads
   * never intentionally collide within the measured run.
   */
  public static byte[] newKey(int keyBytes, int threadIndex, long writeOrdinal) {
    if (keyBytes < Long.BYTES) {
      throw new IllegalArgumentException("NEW_KEY requires at least 8 key bytes");
    }
    if (threadIndex < 0 || writeOrdinal < 0L || writeOrdinal > 0xffff_ffffL) {
      throw new IllegalArgumentException("invalid NEW_KEY writer identity");
    }
    long identity = ((long) threadIndex << 32) | writeOrdinal;
    byte[] key = bytes(keyBytes, identity ^ 0xd1b54a32d192ed03L);
    for (int index = 0; index < Long.BYTES; index++) {
      key[keyBytes - Long.BYTES + index] = (byte) (identity >>> (56 - index * 8));
    }
    return key;
  }

  /** Benchmark-local content key; it keeps raw-key semantics without using OHC's EncodedKey API. */
  public static final class RawKey {
    private final byte[] bytes;
    private final int hashCode;

    private RawKey(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
    }

    public static RawKey copyOf(byte[] source) {
      if (source == null) {
        throw new NullPointerException("source");
      }
      return new RawKey(Arrays.copyOf(source, source.length));
    }

    public int length() {
      return bytes.length;
    }

    @Override
    public boolean equals(Object other) {
      return other == this
          || (other instanceof RawKey && Arrays.equals(bytes, ((RawKey) other).bytes));
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static byte[] bytes(int length, long seed) {
    byte[] bytes = new byte[length];
    long value = seed * 0x9e3779b97f4a7c15L;
    for (int i = 0; i < length; i++) {
      value ^= value >>> 12;
      value ^= value << 25;
      value ^= value >>> 27;
      bytes[i] = (byte) value;
    }
    return bytes;
  }

  private static int[] uniformSequence(int bound) {
    int[] sequence = new int[1 << 16];
    long seed = 1L;
    for (int i = 0; i < sequence.length; i++) {
      seed ^= seed << 13;
      seed ^= seed >>> 7;
      seed ^= seed << 17;
      sequence[i] = (int) Long.remainderUnsigned(seed, bound);
    }
    return sequence;
  }

  private static int[] sequenceForDistribution(int bound, String distribution) {
    return "ZIPF_099".equals(distribution) ? zipfSequence(bound) : uniformSequence(bound);
  }

  private static int[] insertChurnSequence(String distribution) {
    int outsideEntries = WORKING_SET - CAPACITY_ENTRIES;
    int[] resident = sequenceForDistribution(CAPACITY_ENTRIES, distribution);
    int[] outside = sequenceForDistribution(outsideEntries, distribution);
    int[] sequence = new int[ACCESS_SEQUENCE_LENGTH];
    for (int index = 0; index < sequence.length; index++) {
      int sample = index >>> 1;
      if ((index & 1) == 0) {
        sequence[index] = CAPACITY_ENTRIES + outside[sample % outside.length];
      } else {
        sequence[index] = resident[sample % resident.length];
      }
    }
    return sequence;
  }

  private static int[] zipfSequence(int bound) {
    double[] cdf = new double[bound];
    double sum = 0d;
    for (int rank = 1; rank <= bound; rank++) {
      sum += 1d / Math.pow(rank, .99d);
    }
    double running = 0d;
    for (int rank = 1; rank <= bound; rank++) {
      running += 1d / Math.pow(rank, .99d) / sum;
      cdf[rank - 1] = running;
    }
    int[] sequence = new int[1 << 16];
    long seed = 7L;
    for (int i = 0; i < sequence.length; i++) {
      seed ^= seed << 13;
      seed ^= seed >>> 7;
      seed ^= seed << 17;
      double sample = (seed >>> 11) * 0x1.0p-53d;
      int low = 0;
      int high = cdf.length - 1;
      while (low < high) {
        int middle = (low + high) >>> 1;
        if (cdf[middle] < sample) {
          low = middle + 1;
        } else {
          high = middle;
        }
      }
      sequence[i] = low;
    }
    return sequence;
  }

  public static final class Dataset {
    public final byte[][] keys;
    public final byte[][] values;
    public final int[] accessSequence;

    private Dataset(byte[][] keys, byte[][] values, int[] accessSequence) {
      this.keys = keys;
      this.values = values;
      this.accessSequence = accessSequence;
    }
  }

}
