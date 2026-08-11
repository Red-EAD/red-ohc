package com.red.ohc.jmh;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.DirectEntryConsumer;
import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.ValueView;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/**
 * OHC-only direct-read benchmark. Caffeine runs in a separate benchmark process so its result
 * cannot inherit idle OHC maintenance activity.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCBenchmark {
  @Param({"JNA", "UNSAFE"})
  public AllocatorType allocator;
  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  @Param({"READ_100", "READ_99_WRITE_1", "READ_95_WRITE_5"})
  public String workload;

  @Param({"HIT_ONLY", "EVICTION_120"})
  public String residency;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  @Param({"LRU", "W_TINY_LFU", "S3_FIFO"})
  public Eviction eviction;

  private static final int WORKING_SET = 24_576;
  private byte[][] rawKeys;
  private EncodedKey[] encodedKeys;
  private byte[][] values;
  private List<byte[]> directBatchKeys;
  private int[] accessSequence;
  private OffHeapCache<byte[], byte[]> ohc;

  @Setup(Level.Trial)
  public void setup() {
    long payloadCapacity = capacityFor(residency, keyBytes, valueBytes);
    rawKeys = new byte[WORKING_SET][];
    encodedKeys = new EncodedKey[WORKING_SET];
    values = new byte[WORKING_SET][];
    for (int i = 0; i < WORKING_SET; i++) {
      rawKeys[i] = bytes(keyBytes, i);
      encodedKeys[i] = EncodedKey.copyOf(rawKeys[i]);
      values[i] = bytes(valueBytes, i * 31 + 7);
    }
    directBatchKeys = Arrays.asList(Arrays.copyOf(rawKeys, 512));
    accessSequence =
        "ZIPF_099".equals(distribution) ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET);
    ohc =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(payloadCapacity)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .expectedEntries(WORKING_SET)
                .eviction(eviction)
                .allocator(allocator)
                .build();
    for (int i = 0; i < WORKING_SET; i++) {
      ohc.putEncoded(encodedKeys[i], values[i]);
      if ((i & 1023) == 1023) {
        ohc.flushAsync().join();
      }
    }
    ohc.flushAsync().join();
    if ("HIT_ONLY".equals(residency)) {
      if (ohc.size() != WORKING_SET
          || ohc.stats().getEvictionCount() != 0L
          || ohc.stats().getReadMisses() != 0L) {
        throw new IllegalStateException(
            "HIT_ONLY preload was not a complete hit set: size="
                + ohc.size()
                + ", evicted="
                + ohc.stats().getEvictionCount()
                + ", misses="
                + ohc.stats().getReadMisses());
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    ohc.close();
  }

  @Benchmark
  @Threads(1)
  public void ohcDirectOneThread(
      ThreadState state, Blackhole blackhole, WriteResults results) {
    accessOHC(state, blackhole, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void ohcDirectCpuThreads(
      ThreadState state, Blackhole blackhole, WriteResults results) {
    accessOHC(state, blackhole, results);
  }

  @Benchmark
  @Threads(1)
  public void ohcDirectAllOneThread(ThreadState state, Blackhole blackhole) {
    state.blackhole = blackhole;
    blackhole.consume(ohc.getDirectAll(directBatchKeys, state));
  }

  private void accessOHC(ThreadState state, Blackhole blackhole, WriteResults results) {
    int index = state.next(accessSequence);
    if (state.write(workload)) {
      results.record(ohc.putEncoded(encodedKeys[index], values[index]));
      return;
    }
    state.blackhole = blackhole;
    blackhole.consume(ohc.getDirect(rawKeys[index], state));
  }

  @State(Scope.Thread)
  public static class ThreadState implements DirectValueConsumer, DirectEntryConsumer<byte[]> {
    private int cursor;
    private int writes;
    private Blackhole blackhole;

    int next(int[] sequence) {
      int index = sequence[cursor++ & (sequence.length - 1)];
      return index;
    }

    boolean write(String mix) {
      if ("READ_100".equals(mix)) {
        return false;
      }
      int every = "READ_99_WRITE_1".equals(mix) ? 100 : 20;
      return ++writes % every == 0;
    }

    @Override
    public void accept(ValueView value) {
      blackhole.consume(value.getLong(0));
    }

    @Override
    public void accept(byte[] key, ValueView value) {
      blackhole.consume(value.getLong(0));
    }
  }

  private static byte[] bytes(int length, int seed) {
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

  static long capacityFor(String residency, int keyBytes, int valueBytes) {
    long keyAllocation = Math.max(8L, CacheMath.roundUpTo8((long) keyBytes + Long.BYTES));
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    long allocationWeightPerEntry =
        WriterArena.allocationWeight(keyAllocation) + WriterArena.allocationWeight(valueAllocation);
    long residentEntries = "HIT_ONLY".equals(residency) ? WORKING_SET : WORKING_SET * 5L / 6L;
    // Match MaintenancePolicy's allocator-weight accounting and leave 25% headroom for churn.
    return residentEntries * allocationWeightPerEntry * 4L / 3L + allocationWeightPerEntry;
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
      int position = Arrays.binarySearch(cdf, sample);
      sequence[i] = position >= 0 ? position : -position - 1;
    }
    return sequence;
  }
}
