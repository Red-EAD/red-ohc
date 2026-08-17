package com.red.ohc.jmh;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.api.EncodedKey;

/** Caffeine ordinary owned-byte[] counterpart to {@link OHCSerializedBenchmark}. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class CaffeineReadBenchmark {
  private static final int WORKING_SET = 24_576;

  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_90_WRITE_10", "READ_100"})
  public String workload;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  private EncodedKey[] keys;
  private byte[][] values;
  private int[] accessSequence;
  private Cache<EncodedKey, byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new EncodedKey[WORKING_SET];
    values = new byte[WORKING_SET][];
    for (int i = 0; i < WORKING_SET; i++) {
      keys[i] = EncodedKey.copyOf(bytes(keyBytes, i));
      values[i] = bytes(valueBytes, i * 31 + 7);
    }
    accessSequence =
        "ZIPF_099".equals(distribution) ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET);
    cache =
        Caffeine.<EncodedKey, byte[]>newBuilder()
            .maximumSize(SerializedBenchmarkSupport.CAPACITY_ENTRIES)
            .expireAfterWrite(Duration.ofMillis(SerializedBenchmarkSupport.TTL_MILLIS))
            .build();
    for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
      putOwned(cache, keys[i], values[i]);
    }
    cache.cleanUp();
    if (cache.estimatedSize() != SerializedBenchmarkSupport.CAPACITY_ENTRIES) {
      throw new IllegalStateException(
          "HIT_ONLY preload was evicted: size=" + cache.estimatedSize());
    }
  }

  @Benchmark
  @Threads(1)
  public void oneThread(ThreadState state, Blackhole blackhole) {
    access(state, blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void cpuThreads(ThreadState state, Blackhole blackhole) {
    access(state, blackhole);
  }

  private void access(ThreadState state, Blackhole blackhole) {
    int index = state.next(accessSequence);
    if (state.write(workload)) {
      putOwned(cache, keys[index], values[index]);
      return;
    }
    blackhole.consume(getOwned(cache, keys[index]));
  }

  static void putOwned(Cache<EncodedKey, byte[]> cache, EncodedKey key, byte[] value) {
    cache.put(key, SerializedBenchmarkSupport.ownedCopy(value));
  }

  static byte[] getOwned(Cache<EncodedKey, byte[]> cache, EncodedKey key) {
    return SerializedBenchmarkSupport.ownedCopy(cache.getIfPresent(key));
  }

  @State(Scope.Thread)
  public static class ThreadState {
    private long cursor;
    private long operations;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor =
          SerializedBenchmarkSupport.threadStartOffset(
              params.getThreadIndex(),
              params.getThreadCount(),
              SerializedBenchmarkSupport.ACCESS_SEQUENCE_LENGTH);
    }

    int next(int[] sequence) {
      return sequence[Math.floorMod(cursor++, sequence.length)];
    }

    boolean write(String mix) {
      return SerializedBenchmarkSupport.isWrite(mix, ++operations);
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
