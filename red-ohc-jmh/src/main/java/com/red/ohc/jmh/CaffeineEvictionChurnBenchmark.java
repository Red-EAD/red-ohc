package com.red.ohc.jmh;

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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.jmh.SerializedBenchmarkSupport.RawKey;

/**
 * External-throughput counterpart to {@code OHCEvictionChurnBenchmark}. Caffeine has only its
 * default W-TinyLFU policy, so this is comparable to OHC's W_TINY_LFU row, not its S3-FIFO/LRU.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class CaffeineEvictionChurnBenchmark {
  private static final int CAPACITY_ENTRIES = 1 << 15;
  private static final int WORKING_SET = CAPACITY_ENTRIES * 6 / 5;
  private static final int BATCH_SIZE = 1 << 6;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private RawKey[] keys;
  private byte[][] values;
  private Cache<RawKey, byte[]> cache;
  private long maximumWeight;

  @Setup(Level.Trial)
  public void setup() {
    keys = new RawKey[WORKING_SET];
    values = new byte[WORKING_SET][];
    // Caffeine's weigher is payload-only; choose the same target live-entry cardinality as OHC.
    maximumWeight = (long) CAPACITY_ENTRIES * (keyBytes + valueBytes);
    cache =
        Caffeine.<RawKey, byte[]>newBuilder()
            .maximumWeight(maximumWeight)
            .weigher((RawKey key, byte[] value) -> key.length() + value.length)
            .build();
    for (int index = 0; index < WORKING_SET; index++) {
      keys[index] = RawKey.copyOf(OHCWriteAdmissionBenchmark.bytes(keyBytes, index));
      values[index] = OHCWriteAdmissionBenchmark.bytes(valueBytes, index * 31 + 7);
      if (index < CAPACITY_ENTRIES) {
        cache.put(keys[index], Arrays.copyOf(values[index], values[index].length));
      }
    }
    cache.cleanUp();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.cleanUp();
    if (cache.estimatedSize() >= WORKING_SET) {
      throw new IllegalStateException(
          "Caffeine eviction did not converge: size="
              + cache.estimatedSize()
              + ", workingSet="
              + WORKING_SET
              + ", maximumWeight="
              + maximumWeight);
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor) {
    churn(cursor);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor) {
    churn(cursor);
  }

  private void churn(Cursor cursor) {
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next();
      byte[] value = values[slot];
      cache.put(keys[slot], Arrays.copyOf(value, value.length));
    }
  }

  @State(Scope.Thread)
  public static class Cursor {
    private int cursor;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor = params.getThreadIndex() * BATCH_SIZE;
    }

    int next() {
      return cursor++ % WORKING_SET;
    }
  }
}
