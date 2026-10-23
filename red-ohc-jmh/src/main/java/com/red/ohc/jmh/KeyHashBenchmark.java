package com.red.ohc.jmh;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.codec.KeyHash;

/** Amortized key-hash cost across fixed and mixed key lengths and hot or large working sets. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Thread)
public class KeyHashBenchmark {
  private static final int KEYS_PER_INVOCATION = 1_024;
  private static final int MINIMUM_KEY_COUNT = 1_024;
  private static final long SEED = 0x51ceL;

  @Param({"16", "32", "36", "64", "1024"})
  public int keyBytes;

  /** Zero selects the original hot fixture; 64 sizes entries by the maximum key length. */
  @Param({"0", "64"})
  public int workingSetMiB;

  @Param({"FIXED", "MIXED"})
  public String lengths;

  private byte[][] keys;
  private int mask;
  private int cursor;

  @Setup
  public void setup() {
    int count = MINIMUM_KEY_COUNT;
    long targetBytes = (long) workingSetMiB << 20;
    long requestedCount = targetBytes / Math.max(16, keyBytes);
    while (count < requestedCount) {
      count <<= 1;
    }
    keys = new byte[count][];
    mask = count - 1;
    Random random = new Random(SEED);
    for (int index = 0; index < keys.length; index++) {
      int length = "MIXED".equals(lengths) ? random.nextInt(keyBytes + 1) : keyBytes;
      keys[index] = new byte[length];
      random.nextBytes(keys[index]);
    }
  }

  @Benchmark
  @OperationsPerInvocation(KEYS_PER_INVOCATION)
  public void hashKey(Blackhole blackhole) {
    int start = cursor;
    for (int index = 0; index < KEYS_PER_INVOCATION; index++) {
      byte[] key = keys[(start + index) & mask];
      blackhole.consume(KeyHash.hash(key, 0, key.length));
    }
    cursor = (start + KEYS_PER_INVOCATION) & mask;
  }

  @Benchmark
  @OperationsPerInvocation(KEYS_PER_INVOCATION)
  public void traversalControl(Blackhole blackhole) {
    int start = cursor;
    for (int index = 0; index < KEYS_PER_INVOCATION; index++) {
      blackhole.consume(keys[(start + index) & mask].length);
    }
    cursor = (start + KEYS_PER_INVOCATION) & mask;
  }
}
