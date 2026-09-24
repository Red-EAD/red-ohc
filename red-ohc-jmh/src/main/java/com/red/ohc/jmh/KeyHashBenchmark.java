package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.codec.KeyHash;

/** Tracks the serialized-key hash cost; a jump flags a JDK without the multiplyHigh intrinsic. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class KeyHashBenchmark {
  @Param({"16", "32"})
  public int keyBytes;

  private byte[][] keys;

  @Setup
  public void setup() {
    keys = new byte[1024][];
    java.util.Random random = new java.util.Random(0x51ce);
    for (int index = 0; index < keys.length; index++) {
      keys[index] = new byte[keyBytes];
      random.nextBytes(keys[index]);
    }
  }

  @Benchmark
  public void hashKey(Blackhole blackhole) {
    for (byte[] key : keys) {
      blackhole.consume(KeyHash.hash(key, 0, key.length));
    }
  }
}
