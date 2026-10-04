package com.red.ohc.jmh;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.jmh.SerializedBenchmarkSupport.RawKey;

/** Caffeine-only admission benchmark. It intentionally starts no OHC worker threads. */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class CaffeineWriteBenchmark {
  private static final int KEY_COUNT = 1 << 15;
  private static final int KEY_MASK = KEY_COUNT - 1;
  private static final int BATCH_SIZE = 1 << 8;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private RawKey[] keys;
  private byte[][] values;
  private Cache<RawKey, byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new RawKey[KEY_COUNT];
    values = new byte[KEY_COUNT][];
    long payloadCapacity = (long) KEY_COUNT * (keyBytes + valueBytes) * 2L;
    cache =
        Caffeine.<RawKey, byte[]>newBuilder()
            .maximumWeight(payloadCapacity)
            .weigher((RawKey key, byte[] value) -> key.length() + value.length)
            .build();
    for (int i = 0; i < KEY_COUNT; i++) {
      keys[i] = RawKey.copyOf(OHCWriteAdmissionBenchmark.bytes(keyBytes, i));
      values[i] = OHCWriteAdmissionBenchmark.bytes(valueBytes, i * 31 + 7);
      cache.put(keys[i], values[i]);
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor) {
    write(cursor);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor) {
    write(cursor);
  }

  private void write(Cursor cursor) {
    for (int i = 0; i < BATCH_SIZE; i++) {
      int index = cursor.next();
      cache.put(keys[index], values[index]);
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
      return cursor++ & KEY_MASK;
    }
  }
}
