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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;

/** Explicit JNA versus Unsafe write-admission comparison; product default remains JNA. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(3)
@State(Scope.Benchmark)
public class AllocatorBenchmark {
  @Param({"128", "8192"})
  public int valueBytes;

  @Param({"JNA", "UNSAFE"})
  public AllocatorType allocator;

  private OHCache<byte[], byte[]> cache;
  private byte[] value;

  @Setup
  public void setup() {
    value = new byte[valueBytes];
    cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(64L << 20)
            .allocator(allocator)
            .keySerializer(Utils.byteArraySerializer)
            .valueSerializer(Utils.byteArraySerializer)
            .build();
  }

  @TearDown
  public void tearDown() {
    cache.close();
  }

  @Benchmark
  @Threads(1)
  public boolean putOneThread(KeyState state) {
    return cache.put(state.next(), value);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public boolean putCpuThreads(KeyState state) {
    return cache.put(state.next(), value);
  }

  @State(Scope.Thread)
  public static class KeyState {
    private int value;
    private final byte[] key = new byte[16];

    byte[] next() {
      int current = value++;
      key[0] = (byte) current;
      key[1] = (byte) (current >>> 8);
      key[2] = (byte) (current >>> 16);
      key[3] = (byte) (current >>> 24);
      return key;
    }
  }
}
