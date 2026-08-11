package com.red.ohc.jmh;

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
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** OHC generic API benchmark: raw byte[] codec plus off-heap cache access. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Benchmark)
public class OHCSerializedBenchmark {
  @Param({"JNA"})
  public AllocatorType allocator;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  @Param({"READ_100", "READ_95_WRITE_5"})
  public String workload;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    long capacity = (long) SerializedBenchmarkSupport.WORKING_SET * (keyBytes + valueBytes) * 2L;
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(capacity)
                .expectedEntries(SerializedBenchmarkSupport.WORKING_SET)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(allocator)
                .build();
    for (int i = 0; i < SerializedBenchmarkSupport.WORKING_SET; i++) {
      if (!cache.put(dataset.keys[i], dataset.values[i])) {
        throw new IllegalStateException("OHC preload rejected");
      }
      if ((i & 1023) == 1023) {
        cache.flushAsync().join();
      }
    }
    cache.flushAsync().join();
    assertHealthy();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.flushAsync().join();
    try {
      assertHealthy();
    } finally {
      cache.close();
    }
  }

  @Benchmark
  @Threads(1)
  public void oneThread(ThreadState state, Blackhole blackhole, WriteResults results) {
    access(state, blackhole, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void cpuThreads(ThreadState state, Blackhole blackhole, WriteResults results) {
    access(state, blackhole, results);
  }

  private void access(ThreadState state, Blackhole blackhole, WriteResults results) {
    int index = dataset.accessSequence[state.cursor++ & (dataset.accessSequence.length - 1)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      results.record(cache.put(dataset.keys[index], dataset.values[index]));
    } else {
      blackhole.consume(SerializedBenchmarkSupport.firstLong(cache.get(dataset.keys[index])));
    }
  }

  private void assertHealthy() {
    OHCacheStats stats = cache.stats();
    if (stats.getMaintenanceUnhealthy() || stats.getMaintenanceQueueDepth() != 0L) {
      throw new IllegalStateException("invalid OHC serialized measurement");
    }
  }

  @State(Scope.Thread)
  public static class ThreadState {
    private int cursor;
    private int operations;
  }
}
