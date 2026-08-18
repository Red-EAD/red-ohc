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
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.ValueView;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** OHC generic API benchmark: raw byte[] codec plus off-heap cache access. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 5)
@Measurement(iterations = 6, time = 10)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g", "-XX:MaxDirectMemorySize=1g"})
@State(Scope.Benchmark)
public class OHCSerializedBenchmark {
  @Param({"JNA"})
  public AllocatorType allocator;

  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_100", "READ_90_WRITE_10", "WRITE_100"})
  public String workload;

  @Param({"UNIFORM"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    long capacity = SerializedBenchmarkSupport.ohcCapacityBytes(keyBytes, valueBytes);
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(capacity)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .defaultTTLmillis(SerializedBenchmarkSupport.TTL_MILLIS)
                .allocator(allocator)
                .build();
    for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
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
  public void oneThread(ThreadState state, Blackhole blackhole) {
    access(state, blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void cpuThreads(ThreadState state, Blackhole blackhole) {
    access(state, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void directOneThread(ThreadState state, Blackhole blackhole) {
    directAccess(state, blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void directCpuThreads(ThreadState state, Blackhole blackhole) {
    directAccess(state, blackhole);
  }

  private void access(ThreadState state, Blackhole blackhole) {
    int index =
        dataset.accessSequence[Math.floorMod(state.cursor++, dataset.accessSequence.length)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      if (!cache.put(dataset.keys[index], dataset.values[index])) {
        state.rejectedWrites++;
      }
    } else {
      blackhole.consume(cache.get(dataset.keys[index]));
    }
  }

  private void directAccess(ThreadState state, Blackhole blackhole) {
    int index =
        dataset.accessSequence[Math.floorMod(state.cursor++, dataset.accessSequence.length)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      if (!cache.put(dataset.keys[index], dataset.values[index])) {
        state.rejectedWrites++;
      }
    } else {
      state.blackhole = blackhole;
      blackhole.consume(cache.getDirect(dataset.keys[index], state));
    }
  }

  private void assertHealthy() {
    OHCacheStats stats = cache.stats();
    if (stats.maintenanceUnhealthy() || stats.maintenanceQueueDepth() != 0L) {
      throw new IllegalStateException("invalid OHC serialized measurement");
    }
  }

  @State(Scope.Thread)
  public static class ThreadState implements DirectValueConsumer {
    private long cursor;
    private long operations;
    private long rejectedWrites;
    private Blackhole blackhole;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor =
          SerializedBenchmarkSupport.threadStartOffset(
              params.getThreadIndex(),
              params.getThreadCount(),
              SerializedBenchmarkSupport.ACCESS_SEQUENCE_LENGTH);
    }

    @Override
    public void accept(ValueView value) {
      blackhole.consume(value.getLong(0));
    }

    @TearDown(Level.Iteration)
    public void logRejectedWrites() {
      if (rejectedWrites != 0L) {
        System.err.println("OHC diagnostic put_false=" + rejectedWrites);
      }
      rejectedWrites = 0L;
    }
  }
}
