package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import org.ehcache.Cache;
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

/** Ehcache 3 in-memory off-heap benchmark using the same raw byte[] codec as OHC. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Benchmark)
public class EhcacheSerializedBenchmark {
  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_90_WRITE_10", "READ_100"})
  public String workload;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private SerializedBenchmarkSupport.EhcacheStore store;
  private Cache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    long capacity = SerializedBenchmarkSupport.logicalCapacityBytes(keyBytes, valueBytes);
    store = SerializedBenchmarkSupport.newEhcache(capacity, SerializedBenchmarkSupport.TTL_MILLIS);
    cache = store.cache();
    for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
      cache.put(dataset.keys[i], dataset.values[i]);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    store.close();
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
    int index =
        dataset.accessSequence[Math.floorMod(state.cursor++, dataset.accessSequence.length)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      cache.put(dataset.keys[index], dataset.values[index]);
    } else {
      blackhole.consume(cache.get(dataset.keys[index]));
    }
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
  }
}
