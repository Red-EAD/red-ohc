package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import org.mapdb.HTreeMap;
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

/** MapDB direct-memory map benchmark using its byte[] serializers. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Benchmark)
public class MapDbSerializedBenchmark {
  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  @Param({"READ_100", "READ_95_WRITE_5"})
  public String workload;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private SerializedBenchmarkSupport.MapDbStore store;
  private HTreeMap<byte[], byte[]> map;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    store = SerializedBenchmarkSupport.newMapDb();
    map = store.map();
    for (int i = 0; i < SerializedBenchmarkSupport.WORKING_SET; i++) {
      map.put(dataset.keys[i], dataset.values[i]);
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
    int index = dataset.accessSequence[state.cursor++ & (dataset.accessSequence.length - 1)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      map.put(dataset.keys[index], dataset.values[index]);
    } else {
      blackhole.consume(SerializedBenchmarkSupport.firstLong(map.get(dataset.keys[index])));
    }
  }

  @State(Scope.Thread)
  public static class ThreadState {
    private int cursor;
    private int operations;
  }
}
