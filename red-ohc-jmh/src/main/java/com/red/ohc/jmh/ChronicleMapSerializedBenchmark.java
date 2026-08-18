package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import net.openhft.chronicle.map.ChronicleMap;
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

/**
 * Chronicle Map baseline using its byte[] serializers.
 *
 * <p>Chronicle Map 3.27ea1 exposes a fixed entry bound but no native TTL or eviction policy, so it
 * is intentionally excluded from the native-policy comparison cohort.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 5)
@Measurement(iterations = 6, time = 10)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g", "-XX:MaxDirectMemorySize=1g"})
@State(Scope.Benchmark)
public class ChronicleMapSerializedBenchmark {
  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_100", "READ_90_WRITE_10", "WRITE_100"})
  public String workload;

  @Param({"UNIFORM"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private SerializedBenchmarkSupport.ChronicleMapStore store;
  private ChronicleMap<byte[], byte[]> map;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    store =
        SerializedBenchmarkSupport.newChronicleMap(
            keyBytes, valueBytes, SerializedBenchmarkSupport.CAPACITY_ENTRIES);
    map = store.map();
    for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
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
    int index =
        dataset.accessSequence[Math.floorMod(state.cursor++, dataset.accessSequence.length)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      map.put(dataset.keys[index], dataset.values[index]);
    } else {
      blackhole.consume(map.get(dataset.keys[index]));
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
