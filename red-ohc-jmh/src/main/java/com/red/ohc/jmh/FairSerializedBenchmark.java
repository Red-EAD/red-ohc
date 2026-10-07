package com.red.ohc.jmh;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

/** One workload definition for ordinary full-value get/put APIs. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class FairSerializedBenchmark {
  @State(Scope.Benchmark)
  public static class Shared {
    @Param({"RED_OHC"}) public String backend;
    @Param({"32"}) public int keyBytes;
    @Param({"5120"}) public int valueBytes;
    @Param({"UNIFORM"}) public String distribution;
    @Param({"READ_100"}) public String workload;
    @Param({"RESIDENT"}) public String scenario;
    @Param({"268435456"}) public long capacityBytes;
    @Param({"0"}) public long ttlMillis;
    @Param({"MATERIALIZE"}) public String consumption;
    FairCache cache;
    SerializedBenchmarkSupport.Dataset data;
    int keyBound;

    @Setup(Level.Trial)
    public void setup() {
      if (keyBytes < 8 || valueBytes < 1 || capacityBytes < 1 || ttlMillis < 0) {
        throw new IllegalArgumentException("invalid data or capacity configuration");
      }
      if (!("UNIFORM".equals(distribution) || "ZIPF_099".equals(distribution))) {
        throw new IllegalArgumentException("unsupported distribution");
      }
      if (!("MATERIALIZE".equals(consumption) || "FULL_SCAN".equals(consumption))) {
        throw new IllegalArgumentException("unsupported value consumption");
      }
      SerializedBenchmarkSupport.isWrite(workload, 0);
      boolean pressure = "PRESSURE".equals(scenario);
      if (!(pressure || "RESIDENT".equals(scenario) || "TTL".equals(scenario))) {
        throw new IllegalArgumentException("unsupported scenario");
      }
      if (("TTL".equals(scenario)) != (ttlMillis > 0)) {
        throw new IllegalArgumentException("TTL must be positive only in the TTL scenario");
      }
      if ("CHRONICLE".equals(backend) && !"RESIDENT".equals(scenario)) {
        throw new IllegalArgumentException("Chronicle eviction/TTL comparison is unavailable");
      }
      int preload = pressure ? SerializedBenchmarkSupport.CAPACITY_ENTRIES
          : SerializedBenchmarkSupport.WORKING_SET;
      keyBound = SerializedBenchmarkSupport.WORKING_SET;
      data = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
      try {
        cache = FairCache.create(backend, capacityBytes, preload, keyBytes, valueBytes, ttlMillis);
        for (int i = 0; i < preload; i++) {
          if (!cache.put(data.keys[i], data.values[i])) {
            throw new IllegalStateException("backend rejected preload");
          }
        }
        if ("RESIDENT".equals(scenario)) {
          for (int i = 0; i < preload; i++) {
            if (!Arrays.equals(cache.get(data.keys[i]), data.values[i])) {
              throw new IllegalStateException("resident preload does not fit or differs");
            }
          }
        }
      } catch (RuntimeException | Error failure) {
        FairCache.close(cache, failure);
        cache = null;
        throw failure;
      }
    }

    @TearDown(Level.Trial)
    public void teardown() {
      FairCache owned = cache;
      cache = null;
      FairCache.close(owned, null);
    }
  }

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class Counters {
    public long attempted;
    public long completed;
    public long hits;
    public long misses;
    public long writes;
    public long reportedRejections;
    public long errors;
    private long ordinal;
    private int offset;

    @Setup(Level.Trial)
    public void setup(ThreadParams thread) {
      offset = SerializedBenchmarkSupport.threadStartOffset(thread.getThreadIndex(),
          thread.getThreadCount(), SerializedBenchmarkSupport.ACCESS_SEQUENCE_LENGTH);
    }

    @Setup(Level.Iteration)
    public void reset() {
      attempted = completed = hits = misses = writes = reportedRejections = errors = 0;
    }

    @TearDown(Level.Iteration)
    public void reconcile() {
      if (attempted != completed + errors) {
        throw new IllegalStateException("attempt/completion accounting mismatch");
      }
      System.out.println("RED_OHC_COUNTERS attempted=" + attempted + " completed=" + completed
          + " hits=" + hits + " misses=" + misses + " writes=" + writes
          + " reportedRejections=" + reportedRejections + " errors=" + errors);
    }
  }

  @Benchmark
  public void operation(Shared shared, Counters counters, Blackhole blackhole) {
    long operation = counters.ordinal++;
    int sample = (int) (operation + counters.offset)
        & (SerializedBenchmarkSupport.ACCESS_SEQUENCE_LENGTH - 1);
    int key = shared.data.accessSequence[sample] % shared.keyBound;
    counters.attempted++;
    try {
      if (SerializedBenchmarkSupport.isWrite(shared.workload, operation)) {
        counters.writes++;
        if (!shared.cache.put(shared.data.keys[key], shared.data.values[key])) {
          counters.reportedRejections++;
        }
      } else {
        byte[] value = shared.cache.get(shared.data.keys[key]);
        if (value == null) {
          counters.misses++;
        } else {
          counters.hits++;
        }
        if (value != null && "FULL_SCAN".equals(shared.consumption)) {
          int checksum = 1;
          for (byte next : value) {
            checksum = 31 * checksum + next;
          }
          blackhole.consume(checksum);
        } else {
          blackhole.consume(value);
        }
      }
      counters.completed++;
    } catch (RuntimeException | Error failure) {
      counters.errors++;
      throw failure;
    }
  }
}
