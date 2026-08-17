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
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

/** Redis service benchmark with server-side maxmemory eviction and write TTL. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 15)
@Measurement(iterations = 8, time = 30)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms512m", "-Xmx512m"})
@State(Scope.Benchmark)
public class RedisSerializedBenchmark {
  private static final long REDIS_BOOTSTRAP_MAX_MEMORY_BYTES = 256L * 1024L * 1024L;
  private static final long REDIS_GLOBAL_HEADROOM_BYTES = 8L * 1024L * 1024L;
  private static final int REDIS_MEMORY_USAGE_SAMPLE_SIZE = 128;

  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_90_WRITE_10", "READ_100"})
  public String workload;

  @Param({"UNIFORM", "ZIPF_099"})
  public String distribution;

  private SerializedBenchmarkSupport.Dataset dataset;
  private JedisPooled redis;

  @Setup(Level.Trial)
  public void setup() {
    dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
    String host = System.getProperty("redohc.redis.host", "127.0.0.1");
    int port = Integer.getInteger("redohc.redis.port", 6379);
    redis = new JedisPooled(host, port);
    if (!"PONG".equals(redis.ping())) {
      throw new IllegalStateException("Redis ping failed");
    }
    redis.flushDB();
    redis.configSet(
        "maxmemory",
        Long.toString(REDIS_BOOTSTRAP_MAX_MEMORY_BYTES));
    redis.configSet("maxmemory-policy", "allkeys-lru");
    SetParams params = SetParams.setParams().px(SerializedBenchmarkSupport.TTL_MILLIS);
    for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
      redis.set(dataset.keys[i], dataset.values[i], params);
    }
    if (redis.dbSize() != SerializedBenchmarkSupport.CAPACITY_ENTRIES) {
      throw new IllegalStateException("Redis preload was evicted");
    }
    long perEntryMemory = sampledEntryMemory(redis);
    long maxMemory =
        perEntryMemory * SerializedBenchmarkSupport.CAPACITY_ENTRIES
            + REDIS_GLOBAL_HEADROOM_BYTES;
    redis.configSet("maxmemory", Long.toString(maxMemory));
    if (redis.dbSize() != SerializedBenchmarkSupport.CAPACITY_ENTRIES) {
      throw new IllegalStateException("Redis capacity budget evicted preload");
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (redis != null) {
      try {
        redis.flushDB();
      } finally {
        redis.close();
      }
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

  private void access(ThreadState state, Blackhole blackhole) {
    int index =
        dataset.accessSequence[Math.floorMod(state.cursor++, dataset.accessSequence.length)];
    if (SerializedBenchmarkSupport.isWrite(workload, ++state.operations)) {
      redis.set(
          dataset.keys[index],
          dataset.values[index],
          SetParams.setParams().px(SerializedBenchmarkSupport.TTL_MILLIS));
    } else {
      blackhole.consume(redis.get(dataset.keys[index]));
    }
  }

  private long sampledEntryMemory(JedisPooled redis) {
    long total = 0L;
    int sampleSize = Math.min(REDIS_MEMORY_USAGE_SAMPLE_SIZE, dataset.keys.length);
    for (int i = 0; i < sampleSize; i++) {
      Long memory = redis.memoryUsage(dataset.keys[i]);
      if (memory == null) {
        throw new IllegalStateException("Redis memory usage was not reported");
      }
      total += memory;
    }
    return (total + sampleSize - 1L) / sampleSize;
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
