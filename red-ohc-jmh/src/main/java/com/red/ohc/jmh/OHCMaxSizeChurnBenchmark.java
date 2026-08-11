package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** Entry-count churn over a 120% working set, including high-water admission rejections. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCMaxSizeChurnBenchmark {
  private static final int MAX_SIZE = 1 << 15;
  private static final int WORKING_SET = MAX_SIZE * 6 / 5;
  private static final int BATCH_SIZE = 1 << 6;
  private static final int KEY_BYTES = 16;
  private static final int VALUE_BYTES = 256;

  private EncodedKey[] keys;
  private byte[][] values;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new EncodedKey[WORKING_SET];
    values = new byte[WORKING_SET][];
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .maxSize(MAX_SIZE)
                .expectedEntries(MAX_SIZE)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(AllocatorType.UNSAFE)
                .build();
    for (int index = 0; index < WORKING_SET; index++) {
      keys[index] = EncodedKey.copyOf(OHCWriteAdmissionBenchmark.bytes(KEY_BYTES, index));
      values[index] = OHCWriteAdmissionBenchmark.bytes(VALUE_BYTES, index * 31 + 7);
      if (index < MAX_SIZE) {
        putEventually(index);
      }
    }
    cache.flushAsync().join();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.flushAsync().join();
    try {
      OHCacheStats stats = cache.stats();
      if (stats.getMaintenanceUnhealthy()
          || stats.getMaintenanceQueueDepth() != 0L
          || cache.size() > MAX_SIZE) {
        throw new IllegalStateException(
            "invalid maxSize churn: size="
                + cache.size()
                + ", queue="
                + stats.getMaintenanceQueueDepth()
                + ", unhealthy="
                + stats.getMaintenanceUnhealthy());
      }
    } finally {
      cache.close();
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor, Results results) {
    churn(cursor, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor, Results results) {
    churn(cursor, results);
  }

  private void churn(Cursor cursor, Results results) {
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next();
      if (cache.putEncoded(keys[slot], values[slot])) {
        results.accepted++;
      } else {
        results.rejected++;
      }
    }
  }

  private void putEventually(int index) {
    while (!cache.putEncoded(keys[index], values[index])) {
      Thread.yield();
    }
  }

  @State(Scope.Thread)
  public static class Cursor {
    private int cursor;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor = MAX_SIZE + params.getThreadIndex() * BATCH_SIZE;
    }

    int next() {
      return Math.floorMod(cursor++, WORKING_SET);
    }
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Results {
    public long accepted;
    public long rejected;
  }
}
