package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/**
 * Stable 120%-working-set insertion churn using non-blocking producer admission.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCEvictionChurnBenchmark {
  private static final int CAPACITY_ENTRIES = 1 << 15;
  private static final int WORKING_SET = CAPACITY_ENTRIES * 6 / 5;
  private static final int BATCH_SIZE = 1 << 6;

  @Param({"JNA", "UNSAFE"})
  public AllocatorType allocator;

  @Param({"LRU", "W_TINY_LFU", "S3_FIFO"})
  public Eviction eviction;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private byte[][] keys;
  private byte[][] values;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new byte[WORKING_SET][];
    values = new byte[WORKING_SET][];
    long capacity = nativeCapacityFor(CAPACITY_ENTRIES);
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(capacity)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(eviction)
                .allocator(allocator)
                .build();
    for (int index = 0; index < WORKING_SET; index++) {
      keys[index] = OHCWriteAdmissionBenchmark.bytes(keyBytes, index);
      values[index] = OHCWriteAdmissionBenchmark.bytes(valueBytes, index * 31 + 7);
      if (index < CAPACITY_ENTRIES) {
        if (!cache.put(keys[index], values[index])) {
          OHCacheStats stats = cache.stats();
          throw new IllegalStateException(
              "OHC eviction preload rejected at "
                  + index
                  + ": unhealthy="
                  + stats.getMaintenanceUnhealthy());
        }
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
          || stats.getRetirementQueueDepth() != 0L
          || stats.getEvictionCount() == 0L
          || stats.getLiveWeight() > cache.capacity()) {
        throw new IllegalStateException(
            "invalid OHC eviction churn: evictions="
                + stats.getEvictionCount()
                + ", queue="
                + stats.getMaintenanceQueueDepth()
                + ", live="
                + stats.getLiveWeight()
                + ", capacity="
                + cache.capacity());
      }
    } finally {
      cache.close();
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor, WriteResults results) {
    churn(cursor, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor, WriteResults results) {
    churn(cursor, results);
  }

  private void churn(Cursor cursor, WriteResults results) {
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next();
      results.record(cache.put(keys[slot], values[slot]));
    }
  }

  /** The OHC capacity contract is rounded native resident weight, not raw serializer payload. */
  private long nativeCapacityFor(int liveEntries) {
    long keyAllocation = Math.max(8L, CacheMath.roundUpTo8((long) keyBytes + Long.BYTES));
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    long entryWeight =
        WriterArena.allocationWeight(keyAllocation) + WriterArena.allocationWeight(valueAllocation);
    return entryWeight * liveEntries;
  }

  @State(Scope.Thread)
  public static class Cursor {
    private int cursor;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor = params.getThreadIndex() * BATCH_SIZE;
    }

    int next() {
      return cursor++ % WORKING_SET;
    }
  }
}
