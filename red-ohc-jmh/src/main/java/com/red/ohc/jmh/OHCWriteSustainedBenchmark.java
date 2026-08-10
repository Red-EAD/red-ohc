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
import com.red.ohc.api.EncodedKey;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** Sustained producer throughput, throttled only when a shard reaches its queue watermark. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCWriteSustainedBenchmark {
  private static final int KEY_COUNT = 1 << 15;
  private static final int KEY_MASK = KEY_COUNT - 1;
  private static final int BATCH_SIZE = 1 << 6;

  @Param({"JNA", "UNSAFE"})
  public AllocatorType allocator;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private EncodedKey[] keys;
  private byte[][] values;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new EncodedKey[KEY_COUNT];
    values = new byte[KEY_COUNT][];
    long payloadCapacity = (long) KEY_COUNT * (keyBytes + valueBytes) * 2L;
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(payloadCapacity)
                .expectedEntries(KEY_COUNT)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(allocator)
                .build();
    for (int i = 0; i < KEY_COUNT; i++) {
      keys[i] = EncodedKey.copyOf(OHCWriteAdmissionBenchmark.bytes(keyBytes, i));
      values[i] = OHCWriteAdmissionBenchmark.bytes(valueBytes, i * 31 + 7);
      if (!cache.putEncoded(keys[i], values[i])) {
        throw new IllegalStateException("OHC preload rejected");
      }
    }
    cache.flushAsync().join();
    assertHealthyAndDrained();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.flushAsync().join();
    try {
      assertHealthyAndDrained();
    } finally {
      cache.close();
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(WriteCursor cursor) {
    write(cursor);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(WriteCursor cursor) {
    write(cursor);
  }

  private void write(WriteCursor cursor) {
    while (cache.mutationBacklogExceeds()) {
      Thread.yield();
    }
    for (int i = 0; i < BATCH_SIZE; i++) {
      int index = cursor.next();
      if (!cache.putEncoded(keys[index], values[index])) {
        OHCacheStats stats = cache.stats();
        throw new IllegalStateException(
            "OHC encoded write rejected: unhealthy="
                + stats.getMaintenanceUnhealthy()
                + ", queue="
                + stats.getMaintenanceQueueDepth()
                + ", retired="
                + stats.getRetirementQueueDepth());
      }
    }
  }

  private void assertHealthyAndDrained() {
    OHCacheStats stats = cache.stats();
    if (stats.getMaintenanceUnhealthy() || stats.getMaintenanceQueueDepth() != 0L) {
      throw new IllegalStateException("invalid OHC sustained write measurement");
    }
  }

  @State(Scope.Thread)
  public static class WriteCursor {
    private int cursor;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor = params.getThreadIndex() * BATCH_SIZE;
    }

    int next() {
      return cursor++ & KEY_MASK;
    }
  }
}
