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
import org.openjdk.jmh.annotations.Warmup;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.Ticker;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/**
 * End-to-end physical-expiry drain of one TTL bucket. Setup allocates and schedules the entries
 * outside the measured region; the measured method advances a deterministic clock and waits for the
 * maintenance actor to remove every mapping. This deliberately measures actor wakeup, timer-wheel
 * traversal, conditional removal, policy unlink and QSBR retirement preparation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCTimeoutDrainBenchmark {
  private static final long TTL_MILLIS = 64L;
  private static final long EXPIRED_MILLIS = TTL_MILLIS * 2L;

  @Param({"JNA", "UNSAFE"})
  public AllocatorType allocator;

  @Param({"1000", "10000"})
  public int entries;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private EncodedKey[] keys;
  private byte[][] values;
  private MutableTicker ticker;
  private OffHeapCache<byte[], byte[]> cache;
  private long physicalExpiredBefore;

  @Setup(Level.Trial)
  public void createPayloads() {
    keys = new EncodedKey[entries];
    values = new byte[entries][];
    for (int index = 0; index < entries; index++) {
      keys[index] = EncodedKey.copyOf(OHCWriteAdmissionBenchmark.bytes(keyBytes, index));
      values[index] = OHCWriteAdmissionBenchmark.bytes(valueBytes, index * 31 + 7);
    }
  }

  @Setup(Level.Invocation)
  public void scheduleExpiryStorm() {
    ticker = new MutableTicker();
    long capacity = (long) entries * (keyBytes + valueBytes) * 2L;
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(capacity)
                .expectedEntries(entries)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(allocator)
                .ticker(ticker)
                .defaultTTLmillis(TTL_MILLIS)
                .ttlJitterPercent(0d)
                .build();
    for (int index = 0; index < entries; index++) {
      while (cache.mutationBacklogExceeds()) {
        Thread.yield();
      }
      if (!cache.putEncoded(keys[index], values[index])) {
        OHCacheStats stats = cache.stats();
        throw new IllegalStateException(
            "OHC TTL setup rejected entry "
                + index
                + ": queueDepth="
                + stats.getMaintenanceQueueDepth()
                + ", resident="
                + stats.getResidentWeight());
      }
    }
    cache.flushAsync().join();
    physicalExpiredBefore = cache.stats().getPhysicalExpired();
    ticker.setMillis(EXPIRED_MILLIS);
  }

  @Benchmark
  public long drainExpiredBucket() {
    // flush requests a clock refresh and wakes an idle actor; it is not the completion signal.
    cache.flushAsync().join();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
    while (cache.size() != 0L) {
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
            "OHC TTL drain did not finish: " + cache.stats().getTtlBacklog());
      }
      Thread.onSpinWait();
    }
    OHCacheStats stats = cache.stats();
    long expired = stats.getPhysicalExpired() - physicalExpiredBefore;
    if (expired != entries || stats.getMaintenanceUnhealthy()) {
      throw new IllegalStateException(
          "invalid OHC TTL drain: expired="
              + expired
              + ", unhealthy="
              + stats.getMaintenanceUnhealthy());
    }
    return expired;
  }

  @TearDown(Level.Invocation)
  public void closeCache() {
    if (cache != null) {
      cache.close();
    }
    cache = null;
  }

  private static final class MutableTicker implements Ticker {
    private volatile long millis;

    void setMillis(long millis) {
      this.millis = millis;
    }

    @Override
    public long nanos() {
      return millis * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return millis;
    }
  }
}
