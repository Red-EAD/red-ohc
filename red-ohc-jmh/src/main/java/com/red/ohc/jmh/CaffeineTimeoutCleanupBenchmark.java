package com.red.ohc.jmh;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
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
import org.openjdk.jmh.annotations.Warmup;

import com.red.ohc.jmh.SerializedBenchmarkSupport.RawKey;

/**
 * Caffeine's explicitly caller-assisted expiration cleanup. It is intentionally reported beside,
 * not merged with, OHC's actor drain: {@link Cache#cleanUp()} performs maintenance on the caller
 * while OHC keeps native reclamation and policy work on its maintenance actor.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class CaffeineTimeoutCleanupBenchmark {
  private static final long TTL_MILLIS = 64L;
  private static final long EXPIRED_NANOS = TimeUnit.MILLISECONDS.toNanos(TTL_MILLIS * 2L);

  @Param({"1000", "10000"})
  public int entries;

  @Param({"16", "64"})
  public int keyBytes;

  @Param({"256", "1024"})
  public int valueBytes;

  private RawKey[] keys;
  private byte[][] values;
  private MutableTicker ticker;
  private Cache<RawKey, byte[]> cache;

  @Setup(Level.Trial)
  public void createPayloads() {
    keys = new RawKey[entries];
    values = new byte[entries][];
    for (int index = 0; index < entries; index++) {
      keys[index] = RawKey.copyOf(OHCWriteAdmissionBenchmark.bytes(keyBytes, index));
      values[index] = OHCWriteAdmissionBenchmark.bytes(valueBytes, index * 31 + 7);
    }
  }

  @Setup(Level.Invocation)
  public void populateExpiredEntries() {
    ticker = new MutableTicker();
    cache =
        Caffeine.<RawKey, byte[]>newBuilder()
            .ticker(ticker)
            .expireAfterWrite(Duration.ofMillis(TTL_MILLIS))
            .build();
    for (int index = 0; index < entries; index++) {
      byte[] value = values[index];
      cache.put(keys[index], Arrays.copyOf(value, value.length));
    }
    ticker.nanos = EXPIRED_NANOS;
  }

  @Benchmark
  public long cleanUpExpiredEntries() {
    cache.cleanUp();
    long remaining = cache.estimatedSize();
    if (remaining != 0L) {
      throw new IllegalStateException("Caffeine cleanup left expired entries: " + remaining);
    }
    return entries;
  }

  private static final class MutableTicker implements Ticker {
    private volatile long nanos;

    @Override
    public long read() {
      return nanos;
    }
  }
}
