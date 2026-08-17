package com.red.ohc.jmh;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** Measures maxSize admission with explicit accepted/rejected and value-sizing counters. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCMaxSizeAdmissionBenchmark {
  private static final int MAX_SIZE = 1 << 15;
  private static final int WORKING_SET = MAX_SIZE * 6 / 5;
  private static final int BATCH_SIZE = 1 << 6;
  private static final int KEY_BYTES = 16;
  private static final int VALUE_BYTES = 256;

  private byte[][] keys;
  private byte[][] values;
  private CountingByteArraySerializer valueSerializer;
  private OffHeapCache<byte[], byte[]> cache;

  @Setup(Level.Trial)
  public void setup() {
    keys = new byte[WORKING_SET][];
    values = new byte[WORKING_SET][];
    valueSerializer = new CountingByteArraySerializer();
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .maxSize(MAX_SIZE)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(valueSerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(AllocatorType.UNSAFE)
                .build();
    for (int index = 0; index < WORKING_SET; index++) {
      keys[index] = OHCWriteAdmissionBenchmark.bytes(KEY_BYTES, index);
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
      if (stats.maintenanceUnhealthy()
          || stats.maintenanceQueueDepth() != 0L
          || cache.size() > MAX_SIZE) {
        throw new IllegalStateException(
            "invalid maxSize admission: size="
                + cache.size()
                + ", queue="
                + stats.maintenanceQueueDepth()
                + ", unhealthy="
                + stats.maintenanceUnhealthy());
      }
    } finally {
      cache.close();
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor, AdmissionResults results) {
    churn(cursor, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor, AdmissionResults results) {
    churn(cursor, results);
  }

  private void churn(Cursor cursor, AdmissionResults results) {
    valueSerializer.takeCurrentThreadCalls();
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next();
      results.record(cache.put(keys[slot], values[slot]));
    }
    results.serializedSizeCalls += valueSerializer.takeCurrentThreadCalls();
  }

  private void putEventually(int index) {
    while (!cache.put(keys[index], values[index])) {
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
  public static class AdmissionResults {
    public long attempted;
    public long accepted;
    public long rejected;
    public long serializedSizeCalls;

    @Setup(Level.Iteration)
    public void reset() {
      attempted = 0L;
      accepted = 0L;
      rejected = 0L;
      serializedSizeCalls = 0L;
    }

    void record(boolean wasAccepted) {
      attempted++;
      if (wasAccepted) {
        accepted++;
      } else {
        rejected++;
      }
    }
  }

  private static final class CountingByteArraySerializer implements CacheSerializer<byte[]> {
    private final ThreadLocal<AtomicLong> serializedSizeCalls =
        ThreadLocal.withInitial(AtomicLong::new);

    @Override
    public void serialize(byte[] value, ByteBuffer buffer) {
      buffer.put(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
      byte[] value = new byte[buffer.remaining()];
      buffer.get(value);
      return value;
    }

    @Override
    public int serializedSize(byte[] value) {
      serializedSizeCalls.get().incrementAndGet();
      return value.length;
    }

    private long takeCurrentThreadCalls() {
      return serializedSizeCalls.get().getAndSet(0L);
    }
  }
}
