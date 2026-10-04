package com.red.ohc.jmh;

import java.util.Locale;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.ValueView;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

/** OHC generic API benchmark: raw byte[] codec plus off-heap cache access. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 8, time = 10)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g", "-XX:MaxDirectMemorySize=1g"})
@State(Scope.Benchmark)
public class OHCSerializedBenchmark {
  // Keep the default smoke narrow; expand dimensions explicitly with JMH -p overrides.

  @Param({"32"})
  public int keyBytes;

  @Param({"5120"})
  public int valueBytes;

  @Param({"READ_90_WRITE_10"})
  public String workload;

  @Param({"UNIFORM"})
  public String distribution;

  @Param({"MIXED", "REPLACE_ONLY", "INSERT_CHURN", "NEW_KEY"})
  public String writeShape;

  private SerializedBenchmarkSupport.Dataset dataset;
  private int[] writeSequence;
  private OffHeapCache<byte[], byte[]> cache;
  private WorkloadMode workloadMode;
  private boolean newKeyShape;

  @Setup(Level.Trial)
  public void setup() {
    try {
      workloadMode = WorkloadMode.parse(workload);
      newKeyShape = SerializedBenchmarkSupport.isNewKeyShape(writeShape);
      dataset = SerializedBenchmarkSupport.dataset(keyBytes, valueBytes, distribution);
      writeSequence = SerializedBenchmarkSupport.writeSequence(writeShape, distribution);
      long capacity = SerializedBenchmarkSupport.ohcCapacityBytes(keyBytes, valueBytes);
      cache =
          (OffHeapCache<byte[], byte[]>)
              OHCacheBuilder.<byte[], byte[]>newBuilder()
                  .capacity(capacity)
                  .keySerializer(Utils.byteArraySerializer)
                  .valueSerializer(Utils.lightweightValueSerializer)
                  .eviction(Eviction.S3_FIFO)
                  .defaultTTLmillis(SerializedBenchmarkSupport.TTL_MILLIS)
                  .build();
      for (int i = 0; i < SerializedBenchmarkSupport.CAPACITY_ENTRIES; i++) {
        cache.put(dataset.keys[i], dataset.values[i]);
        if ((i & 1023) == 1023) {
          cache.flushAsync().join();
        }
      }
      cache.flushAsync().join();
      if (cache.get(dataset.keys[0]) == null
          || cache.get(dataset.keys[SerializedBenchmarkSupport.CAPACITY_ENTRIES - 1]) == null) {
        throw new IllegalStateException("OHC preload is not resident");
      }
      assertHealthy();

    } catch (Throwable failure) {
      SerializedBenchmarkSupport.stopOHC(cache, failure);
      cache = null;
      throw failure;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    long flushNanos = 0L;
    long idleNanos = 0L;
    Throwable failure = null;
    try {
      long flushStart = System.nanoTime();
      cache.flushAsync().join();
      flushNanos = Math.max(0L, System.nanoTime() - flushStart);
      long idleStart = System.nanoTime();
      awaitIdle();
      idleNanos = Math.max(0L, System.nanoTime() - idleStart);
      assertHealthy();
    } catch (Throwable operationFailure) {
      failure = operationFailure;
      throw operationFailure;
    } finally {
      long stopStart = System.nanoTime();
      try {
        SerializedBenchmarkSupport.stopOHC(cache, failure);
      } finally {
        System.out.printf(
            Locale.ROOT,
            "lifecycle-end: flushNanos=%d, idleNanos=%d, stopNanos=%d%n",
            flushNanos,
            idleNanos,
            Math.max(0L, System.nanoTime() - stopStart));
      }
    }
  }

  @TearDown(Level.Iteration)
  public void iterationActorStats() {
    OHCacheStats stats = cache.stats();
    System.out.printf(
        Locale.ROOT,
        "actor-stats: wake=%d, pass=%d, cont=%d, activeNs=%d, parkNs=%d, "
            + "collected=%d, blocked=%d, blockedNs=%d, ringDrop=%d, retireDepth=%d%n",
        stats.maintenanceWakeCount(),
        stats.maintenancePassCount(),
        stats.maintenanceImmediateContinuationCount(),
        stats.maintenanceActiveNanosTotal(),
        stats.maintenanceParkNanosTotal(),
        stats.maintenanceCollectedRecordsTotal(),
        stats.retirementReclaimBlockedCount(),
        stats.retirementReclaimBlockedNanos(),
        stats.accessRingDroppedCount(),
        stats.retirementQueueDepth());
  }

  @Benchmark
  @Threads(1)
  public void oneThread(ThreadState state, BenchmarkWindowResults window, Blackhole blackhole) {
    access(state, blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void cpuThreads(ThreadState state, BenchmarkWindowResults window, Blackhole blackhole) {
    access(state, blackhole);
  }

  /** Same workload with end-of-iteration retirement coordination diagnostics. */
  @Benchmark
  @Threads(Threads.MAX)
  public void diagnosticCpuThreads(
      ThreadState state, BenchmarkWindowResults window, DebtResults debt, Blackhole blackhole) {
    access(state, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void directOneThread(
      ThreadState state,
      DirectReadState directState,
      BenchmarkWindowResults window,
      Blackhole blackhole) {
    directAccess(state, directState, blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void directCpuThreads(
      ThreadState state,
      DirectReadState directState,
      BenchmarkWindowResults window,
      Blackhole blackhole) {
    directAccess(state, directState, blackhole);
  }

  private void access(ThreadState state, Blackhole blackhole) {
    state.attemptedOperations++;
    boolean write = workloadMode.isWrite(++state.operations);
    int index = nextIndex(state, write);
    try {
      if (write) {
        state.attemptedWrites++;
        if (write(cache, writeKey(state, index), dataset.values[index])) {
          state.acceptedWrites++;
        } else {
          state.rejectedWrites++;
        }
      } else {
        blackhole.consume(cache.get(dataset.keys[index]));
      }
      state.completedOperations++;
    } catch (RuntimeException | Error failure) {
      state.exceptionCount++;
      state.incompleteOperations++;
      throw failure;
    }
  }

  private void directAccess(ThreadState state, DirectReadState directState, Blackhole blackhole) {
    state.attemptedOperations++;
    boolean write = workloadMode.isWrite(++state.operations);
    int index = nextIndex(state, write);
    try {
      if (write) {
        state.attemptedWrites++;
        if (write(cache, writeKey(state, index), dataset.values[index])) {
          state.acceptedWrites++;
        } else {
          state.rejectedWrites++;
        }
      } else {
        directState.blackhole = blackhole;
        blackhole.consume(cache.getDirect(dataset.keys[index], directState));
      }
      state.completedOperations++;
    } catch (RuntimeException | Error failure) {
      state.exceptionCount++;
      state.incompleteOperations++;
      throw failure;
    }
  }

  private int nextIndex(ThreadState state, boolean write) {
    return nextIndex(state, write, dataset.accessSequence, writeSequence);
  }

  private byte[] writeKey(ThreadState state, int index) {
    return newKeyShape
        ? SerializedBenchmarkSupport.newKey(keyBytes, state.threadIndex, state.newKeyCursor++)
        : dataset.keys[index];
  }

  private boolean write(OffHeapCache<byte[], byte[]> cache, byte[] key, byte[] value) {
    cache.put(key, value);
    return true;
  }

  static int nextIndex(ThreadState state, boolean write, int[] readSequence, int[] writeSequence) {
    int[] sequence = write ? writeSequence : readSequence;
    long cursor = write ? state.writeCursor++ : state.readCursor++;
    return sequence[Math.floorMod(cursor, sequence.length)];
  }

  private void assertHealthy() {
    OHCacheStats stats = cache.stats();
    if (stats.maintenanceUnhealthy() || stats.lifecycleJournalLagRecords() != 0L) {
      throw new IllegalStateException("invalid OHC serialized measurement");
    }
  }

  private void awaitIdle() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
    for (; ; ) {
      OHCacheStats stats = cache.stats();
      if (!stats.maintenanceUnhealthy()
          && stats.lifecycleJournalLagRecords() == 0L
          && stats.retirementSafeSegmentCount() == 0L
          && stats.activeReaderCount() == 0L) {
        return;
      }
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException("OHC serialized measurement did not become idle");
      }
      Thread.yield();
    }
  }

  @AuxCounters(AuxCounters.Type.OPERATIONS)
  @State(Scope.Thread)
  public static class ThreadState {
    private long readCursor;
    private long writeCursor;
    private long newKeyCursor;
    private long operations;
    public long attemptedOperations;
    public long completedOperations;
    public long exceptionCount;
    public long incompleteOperations;
    public long attemptedWrites;
    public long acceptedWrites;
    public long rejectedWrites;
    private int threadIndex;

    @Setup(Level.Trial)
    public void setup(OHCSerializedBenchmark benchmark, ThreadParams params) {
      threadIndex = params.getThreadIndex();
      long offset =
          SerializedBenchmarkSupport.threadStartOffset(
              params.getThreadIndex(),
              params.getThreadCount(),
              SerializedBenchmarkSupport.ACCESS_SEQUENCE_LENGTH);
      readCursor = offset;
      writeCursor = offset;
    }
  }

  @State(Scope.Thread)
  public static class DirectReadState implements DirectValueConsumer {
    @Param({"DIRECT_PRIMITIVE", "DIRECT_FULL_SCAN", "DIRECT_COPY"})
    public String directReadShape;

    private DirectReadShape shape;
    private Blackhole blackhole;
    private byte[] copyTarget;

    @Setup(Level.Trial)
    public void setup(OHCSerializedBenchmark benchmark) {
      shape = DirectReadShape.parse(directReadShape);
      copyTarget = shape == DirectReadShape.DIRECT_COPY ? new byte[benchmark.valueBytes] : null;
    }

    @Override
    public void accept(ValueView value) {
      switch (shape) {
        case DIRECT_PRIMITIVE:
          blackhole.consume(value.getLong(0));
          return;
        case DIRECT_FULL_SCAN:
          blackhole.consume(scan(value));
          return;
        case DIRECT_COPY:
          int length = value.length();
          if (copyTarget.length < length) {
            copyTarget = new byte[length];
          }
          value.copyTo(copyTarget, 0);
          blackhole.consume(copyTarget);
          return;
        default:
          throw new AssertionError("unknown direct read shape: " + shape);
      }
    }

    private long scan(ValueView value) {
      long checksum = 0L;
      int length = value.length();
      int offset = 0;
      for (; offset + Long.BYTES <= length; offset += Long.BYTES) {
        checksum = Long.rotateLeft(checksum ^ value.getLong(offset), 1);
      }
      for (; offset < length; offset++) {
        checksum = checksum * 31L + value.getByte(offset);
      }
      return checksum;
    }
  }

  private enum DirectReadShape {
    DIRECT_PRIMITIVE,
    DIRECT_FULL_SCAN,
    DIRECT_COPY;

    private static DirectReadShape parse(String value) {
      try {
        return valueOf(value);
      } catch (IllegalArgumentException failure) {
        throw new IllegalArgumentException("unsupported direct read shape: " + value, failure);
      }
    }
  }

  private enum WorkloadMode {
    READ_100 {
      @Override
      boolean isWrite(long operation) {
        return false;
      }
    },
    WRITE_100 {
      @Override
      boolean isWrite(long operation) {
        return true;
      }
    },
    READ_90_WRITE_10 {
      @Override
      boolean isWrite(long operation) {
        return Math.floorMod(operation, 10L) == 0L;
      }
    };

    private static WorkloadMode parse(String value) {
      try {
        return valueOf(value);
      } catch (IllegalArgumentException failure) {
        throw new IllegalArgumentException("unsupported workload: " + value, failure);
      }
    }

    abstract boolean isWrite(long operation);
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class DebtResults {
    private boolean capture;
    private long generatedBefore;
    private long completedBefore;
    private long reclaimBatchesBefore;
    private long endNativeDebtHeadroomBytes;
    public long retirementGeneratedBytesDelta;
    public long retirementCompletedBytesDelta;
    public long retirementReclaimBatchCountDelta;
    public long retirementSafeSegmentCount;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      capture = params.getThreadIndex() == 0;
    }

    @Setup(Level.Iteration)
    public void captureBaseline(OHCSerializedBenchmark benchmark) {
      if (!capture) {
        return;
      }
      OHCacheStats stats = benchmark.cache.stats();
      generatedBefore = stats.retirementGeneratedBytesTotal();
      completedBefore = stats.retirementCompletedBytesTotal();
      reclaimBatchesBefore = stats.retirementReclaimBatchCount();
    }

    @TearDown(Level.Iteration)
    public void capture(OHCSerializedBenchmark benchmark) {
      if (!capture) {
        return;
      }
      OHCacheStats stats = benchmark.cache.stats();
      // EVENTS are raw, unnormalised scalar results in JMH. Monotonic counters are converted to
      // per-iteration deltas so warmup and preload history is never included in the measurement.
      // Gauges are printed per iteration because JMH sums EVENTS across iterations.
      endNativeDebtHeadroomBytes = stats.nativeDebtHeadroomBytes();
      retirementGeneratedBytesDelta = stats.retirementGeneratedBytesTotal() - generatedBefore;
      retirementCompletedBytesDelta = stats.retirementCompletedBytesTotal() - completedBefore;
      retirementReclaimBatchCountDelta = stats.retirementReclaimBatchCount() - reclaimBatchesBefore;
      retirementSafeSegmentCount = stats.retirementSafeSegmentCount();
      System.out.printf(
          Locale.ROOT,
          "retirement-end: nativeDebtHeadroomBytes=%d, reclaimBatches=%d, safeSegments=%d%n",
          endNativeDebtHeadroomBytes,
          retirementReclaimBatchCountDelta,
          retirementSafeSegmentCount);
    }
  }
}
