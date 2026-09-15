package com.red.ohc.jmh;

import java.lang.reflect.Field;
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
import com.red.ohc.storage.NativeMemory;

/** Measures maxSize replacement and insert churn on the product put path. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCMaxSizeAdmissionBenchmark {
  private static final int MAX_SIZE = 1 << 15;
  private static final int WORKING_SET = MAX_SIZE * 6 / 5;
  private static final int BATCH_SIZE = 1 << 6;
  private static final int KEY_BYTES = 32;

  @Param({"UNSAFE", "JNA"})
  public AllocatorType allocator;

  @Param({"256", "5120", "32768"})
  public int valueBytes;

  @Param({"REPLACE_ONLY", "INSERT_CHURN"})
  public String scenario;

  private byte[][] keys;
  private byte[][] values;
  private OffHeapCache<byte[], byte[]> cache;
  private volatile AdmissionResults lastResults;
  private long smallAllocationFallbackBaseline;
  private boolean replaceOnly;

  @Setup(Level.Trial)
  public void setup() {
    keys = new byte[WORKING_SET][];
    values = new byte[WORKING_SET][];
    replaceOnly = "REPLACE_ONLY".equals(scenario);
    cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .maxSize(MAX_SIZE)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(allocator)
                .build();
    for (int index = 0; index < WORKING_SET; index++) {
      keys[index] = OHCWriteAdmissionBenchmark.bytes(KEY_BYTES, index);
      values[index] = OHCWriteAdmissionBenchmark.bytes(valueBytes, index * 31 + 7);
      if (index < MAX_SIZE) {
        putEventually(index);
      }
    }
    cache.flushAsync().join();
  }

  @Setup(Level.Iteration)
  public void captureMaintenanceBaseline() {
    lastResults = null;
    smallAllocationFallbackBaseline = smallAllocationFallbackCount();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    cache.flushAsync().join();
    try {
      OHCacheStats stats = cache.stats();
      if (stats.maintenanceUnhealthy()
          || stats.maintenanceQueueDepth() != 0L
          || stats.nativeAllocationFailureCount() != 0L
          || retirementQueueDepth() != 0L
          || cache.size() > MAX_SIZE) {
        throw new IllegalStateException(
            "invalid maxSize admission: size="
                + cache.size()
                + ", queue="
                + stats.maintenanceQueueDepth()
                + ", retirement="
                + retirementQueueDepth()
                + ", nativeFailures="
                + stats.nativeAllocationFailureCount()
                + ", unhealthy="
                + stats.maintenanceUnhealthy());
      }
    } finally {
      cache.close();
      if (cache.totalAllocatedBytes() != 0L) {
        throw new IllegalStateException(
            "native memory did not return to zero: " + cache.totalAllocatedBytes());
      }
    }
  }

  @TearDown(Level.Iteration)
  public void captureMaintenanceStats() throws Exception {
    AdmissionResults results = lastResults;
    if (results == null) {
      return;
    }
    OHCacheStats stats = cache.stats();
    results.maintenanceQueueDepth = stats.maintenanceQueueDepth();
    results.nativeAllocationFailures = stats.nativeAllocationFailureCount();
    results.retirementQueueDepth = retirementQueueDepth();
    results.maintenancePassWorkNanos = stats.maintenancePassWorkNanos();
    results.maintenanceActiveNanosTotal = stats.maintenanceActiveNanosTotal();
    results.maintenanceParkNanosTotal = stats.maintenanceParkNanosTotal();
    results.maintenanceImmediateContinuationCount =
        stats.maintenanceImmediateContinuationCount();
    results.retirementSealScannedLanes = stats.retirementSealScannedLanes();
    results.retirementSealSealedLanes = stats.retirementSealSealedLanes();
    results.retirementSealRecords = stats.retirementSealRecords();
    results.retirementSealRecordsTotal = stats.retirementSealRecordsTotal();
    results.retirementReclaimRecordsTotal = stats.retirementReclaimRecordsTotal();
    results.retirementSealScannedLanesTotal = stats.retirementSealScannedLanesTotal();
    results.retirementSealHeadOfLineStops = stats.retirementSealHeadOfLineStops();
    results.retirementReclaimBlockedCount = stats.retirementReclaimBlockedCount();
    results.retirementReclaimBlockedNanos = stats.retirementReclaimBlockedNanos();
    results.retirementRetryWakeCount = stats.retirementRetryWakeCount();
    results.activeReaderCount = stats.activeReaderCount();
    results.accessRingDroppedCount = stats.accessRingDroppedCount();
    results.retirementPublishedRecordsTotal = stats.retirementPublishedRecordsTotal();
    results.retirementCompletedRecordsTotal = stats.retirementCompletedRecordsTotal();
    results.retirementLagRecords = stats.retirementLagRecords();
    results.retirementUnsafeRecords = stats.retirementUnsafeRecords();
    results.retirementUnsafeBytes = stats.retirementUnsafeBytes();
    results.retirementSafeRecords = stats.retirementSafeRecords();
    results.retirementSafeBytes = stats.retirementSafeBytes();
    results.retirementClaimedRecords = stats.retirementClaimedRecords();
    results.retirementClaimedBytes = stats.retirementClaimedBytes();
    results.retirementActorReclaimedRecords = stats.retirementActorReclaimedRecords();
    results.retirementSafeSegmentCount = stats.retirementSafeSegmentCount();
    results.retirementReclaimBatchCount = stats.retirementReclaimBatchCount();
    results.retirementOldestSafeWaitNanos = stats.retirementOldestSafeWaitNanos();
    results.mailboxHeadUnpublishedCount = stats.mailboxHeadUnpublishedCount();
    results.retirementAllocatedSegments = stats.retirementAllocatedSegments();
    results.retirementReusedSegments = stats.retirementReusedSegments();
    results.retirementTrimmedSegments = stats.retirementTrimmedSegments();
    results.asyncMutationQueueDepth = stats.asyncMutationQueueDepth();
    results.asyncMutationPublishedRecords = stats.asyncMutationPublishedRecords();
    results.asyncMutationCompletedRecords = stats.asyncMutationCompletedRecords();
    results.asyncMutationLagRecords = stats.asyncMutationLagRecords();
    results.lifecycleJournalPublishedRecords = stats.lifecycleJournalPublishedRecords();
    results.lifecycleJournalCompletedRecords = stats.lifecycleJournalCompletedRecords();
    results.lifecycleJournalLagRecords = stats.lifecycleJournalLagRecords();
    results.lifecycleJournalAllocatedSegments = stats.lifecycleJournalAllocatedSegments();
    results.lifecycleJournalHeadOfLineStopCount =
        stats.lifecycleJournalHeadOfLineStopCount();
    results.allocatorPageAllocatedCount = stats.allocatorPageAllocatedCount();
    results.allocatorPageReusedCount = stats.allocatorPageReusedCount();
    results.allocatorPageReadyCount = stats.allocatorPageReadyCount();
    results.allocatorPageTrimmedCount = stats.allocatorPageTrimmedCount();
    results.writerResourceActiveCount = stats.writerResourceActiveCount();
    results.writerResourceRetiringCount = stats.writerResourceRetiringCount();
    results.writerResourcePooledCount = stats.writerResourcePooledCount();
    results.nativeAllocatedBytes = cache.totalAllocatedBytes();
    results.directFallbacks = smallAllocationFallbackCount() - smallAllocationFallbackBaseline;
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void oneThread(Cursor cursor, BenchmarkWindowResults window) {
    churn(cursor);
  }

  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(BATCH_SIZE)
  public void eightThreads(Cursor cursor, BenchmarkWindowResults window) {
    churn(cursor);
  }

  /** Diagnostic-only variant; keep counters out of the primary product-path measurement. */
  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(BATCH_SIZE)
  public void diagnosticEightThreads(
      Cursor cursor, AdmissionResults results, BenchmarkWindowResults window) {
    diagnosticChurn(cursor, results);
  }

  /** Per-put latency view for the same replacement workload; use JMH's p99 percentile output. */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(8)
  public void eightThreadsLatency(Cursor cursor, BenchmarkWindowResults window) {
    int slot = cursor.next(replaceOnly);
    cache.put(keys[slot], values[slot]);
  }

  /** Release-gate workload: 64 exclusive writers over disjoint resident-key shards. */
  @Benchmark
  @Threads(64)
  @OperationsPerInvocation(BATCH_SIZE)
  public void sixtyFourThreads(Cursor cursor, BenchmarkWindowResults window) {
    churn(cursor);
  }

  /** Semantic-gate companion; counters are intentionally kept out of the throughput method. */
  @Benchmark
  @Threads(64)
  @OperationsPerInvocation(BATCH_SIZE)
  public void diagnosticSixtyFourThreads(
      Cursor cursor, AdmissionResults results, BenchmarkWindowResults window) {
    diagnosticChurn(cursor, results);
  }

  /** p99 view for the same 64-writer release-gate workload. */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(64)
  public void sixtyFourThreadsLatency(Cursor cursor, BenchmarkWindowResults window) {
    int slot = cursor.next(replaceOnly);
    cache.put(keys[slot], values[slot]);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void cpuThreads(Cursor cursor, BenchmarkWindowResults window) {
    churn(cursor);
  }

  private void churn(Cursor cursor) {
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next(replaceOnly);
      cache.put(keys[slot], values[slot]);
    }
  }

  private void diagnosticChurn(Cursor cursor, AdmissionResults results) {
    if (lastResults == null) {
      lastResults = results;
    }
    for (int index = 0; index < BATCH_SIZE; index++) {
      int slot = cursor.next(replaceOnly);
      try {
        cache.put(keys[slot], values[slot]);
        results.record(slot < MAX_SIZE, true);
      } catch (Throwable failure) {
        results.record(slot < MAX_SIZE, false);
        results.exceptionCount++;
        results.incompleteOperations++;
        throw failure;
      }
    }
  }

  private void putEventually(int index) {
    cache.put(keys[index], values[index]);
  }

  @State(Scope.Thread)
  public static class Cursor {
    private int cursor;
    private int threadIndex;
    private int threadCount;

    @Setup(Level.Trial)
    public void setup(ThreadParams params) {
      cursor = 0;
      threadIndex = params.getThreadIndex();
      threadCount = params.getThreadCount();
    }

    int next(boolean replaceOnly) {
      int ordinal = cursor++;
      if (replaceOnly) {
        return shardIndex(MAX_SIZE, ordinal);
      }
      if ((ordinal & 3) == 0) {
        return MAX_SIZE + shardIndex(WORKING_SET - MAX_SIZE, ordinal >>> 2);
      }
      return shardIndex(MAX_SIZE, ordinal);
    }

    private int shardIndex(int domainSize, int ordinal) {
      int start = domainSize * threadIndex / threadCount;
      int end = domainSize * (threadIndex + 1) / threadCount;
      return start + Math.floorMod(ordinal, Math.max(1, end - start));
    }
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class AdmissionResults {
    public long attempted;
    public long accepted;
    public long rejected;
    public long exceptionCount;
    public long incompleteOperations;
    public long residentAttempted;
    public long residentAccepted;
    public long residentRejected;
    public long newKeyAttempted;
    public long newKeyAccepted;
    public long newKeyRejected;
    public long maintenanceQueueDepth;
    public long nativeAllocationFailures;
    public long retirementQueueDepth;
    public long maintenancePassWorkNanos;
    public long maintenanceActiveNanosTotal;
    public long maintenanceParkNanosTotal;
    public long maintenanceImmediateContinuationCount;
    public long retirementSealScannedLanes;
    public long retirementSealSealedLanes;
    public long retirementSealRecords;
    public long retirementSealRecordsTotal;
    public long retirementReclaimRecordsTotal;
    public long retirementSealScannedLanesTotal;
    public long retirementSealHeadOfLineStops;
    public long retirementReclaimBlockedCount;
    public long retirementReclaimBlockedNanos;
    public long retirementRetryWakeCount;
    public long activeReaderCount;
    public long accessRingDroppedCount;
    public long retirementPublishedRecordsTotal;
    public long retirementCompletedRecordsTotal;
    public long retirementLagRecords;
    public long retirementUnsafeRecords;
    public long retirementUnsafeBytes;
    public long retirementSafeRecords;
    public long retirementSafeBytes;
    public long retirementClaimedRecords;
    public long retirementClaimedBytes;
    public long retirementActorReclaimedRecords;
    public long retirementSafeSegmentCount;
    public long retirementReclaimBatchCount;
    public long retirementOldestSafeWaitNanos;
    public long mailboxHeadUnpublishedCount;
    public long retirementAllocatedSegments;
    public long retirementReusedSegments;
    public long retirementTrimmedSegments;
    public long asyncMutationQueueDepth;
    public long asyncMutationPublishedRecords;
    public long asyncMutationCompletedRecords;
    public long asyncMutationLagRecords;
    public long lifecycleJournalPublishedRecords;
    public long lifecycleJournalCompletedRecords;
    public long lifecycleJournalLagRecords;
    public long lifecycleJournalAllocatedSegments;
    public long lifecycleJournalHeadOfLineStopCount;
    public long allocatorPageAllocatedCount;
    public long allocatorPageReusedCount;
    public long allocatorPageReadyCount;
    public long allocatorPageTrimmedCount;
    public long writerResourceActiveCount;
    public long writerResourceRetiringCount;
    public long writerResourcePooledCount;
    public long nativeAllocatedBytes;
    public long directFallbacks;

    @Setup(Level.Iteration)
    public void reset() {
      attempted = 0L;
      accepted = 0L;
      rejected = 0L;
      exceptionCount = 0L;
      incompleteOperations = 0L;
      residentAttempted = 0L;
      residentAccepted = 0L;
      residentRejected = 0L;
      newKeyAttempted = 0L;
      newKeyAccepted = 0L;
      newKeyRejected = 0L;
      maintenanceQueueDepth = 0L;
      nativeAllocationFailures = 0L;
      retirementQueueDepth = 0L;
      maintenancePassWorkNanos = 0L;
      maintenanceActiveNanosTotal = 0L;
      maintenanceParkNanosTotal = 0L;
      maintenanceImmediateContinuationCount = 0L;
      retirementSealScannedLanes = 0L;
      retirementSealSealedLanes = 0L;
      retirementSealRecords = 0L;
      retirementSealRecordsTotal = 0L;
      retirementReclaimRecordsTotal = 0L;
      retirementSealScannedLanesTotal = 0L;
      retirementSealHeadOfLineStops = 0L;
      retirementReclaimBlockedCount = 0L;
      retirementReclaimBlockedNanos = 0L;
      retirementRetryWakeCount = 0L;
      activeReaderCount = 0L;
      accessRingDroppedCount = 0L;
      retirementPublishedRecordsTotal = 0L;
      retirementCompletedRecordsTotal = 0L;
      retirementLagRecords = 0L;
      retirementUnsafeRecords = 0L;
      retirementUnsafeBytes = 0L;
      retirementSafeRecords = 0L;
      retirementSafeBytes = 0L;
      retirementClaimedRecords = 0L;
      retirementClaimedBytes = 0L;
      retirementActorReclaimedRecords = 0L;
      retirementSafeSegmentCount = 0L;
      retirementReclaimBatchCount = 0L;
      retirementOldestSafeWaitNanos = 0L;
      mailboxHeadUnpublishedCount = 0L;
      retirementAllocatedSegments = 0L;
      retirementReusedSegments = 0L;
      retirementTrimmedSegments = 0L;
      asyncMutationQueueDepth = 0L;
      asyncMutationPublishedRecords = 0L;
      asyncMutationCompletedRecords = 0L;
      asyncMutationLagRecords = 0L;
      lifecycleJournalPublishedRecords = 0L;
      lifecycleJournalCompletedRecords = 0L;
      lifecycleJournalLagRecords = 0L;
      lifecycleJournalAllocatedSegments = 0L;
      lifecycleJournalHeadOfLineStopCount = 0L;
      allocatorPageAllocatedCount = 0L;
      allocatorPageReusedCount = 0L;
      allocatorPageReadyCount = 0L;
      allocatorPageTrimmedCount = 0L;
      writerResourceActiveCount = 0L;
      writerResourceRetiringCount = 0L;
      writerResourcePooledCount = 0L;
      nativeAllocatedBytes = 0L;
      directFallbacks = 0L;
    }

    void record(boolean residentCandidate, boolean wasAccepted) {
      attempted++;
      if (residentCandidate) {
        residentAttempted++;
      } else {
        newKeyAttempted++;
      }
      if (wasAccepted) {
        accepted++;
        if (residentCandidate) {
          residentAccepted++;
        } else {
          newKeyAccepted++;
        }
      } else {
        rejected++;
        if (residentCandidate) {
          residentRejected++;
        } else {
          newKeyRejected++;
        }
      }
    }
  }

  private long retirementQueueDepth() throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    Object worker = workerField.get(cache);
    return ((com.red.ohc.maintenance.MaintenanceEventLoop) worker).retirementQueueDepth();
  }

  private long smallAllocationFallbackCount() {
    try {
      Field memoryField = OffHeapCache.class.getDeclaredField("memory");
      memoryField.setAccessible(true);
      NativeMemory.Memory nativeMemory = (NativeMemory.Memory) memoryField.get(cache);
      return nativeMemory.smallAllocationFallbackCount();
    } catch (ReflectiveOperationException failure) {
      throw new AssertionError("cannot inspect allocator fallback counter", failure);
    }
  }

}
