package com.red.ohc.jmh;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.AccessRing;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/**
 * Per-op cost ladder of four high-frequency engine components in isolation, with no index, no
 * allocator traffic and no actor thread in the loop. Every arm is a single-thread round trip, so
 * the numbers are the component bookkeeping floor a writer or reader pays before any cache work.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ComponentLadderBenchmark {
  private static final Entry TOKEN_ENTRY = new Entry(0L, 0, 1L);
  /** Ring record columns: one aligned value address, one generation, one policy state. */
  static final long RING_VALUE_ADDRESS = 0x40L;
  static final long RING_GENERATION = 7L;
  static final int RING_POLICY_STATE = 3;
  /** Lane mutation record seeds, taken from WriterLifecycleLaneTest's primitive-seed test. */
  static final int LANE_KEY_HASH = 0x5566_7788;
  static final long LANE_VALUE_ALLOCATION = 2_048L;
  static final long LANE_MUTATION_VERSION = 17L;
  /** Ledger entry shape: a 16-byte key and a 64-byte published value allocation. */
  static final int ADMISSION_KEY_LENGTH = 16;
  static final long ADMISSION_VALUE_ADDRESS = 0x40L;
  static final long ADMISSION_VALUE_ALLOCATION = 64L;

  /**
   * Reader scope enter/exit pair ({@code OffHeapCache.enterWriterReader} plus {@code exit}): the
   * QSBR sequence-word publish at entry and the quiescence store plus notification probe at exit.
   */
  @Benchmark
  public long readerScopeEnterExit(ReaderScopeState state) {
    state.guard.enterLookupAfterAdmission(state.context);
    state.guard.exit(state.context);
    return state.context.readerDepth();
  }

  /** AccessRing publish-then-consume round trip: the four-column record bookkeeping only. */
  @Benchmark
  public long accessRingOfferPoll(RingState state) {
    AccessRing ring = state.ring;
    ring.offer(TOKEN_ENTRY, RING_VALUE_ADDRESS, RING_GENERATION, RING_POLICY_STATE);
    ring.poll(state.consumer);
    return state.polledValueAddress;
  }

  /**
   * Full writer lifecycle lane cycle: reserve, writeMutation, commit, then the actor-side poll and
   * release, so one record traverses both cursors of the lane every operation.
   */
  @Benchmark
  public long laneReserveCommitPoll(LaneState state) {
    WriterLifecycleLane lane = state.lane;
    long sequence = lane.reserve();
    lane.writeMutation(
        sequence, state.entry, LANE_KEY_HASH, LANE_VALUE_ALLOCATION, LANE_MUTATION_VERSION);
    lane.commit(sequence);
    lane.poll(state.record);
    lane.release(state.record);
    return sequence;
  }

  /**
   * LogicalAdmission write-path ledger cycle: reserve the candidate charge, publish the mapping,
   * then remove it. The reservation carries the mapping's own charge exactly as the insert-first
   * transaction does, so publish uses {@code markPresentAfterCharge} (the charging
   * {@code markPresent} would double-count) and the ledger returns to zero every operation.
   */
  @Benchmark
  public long logicalAdmissionCharge(AdmissionState state) {
    LogicalAdmission admission = state.admission;
    boolean reserved = admission.tryChargeDelta(state.charge);
    admission.markPresentAfterCharge(state.entry);
    admission.markAbsent(state.entry);
    return reserved ? state.charge : 0L;
  }

  /**
   * Reader-scope fixture assembled exactly as ReaderGuardTest does it: a native reader registry
   * slot table, the maintenance event loop that owns registration and quiescence, and the guard
   * plus per-thread context. The loop thread is constructed but never started, so no actor runs
   * concurrently and the pair is measured without wake traffic.
   */
  @State(Scope.Thread)
  public static class ReaderScopeState {
    NativeMemory.Memory memory;
    ReaderRegistry readers;
    ReaderGuard guard;
    ThreadContext context;

    @Setup
    public void setup() {
      memory = new NativeMemory.Memory();
      readers = new ReaderRegistry(memory);
      MaintenanceEventLoop loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(),
              memory,
              Ticker.DEFAULT,
              1L << 20,
              Eviction.LRU,
              readers,
              Long.MAX_VALUE);
      guard = new ReaderGuard(loop);
      context = new ThreadContext(null);
    }

    @TearDown
    public void tearDown() {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  /**
   * Bare ring with no non-empty signal bound, mirroring the lazy per-thread allocation in
   * {@code ThreadContext.accessRing} but without the actor wake callback. Producer and consumer are
   * the same thread, so this measures ring bookkeeping only, never cross-core wake-up latency; the
   * armed-state probe is skipped after the first offer for the same reason it is skipped in
   * production while the actor stays awake.
   */
  @State(Scope.Thread)
  public static class RingState {
    AccessRing ring;
    AccessConsumer consumer;
    long polledValueAddress;

    @Setup
    public void setup() {
      ring = new AccessRing();
      consumer =
          (entry, observedValueAddress, observedGeneration, observedPolicyState) ->
              polledValueAddress = observedValueAddress;
    }
  }

  /** Unbound lane (no journal, no ready signal): both cursors are owned by this thread. */
  @State(Scope.Thread)
  public static class LaneState {
    WriterLifecycleLane lane;
    WriterLifecycleLane.Record record;
    Entry entry;

    @Setup
    public void setup() {
      lane = new WriterLifecycleLane(64);
      record = new WriterLifecycleLane.Record();
      entry = new Entry(0L, 0, 1L);
    }
  }

  /**
   * Byte-bounded ledger over one real native-key entry, built the way LogicalAdmissionTest's
   * native-entry test does it. A live entry is required because the byte-bounded charge reads the
   * published native value allocation; the entry cycles absent to present and back every op.
   */
  @State(Scope.Thread)
  public static class AdmissionState {
    NativeMemory.Memory memory;
    WriterArena arena;
    LogicalAdmission admission;
    Entry entry;
    long charge;

    @Setup
    public void setup() {
      memory = new NativeMemory.Memory();
      arena = memory.newWriterArena();
      long address = arena.allocate(Entry.keyPhysicalAllocationLengthForKeyLength(ADMISSION_KEY_LENGTH));
      long tagged = Entry.absentTaggedValue(ADMISSION_VALUE_ADDRESS, false);
      entry = new Entry(address, ADMISSION_KEY_LENGTH, tagged);
      entry.initializeKeyHash(LANE_KEY_HASH);
      entry.initializeNativeMetadata();
      entry.currentValueAllocation(ADMISSION_VALUE_ALLOCATION);
      admission = new LogicalAdmission(1_000L, false);
      charge = CacheMath.logicalEntryBytes(entry.keyAllocationLength(), entry.currentValueAllocation());
    }

    @TearDown
    public void tearDown() {
      memory.closeArenas();
    }
  }
}
