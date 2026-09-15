package com.red.ohc.maintenance;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReference;

import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/**
 * A fixed-layout native retirement segment. Its three payload columns live off heap; Java retains
 * only the descriptor and segment-level state.
 */
public final class RetirementSegment {
  public static final int CAPACITY = 256;
  private static final int WORD_BYTES = Long.BYTES;
  private static final long PAYLOAD_WORDS_PER_RECORD = 3L;
  private static final int COUNT_BITS = 9;
  private static final int GENERATION_SHIFT = COUNT_BITS + 1;
  private static final long COUNT_MASK = (1L << COUNT_BITS) - 1L;
  private static final long CLOSED = 1L << COUNT_BITS;
  private static final long GENERATION_MASK = -1L >>> GENERATION_SHIFT;
  private static final int RECYCLE_CLOSED = 1 << 31;
  private static final VarHandle RESERVATION_STATE;
  private static final VarHandle COMMITTED_TAIL;
  private static final VarHandle SEGMENT_CLAIM_STATE;
  private static final VarHandle LINK_IN_FLIGHT;
  private static final VarHandle OWNERSHIP_GATE;
  private static final VarHandle PAYLOAD_FREED;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      RESERVATION_STATE =
          lookup.findVarHandle(RetirementSegment.class, "reservationState", long.class);
      COMMITTED_TAIL =
          lookup.findVarHandle(RetirementSegment.class, "committedTail", int.class);
      SEGMENT_CLAIM_STATE =
          lookup.findVarHandle(RetirementSegment.class, "segmentClaimState", int.class);
      LINK_IN_FLIGHT = lookup.findVarHandle(RetirementSegment.class, "linkInFlight", int.class);
      OWNERSHIP_GATE = lookup.findVarHandle(RetirementSegment.class, "ownershipGate", int.class);
      PAYLOAD_FREED = lookup.findVarHandle(RetirementSegment.class, "payloadFreed", int.class);
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private final NativeMemory.Memory memory;
  private final int capacity;
  private final long payloadAddress;
  private final long addressesAddress;
  private final long allocationsAddress;
  private final long handlesAddress;
  private final long payloadBytes;
  private volatile long reservationState;
  private volatile int committedTail;
  private volatile int segmentClaimState;
  private volatile int linkInFlight;
  private volatile int ownershipGate;
  private final AtomicReference<RetirementSegment> next = new AtomicReference<>();
  private volatile int payloadFreed;

  private volatile long baseSequence;
  private volatile int snapshotTail = -1;
  private volatile long sealedEpoch;
  private volatile long safeTicket;
  private volatile int retiredRecordCount;
  private volatile long retiredBytes;
  /** Physical native records already released from a claimed segment. */
  private int releasedRecordCount;
  private long releasedBytes;
  /** Logical committed slots already reported as completed; includes empty/structural slots. */
  private int releasedSlotCount;
  private volatile boolean structuralRetirement;
  private volatile boolean sealed;
  private volatile boolean detached;
  private volatile int laneIndex;

  RetirementSegment(
      NativeMemory.Memory memory,
      int capacity,
      long baseSequence,
      int laneIndex) {
    if (memory == null) {
      throw new NullPointerException("memory");
    }
    if (Integer.bitCount(capacity) != 1 || capacity < 2 || capacity > CAPACITY) {
      throw new IllegalArgumentException("capacity must be a power of two between 2 and 256");
    }
    this.memory = memory;
    this.capacity = capacity;
    this.payloadBytes =
        Math.multiplyExact(
            Math.multiplyExact((long) capacity, PAYLOAD_WORDS_PER_RECORD), WORD_BYTES);
    this.payloadAddress = memory.allocateRaw(payloadBytes);
    this.addressesAddress = payloadAddress;
    this.allocationsAddress = addressesAddress + (long) capacity * WORD_BYTES;
    this.handlesAddress = allocationsAddress + (long) capacity * WORD_BYTES;
    reset(baseSequence, laneIndex);
  }

  long payloadBytes() {
    return payloadBytes;
  }

  boolean freePayload() {
    if (!PAYLOAD_FREED.compareAndSet(this, 0, 1)) {
      return false;
    }
    memory.free(payloadAddress, payloadBytes);
    return true;
  }

  int tryReserve(
      long expectedGeneration,
      int expectedLaneIndex,
      AtomicReference<RetirementSegment> ownerProducer) {
    for (;;) {
      if (ownerProducer == null
          || ownerProducer.get() != this
          || laneIndex != expectedLaneIndex) {
        return -1;
      }
      long state = reservationState();
      if (generation(state) != expectedGeneration || (state & CLOSED) != 0L) {
        return -1;
      }
      int count = (int) (state & COUNT_MASK);
      if (count >= capacity) {
        return -1;
      }
      if (RESERVATION_STATE.compareAndSet(this, state, state + 1L)) {
        return count;
      }
    }
  }

  /** Reserves a slot and stamps the reusable ticket from the same state read as the CAS. */
  int tryReserveForLane(
      int expectedLaneIndex,
      AtomicReference<RetirementSegment> ownerProducer,
      Reservation reservation) {
    if (reservation == null) {
      throw new NullPointerException("reservation");
    }
    for (;;) {
      // Capture the generation before validating ownership. Reuse after this read invalidates
      // the CAS; reuse before it is rejected by the owner check below.
      long state = reservationState();
      if (ownerProducer == null
          || ownerProducer.get() != this
          || laneIndex != expectedLaneIndex) {
        return -1;
      }
      if ((state & CLOSED) != 0L) {
        return -1;
      }
      int count = (int) (state & COUNT_MASK);
      if (count >= capacity) {
        return -1;
      }
      if (RESERVATION_STATE.compareAndSet(this, state, state + 1L)) {
        reservation.set(this, count, generation(state), expectedLaneIndex);
        return count;
      }
    }
  }

  int closeForSnapshot() {
    for (;;) {
      long state = reservationState();
      if ((state & CLOSED) != 0L) {
        return (int) (state & COUNT_MASK);
      }
      if (RESERVATION_STATE.compareAndSet(this, state, state | CLOSED)) {
        int tail = (int) (state & COUNT_MASK);
        snapshotTail = tail;
        return tail;
      }
    }
  }

  boolean isClosed() {
    return (reservationState() & CLOSED) != 0L;
  }

  int snapshotTail() {
    return snapshotTail;
  }

  int reservationCount() {
    return (int) (reservationState() & COUNT_MASK);
  }

  long baseSequence() {
    return baseSequence;
  }

  long generation() {
    return generation(reservationState());
  }

  RetirementSegment next() {
    return next.get();
  }

  boolean compareAndSetNext(RetirementSegment expected, RetirementSegment update) {
    return next.compareAndSet(expected, update);
  }

  boolean beginLink() {
    if (!enterOwnership()) {
      return false;
    }
    LINK_IN_FLIGHT.getAndAdd(this, 1);
    if (detached) {
      LINK_IN_FLIGHT.getAndAdd(this, -1);
      leaveOwnership();
      return false;
    }
    return true;
  }

  void endLink() {
    LINK_IN_FLIGHT.getAndAdd(this, -1);
    leaveOwnership();
  }

  void clearNext() {
    next.set(null);
  }

  boolean tryBeginRecycle() {
    if (!segmentClaimFinished()) {
      return false;
    }
    if (!OWNERSHIP_GATE.compareAndSet(this, 0, RECYCLE_CLOSED)) {
      return false;
    }
    return true;
  }

  void write(int index, long address, long allocation) {
    checkReserved(index);
    writeColumns(index, address, allocation);
  }

  /** Writes a slot reserved by the trusted lane reservation fast path. */
  void writeReserved(int index, long address, long allocation) {
    writeColumns(index, address, allocation);
  }

  /** Marks a record whose native key/index memory requires the broader lookup epoch. */
  void writeStructuralReserved(int index, long address, long allocation) {
    writeReserved(index, address, allocation);
  }

  void commit(int index) {
    checkReserved(index);
    publishCompletion(index);
  }

  /** Publishes a slot reserved by the trusted lane reservation fast path. */
  void commitReserved(int index) {
    commitReserved(index, 0L, false);
  }

  /** Publishes a slot; journal-wide retirement totals are accounted when the segment is sealed. */
  void commitReserved(int index, long allocation, boolean structural) {
    if (structural) {
      structuralRetirement = true;
    }
    if (allocation != 0L) {
      retiredRecordCount++;
      retiredBytes += WriterArena.allocationWeight(allocation);
    }
    publishCompletion(index);
  }

  void cancel(int index) {
    checkReserved(index);
    clearAllColumns(index);
    publishCompletion(index);
  }

  /** Cancels a slot reserved by the trusted lane reservation fast path. */
  void cancelReserved(int index) {
    clearAllColumns(index);
    publishCompletion(index);
  }

  void validateReservation(Reservation reservation) {
    if (reservation == null
        || reservation.segment != this
        || reservation.generation() != generation()
        || reservation.laneIndex() != laneIndex) {
      throw new IllegalStateException("retirement reservation belongs to another segment generation");
    }
    checkReserved(reservation.index());
  }

  boolean isComplete() {
    int tail = snapshotTail;
    if (!isClosed() || tail < 0) {
      return false;
    }
    return (int) COMMITTED_TAIL.getAcquire(this) >= tail;
  }

  boolean seal(long epoch) {
    if (!isComplete() || sealed) {
      return false;
    }
    sealedEpoch = epoch;
    sealed = true;
    return true;
  }

  boolean isSealed() {
    return sealed;
  }

  long sealedEpoch() {
    return sealedEpoch;
  }

  /** Claims a complete segment with one CAS. No per-record ownership is needed. */
  boolean tryClaimSegment() {
    return sealed && SEGMENT_CLAIM_STATE.compareAndSet(this, 0, 1);
  }

  /** Releases one claimed segment directly from its native columns without a Java payload copy. */
  int releaseRecords(NativeMemory.Memory releaseMemory) {
    if (releaseMemory == null) {
      throw new NullPointerException("releaseMemory");
    }
    int tail = snapshotTail;
    if (tail < 0) {
      throw new IllegalStateException("retirement segment is not sealed");
    }
    int releasedSlotsBefore = releasedSlotCount;
    for (int index = 0; index < tail; index++) {
      long addressOffset = addressesAddress + (long) index * WORD_BYTES;
      long address = NativeMemory.getLong(addressOffset);
      if (address != 0L) {
        long allocation =
            NativeMemory.getLong(allocationsAddress + (long) index * WORD_BYTES);
        releaseMemory.releaseEntry(address, allocation);
        // The allocator release methods validate ownership before mutating it. Clear the slot
        // only after a successful release so a task failure can be retried by teardown without
        // losing an unreleased native block.
        clearAddressColumn(index);
        recordReleased(1, WriterArena.allocationWeight(allocation));
        releasedSlotCount++;
      }
    }
    // A successful segment release completes every committed slot, including structural slots
    // that do not own a native value allocation. The incremental progress above is retained if
    // the allocator throws part-way through the loop so a later retry cannot double-count freed
    // data. A zero address has no allocator work left to perform, so its corresponding logical
    // retirement counters can be completed here as well.
    releasedSlotCount = tail;
    releasedRecordCount = retiredRecordCount;
    releasedBytes = retiredBytes;
    return tail - releasedSlotsBefore;
  }

  /** Releases a claimed segment with actor-owned page grouping scratch and no column copy. */
  int releaseRecords(NativeMemory.Memory releaseMemory, ThreadContext context) {
    if (releaseMemory == null) {
      throw new NullPointerException("releaseMemory");
    }
    if (context == null) {
      throw new NullPointerException("context");
    }
    int tail = snapshotTail;
    if (tail < 0) {
      throw new IllegalStateException("retirement segment is not sealed");
    }
    int releasedSlotsBefore = releasedSlotCount;
    boolean success = false;
    try {
      releaseMemory.releaseEntryBatch(
          context, addressesAddress, allocationsAddress, handlesAddress, tail);
      success = true;
    } finally {
      int released = context.releaseBatchRecords();
      long bytes = context.releaseBatchBytes();
      if (released != 0) {
        recordReleased(released, bytes);
        releasedSlotCount += released;
      }
      if (success) {
        releasedSlotCount = tail;
        releasedRecordCount = retiredRecordCount;
        releasedBytes = retiredBytes;
      }
    }
    return releasedSlotCount - releasedSlotsBefore;
  }

  int pendingRecordCount() {
    return retiredRecordCount - releasedRecordCount;
  }

  long pendingBytes() {
    return retiredBytes - releasedBytes;
  }

  int releasedRecordCount() {
    return releasedRecordCount;
  }

  int releasedSlotCount() {
    return releasedSlotCount;
  }

  long releasedBytes() {
    return releasedBytes;
  }

  private void recordReleased(int records, long bytes) {
    if (records < 0 || bytes < 0L) {
      throw new IllegalArgumentException("invalid released retirement progress");
    }
    releasedRecordCount += records;
    releasedBytes += bytes;
    if (releasedRecordCount > retiredRecordCount || releasedBytes > retiredBytes) {
      throw new IllegalStateException("retirement release progress exceeds segment totals");
    }
  }

  void finishSegmentClaim() {
    if (!SEGMENT_CLAIM_STATE.compareAndSet(this, 1, 2)) {
      throw new IllegalStateException("retirement segment was not claimed");
    }
  }

  void abortSegmentClaim() {
    if (!SEGMENT_CLAIM_STATE.compareAndSet(this, 1, 0)) {
      throw new IllegalStateException("retirement segment was not claimed");
    }
  }

  boolean segmentClaimFinished() {
    return segmentClaimState() == 2;
  }

  int laneIndex() {
    return laneIndex;
  }

  long safeTicket() {
    return safeTicket;
  }

  void safeTicket(long safeTicket) {
    this.safeTicket = safeTicket;
  }

  int retiredRecordCount() {
    return retiredRecordCount;
  }

  long retiredBytes() {
    return retiredBytes;
  }

  boolean requiresStructuralEpoch() {
    return structuralRetirement;
  }

  long addressAt(int index) {
    checkSnapshotIndex(index);
    return NativeMemory.getLong(addressesAddress + (long) index * WORD_BYTES);
  }

  long allocationAt(int index) {
    checkSnapshotIndex(index);
    return NativeMemory.getLong(allocationsAddress + (long) index * WORD_BYTES);
  }

  long handleAt(int index) {
    checkSnapshotIndex(index);
    return NativeMemory.getLong(handlesAddress + (long) index * WORD_BYTES);
  }

  void markDetached() {
    detached = true;
  }

  void reset(long newBaseSequence, int newLaneIndex) {
    if ((int) PAYLOAD_FREED.getVolatile(this) != 0) {
      throw new IllegalStateException("retirement segment payload was freed");
    }
    for (int index = 0; index < capacity; index++) {
      clearAllColumns(index);
    }
    long nextGeneration = (generation() + 1L) & GENERATION_MASK;
    if (nextGeneration == 0L) {
      nextGeneration = 1L;
    }
    SEGMENT_CLAIM_STATE.setVolatile(this, 0);
    LINK_IN_FLIGHT.setVolatile(this, 0);
    COMMITTED_TAIL.setVolatile(this, 0);
    snapshotTail = -1;
    sealedEpoch = 0L;
    safeTicket = 0L;
    retiredRecordCount = 0;
    retiredBytes = 0L;
    releasedRecordCount = 0;
    releasedBytes = 0L;
    releasedSlotCount = 0;
    structuralRetirement = false;
    sealed = false;
    detached = false;
    next.set(null);
    baseSequence = newBaseSequence;
    laneIndex = newLaneIndex;
    RESERVATION_STATE.setVolatile(this, nextGeneration << GENERATION_SHIFT);
    OWNERSHIP_GATE.setVolatile(this, 0);
  }

  /** Assigns the logical sequence domain immediately before a pooled segment is linked. */
  void rebaseForLink(long newBaseSequence) {
    if (snapshotTail >= 0 || sealed || detached || next.get() != null) {
      throw new IllegalStateException("retirement segment is not an installable candidate");
    }
    baseSequence = newBaseSequence;
  }

  private boolean enterOwnership() {
    for (;;) {
      int state = (int) OWNERSHIP_GATE.getVolatile(this);
      if ((state & RECYCLE_CLOSED) != 0) {
        return false;
      }
      if (OWNERSHIP_GATE.compareAndSet(this, state, state + 1)) {
        return true;
      }
    }
  }

  private void leaveOwnership() {
    OWNERSHIP_GATE.getAndAdd(this, -1);
  }

  private void checkReserved(int index) {
    int count = (int) (reservationState() & COUNT_MASK);
    if (index < 0 || index >= count) {
      throw new IllegalStateException("retirement slot was not reserved: " + index);
    }
  }

  private void checkSnapshotIndex(int index) {
    int tail = snapshotTail;
    if (index < 0 || index >= tail) {
      throw new IndexOutOfBoundsException("retirement record index: " + index);
    }
  }

  private void writeColumns(int index, long address, long allocation) {
    NativeMemory.putLong(addressesAddress + (long) index * WORD_BYTES, address);
    NativeMemory.putLong(allocationsAddress + (long) index * WORD_BYTES, allocation);
    NativeMemory.putLong(
        handlesAddress + (long) index * WORD_BYTES,
        NativeMemory.Memory.entryAllocatorHandle(address));
  }

  /** Clears only the live marker after a native release has completed successfully. */
  private void clearAddressColumn(int index) {
    NativeMemory.putLong(addressesAddress + (long) index * WORD_BYTES, 0L);
  }

  /** Clears every payload column before a reservation/descriptor can be reused. */
  private void clearAllColumns(int index) {
    clearAddressColumn(index);
    NativeMemory.putLong(allocationsAddress + (long) index * WORD_BYTES, 0L);
    NativeMemory.putLong(handlesAddress + (long) index * WORD_BYTES, 0L);
  }

  private long reservationState() {
    return (long) RESERVATION_STATE.getVolatile(this);
  }

  private static long generation(long state) {
    return (state >>> GENERATION_SHIFT) & GENERATION_MASK;
  }

  private int segmentClaimState() {
    return (int) SEGMENT_CLAIM_STATE.getVolatile(this);
  }

  private void publishCompletion(int index) {
    if (committedTail != index) {
      throw new IllegalStateException("retirement records must complete in reservation order");
    }
    COMMITTED_TAIL.setRelease(this, index + 1);
  }

  /** Reusable writer-side reservation view; it carries no heap allocation on the put hot path. */
  public static final class Reservation {
    private RetirementSegment segment;
    private int index;
    private long generation;
    private int laneIndex;
    private long allocation;
    private boolean structural;
    private boolean trusted;

    public long sequence() {
      if (segment == null) {
        throw new IllegalStateException("retirement reservation is empty");
      }
      return segment.baseSequence + index;
    }

    void set(RetirementSegment segment, int index, long generation, int laneIndex) {
      this.segment = segment;
      this.index = index;
      this.generation = generation;
      this.laneIndex = laneIndex;
      allocation = 0L;
      structural = false;
      trusted = true;
    }

    void recordWrite(long allocation, boolean structural) {
      this.allocation = allocation;
      this.structural = structural;
    }

    RetirementSegment segment() {
      return segment;
    }

    int index() {
      return index;
    }

    long generation() {
      return generation;
    }

    int laneIndex() {
      return laneIndex;
    }

    long allocation() {
      return allocation;
    }

    boolean structural() {
      return structural;
    }

    boolean trusted() {
      return trusted;
    }

    void clear() {
      trusted = false;
      segment = null;
      index = 0;
      generation = 0L;
      laneIndex = -1;
      allocation = 0L;
      structural = false;
    }
  }
}
