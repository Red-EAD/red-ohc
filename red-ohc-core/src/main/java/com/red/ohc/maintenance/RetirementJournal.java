package com.red.ohc.maintenance;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;

/** The single native retirement transport shared by writer lanes and the maintenance actor. */
public final class RetirementJournal {
  /**
   * Result of one bounded actor reclaim attempt.
   *
   * <p>The three counters deliberately have different units: a segment is a storage/reclaim
   * batch, a record is a committed logical slot, and bytes are physical native payload bytes.
   */
  public static final class ReclaimResult {
    public static final ReclaimResult EMPTY = new ReclaimResult(0, 0, 0, 0L);

    public final int segments;
    public final int records;
    public final int physicalRecords;
    public final long bytes;

    private ReclaimResult(int segments, int records, int physicalRecords, long bytes) {
      if (segments < 0 || records < 0 || physicalRecords < 0 || bytes < 0L) {
        throw new IllegalArgumentException("invalid retirement reclaim result");
      }
      this.segments = segments;
      this.records = records;
      this.physicalRecords = physicalRecords;
      this.bytes = bytes;
    }
  }

  public static final int SEGMENT_CAPACITY = RetirementSegment.CAPACITY;
  static final int WRITER_WAKE_RECORDS = 32;
  static final int WORK_STATE_READY = 1;
  static final int WORK_STATE_OPEN = 1 << 1;
  static final int WORK_STATE_SEALED = 1 << 2;
  static final int WORK_STATE_SAFE = 1 << 3;
  private static final int ACTOR_LANE_OFFSET = 1;
  private static final int QUEUE_CHUNK_SIZE = 16;

  private final NativeMemory.Memory memory;
  private final Object laneCreationLock = new Object();
  private volatile Lane[] writerLanes = new Lane[0];
  private final Lane actorLane;
  private final MpscUnboundedArrayQueue<Lane> readyLanes =
      new MpscUnboundedArrayQueue<>(QUEUE_CHUNK_SIZE);
  /** SAFE segments have multiple publishers and exactly one maintenance-actor consumer. */
  private final MpscUnboundedArrayQueue<RetirementSegment> safeSegments =
      new MpscUnboundedArrayQueue<>(QUEUE_CHUNK_SIZE);
  private final MpmcUnboundedXaddArrayQueue<RetirementSegment> reusableSegments =
      new MpmcUnboundedXaddArrayQueue<>(QUEUE_CHUNK_SIZE);
  private final Set<RetirementSegment> allocatedSegmentDescriptors =
      ConcurrentHashMap.newKeySet();
  private final AtomicInteger openProducerCount = new AtomicInteger();
  private final AtomicInteger readyLaneCount = new AtomicInteger();
  private final AtomicInteger sealedSegmentCount = new AtomicInteger();
  private final AtomicInteger safeSegmentCount = new AtomicInteger();
  private final AtomicLong safePublishedTicket = new AtomicLong();
  private final AtomicLong allocatedSegments = new AtomicLong();
  private final AtomicLong allocatedPayloadBytes = new AtomicLong();
  private final AtomicLong completedRecords = new AtomicLong();
  private final AtomicLong generatedBytes = new AtomicLong();
  private final AtomicLong completedBytes = new AtomicLong();
  private final AtomicLong sealedRecords = new AtomicLong();
  private final AtomicLong actorReclaimedRecords = new AtomicLong();
  private final AtomicLong retiredBytes = new AtomicLong();
  private final AtomicInteger retiredEntries = new AtomicInteger();
  /** Committed physical retirement bytes that have not completed native release. */
  private final AtomicLong admittedRetirementBytes = new AtomicLong();
  private final AtomicLong unsafeRecords = new AtomicLong();
  private final AtomicLong unsafeBytes = new AtomicLong();
  private final AtomicLong safeRecords = new AtomicLong();
  private final AtomicLong safeBytes = new AtomicLong();
  private final AtomicLong claimedRecords = new AtomicLong();
  private final AtomicLong claimedBytes = new AtomicLong();
  private final AtomicLong reusedSegments = new AtomicLong();
  private final AtomicLong trimmedSegments = new AtomicLong();
  private final AtomicBoolean readyHint = new AtomicBoolean();
  private final ThreadContext actorContext = new ThreadContext(null);
  private volatile Runnable readySignal;
  private volatile Consumer<Throwable> readySignalFailure;
  private volatile Runnable openProducerSignal;
  private volatile boolean closed;
  private volatile int lastSealScannedLanes;
  private volatile int lastSealSealedLanes;
  private volatile long lastSealRecords;
  private final AtomicLong sealScannedLanesTotal = new AtomicLong();

  public RetirementJournal(NativeMemory.Memory memory) {
    this(memory, 0);
  }

  RetirementJournal(NativeMemory.Memory memory, int writerLaneCount) {
    if (memory == null) {
      throw new NullPointerException("memory");
    }
    if (writerLaneCount < 0) {
      throw new IllegalArgumentException("writerLaneCount must be non-negative");
    }
    this.memory = memory;
    this.actorLane = new Lane(0);
    for (int index = 0; index < writerLaneCount; index++) {
      createLane();
    }
  }

  /** Cold-path lane creation; the returned lane has exactly one writer producer. */
  public Lane createLane() {
    synchronized (laneCreationLock) {
      Lane[] current = writerLanes;
      Lane lane = new Lane(current.length + ACTOR_LANE_OFFSET);
      Runnable signal = readySignal;
      if (signal != null) {
        lane.bindReadySignal(signal, readySignalFailure);
      }
      Lane[] expanded = Arrays.copyOf(current, current.length + 1);
      expanded[current.length] = lane;
      writerLanes = expanded;
      return lane;
    }
  }

  public Lane lane(int index) {
    if (index == 0) {
      return actorLane;
    }
    Lane[] snapshot = writerLanes;
    int writerIndex = index - ACTOR_LANE_OFFSET;
    if (writerIndex < 0 || writerIndex >= snapshot.length) {
      throw new IndexOutOfBoundsException("retirement lane: " + index);
    }
    return snapshot[writerIndex];
  }

  public Lane actorLane() {
    return actorLane;
  }

  public int laneCount() {
    return writerLanes.length;
  }

  public void bindReadySignal(Runnable signal) {
    bindReadySignal(signal, null);
  }

  public void bindReadySignal(Runnable signal, Consumer<Throwable> failureHandler) {
    if (signal == null) {
      throw new NullPointerException("signal");
    }
    synchronized (laneCreationLock) {
      readySignal = signal;
      readySignalFailure = failureHandler;
      for (Lane lane : writerLanes) {
        lane.bindReadySignal(signal, failureHandler);
      }
      actorLane.bindReadySignal(signal, failureHandler);
    }
    int state = workState();
    if ((state & (WORK_STATE_READY | WORK_STATE_SEALED | WORK_STATE_SAFE)) != 0) {
      signalReady(signal, failureHandler);
    }
  }

  void bindOpenProducerSignal(Runnable signal) {
    if (signal == null) {
      throw new NullPointerException("signal");
    }
    openProducerSignal = signal;
    if (openProducerCount.get() != 0) {
      signal.run();
    }
  }

  public boolean hasWork() {
    return workState() != 0;
  }

  public boolean hasRunnableWork() {
    return (workState() & (WORK_STATE_READY | WORK_STATE_SEALED)) != 0;
  }

  public boolean hasActorWork() {
    return (workState() & WORK_STATE_READY) != 0;
  }

  public boolean hasOpenProducerRecords() {
    return openProducerCount.get() != 0;
  }

  public boolean hasSealedSegments() {
    return sealedSegmentCount.get() != 0;
  }

  public boolean hasSafeSegments() {
    return safeSegmentCount.get() != 0;
  }

  /** Actor-owned ticket of the current SAFE queue head, or zero when the queue is empty. */
  long oldestSafeTicket() {
    RetirementSegment segment = safeSegments.relaxedPeek();
    return segment == null ? 0L : segment.safeTicket();
  }

  int workState() {
    int state = 0;
    if (readyHint.get() || readyLaneCount.get() != 0) {
      state |= WORK_STATE_READY;
    }
    if (openProducerCount.get() != 0) {
      state |= WORK_STATE_OPEN;
    }
    if (sealedSegmentCount.get() != 0) {
      state |= WORK_STATE_SEALED;
    }
    if (safeSegmentCount.get() != 0) {
      state |= WORK_STATE_SAFE;
    }
    return state;
  }

  public long reservedRecords() {
    long total = 0L;
    for (Lane lane : writerLanes) {
      total += lane.reservedRecords();
    }
    return total + actorLane.reservedRecords();
  }

  public long reservedRecordsTotal() {
    long total = 0L;
    for (Lane lane : writerLanes) {
      total += lane.reservedRecordsTotal();
    }
    return total + actorLane.reservedRecordsTotal();
  }

  public long queuedRecords() {
    return lagRecords();
  }

  public long[] captureWatermark() {
    Lane[] snapshot = writerLanes;
    long[] watermark = new long[snapshot.length + ACTOR_LANE_OFFSET];
    captureWatermark(watermark);
    return watermark;
  }

  public void captureWatermark(long[] watermark) {
    checkWatermark(watermark);
    watermark[0] = actorLane.reservationWatermark();
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      watermark[index] = snapshot[index - ACTOR_LANE_OFFSET].reservationWatermark();
    }
  }

  public long[] captureTurnWatermark() {
    return captureWatermark();
  }

  public void captureTurnWatermark(long[] watermark) {
    captureWatermark(watermark);
  }

  public long[] captureAndCutWatermark() {
    Lane[] snapshot = writerLanes;
    long[] watermark = new long[snapshot.length + ACTOR_LANE_OFFSET];
    captureAndCutWatermark(watermark);
    return watermark;
  }

  public void captureAndCutWatermark(long[] watermark) {
    checkWatermark(watermark);
    watermark[0] = actorLane.cutAndCaptureWatermark();
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      watermark[index] = snapshot[index - ACTOR_LANE_OFFSET].cutAndCaptureWatermark();
    }
  }

  /**
   * Extends only the actor lane of an existing flush fence when the actor owns every reservation.
   * Writer reservations published after the fence remain outside it.
   */
  public boolean captureAndCutActorWatermark(long[] watermark) {
    checkWatermark(watermark);
    if (actorLane.hasUncommittedReservations()) {
      return false;
    }
    watermark[0] = actorLane.cutAndCaptureWatermark();
    return true;
  }

  /**
   * Cuts the current writer records and arms empty lanes so the first later commit cannot miss the
   * pressure turn that observed an in-flight debt reservation.
   */
  public void captureAndCutOrArmWatermark(long[] watermark) {
    checkWatermark(watermark);
    watermark[0] = actorLane.cutAndCaptureWatermark();
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      watermark[index] =
          snapshot[index - ACTOR_LANE_OFFSET].cutOrArmAndCaptureWatermark();
    }
  }

  /** Cuts only lanes that published a ready boundary; unrelated partial producers stay open. */
  public void captureAndCutReadyWatermark(long[] watermark) {
    checkWatermark(watermark);
    watermark[0] = actorLane.cutReadyAndCaptureWatermark();
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      watermark[index] =
          snapshot[index - ACTOR_LANE_OFFSET].cutReadyAndCaptureWatermark();
    }
  }

  public boolean watermarkComplete(long[] watermark) {
    checkWatermark(watermark);
    if (!actorLane.watermarkComplete(watermark[0])) {
      return false;
    }
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      if (!snapshot[index - ACTOR_LANE_OFFSET].watermarkComplete(watermark[index])) {
        return false;
      }
    }
    return true;
  }

  public long captureSafePublishedTicket() {
    return safePublishedTicket.get();
  }

  public long allocatedSegments() {
    return allocatedSegments.get();
  }

  public long allocatedBytes() {
    return allocatedPayloadBytes.get();
  }

  public long safeSegmentDebt() {
    return safeSegmentCount.get();
  }

  public long sealedRecordsTotal() {
    return sealedRecords.get();
  }

  public long actorReclaimedRecordsTotal() {
    return actorReclaimedRecords.get();
  }

  public long publishedRecordsTotal() {
    long total = 0L;
    for (Lane lane : writerLanes) {
      total += lane.committedRecordsTotal();
    }
    return total + actorLane.committedRecordsTotal();
  }

  public long completedRecordsTotal() {
    return completedRecords.get();
  }

  /** Monotonic bytes committed to retirement records before physical reclaim. */
  public long generatedBytesTotal() {
    return addSaturated(generatedBytes.get(), unsealedRetiredBytes());
  }

  /** Monotonic bytes released from retirement segments. */
  public long completedBytesTotal() {
    return completedBytes.get();
  }

  public long lagRecords() {
    return Math.max(0L, publishedRecordsTotal() - completedRecords.get());
  }

  public long unsafeRecords() {
    return unsafeRecords.get();
  }

  public long unsafeBytes() {
    return unsafeBytes.get();
  }

  public long safeRecords() {
    return safeRecords.get();
  }

  public long safeBytes() {
    return safeBytes.get();
  }

  public long claimedRecords() {
    return claimedRecords.get();
  }

  public long claimedBytes() {
    return claimedBytes.get();
  }

  public long reusedSegments() {
    return reusedSegments.get();
  }

  public long trimmedSegments() {
    return trimmedSegments.get();
  }

  public long retiredBytes() {
    return addSaturated(retiredBytes.get(), unsealedRetiredBytes());
  }

  /** Returns the current physical retirement backlog. */
  long retirementDebtBytes() {
    return addSaturated(admittedRetirementBytes.get(), unsealedRetiredBytes());
  }

  public int retiredEntries() {
    long total = (long) retiredEntries.get() + unsealedRetiredEntries();
    return total >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) total;
  }

  public boolean cutAllProducersAtWatermark() {
    for (Lane lane : writerLanes) {
      lane.forceCutOpenProducer();
    }
    actorLane.forceCutOpenProducer();
    return true;
  }

  public int sealReadySegments(long epoch) {
    int sealed = 0;
    int sealedLanes = 0;
    actorLane.forceCutOpenProducer();
    for (Lane lane : writerLanes) {
      int laneRecords = sealLane(lane, epoch, Long.MAX_VALUE);
      sealed += laneRecords;
      sealedLanes += laneRecords == 0 ? 0 : 1;
    }
    int actorRecords = sealLane(actorLane, epoch, Long.MAX_VALUE);
    sealed += actorRecords;
    sealedLanes += actorRecords == 0 ? 0 : 1;
    recordSealScan(writerLanes.length + ACTOR_LANE_OFFSET, sealedLanes, sealed);
    if (sealed != 0) {
      readyHint.set(true);
    }
    return sealed;
  }

  public int sealSnapshotSegments(long epoch, long[] watermark) {
    checkWatermark(watermark);
    int actorRecords = sealLane(actorLane, epoch, watermark[0]);
    int sealed = actorRecords;
    int sealedLanes = actorRecords == 0 ? 0 : 1;
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      int laneRecords =
          sealLane(snapshot[index - ACTOR_LANE_OFFSET], epoch, watermark[index]);
      sealed += laneRecords;
      sealedLanes += laneRecords == 0 ? 0 : 1;
    }
    recordSealScan(watermark.length, sealedLanes, sealed);
    if (sealed != 0) {
      readyHint.set(true);
    }
    return sealed;
  }

  /** Returns whether the captured cut contains at least one complete segment ready to seal. */
  public boolean hasSealableSnapshot(long[] watermark) {
    checkWatermark(watermark);
    if (actorLane.hasSealableThrough(watermark[0])) {
      return true;
    }
    Lane[] snapshot = writerLanes;
    for (int index = 1; index < watermark.length; index++) {
      if (snapshot[index - ACTOR_LANE_OFFSET].hasSealableThrough(watermark[index])) {
        return true;
      }
    }
    return false;
  }

  public int publishSafe(long minimumActiveEpoch) {
    return publishSafe(minimumActiveEpoch, minimumActiveEpoch);
  }

  public int publishSafe(long minimumLookupEpoch, long minimumValueEpoch) {
    int published = 0;
    for (Lane lane : writerLanes) {
      published += publishSafeLane(lane, minimumLookupEpoch, minimumValueEpoch, false);
    }
    published += publishSafeLane(actorLane, minimumLookupEpoch, minimumValueEpoch, false);
    return published;
  }

  private int publishAllSafe() {
    int published = 0;
    for (Lane lane : writerLanes) {
      published += publishSafeLane(lane, Long.MAX_VALUE, Long.MAX_VALUE, true);
    }
    published += publishSafeLane(actorLane, Long.MAX_VALUE, Long.MAX_VALUE, true);
    return published;
  }

  private int sealLane(Lane lane, long epoch, long watermark) {
    if (!lane.tryAcquireProgress()) {
      return 0;
    }
    try {
      return lane.sealThrough(epoch, watermark);
    } finally {
      lane.releaseProgress();
    }
  }

  private int publishSafeLane(
      Lane lane, long minimumLookupEpoch, long minimumValueEpoch, boolean includeBoundary) {
    if (!lane.tryAcquireProgress()) {
      return 0;
    }
    try {
      return lane.publishSafe(minimumLookupEpoch, minimumValueEpoch, includeBoundary);
    } finally {
      lane.releaseProgress();
    }
  }

  /** Reclaims SAFE segments on the maintenance actor; this is the sole queue consumer. */
  ReclaimResult reclaimActorResult(NativeMemory.Memory releaseMemory, int maximumRecords) {
    return reclaimInternal(
        releaseMemory,
        maximumRecords,
        null,
        captureSafePublishedTicket());
  }

  ReclaimResult reclaimActorResult(
      NativeMemory.Memory releaseMemory, int maximumRecords, long[] watermark) {
    return reclaimInternal(
        releaseMemory,
        maximumRecords,
        watermark,
        captureSafePublishedTicket());
  }

  ReclaimResult reclaimActorResult(ReaderRegistry readers, int maximumRecords) {
    if (readers == null) {
      throw new NullPointerException("readers");
    }
    long[] minimumEpochs = new long[2];
    readers.minActiveEpochs(minimumEpochs);
    publishSafe(minimumEpochs[0], minimumEpochs[1]);
    return reclaimInternal(
        memory,
        maximumRecords,
        null,
        captureSafePublishedTicket());
  }

  /** Reclaims a bounded SAFE batch on the maintenance actor, preserving actor reclaim accounting. */
  ReclaimResult reclaimActorSafeBatchResult(
      NativeMemory.Memory releaseMemory, int maximumSegments) {
    return reclaimInternal(
        releaseMemory,
        Integer.MAX_VALUE,
        null,
        Long.MAX_VALUE,
        maximumSegments);
  }

  public boolean consumeReadyHint() {
    return readyHint.getAndSet(false);
  }

  public void finishReadyDrains() {
    readyHint.compareAndSet(true, false);
    int budget = Math.max(0, readyLaneCount.get());
    for (int index = 0; index < budget; index++) {
      Lane lane = readyLanes.poll();
      if (lane == null) {
        break;
      }
      finishReadyDrain(lane);
    }
    if (readyLaneCount.get() != 0 || !readyLanes.isEmpty()) {
      readyHint.set(true);
    }
  }

  private boolean finishReadyDrain(Lane lane) {
    if (!lane.tryAcquireProgress()) {
      readyLanes.offer(lane);
      return true;
    }
    try {
      return lane.finishReadyDrain();
    } finally {
      lane.releaseProgress();
    }
  }

  public void requestSeal() {
    readyHint.set(true);
  }

  public boolean hasReadyHint() {
    return readyHint.get();
  }

  public boolean hasPendingReclaim() {
    return sealedSegmentCount.get() != 0 || safeSegmentCount.get() != 0;
  }

  public int lastSealScannedLanes() {
    return lastSealScannedLanes;
  }

  public int lastSealSealedLanes() {
    return lastSealSealedLanes;
  }

  public int lastSealRecords() {
    return (int) Math.min(Integer.MAX_VALUE, lastSealRecords);
  }

  public long sealRecordsTotal() {
    return sealedRecords.get();
  }

  public long reclaimRecordsTotal() {
    return completedRecords.get();
  }

  public long sealScannedLanesTotal() {
    return sealScannedLanesTotal.get();
  }

  private void recordSealScan(int scannedLanes, int sealedLanes, long records) {
    lastSealScannedLanes = scannedLanes;
    lastSealSealedLanes = sealedLanes;
    lastSealRecords = records;
    sealScannedLanesTotal.addAndGet(scannedLanes);
  }

  public long sealHeadOfLineStops() {
    return 0L;
  }

  public void append(long address, long allocation) {
    actorLane.append(address, allocation);
  }

  public void appendStructural(long address, long allocation) {
    actorLane.appendStructural(address, allocation);
  }

  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    Throwable failure = null;
    try {
      cutAllProducersAtWatermark();
    } catch (Throwable cleanupFailure) {
      failure = appendCloseFailure(failure, cleanupFailure);
    }
    try {
      long[] watermark = captureWatermark();
      while (sealSnapshotSegments(Long.MAX_VALUE, watermark) != 0) {
        watermark = captureWatermark();
      }
    } catch (Throwable cleanupFailure) {
      failure = appendCloseFailure(failure, cleanupFailure);
    }
    try {
      publishAllSafe();
    } catch (Throwable cleanupFailure) {
      failure = appendCloseFailure(failure, cleanupFailure);
    }
    try {
      while (safeSegmentCount.get() != 0) {
        ReclaimResult reclaimed =
            reclaimInternal(
                memory,
                Integer.MAX_VALUE,
                null,
                Long.MAX_VALUE);
        if (reclaimed.records == 0) {
          break;
        }
      }
    } catch (Throwable cleanupFailure) {
      failure = appendCloseFailure(failure, cleanupFailure);
    }
    RetirementSegment[] descriptors =
        allocatedSegmentDescriptors.toArray(new RetirementSegment[0]);
    for (RetirementSegment segment : descriptors) {
      try {
        trimSegment(segment);
      } catch (Throwable cleanupFailure) {
        failure = appendCloseFailure(failure, cleanupFailure);
      }
    }
    if (failure != null) {
      throwUnchecked(failure);
    }
  }

  private static Throwable appendCloseFailure(Throwable first, Throwable next) {
    if (first == null) {
      return next;
    }
    if (first != next) {
      first.addSuppressed(next);
    }
    return first;
  }

  private static void throwUnchecked(Throwable failure) {
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    throw new RuntimeException(failure);
  }

  private void returnReusable(RetirementSegment segment) {
    if (!reusableSegments.offer(segment)) {
      throw new IllegalStateException("unbounded retirement segment pool rejected a segment");
    }
  }

  /** Publishes a SAFE segment for the single actor consumer without a queue lock. */
  private void enqueueSafeSegment(RetirementSegment segment) {
    int previous = safeSegmentCount.getAndIncrement();
    try {
      if (!safeSegments.offer(segment)) {
        throw new IllegalStateException("unbounded safe retirement queue rejected a segment");
      }
    } catch (Throwable failure) {
      safeSegmentCount.decrementAndGet();
      throw failure;
    }
    if (previous == 0) {
      Runnable signal = readySignal;
      if (signal != null) {
        signalReady(signal, readySignalFailure);
      }
    }
  }

  private void trimSegment(RetirementSegment segment) {
    boolean freed = segment.freePayload();
    allocatedSegmentDescriptors.remove(segment);
    if (freed) {
      allocatedPayloadBytes.addAndGet(-segment.payloadBytes());
      trimmedSegments.incrementAndGet();
    }
  }

  private void signalOpenProducer() {
    Runnable signal = openProducerSignal;
    if (signal != null) {
      signal.run();
    }
  }

  private ReclaimResult reclaimInternal(
      NativeMemory.Memory releaseMemory,
      int maximumRecords,
      long[] watermark,
      long maximumSafeTicket) {
    return reclaimInternal(
        releaseMemory,
        maximumRecords,
        watermark,
        maximumSafeTicket,
        Integer.MAX_VALUE);
  }

  private ReclaimResult reclaimInternal(
      NativeMemory.Memory releaseMemory,
      int maximumRecords,
      long[] watermark,
      long maximumSafeTicket,
      int maximumSegments) {
    if (releaseMemory == null) {
      throw new NullPointerException("releaseMemory");
    }
    if (maximumRecords <= 0 || maximumSegments <= 0) {
      return ReclaimResult.EMPTY;
    }
    if (watermark != null) {
      checkWatermark(watermark);
    }
    actorContext.beginReleasePageMemo();
    try {
      int reclaimed = 0;
      int reclaimedPhysicalRecords = 0;
      long reclaimedBytes = 0L;
      int reclaimedSegments = 0;
      int attempts = Math.min(maximumSegments, Math.max(1, safeSegmentCount.get()));
      TurnReleaseWave wave = turnReleaseWave();
      while (reclaimed < maximumRecords && reclaimedSegments < maximumSegments) {
        // Phase 0: claim one bounded wave of SAFE segments (unchanged poll/requeue/claim
        // protocol; the wave only widens the release granularity).
        wave.reset();
        boolean queueEmpty = false;
        while (reclaimedSegments < maximumSegments
            && wave.count < TurnReleaseWave.MAX_SEGMENTS
            && attempts-- > 0) {
          RetirementSegment segment = safeSegments.relaxedPoll();
          if (segment == null) {
            queueEmpty = true;
            break;
          }
          int remainingSafeSegments = safeSegmentCount.decrementAndGet();
          if (remainingSafeSegments < 0) {
            throw new IllegalStateException("retirement safe segment count underflow");
          }
          if (segment.safeTicket() > maximumSafeTicket
              || (watermark != null
                  && (segment.laneIndex() >= watermark.length
                      || segment.baseSequence() + segment.snapshotTail()
                          > watermark[segment.laneIndex()]))) {
            enqueueSafeSegment(segment);
            continue;
          }
          if (!segment.tryClaimSegment()) {
            continue;
          }
          int slot = wave.count;
          wave.segments[slot] = segment;
          wave.tails[slot] = segment.snapshotTail();
          wave.addressesColumns[slot] = segment.addressesAddress();
          wave.allocationsColumns[slot] = segment.allocationsAddress();
          wave.handlesColumns[slot] = segment.handlesAddress();
          wave.retiredRecords[slot] = segment.pendingRecordCount();
          wave.retiredBytes[slot] = segment.pendingBytes();
          wave.beforeReleasedSlots[slot] = segment.releasedSlotCount();
          wave.beforeReleasedRecords[slot] = segment.releasedRecordCount();
          wave.beforeReleasedBytes[slot] = segment.releasedBytes();
          safeRecords.addAndGet(-wave.retiredRecords[slot]);
          safeBytes.addAndGet(-wave.retiredBytes[slot]);
          claimedRecords.addAndGet(wave.retiredRecords[slot]);
          claimedBytes.addAndGet(wave.retiredBytes[slot]);
          wave.count++;
          reclaimedSegments++;
          // Mirrors the previous post-hoc record bound: the crossing segment still releases
          // fully, then claiming stops.
          wave.records += wave.tails[slot] - wave.beforeReleasedSlots[slot];
          if (reclaimed + wave.records >= maximumRecords) {
            break;
          }
        }
        if (wave.count == 0) {
          break;
        }
        // Reset progress before the release so a scratch-growth failure settles on zeros.
        actorContext.resetReleaseTurnProgress(wave.count);
        try {
          releaseMemory.releaseEntryBatchTurn(
              actorContext,
              wave.addressesColumns,
              wave.allocationsColumns,
              wave.handlesColumns,
              wave.tails,
              wave.count);
        } catch (Throwable failure) {
          settleFailedWave(wave, failure);
          throw failure;
        }
        // Phase C: settle in claim order; all progress is captured before finishSegmentClaim
        // (FINISHED lets a concurrent owner recycle and reset the descriptor).
        int[] clearedRecords = actorContext.releaseTurnClearedRecords();
        long[] clearedBytes = actorContext.releaseTurnClearedBytes();
        for (int slot = 0; slot < wave.count; slot++) {
          RetirementSegment segment = wave.segments[slot];
          int ownerLaneIndex = segment.laneIndex();
          segment.applyReleaseProgress(clearedRecords[slot], clearedBytes[slot]);
          segment.completeReleaseProgress();
          claimedRecords.addAndGet(-wave.retiredRecords[slot]);
          claimedBytes.addAndGet(-wave.retiredBytes[slot]);
          int releasedSlots = segment.releasedSlotCount() - wave.beforeReleasedSlots[slot];
          int releasedRecords = segment.releasedRecordCount() - wave.beforeReleasedRecords[slot];
          long releasedBytes = segment.releasedBytes() - wave.beforeReleasedBytes[slot];
          recordCompleted(releasedSlots, releasedRecords, releasedBytes);
          segment.finishSegmentClaim();
          wave.rememberFinishedLane(ownerLaneIndex);
          reclaimed += releasedSlots;
          reclaimedPhysicalRecords += releasedRecords;
          reclaimedBytes = addSaturated(reclaimedBytes, releasedBytes);
        }
        // One completion drain per touched lane for the whole wave.
        for (int index = 0; index < wave.finishedLaneCount; index++) {
          lane(wave.finishedLaneIndexes[index]).finishedSegmentsDrained();
        }
        wave.finishedLaneCount = 0;
        if (queueEmpty) {
          break;
        }
      }
      return new ReclaimResult(
          reclaimedSegments, reclaimed, reclaimedPhysicalRecords, reclaimedBytes);
    } finally {
      actorContext.endReleasePageMemo();
    }
  }

  /**
   * Settles a wave whose merged release failed part-way. Progress is durable per cleared
   * address: segments whose every record was released are finished exactly like the success
   * path; the rest abort their claim, reverse the claimed→safe ledger move for their remaining
   * pending, and re-queue at the tail for a later retry that skips cleared addresses.
   */
  private void settleFailedWave(TurnReleaseWave wave, Throwable failure) {
    int[] clearedRecords = actorContext.releaseTurnClearedRecords();
    long[] clearedBytes = actorContext.releaseTurnClearedBytes();
    int[] preZeroRecords = actorContext.releaseTurnPreZeroRecords();
    for (int slot = 0; slot < wave.count; slot++) {
      RetirementSegment segment = wave.segments[slot];
      boolean complete;
      try {
        segment.applyReleaseProgress(clearedRecords[slot], clearedBytes[slot]);
        complete = clearedRecords[slot] + preZeroRecords[slot] == wave.tails[slot];
        if (complete) {
          segment.completeReleaseProgress();
        }
      } catch (Throwable secondary) {
        failure.addSuppressed(secondary);
        complete = false;
      }
      try {
        if (!complete) {
          segment.abortSegmentClaim();
        }
        int releasedSlots = segment.releasedSlotCount() - wave.beforeReleasedSlots[slot];
        int releasedRecords = segment.releasedRecordCount() - wave.beforeReleasedRecords[slot];
        long releasedBytes = segment.releasedBytes() - wave.beforeReleasedBytes[slot];
        claimedRecords.addAndGet(-wave.retiredRecords[slot]);
        claimedBytes.addAndGet(-wave.retiredBytes[slot]);
        if (complete) {
          // Capture the owner lane before FINISHED (same rule as the success path).
          int ownerLaneIndex = segment.laneIndex();
          recordCompleted(releasedSlots, releasedRecords, releasedBytes);
          segment.finishSegmentClaim();
          wave.rememberFinishedLane(ownerLaneIndex);
        } else {
          int remainingRecords = segment.pendingRecordCount();
          long remainingBytes = segment.pendingBytes();
          safeRecords.addAndGet(remainingRecords);
          safeBytes.addAndGet(remainingBytes);
          recordCompleted(releasedSlots, releasedRecords, releasedBytes);
          enqueueSafeSegment(segment);
        }
      } catch (Throwable secondary) {
        failure.addSuppressed(secondary);
      }
    }
    for (int index = 0; index < wave.finishedLaneCount; index++) {
      lane(wave.finishedLaneIndexes[index]).finishedSegmentsDrained();
    }
    wave.finishedLaneCount = 0;
  }

  private TurnReleaseWave turnReleaseWave() {
    Object held = actorContext.releaseTurnWave();
    if (held instanceof TurnReleaseWave) {
      return (TurnReleaseWave) held;
    }
    TurnReleaseWave wave = new TurnReleaseWave();
    actorContext.releaseTurnWave(wave);
    return wave;
  }

  /** Per-thread claim bookkeeping for one merged release wave (fixed 128-segment cap). */
  private static final class TurnReleaseWave {
    static final int MAX_SEGMENTS = 128;

    final RetirementSegment[] segments = new RetirementSegment[MAX_SEGMENTS];
    final int[] tails = new int[MAX_SEGMENTS];
    final long[] addressesColumns = new long[MAX_SEGMENTS];
    final long[] allocationsColumns = new long[MAX_SEGMENTS];
    final long[] handlesColumns = new long[MAX_SEGMENTS];
    final int[] retiredRecords = new int[MAX_SEGMENTS];
    final long[] retiredBytes = new long[MAX_SEGMENTS];
    final int[] beforeReleasedSlots = new int[MAX_SEGMENTS];
    final int[] beforeReleasedRecords = new int[MAX_SEGMENTS];
    final long[] beforeReleasedBytes = new long[MAX_SEGMENTS];
    int count;
    int records;
    /** Distinct owner lanes finished this wave; drained once at wave end. */
    final int[] finishedLaneIndexes = new int[MAX_SEGMENTS];
    int finishedLaneCount;

    void reset() {
      for (int slot = 0; slot < count; slot++) {
        segments[slot] = null;
      }
      count = 0;
      records = 0;
      finishedLaneCount = 0;
    }

    void rememberFinishedLane(int laneIndex) {
      for (int index = 0; index < finishedLaneCount; index++) {
        if (finishedLaneIndexes[index] == laneIndex) {
          return;
        }
      }
      finishedLaneIndexes[finishedLaneCount++] = laneIndex;
    }
  }

  private void recordCompleted(int logicalRecords, int physicalRecords, long bytes) {
    if (logicalRecords == 0 && physicalRecords == 0 && bytes == 0L) {
      return;
    }
    actorReclaimedRecords.addAndGet(logicalRecords);
    completedRecords.addAndGet(logicalRecords);
    completedBytes.addAndGet(bytes);
    retiredBytes.addAndGet(-bytes);
    retiredEntries.addAndGet(-physicalRecords);
    subtractAdmittedRetirementBytes(bytes);
  }

  private void onSegmentSealed(RetirementSegment segment, int count) {
    sealedRecords.addAndGet(count);
    int retiredRecordCount = segment.retiredRecordCount();
    long retiredRecordBytes = segment.retiredBytes();
    // A sealed segment is the producer batch. Account the four journal-wide counters once here,
    // after every record in the segment is durable, instead of touching global atomics on every
    // replacement commit.
    if (retiredRecordBytes != 0L) {
      addAdmittedRetirementBytes(retiredRecordBytes);
      generatedBytes.addAndGet(retiredRecordBytes);
      retiredBytes.addAndGet(retiredRecordBytes);
    }
    if (retiredRecordCount != 0) {
      retiredEntries.addAndGet(retiredRecordCount);
    }
    unsafeRecords.addAndGet(retiredRecordCount);
    unsafeBytes.addAndGet(retiredRecordBytes);
  }

  private long unsealedRetiredBytes() {
    long total = 0L;
    for (Lane lane : writerLanes) {
      total = addSaturated(total, lane.unsealedRetiredBytes());
    }
    return addSaturated(total, actorLane.unsealedRetiredBytes());
  }

  private long unsealedRetiredEntries() {
    long total = 0L;
    for (Lane lane : writerLanes) {
      total = addSaturated(total, lane.unsealedRetiredEntries());
    }
    return addSaturated(total, actorLane.unsealedRetiredEntries());
  }

  private static long addSaturated(long left, long right) {
    return right > 0L && left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private void checkWatermark(long[] watermark) {
    if (watermark == null
        || watermark.length < ACTOR_LANE_OFFSET
        || watermark.length > writerLanes.length + ACTOR_LANE_OFFSET) {
      throw new IllegalArgumentException("retirement watermark does not match lane count");
    }
  }

  private void addAdmittedRetirementBytes(long bytes) {
    if (bytes <= 0L) {
      return;
    }
    for (;;) {
      long current = admittedRetirementBytes.get();
      if (current > Long.MAX_VALUE - bytes) {
        throw new IllegalStateException("retirement debt overflow");
      }
      if (admittedRetirementBytes.compareAndSet(current, current + bytes)) {
        return;
      }
    }
  }

  private void subtractAdmittedRetirementBytes(long bytes) {
    if (bytes <= 0L) {
      return;
    }
    for (;;) {
      long current = admittedRetirementBytes.get();
      if (current < bytes) {
        throw new IllegalStateException("retirement debt underflow");
      }
      if (admittedRetirementBytes.compareAndSet(current, current - bytes)) {
        return;
      }
    }
  }

  private RetirementSegment newSegment(Lane owner, long baseSequence) {
    RetirementSegment segment = reusableSegments.poll();
    if (segment == null) {
      segment = new RetirementSegment(memory, SEGMENT_CAPACITY, baseSequence, owner.index);
      allocatedSegments.incrementAndGet();
      allocatedPayloadBytes.addAndGet(segment.payloadBytes());
      try {
        allocatedSegmentDescriptors.add(segment);
      } catch (Throwable failure) {
        trimSegment(segment);
        throw failure;
      }
    } else {
      reusedSegments.incrementAndGet();
      segment.reset(baseSequence, owner.index);
    }
    return segment;
  }

  public final class Lane {
    private final int index;
    private final AtomicReference<RetirementSegment> producer;
    private final ArrayDeque<RetirementSegment> sealedSegments =
        new ArrayDeque<>(SEGMENT_CAPACITY);
    private volatile long reserved;
    private volatile long committed;
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicBoolean progressOwner = new AtomicBoolean();
    private final AtomicInteger completionAdvanceWork = new AtomicInteger();
    private final AtomicReference<RetirementSegment> completionCursor;
    private volatile RetirementSegment sealCursor;
    private volatile RetirementSegment countedOpenProducer;
    private volatile Runnable laneReadySignal;
    private volatile Consumer<Throwable> laneReadySignalFailure;
    private volatile RetirementSegment publicationWakeSegment;
    private volatile long completionPrefix;
    private final RetirementSegment.Reservation actorReservation =
        new RetirementSegment.Reservation();

    private Lane(int index) {
      this.index = index;
      producer = new AtomicReference<>();
      RetirementSegment initial = newSegment(this, 0L);
      producer.set(initial);
      sealCursor = initial;
      completionCursor = new AtomicReference<>(initial);
    }

    private void append(long address, long allocation) {
      if (!reserve(actorReservation)) {
        throw new IllegalStateException("retirement journal is closed");
      }
      write(actorReservation, address, allocation);
      commit(actorReservation);
    }

    private void appendStructural(long address, long allocation) {
      if (!reserve(actorReservation)) {
        throw new IllegalStateException("retirement journal is closed");
      }
      writeStructural(actorReservation, address, allocation);
      commit(actorReservation);
    }

    public boolean reserve(RetirementSegment.Reservation reservation) {
      if (reservation == null) {
        throw new NullPointerException("reservation");
      }
      for (;;) {
        if (closed) {
          return false;
        }
        RetirementSegment current = producer.get();
        int slot = current.tryReserveForLane(index, producer, reservation);
        if (slot >= 0) {
          if (slot == 0) {
            markOpenProducer(current);
          }
          reserved++;
          return true;
        }
        synchronized (this) {
          // A failed reservation does not pin the segment. Keep this owner check and the
          // handoff under one monitor so actor recycling cannot turn current into another lane.
          if (producer.get() != current) {
            continue;
          }
          closeProducerForSnapshot(current);
          signalReady();
          RetirementSegment next = installNext(current);
          if (next == null) {
            throw new OutOfMemoryError("unable to allocate retirement segment");
          }
        }
      }
    }

    public void write(RetirementSegment.Reservation reservation, long address, long allocation) {
      RetirementSegment segment = segment(reservation);
      segment.writeReserved(reservation.index(), address, allocation);
      reservation.recordWrite(allocation, false);
    }

    public void writeStructural(
        RetirementSegment.Reservation reservation, long address, long allocation) {
      RetirementSegment segment = segment(reservation);
      segment.writeStructuralReserved(reservation.index(), address, allocation);
      reservation.recordWrite(allocation, true);
    }

    public void commit(RetirementSegment.Reservation reservation) {
      RetirementSegment segment = segment(reservation);
      int slot = reservation.index();
      boolean signal = index == 0 || slot + 1 >= WRITER_WAKE_RECORDS;
      boolean segmentBoundary = slot + 1 == SEGMENT_CAPACITY;
      boolean armedWake = publicationWakeSegment == segment;
      if (segmentBoundary || armedWake) {
        closeProducerForSnapshot(segment);
      }
      // Publishing the last completion lets the actor reclaim and reuse this segment immediately.
      // Finish every segment access first; only lane-local accounting and notification may follow.
      segment.commitReserved(slot, reservation.allocation(), reservation.structural());
      committed++;
      reservation.clear();
      // The actor may arm and drain while completion is pending. Recheck only the lane marker,
      // never the now-recyclable segment, so that a late publication cannot lose its wakeup.
      if (signal || armedWake || publicationWakeSegment == segment) {
        signalReady();
      }
    }

    public void cancel(RetirementSegment.Reservation reservation) {
      RetirementSegment segment = segment(reservation);
      int slot = reservation.index();
      boolean signal = index == 0 || slot + 1 >= WRITER_WAKE_RECORDS;
      boolean armedWake = publicationWakeSegment == segment;
      if (slot + 1 == SEGMENT_CAPACITY || armedWake) {
        closeProducerForSnapshot(segment);
      }
      // Cancellation publishes completion too, and has the same immediate-reuse boundary.
      segment.cancelReserved(slot);
      committed++;
      reservation.clear();
      if (signal || armedWake || publicationWakeSegment == segment) {
        signalReady();
      }
    }

    private int sealThrough(long epoch, long maximumSequenceInclusive) {
      int records = 0;
      for (;;) {
        RetirementSegment segment = sealCursor;
        if (segment == null || !segment.isClosed() || !segment.isComplete()) {
          break;
        }
        if (publicationWakeSegment == segment) {
          publicationWakeSegment = null;
        }
        int tail = segment.snapshotTail();
        long end = segment.baseSequence() + tail;
        if (end > maximumSequenceInclusive) {
          break;
        }
        if (!segment.isSealed()) {
          if (!segment.seal(epoch)) {
            break;
          }
          sealedSegments.addLast(segment);
          RetirementJournal.this.sealedSegmentCount.incrementAndGet();
          onSegmentSealed(segment, tail);
          records += tail;
        }
        if (producer.get() == segment && segment.next() == null) {
          installNext(segment);
        }
        RetirementSegment next = segment.next();
        if (next == null) {
          break;
        }
        sealCursor = next;
      }
      if (records != 0) {
        signalReady();
      }
      return records;
    }

    private boolean hasSealableThrough(long maximumSequenceInclusive) {
      RetirementSegment segment = sealCursor;
      return segment != null
          && segment.isClosed()
          && segment.isComplete()
          && segment.baseSequence() + segment.snapshotTail() <= maximumSequenceInclusive;
    }

    private int publishSafe(long minimumLookupEpoch, long minimumValueEpoch) {
      return publishSafe(minimumLookupEpoch, minimumValueEpoch, false);
    }

    private int publishAllSafe() {
      return publishSafe(Long.MAX_VALUE, Long.MAX_VALUE, true);
    }

    private int publishSafe(
        long minimumLookupEpoch, long minimumValueEpoch, boolean includeBoundary) {
      int published = 0;
      for (;;) {
        RetirementSegment segment = sealedSegments.peek();
        long minimumActiveEpoch =
            segment != null && segment.requiresStructuralEpoch()
                ? minimumLookupEpoch
                : minimumValueEpoch;
        if (segment == null
            || (includeBoundary
                ? segment.sealedEpoch() > minimumActiveEpoch
                : segment.sealedEpoch() >= minimumActiveEpoch)) {
          break;
        }
        segment = sealedSegments.poll();
        if (segment == null) {
          continue;
        }
        segment.markDetached();
        segment.safeTicket(safePublishedTicket.incrementAndGet());
        int records = segment.retiredRecordCount();
        long bytes = segment.retiredBytes();
        unsafeRecords.addAndGet(-records);
        unsafeBytes.addAndGet(-bytes);
        safeRecords.addAndGet(records);
        safeBytes.addAndGet(bytes);
        enqueueSafeSegment(segment);
        int remainingSealed = RetirementJournal.this.sealedSegmentCount.decrementAndGet();
        if (remainingSealed < 0) {
          throw new IllegalStateException("retirement sealed segment count underflow");
        }
        published++;
      }
      return published;
    }

    private boolean hasActorWork() {
      return ready.get()
          || hasClosedCompleteProducer()
          || hasWakeableOpenProducer()
          || hasRunnableCompletion();
    }

    private boolean hasWakeableOpenProducer() {
      RetirementSegment current = producer.get();
      return current != null
          && !current.isClosed()
          && current.reservationCount() >= WRITER_WAKE_RECORDS;
    }

    private boolean hasRunnableCompletion() {
      RetirementSegment current = completionCursor.get();
      return current != null
          && current != producer.get()
          && current != sealCursor
          && current.segmentClaimFinished();
    }

    private boolean hasClosedCompleteProducer() {
      RetirementSegment current = producer.get();
      return current != null && current.isClosed() && current.isComplete();
    }

    private synchronized void forceCutOpenProducer() {
      RetirementSegment current = producer.get();
      if (current != null && !current.isClosed() && current.reservationCount() != 0) {
        cutProducer(current);
        signalReady();
      }
    }

    public synchronized long cutAndCaptureWatermark() {
      RetirementSegment current = producer.get();
      if (current == null) {
        return completionPrefix;
      }
      if (current.reservationCount() == 0) {
        return current.baseSequence();
      }
      int tail = cutProducer(current);
      signalReady();
      return current.baseSequence() + tail;
    }

    private synchronized long cutOrArmAndCaptureWatermark() {
      RetirementSegment current = producer.get();
      if (current == null) {
        return completionPrefix;
      }
      // Publish the arm before reading the reservation count. A commit racing before the arm is
      // visible in the count below; a commit racing after it closes and signals this segment.
      publicationWakeSegment = current;
      if (current.reservationCount() == 0) {
        return current.baseSequence();
      }
      int tail = cutProducer(current);
      signalReady();
      return current.baseSequence() + tail;
    }

    private synchronized long cutReadyAndCaptureWatermark() {
      RetirementSegment current = producer.get();
      if (current == null) {
        return completionPrefix;
      }
      int count = current.reservationCount();
      if (!current.isClosed()
          && !(index == 0 && count != 0)
          && count < WRITER_WAKE_RECORDS) {
        return current.baseSequence();
      }
      if (count == 0) {
        return current.baseSequence();
      }
      int tail = cutProducer(current);
      signalReady();
      return current.baseSequence() + tail;
    }

    private synchronized long reservationWatermark() {
      RetirementSegment current = producer.get();
      return current == null ? completionPrefix : current.baseSequence() + current.reservationCount();
    }

    private int cutProducer(RetirementSegment segment) {
      publicationWakeSegment = segment;
      int tail = closeProducerForSnapshot(segment);
      if (segment.isComplete()) {
        publicationWakeSegment = null;
      }
      return tail;
    }

    private void markOpenProducer(RetirementSegment segment) {
      boolean signalOpen = false;
      synchronized (this) {
        if (segment.isClosed()) {
          return;
        }
        if (countedOpenProducer == segment) {
          return;
        }
        if (countedOpenProducer != null) {
          throw new IllegalStateException("retirement lane already has an open producer");
        }
        countedOpenProducer = segment;
        signalOpen = openProducerCount.incrementAndGet() == 1;
      }
      if (signalOpen) {
        signalOpenProducer();
      }
    }

    private int closeProducerForSnapshot(RetirementSegment segment) {
      synchronized (this) {
        int tail = segment.closeForSnapshot();
        if (countedOpenProducer == segment) {
          countedOpenProducer = null;
          int remainingOpen = openProducerCount.decrementAndGet();
          if (remainingOpen < 0) {
            throw new IllegalStateException("retirement open producer count underflow");
          }
        }
        return tail;
      }
    }

    private long reservedRecords() {
      return reserved - committed;
    }

    /** Returns the producer batch not yet folded into the journal-wide counters. */
    private long unsealedRetiredBytes() {
      long total = 0L;
      RetirementSegment cursor = sealCursor;
      RetirementSegment currentProducer = producer.get();
      while (cursor != null) {
        if (!cursor.isSealed()) {
          total = addSaturated(total, cursor.retiredBytes());
        }
        if (cursor == currentProducer) {
          break;
        }
        cursor = cursor.next();
      }
      return total;
    }

    private long unsealedRetiredEntries() {
      long total = 0L;
      RetirementSegment cursor = sealCursor;
      RetirementSegment currentProducer = producer.get();
      while (cursor != null) {
        if (!cursor.isSealed()) {
          total = addSaturated(total, cursor.retiredRecordCount());
        }
        if (cursor == currentProducer) {
          break;
        }
        cursor = cursor.next();
      }
      return total;
    }

    private long reservedRecordsTotal() {
      return reserved;
    }

    private long committedRecordsTotal() {
      return committed;
    }

    private boolean hasUncommittedReservations() {
      return reserved != committed;
    }

    public boolean watermarkComplete(long watermark) {
      advanceCompletion();
      return completionPrefix >= watermark;
    }

    /**
     * Drains completions for every segment this lane's reclaim wave already FINISHED. The WIP
     * counter absorbs any number of finishSegmentClaim calls between two invocations, so a wave
     * pays one advance/drain per lane instead of one per segment.
     */
    private void finishedSegmentsDrained() {
      advanceCompletion();
      if (hasRunnableCompletion()) {
        signalReady();
      }
    }

    private void advanceCompletion() {
      if (completionAdvanceWork.getAndIncrement() != 0) {
        return;
      }
      drainCompletionAdvances(1);
    }

    private void drainCompletionAdvances(int missed) {
      try {
        for (;;) {
          advanceCompletionPrefix();
          int pending = completionAdvanceWork.addAndGet(-missed);
          if (pending == 0) {
            return;
          }
          missed = pending;
        }
      } catch (Throwable failure) {
        // Preserve the old owner-release behavior on exceptional paths. Normal contenders are
        // counted by the WIP loop and cannot lose a handoff or grow the Java stack.
        completionAdvanceWork.set(0);
        throw failure;
      }
    }

    private void advanceCompletionPrefix() {
      for (;;) {
        RetirementSegment current = completionCursor.get();
        if (current == null || !current.segmentClaimFinished()) {
          return;
        }
        if (producer.get() == current || sealCursor == current || !current.tryBeginRecycle()) {
          return;
        }
        RetirementSegment next = current.next();
        completionPrefix =
            Math.max(completionPrefix, current.baseSequence() + current.snapshotTail());
        completionCursor.set(next);
        if (next == null) {
          return;
        }
        current.clearNext();
        returnReusable(current);
      }
    }

    /**
     * Serializes the cold producer handoff between the writer and the maintenance actor.
     *
     * <p>The reservation fast path remains a single producer CAS. Only a full/closed segment
     * reaches this method; without this boundary the writer and actor could both observe the same
     * old producer, race its successor publication, and make the writer repeatedly retry a stale
     * producer while the owning Entry remains locked.
     */
    private synchronized RetirementSegment installNext(RetirementSegment current) {
      RetirementSegment published = producer.get();
      if (published != current) {
        return published;
      }
      if (!current.beginLink()) {
        for (;;) {
          RetirementSegment existing = current.next();
          if (existing != null) {
            producer.compareAndSet(current, existing);
            advanceCompletion();
            return existing;
          }
          RetirementSegment currentProducer = producer.get();
          if (currentProducer != current) {
            return currentProducer;
          }
          int tail = current.snapshotTail();
          long base = current.baseSequence() + (tail < 0 ? current.reservationCount() : tail);
          RetirementSegment candidate = newSegment(this, base);
          if (current.compareAndSetNext(null, candidate)) {
            producer.compareAndSet(current, candidate);
            advanceCompletion();
            return candidate;
          }
          returnReusable(candidate);
        }
      }
      try {
        RetirementSegment next = current.next();
        if (next == null) {
          next = newSegment(this, current.baseSequence() + current.reservationCount());
          if (!current.compareAndSetNext(null, next)) {
            returnReusable(next);
            next = current.next();
          }
        }
        producer.compareAndSet(current, next);
        return next;
      } finally {
        current.endLink();
        advanceCompletion();
      }
    }

    private void bindReadySignal(Runnable signal, Consumer<Throwable> failureHandler) {
      laneReadySignal = signal;
      laneReadySignalFailure = failureHandler;
    }

    private boolean finishReadyDrain() {
      // segmentFinished() can race another advanceCompletion() owner after the reclaim claim has
      // become FINISHED. The actor observes hasRunnableCompletion() in that case, so retry the
      // cursor advance before deciding whether this lane still needs another ready turn.
      advanceCompletion();
      if (ready.compareAndSet(true, false)) {
        int remainingReadyLanes = readyLaneCount.decrementAndGet();
        if (remainingReadyLanes < 0) {
          throw new IllegalStateException("retirement ready lane count underflow");
        }
      }
      boolean remaining = hasActorWork();
      if (remaining) {
        signalReady();
      }
      return remaining || ready.get();
    }

    private boolean tryAcquireProgress() {
      return progressOwner.compareAndSet(false, true);
    }

    private void releaseProgress() {
      progressOwner.set(false);
      if (hasActorWork()) {
        signalReady();
      }
    }

    private RetirementSegment segment(RetirementSegment.Reservation reservation) {
      if (reservation == null || reservation.laneIndex() != index) {
        throw new IllegalStateException("retirement reservation belongs to another lane");
      }
      RetirementSegment segment = reservation.segment();
      if (segment == null) {
        throw new IllegalStateException("retirement reservation is empty");
      }
      if (!reservation.trusted()) {
        segment.validateReservation(reservation);
      }
      return segment;
    }

    private void signalReady() {
      if (ready.get() || !ready.compareAndSet(false, true)) {
        return;
      }
      readyLaneCount.incrementAndGet();
      if (!readyLanes.offer(this)) {
        throw new IllegalStateException("retirement ready lane queue rejected a lane");
      }
      readyHint.set(true);
      Runnable signal = laneReadySignal != null ? laneReadySignal : readySignal;
      if (signal != null) {
        try {
          signal.run();
        } catch (Throwable failure) {
          Consumer<Throwable> failureHandler =
              laneReadySignalFailure != null ? laneReadySignalFailure : readySignalFailure;
          if (failureHandler != null) {
            try {
              failureHandler.accept(failure);
            } catch (Throwable handlerFailure) {
              if (handlerFailure != failure) {
                failure.addSuppressed(handlerFailure);
              }
            }
          }
          // The slot/completion watermark is already published. Keep it durable and let the
          // failure handler wake or stop the owner rather than retrying the producer commit.
        }
      }
    }

  }

  private void signalReady(Runnable signal, Consumer<Throwable> failureHandler) {
    try {
      signal.run();
    } catch (Throwable failure) {
      if (failureHandler != null) {
        try {
          failureHandler.accept(failure);
        } catch (Throwable handlerFailure) {
          if (handlerFailure != failure) {
            failure.addSuppressed(handlerFailure);
          }
        }
      }
      // The journal has no producer rollback after a state transition becomes visible. Treat the
      // signal as best effort and retain the published work for the next owner/teardown drain.
    }
  }

}
