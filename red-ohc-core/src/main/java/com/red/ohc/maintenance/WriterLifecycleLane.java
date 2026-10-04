package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Consumer;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import com.red.ohc.api.RemovalCause;
import com.red.ohc.index.Entry;

/**
 * One writer-owned lifecycle lane. The writer alone advances the producer cursor; the maintenance
 * actor alone advances the consumer cursor. Publication remains per-slot so a paused reservation
 * never exposes an incomplete record or blocks another lane.
 */
public final class WriterLifecycleLane {
  public static final int REMOVE = 1;
  public static final int MUTATION = 2;

  /** A mutation record whose primitive seed could not be tied to the published version. */
  public static final long UNSEEDED_MUTATION_VERSION = Long.MIN_VALUE;

  private static final long EMPTY = Long.MIN_VALUE;

  /**
   * Cached enum table: {@code RemovalCause.values()} clones a fresh array per call and this poll
   * runs on the maintenance actor for every expiry-cause record. Ordinals of a public enum are
   * de-facto frozen; the clone is never mutated.
   */
  private static final RemovalCause[] REMOVAL_CAUSES = RemovalCause.values();

  private final int segmentCapacity;
  private final long segmentMask;
  private final SegmentPool segmentPool;
  private final AtomicBoolean readySignalled = new AtomicBoolean();

  /** Bound by the owning journal before the lane becomes visible to producers. */
  private WriterLifecycleJournal journal;

  private int laneIndex;

  private volatile long producerSequence;
  private volatile long publishedRecordsTotal;
  private volatile long completedRecordsTotal;
  private volatile long allocatedSegments = 1L;
  private volatile long headOfLineStopCount;
  private volatile Runnable readySignal;
  private volatile Consumer<Throwable> readySignalFailure;
  private Segment producerSegment;
  private Segment consumerSegment;
  private int consumerIndex;

  public WriterLifecycleLane(int segmentCapacity) {
    this(segmentCapacity, new SegmentPool(segmentCapacity));
  }

  WriterLifecycleLane(int segmentCapacity, SegmentPool segmentPool) {
    if (Integer.bitCount(segmentCapacity) != 1 || segmentCapacity < 2) {
      throw new IllegalArgumentException("segmentCapacity must be a power of two >= 2");
    }
    if (segmentPool == null) {
      throw new NullPointerException("segmentPool");
    }
    this.segmentCapacity = segmentCapacity;
    this.segmentMask = segmentCapacity - 1L;
    this.segmentPool = segmentPool;
    Segment initial = new Segment(this, segmentCapacity, 0L);
    producerSegment = initial;
    consumerSegment = initial;
  }

  /** Reserves a slot and publishes the new fixed-watermark boundary with a volatile write. */
  public long reserve() {
    long sequence = producerSequence;
    if ((sequence & segmentMask) == 0L && sequence != producerSegment.baseSequence) {
      rollover(sequence);
    }
    producerSequence = sequence + 1L;
    return sequence;
  }

  public void writeRemoval(
      long sequence,
      Entry entry,
      long valueAddress,
      long allocation,
      long generation,
      RemovalCause cause) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    segment.entries[index] = entry;
    segment.valueAddresses[index] = valueAddress;
    segment.allocations[index] = allocation;
    segment.generations[index] = generation;
    segment.operations[index] = REMOVE;
    segment.causes[index] = (byte) (cause == null ? -1 : cause.ordinal());
  }

  public void writeMutation(
      long sequence, Entry entry, int keyHash, long valueAllocation, long mutationVersion) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    segment.entries[index] = entry;
    // MUTATION reuses the existing primitive columns: valueAddresses carries the immutable key
    // hash, allocations carries the value allocation seed, and generations carries the captured
    // mutation version. REMOVE keeps its original column meanings.
    segment.valueAddresses[index] = keyHash;
    segment.allocations[index] = valueAllocation;
    segment.generations[index] = mutationVersion;
    segment.operations[index] = MUTATION;
    segment.causes[index] = -1;
  }

  public void commit(long sequence) {
    commit(sequence, true);
  }

  public void commit(long sequence, boolean wake) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    segment.published.lazySet(index, sequence);
    publishedRecordsTotal++;
    if (wake) {
      signalReady();
    }
  }

  public void cancel(long sequence) {
    cancel(sequence, true);
  }

  public void cancel(long sequence, boolean wake) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    segment.entries[index] = null;
    segment.valueAddresses[index] = 0L;
    segment.allocations[index] = 0L;
    segment.generations[index] = 0L;
    segment.operations[index] = 0;
    segment.causes[index] = -1;
    segment.published.lazySet(index, sequence);
    publishedRecordsTotal++;
    if (wake) {
      signalReady();
    }
  }

  public boolean poll(Record record) {
    if (record == null) {
      throw new NullPointerException("record");
    }
    if (consumerIndex == segmentCapacity && !advanceConsumerSegment()) {
      return false;
    }
    long sequence = consumerSegment.baseSequence + consumerIndex;
    if (consumerSegment.published.get(consumerIndex) != sequence) {
      if (hasCommittedRecords()) {
        headOfLineStopCount++;
      }
      return false;
    }
    record.segment = consumerSegment;
    record.index = consumerIndex;
    record.sequence = sequence;
    record.entry = consumerSegment.entries[consumerIndex];
    record.valueAddress = consumerSegment.valueAddresses[consumerIndex];
    record.allocation = consumerSegment.allocations[consumerIndex];
    record.generation = consumerSegment.generations[consumerIndex];
    record.operation = consumerSegment.operations[consumerIndex];
    int cause = consumerSegment.causes[consumerIndex];
    record.cause = cause < 0 ? null : REMOVAL_CAUSES[cause];
    return true;
  }

  public void release(Record record) {
    if (record == null || record.segment != consumerSegment || record.index != consumerIndex) {
      throw new IllegalStateException("lifecycle records must be released in order");
    }
    int index = record.index;
    consumerSegment.entries[index] = null;
    consumerSegment.valueAddresses[index] = 0L;
    consumerSegment.allocations[index] = 0L;
    consumerSegment.generations[index] = 0L;
    consumerSegment.operations[index] = 0;
    consumerSegment.causes[index] = -1;
    consumerSegment.published.lazySet(index, EMPTY);
    consumerIndex++;
    completedRecordsTotal++;
    record.clear();
  }

  public boolean watermarkComplete(long watermark) {
    return consumerSequence() >= watermark;
  }

  public long queuedRecords() {
    return Math.max(0L, publishedRecordsTotal - completedRecordsTotal);
  }

  public long reservedRecords() {
    return Math.max(0L, producerSequence - consumerSequence());
  }

  public long reservationWatermark() {
    return producerSequence;
  }

  public long publishedRecordsTotal() {
    return publishedRecordsTotal;
  }

  public long completedRecordsTotal() {
    return completedRecordsTotal;
  }

  public long allocatedSegments() {
    return allocatedSegments;
  }

  public long headOfLineStopCount() {
    return headOfLineStopCount;
  }

  public boolean hasCommittedRecords() {
    return publishedRecordsTotal != completedRecordsTotal;
  }

  public boolean hasHeadCommitted() {
    Segment segment = consumerSegment;
    int index = consumerIndex;
    if (index == segmentCapacity) {
      segment = segment.next;
      index = 0;
      if (segment == null) {
        return false;
      }
    }
    return segment.published.get(index) == segment.baseSequence + index;
  }

  public void finishReadyDrain() {
    if (readySignalled.compareAndSet(true, false)) {
      if (journal != null) {
        journal.readyLaneCleared();
      }
    }
    if (hasHeadCommitted()) {
      signalReady();
    }
  }

  /**
   * Refreshes the coalesced ready marker after an actor probe or drain. A producer may publish the
   * reservation head between the first check and marker clear, so the second check must happen
   * after the clear to preserve the wake-up.
   */
  boolean refreshReadySignal() {
    if (hasHeadCommitted()) {
      signalReady();
      return true;
    }
    if (readySignalled.compareAndSet(true, false)) {
      if (journal != null) {
        journal.readyLaneCleared();
      }
    }
    if (!hasHeadCommitted()) {
      return false;
    }
    signalReady();
    return true;
  }

  void bindJournal(WriterLifecycleJournal owner, int index) {
    this.journal = owner;
    this.laneIndex = index;
  }

  int laneIndex() {
    return laneIndex;
  }

  void bindReadySignal(Runnable signal) {
    bindReadySignal(signal, null);
  }

  void bindReadySignal(Runnable signal, Consumer<Throwable> failureHandler) {
    readySignal = signal;
    readySignalFailure = failureHandler;
    if (signal != null) {
      refreshReadySignal();
    }
  }

  private void rollover(long baseSequence) {
    Segment next = segmentPool.acquire();
    if (next == null) {
      next = new Segment(this, segmentCapacity, baseSequence);
      allocatedSegments++;
    } else {
      next.reset(this, baseSequence);
    }
    producerSegment.next = next;
    producerSegment = next;
  }

  private Segment producerSegmentFor(long sequence) {
    Segment segment = producerSegment;
    if (sequence < segment.baseSequence) {
      segment = consumerSegment;
      while (segment != null && sequence >= segment.baseSequence + segmentCapacity) {
        segment = segment.next;
      }
    }
    if (segment == null
        || sequence < segment.baseSequence
        || sequence >= segment.baseSequence + segmentCapacity
        || sequence >= producerSequence) {
      throw new IllegalStateException("lifecycle sequence is not reserved: " + sequence);
    }
    return segment;
  }

  private boolean advanceConsumerSegment() {
    Segment consumed = consumerSegment;
    Segment next = consumed.next;
    if (next == null) {
      return false;
    }
    consumerSegment = next;
    consumerIndex = 0;
    consumed.next = null;
    segmentPool.recycle(consumed);
    return true;
  }

  private long consumerSequence() {
    return consumerSegment.baseSequence + consumerIndex;
  }

  private void signalReady() {
    if (journal != null) {
      // Count the set before the marker becomes visible: a racing finishReadyDrain can otherwise
      // clear the marker between the CAS and the journal count, decrementing an uncounted set.
      while (true) {
        if (readySignalled.get()) {
          return;
        }
        journal.beginReadyLaneSignal();
        if (readySignalled.compareAndSet(false, true)) {
          break;
        }
        journal.retractReadyLaneSignal();
      }
      journal.publishReadyLane(this);
    } else {
      if (!readySignalled.compareAndSet(false, true)) {
        return;
      }
    }
    Runnable signal = readySignal;
    if (signal != null) {
      try {
        signal.run();
      } catch (Throwable failure) {
        Consumer<Throwable> failureHandler = readySignalFailure;
        if (failureHandler != null) {
          try {
            failureHandler.accept(failure);
          } catch (Throwable handlerFailure) {
            if (handlerFailure != failure) {
              failure.addSuppressed(handlerFailure);
            }
          }
        }
        // The publication is already durable. Do not let a wake-up failure make the producer retry
        // the same sequence and double-publish it; a bound failure handler is responsible for
        // forcing the actor's terminal/unpark path.
      }
    }
  }

  static final class SegmentPool {
    private final MpmcUnboundedXaddArrayQueue<Segment> segments;

    SegmentPool(int chunkSize) {
      segments = new MpmcUnboundedXaddArrayQueue<>(chunkSize);
    }

    Segment acquire() {
      return segments.poll();
    }

    void recycle(Segment segment) {
      segments.offer(segment);
    }
  }

  public static final class Record {
    private Segment segment;
    private int index;
    public long sequence;
    public Entry entry;
    public long valueAddress;
    public long allocation;
    public long generation;
    public int operation;
    public RemovalCause cause;

    public void clear() {
      segment = null;
      index = 0;
      sequence = 0L;
      entry = null;
      valueAddress = 0L;
      allocation = 0L;
      generation = 0L;
      operation = 0;
      cause = null;
    }
  }

  private static final class Segment {
    private final AtomicLongArray published;
    private final Entry[] entries;
    private final long[] valueAddresses;
    private final long[] allocations;
    private final long[] generations;
    private final int[] operations;
    private final byte[] causes;
    private volatile long baseSequence;
    private volatile Segment next;

    private Segment(WriterLifecycleLane owner, int capacity, long baseSequence) {
      published = new AtomicLongArray(capacity);
      entries = new Entry[capacity];
      valueAddresses = new long[capacity];
      allocations = new long[capacity];
      generations = new long[capacity];
      operations = new int[capacity];
      causes = new byte[capacity];
      reset(owner, baseSequence);
    }

    private void reset(WriterLifecycleLane owner, long newBaseSequence) {
      baseSequence = newBaseSequence;
      next = null;
      for (int index = 0; index < published.length(); index++) {
        published.lazySet(index, EMPTY);
      }
    }
  }
}
