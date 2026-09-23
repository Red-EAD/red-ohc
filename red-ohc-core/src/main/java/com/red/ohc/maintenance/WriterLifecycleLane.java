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
  private final Runnable readinessChanged;
  private Segment producerSegment;
  private Segment consumerSegment;
  private int consumerIndex;

  public WriterLifecycleLane(int segmentCapacity) {
    this(segmentCapacity, new SegmentPool(segmentCapacity), null);
  }

  WriterLifecycleLane(int segmentCapacity, SegmentPool segmentPool) {
    this(segmentCapacity, segmentPool, null);
  }

  WriterLifecycleLane(int segmentCapacity, SegmentPool segmentPool, Runnable readinessChanged) {
    if (Integer.bitCount(segmentCapacity) != 1 || segmentCapacity < 2) {
      throw new IllegalArgumentException("segmentCapacity must be a power of two >= 2");
    }
    if (segmentPool == null) {
      throw new NullPointerException("segmentPool");
    }
    this.segmentCapacity = segmentCapacity;
    this.segmentMask = segmentCapacity - 1L;
    this.segmentPool = segmentPool;
    this.readinessChanged = readinessChanged;
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
      long sequence,
      Entry entry,
      int keyHash,
      long valueAllocation,
      long mutationVersion) {
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
    commit(sequence, wake, false);
  }

  /** Publishes a mutation and returns the mailbox node owned by the committed slot. */
  MailboxMessage commitMutationForMailbox(
      long sequence,
      Entry entry,
      int keyHash,
      long valueAllocation,
      long mutationVersion) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    segment.entries[index] = entry;
    segment.valueAddresses[index] = keyHash;
    segment.allocations[index] = valueAllocation;
    segment.generations[index] = mutationVersion;
    segment.operations[index] = MUTATION;
    segment.causes[index] = -1;
    MailboxMessage mailboxMessage = segment.mailboxMessages[index];
    mailboxMessage.prepare(sequence);
    commit(segment, index, sequence, false, true);
    return mailboxMessage;
  }

  /** Publishes a lifecycle slot whose consumption is represented by a FIFO actor message. */
  MailboxMessage commitForMailbox(long sequence) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    MailboxMessage mailboxMessage = segment.mailboxMessages[index];
    mailboxMessage.prepare(sequence);
    commit(segment, index, sequence, false, true);
    return mailboxMessage;
  }

  private void commit(long sequence, boolean wake, boolean mailboxOwned) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    commit(segment, index, sequence, wake, mailboxOwned);
  }

  private void commit(
      Segment segment, int index, long sequence, boolean wake, boolean mailboxOwned) {
    // Publish ownership before the release-store of the sequence. The actor must never observe a
    // committed lifecycle slot while still seeing the default unmanaged ownership bit.
    segment.mailboxOwned[index] = mailboxOwned;
    segment.published.lazySet(index, sequence);
    publishedRecordsTotal++;
    if (wake) {
      signalReady();
    }
  }

  public void cancel(long sequence) {
    cancel(sequence, true);
  }

  /** Publishes a cancelled slot, optionally leaving wake-up ownership to another transport. */
  public void cancel(long sequence, boolean wake) {
    cancel(sequence, wake, false);
  }

  /** Publishes a cancelled lifecycle slot whose consumption is represented by a FIFO message. */
  MailboxMessage cancelForMailbox(long sequence) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    MailboxMessage mailboxMessage = segment.mailboxMessages[index];
    mailboxMessage.prepare(sequence);
    cancel(segment, index, sequence, false, true);
    return mailboxMessage;
  }

  private void cancel(long sequence, boolean wake, boolean mailboxOwned) {
    Segment segment = producerSegmentFor(sequence);
    int index = (int) (sequence & segmentMask);
    cancel(segment, index, sequence, wake, mailboxOwned);
  }

  private void cancel(
      Segment segment, int index, long sequence, boolean wake, boolean mailboxOwned) {
    segment.entries[index] = null;
    segment.valueAddresses[index] = 0L;
    segment.allocations[index] = 0L;
    segment.generations[index] = 0L;
    segment.operations[index] = 0;
    segment.causes[index] = -1;
    segment.mailboxOwned[index] = mailboxOwned;
    segment.published.lazySet(index, sequence);
    publishedRecordsTotal++;
    if (wake) {
      signalReady();
    }
  }

  public boolean poll(Record record) {
    return poll(record, true);
  }

  /** Polls only records that are driven by the explicit maintenance wake path. */
  boolean pollUnmanaged(Record record) {
    return poll(record, false);
  }

  private boolean poll(Record record, boolean includeMailboxOwned) {
    if (record == null) {
      throw new NullPointerException("record");
    }
    if (consumerIndex == segmentCapacity && !advanceConsumerSegment()) {
      return false;
    }
    long sequence = consumerSegment.baseSequence + consumerIndex;
    if (consumerSegment.published.get(consumerIndex) != sequence
        || (!includeMailboxOwned && consumerSegment.mailboxOwned[consumerIndex])) {
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
    consumerSegment.mailboxOwned[index] = false;
    consumerSegment.mailboxMessages[index].release();
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
    return hasHeadCommitted(true);
  }

  boolean hasUnmanagedHead() {
    return hasHeadCommitted(false);
  }

  private boolean hasHeadCommitted(boolean includeMailboxOwned) {
    Segment segment = consumerSegment;
    int index = consumerIndex;
    if (index == segmentCapacity) {
      segment = segment.next;
      index = 0;
      if (segment == null) {
        return false;
      }
    }
    return segment.published.get(index) == segment.baseSequence + index
        && (includeMailboxOwned || !segment.mailboxOwned[index]);
  }

  public void finishReadyDrain() {
    if (readySignalled.compareAndSet(true, false)) {
      if (journal != null) {
        journal.readyLaneCleared();
      }
      notifyReadinessChanged();
    }
    if (hasUnmanagedHead()) {
      signalReady();
    }
  }

  /**
   * Refreshes the coalesced ready marker after an actor probe or drain. A producer may publish the
   * reservation head between the first check and marker clear, so the second check must happen
   * after the clear to preserve the wake-up.
   */
  boolean refreshReadySignal() {
    if (hasUnmanagedHead()) {
      signalReady();
      return true;
    }
    if (readySignalled.compareAndSet(true, false)) {
      if (journal != null) {
        journal.readyLaneCleared();
      }
      notifyReadinessChanged();
    }
    if (!hasUnmanagedHead()) {
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

  /** Returns the fixed mailbox node owned by this lifecycle slot. */
  MailboxMessage mailboxMessage(long sequence) {
    Segment segment = producerSegmentFor(sequence);
    return segment.mailboxMessages[(int) (sequence & segmentMask)];
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
    if (!readySignalled.compareAndSet(false, true)) {
      return;
    }
    if (journal != null) {
      journal.readyLaneSignalled(this);
    }
    notifyReadinessChanged();
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

  private void notifyReadinessChanged() {
    Runnable callback = readinessChanged;
    if (callback != null) {
      callback.run();
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

  /**
   * Reusable lifecycle mailbox node owned by one segment slot.
   *
   * <p>The node is never pooled independently: its segment cannot be recycled until the actor has
   * released the corresponding lifecycle record, which also clears this node for the next wrap.
   */
  static final class MailboxMessage {
    private WriterLifecycleLane owner;
    private final Segment segment;
    private final int index;
    private long sequence;
    private boolean queued;

    private MailboxMessage(WriterLifecycleLane owner, Segment segment, int index) {
      this.owner = owner;
      this.segment = segment;
      this.index = index;
    }

    void bind(WriterLifecycleLane owner) {
      this.owner = owner;
    }

    void prepare(long expectedSequence) {
      if (queued) {
        throw new IllegalStateException("lifecycle mailbox slot is already queued");
      }
      long actual = segment.baseSequence + index;
      if (owner == null || actual != expectedSequence) {
        throw new IllegalStateException("lifecycle mailbox sequence does not match its slot");
      }
      sequence = expectedSequence;
      queued = true;
    }

    void release() {
      queued = false;
      sequence = 0L;
    }

    WriterLifecycleLane owner() {
      return owner;
    }

    long sequence() {
      if (!queued) {
        throw new IllegalStateException("lifecycle mailbox slot is not queued");
      }
      return sequence;
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
    private final boolean[] mailboxOwned;
    private final MailboxMessage[] mailboxMessages;
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
      mailboxOwned = new boolean[capacity];
      mailboxMessages = new MailboxMessage[capacity];
      for (int index = 0; index < capacity; index++) {
        mailboxMessages[index] = new MailboxMessage(owner, this, index);
      }
      reset(owner, baseSequence);
    }

    private void reset(WriterLifecycleLane owner, long newBaseSequence) {
      baseSequence = newBaseSequence;
      next = null;
      for (int index = 0; index < published.length(); index++) {
        published.lazySet(index, EMPTY);
        mailboxOwned[index] = false;
        mailboxMessages[index].bind(owner);
        mailboxMessages[index].release();
      }
    }
  }
}
