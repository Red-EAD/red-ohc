package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.red.ohc.api.RemovalCause;
import com.red.ohc.index.Entry;

/**
 * Fixed-capacity reliable removal transport. A producer reserves a sequence before changing the
 * CHM, then commits the Entry or a tombstone after retirement records are complete. The worker
 * consumes strictly in sequence, so a later remove cannot overtake an interrupted producer.
 */
public final class ReliableRemovalQueue {
  private final Slot[] slots;
  private final int mask;
  private final AtomicLong producer = new AtomicLong();
  private volatile long consumer;
  private final AtomicBoolean committedHint = new AtomicBoolean();

  public ReliableRemovalQueue(int capacity) {
    if (Integer.bitCount(capacity) != 1 || capacity < 2) {
      throw new IllegalArgumentException("capacity must be a power of two >= 2");
    }
    this.slots = new Slot[capacity];
    for (int index = 0; index < capacity; index++) {
      slots[index] = new Slot();
      slots[index].sequence = index;
    }
    this.mask = capacity - 1;
  }

  /** Attempts one producer-side slot reservation without spinning on a contended slot. */
  public boolean tryReserve(Reservation reservation) {
    if (reservation.active()) {
      throw new IllegalStateException("reservation is active");
    }
    long start = producer.get();
    Slot slot = slots[(int) start & mask];
    if (slot.sequence != start || !producer.compareAndSet(start, start + 1L)) {
      return false;
    }
    reservation.sequence = start;
    reservation.slot = slot;
    return true;
  }

  public void commit(Reservation reservation, Entry entry) {
    publish(reservation, entry, 0L, 0L, null);
  }

  void commit(
      Reservation reservation,
      Entry entry,
      long valueAddress,
      long valueAllocation,
      RemovalCause cause) {
    publish(reservation, entry, valueAddress, valueAllocation, cause);
  }

  public void cancel(Reservation reservation) {
    if (!reservation.active()) {
      return;
    }
    publish(reservation, null, 0L, 0L, null);
  }

  /**
   * Drains committed entries and tombstones after all producers have stopped. An uncommitted head
   * is left untouched because its producer is still responsible for committing or cancelling the
   * reservation.
   */
  public int drain() {
    int drained = 0;
    Record record = new Record();
    while (true) {
      if (pollNext(record)) {
        drained++;
        continue;
      }
      return drained;
    }
  }

  /** Consumes one committed head into an actor-owned record without allocating. */
  public boolean pollNext(Record record) {
    if (record == null) {
      throw new NullPointerException("record");
    }
    Slot slot = head();
    if (slot == null) {
      // A reserved but not-yet-committed head is not immediate work. Clear the coalesced
      // hint, then recheck the head so a commit racing this clear cannot be stranded.
      committedHint.set(false);
      if (head() != null) {
        committedHint.set(true);
      }
      return false;
    }
    record.entry = slot.value;
    record.valueAddress = slot.valueAddress;
    record.valueAllocation = slot.valueAllocation;
    record.cause = slot.cause;
    finish(slot);
    return true;
  }

  public long size() {
    return producer.get() - consumer;
  }

  /** Producer publication hint; the actor confirms readiness only through {@link #pollNext}. */
  boolean hasCommittedHint() {
    return committedHint.get();
  }

  private void publish(
      Reservation reservation,
      Entry entry,
      long valueAddress,
      long valueAllocation,
      RemovalCause cause) {
    if (!reservation.active()) {
      throw new IllegalStateException("missing reservation");
    }
    Slot slot = reservation.slot;
    slot.valueAddress = valueAddress;
    slot.valueAllocation = valueAllocation;
    slot.cause = cause;
    slot.value = entry;
    slot.sequence = reservation.sequence + 1L;
    reservation.clear();
    committedHint.set(true);
  }

  private Slot head() {
    Slot slot = slots[(int) consumer & mask];
    return slot.sequence == consumer + 1L ? slot : null;
  }

  private void finish(Slot slot) {
    slot.value = null;
    slot.valueAddress = 0L;
    slot.valueAllocation = 0L;
    slot.cause = null;
    slot.sequence = consumer + slots.length;
    consumer++;
    if (consumer == producer.get()) {
      committedHint.set(false);
    }
  }

  public static final class Record {
    Entry entry;
    long valueAddress;
    long valueAllocation;
    RemovalCause cause;

    public void clear() {
      entry = null;
      valueAddress = 0L;
      valueAllocation = 0L;
      cause = null;
    }
  }

  public static final class Reservation {
    private long sequence;
    private Slot slot;

    public boolean active() {
      return slot != null;
    }

    private void clear() {
      slot = null;
      sequence = 0L;
    }
  }

  private static final class Slot {
    volatile long sequence;
    volatile Entry value;
    long valueAddress;
    long valueAllocation;
    RemovalCause cause;
  }
}
