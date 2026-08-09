package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicLong;

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

  public boolean reserve(Reservation reservation) {
    if (reservation.active()) {
      throw new IllegalStateException("reservation is active");
    }
    while (true) {
      long start = producer.get();
      Slot slot = slots[(int) start & mask];
      if (slot.sequence != start) {
        return false;
      }
      if (producer.compareAndSet(start, start + 1L)) {
        reservation.sequence = start;
        reservation.slot = slot;
        return true;
      }
    }
  }

  public void commit(Reservation reservation, Entry entry) {
    publish(reservation, entry);
  }

  public void cancel(Reservation reservation) {
    if (!reservation.active()) {
      return;
    }
    publish(reservation, null);
  }

  /**
   * Drains committed entries and tombstones after all producers have stopped. An uncommitted head
   * is left untouched because its producer is still responsible for committing or cancelling the
   * reservation.
   */
  public int drain() {
    int drained = 0;
    while (true) {
      Entry entry = poll();
      if (entry != null) {
        drained++;
        continue;
      }
      if (pollTombstone()) {
        drained++;
        continue;
      }
      return drained;
    }
  }

  public Entry poll() {
    Slot slot = head();
    if (slot == null || slot.value == null) {
      return null;
    }
    Entry entry = slot.value;
    finish(slot);
    return entry;
  }

  public boolean pollTombstone() {
    Slot slot = head();
    if (slot == null || slot.value != null) {
      return false;
    }
    finish(slot);
    return true;
  }

  public long size() {
    return producer.get() - consumer;
  }

  /** Returns whether the consumer can observe a committed value at the current head. */
  boolean hasCommittedHead() {
    return head() != null;
  }

  private void publish(Reservation reservation, Entry entry) {
    if (!reservation.active()) {
      throw new IllegalStateException("missing reservation");
    }
    Slot slot = reservation.slot;
    slot.value = entry;
    slot.sequence = reservation.sequence + 1L;
    reservation.clear();
  }

  private Slot head() {
    Slot slot = slots[(int) consumer & mask];
    return slot.sequence == consumer + 1L ? slot : null;
  }

  private void finish(Slot slot) {
    slot.value = null;
    slot.sequence = consumer + slots.length;
    consumer++;
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
  }
}
