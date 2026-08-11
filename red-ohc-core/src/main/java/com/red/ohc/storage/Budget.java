package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/** Exact native-resident admission budget backed by fixed writer-aligned credit stripes. */
public final class Budget {
  private static final long MAX_REFILL_BYTES = 64L << 10;
  private static final int CACHE_LINE_LONGS = 8;

  private final long capacity;
  private final AtomicLong available;
  private final AtomicLongArray stripeCredits;
  private final int stripeMask;

  /** Test and low-level callers that do not have a NativeMemory stripe count use one stripe. */
  public Budget(long capacity) {
    this(capacity, 1);
  }

  public Budget(long capacity, int stripeCount) {
    if (capacity <= 0L) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    if (Integer.bitCount(stripeCount) != 1 || stripeCount <= 0) {
      throw new IllegalArgumentException("stripeCount must be a positive power of two");
    }
    this.capacity = capacity;
    this.available = new AtomicLong(capacity);
    this.stripeCredits = new AtomicLongArray(stripeCount * CACHE_LINE_LONGS);
    this.stripeMask = stripeCount - 1;
  }

  /** Attempts one producer-side reservation without waiting for maintenance. */
  public boolean tryReserve(long bytes) {
    if (bytes <= 0L || bytes > capacity) {
      return false;
    }
    int offset = stripeOffset(Thread.currentThread().getId());
    while (true) {
      long credit = stripeCredits.get(offset);
      if (credit >= bytes) {
        if (stripeCredits.compareAndSet(offset, credit, credit - bytes)) {
          return true;
        }
        Thread.onSpinWait();
        continue;
      }

      long needed = bytes - credit;
      long free = available.get();
      if (free < needed) {
        return false;
      }
      long request = Math.max(needed, Math.min(MAX_REFILL_BYTES, capacity));
      long grant = Math.min(free, request);
      if (available.compareAndSet(free, free - grant)) {
        addStripeCredit(offset, grant);
      }
    }
  }

  /** Returns a reservation to the stripe selected by the calling thread. */
  public void refund(long bytes) {
    if (bytes <= 0L) {
      return;
    }
    addStripeCredit(stripeOffset(Thread.currentThread().getId()), bytes);
  }

  /** Returns reclaimed resident bytes directly to the global balance. */
  public void release(long bytes) {
    if (bytes <= 0L) {
      return;
    }
    while (true) {
      long free = available.get();
      long updated = free + bytes;
      if (updated < free || updated > capacity) {
        throw new IllegalStateException("native budget overflow: " + updated + ">" + capacity);
      }
      if (available.compareAndSet(free, updated)) {
        return;
      }
      Thread.onSpinWait();
    }
  }

  /** Returns idle credit from every fixed stripe to the global balance. */
  public void reclaimIdleCredits() {
    for (int stripe = 0; stripe <= stripeMask; stripe++) {
      long credit = stripeCredits.getAndSet(stripeOffset(stripe), 0L);
      if (credit != 0L) {
        release(credit);
      }
    }
  }

  /** A stats-only snapshot; producers are not stopped, so a concurrent value is approximate. */
  public long reserved() {
    long free = available.get();
    for (int stripe = 0; stripe <= stripeMask; stripe++) {
      free += stripeCredits.get(stripeOffset(stripe));
    }
    long resident = capacity - free;
    return resident <= 0L ? 0L : Math.min(capacity, resident);
  }

  public long capacity() {
    return capacity;
  }

  public void clear() {
    for (int stripe = 0; stripe <= stripeMask; stripe++) {
      stripeCredits.set(stripeOffset(stripe), 0L);
    }
    available.set(capacity);
  }

  int stripeCount() {
    return stripeMask + 1;
  }

  int stripeIndex(long threadId) {
    return ((int) threadId) & stripeMask;
  }

  long availableBalance() {
    return available.get();
  }

  long stripeCredit(int stripe) {
    return stripeCredits.get(stripeOffset(stripe));
  }

  private int stripeOffset(long threadId) {
    return stripeOffset(stripeIndex(threadId));
  }

  private static int stripeOffset(int stripe) {
    return stripe * CACHE_LINE_LONGS;
  }

  private void addStripeCredit(int offset, long bytes) {
    while (true) {
      long credit = stripeCredits.get(offset);
      long updated = credit + bytes;
      if (updated < credit || updated > capacity) {
        throw new IllegalStateException("native budget stripe overflow: " + updated + ">" + capacity);
      }
      if (stripeCredits.compareAndSet(offset, credit, updated)) {
        return;
      }
      Thread.onSpinWait();
    }
  }
}
