package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.jctools.queues.MpscArrayQueue;

/** Exact native-resident admission budget backed by fixed writer-aligned credit stripes. */
public final class Budget {
  private static final long MAX_REFILL_BYTES = 64L << 10;
  private static final int MAX_RESERVE_RETRIES = NativeMemory.LOGICAL_CPU_COUNT;
  private static final int CACHE_LINE_LONGS = 8;
  private static final int RESERVE_SUCCESS = 1;
  private static final int RESERVE_RETRY = 0;
  private static final int RESERVE_RETRY_WITH_SPIN = -1;
  private static final int RESERVE_FAILURE = -2;

  private final long capacity;
  private final AtomicLong available;
  private final AtomicLongArray stripeCredits;
  /** Serializes refill publication for each stripe so a producer cannot observe a half-refill. */
  private final AtomicIntegerArray stripeRefills;
  private final AtomicIntegerArray dirtyStripes;
  private final StripeToken[] stripeTokens;
  /** At most one token per stripe can be outstanding, so fixed reusable storage is sufficient. */
  private final MpscArrayQueue<StripeToken> dirtyStripeQueue;
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
    this.stripeRefills = new AtomicIntegerArray(stripeCount);
    this.dirtyStripes = new AtomicIntegerArray(stripeCount);
    this.stripeTokens = new StripeToken[stripeCount];
    for (int stripe = 0; stripe < stripeCount; stripe++) {
      stripeTokens[stripe] = new StripeToken(stripe);
    }
    this.dirtyStripeQueue = new MpscArrayQueue<>(Math.max(2, stripeCount));
    this.stripeMask = stripeCount - 1;
  }

  /** Attempts admission without waiting indefinitely on a contended fixed stripe. */
  public boolean tryReserve(long bytes, int stripe) {
    if (bytes <= 0L || bytes > capacity) {
      return false;
    }
    int offset = stripeOffset(stripe);
    for (int attempt = 0; attempt < MAX_RESERVE_RETRIES; attempt++) {
      int consumed = tryConsumeStripeCredit(offset, bytes);
      if (consumed == RESERVE_SUCCESS) {
        return true;
      }
      if (consumed == RESERVE_RETRY_WITH_SPIN) {
        Thread.onSpinWait();
        continue;
      }
      int refill = tryRefillStripe(stripe, offset, bytes);
      if (refill == RESERVE_SUCCESS) {
        return true;
      }
      if (refill == RESERVE_FAILURE) {
        return false;
      }
      if (refill == RESERVE_RETRY_WITH_SPIN) {
        Thread.onSpinWait();
      }
    }
    return false;
  }

  private int tryConsumeStripeCredit(int offset, long bytes) {
    long credit = stripeCredits.get(offset);
    if (credit < bytes) {
      return RESERVE_RETRY;
    }
    return stripeCredits.compareAndSet(offset, credit, credit - bytes)
        ? RESERVE_SUCCESS
        : RESERVE_RETRY_WITH_SPIN;
  }

  private int tryRefillStripe(int stripe, int offset, long bytes) {
    if (!stripeRefills.compareAndSet(stripe, 0, 1)) {
      return RESERVE_RETRY_WITH_SPIN;
    }
    try {
      long credit = stripeCredits.get(offset);
      if (credit >= bytes) {
        return stripeCredits.compareAndSet(offset, credit, credit - bytes)
            ? RESERVE_SUCCESS
            : RESERVE_RETRY_WITH_SPIN;
      }

      long needed = bytes - credit;
      long free = available.get();
      if (free < needed) {
        return RESERVE_FAILURE;
      }
      long request = Math.max(needed, Math.min(MAX_REFILL_BYTES, capacity));
      long grant = Math.min(free, request);
      if (!available.compareAndSet(free, free - grant)) {
        return RESERVE_RETRY_WITH_SPIN;
      }
      if (tryAddStripeCredit(offset, grant)) {
        credit = stripeCredits.get(offset);
        if (credit >= bytes && stripeCredits.compareAndSet(offset, credit, credit - bytes)) {
          return RESERVE_SUCCESS;
        }
        return RESERVE_RETRY;
      }
      available.getAndAdd(grant);
      return RESERVE_FAILURE;
    } finally {
      stripeRefills.set(stripe, 0);
    }
  }

  /** Returns a reservation to an already-cached fixed stripe. */
  public void refund(long bytes, int stripe) {
    if (bytes <= 0L) {
      return;
    }
    addStripeCredit(stripeOffset(stripe), bytes);
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
  public long reclaimIdleCredits() {
    if (dirtyStripeQueue.isEmpty()) {
      return 0L;
    }
    long reclaimed = 0L;
    StripeToken token;
    while ((token = dirtyStripeQueue.poll()) != null) {
      int stripe = token.index;
      long credit = stripeCredits.getAndSet(stripeOffset(stripe), 0L);
      if (credit != 0L) {
        release(credit);
        reclaimed += credit;
      }
      dirtyStripes.set(stripe, 0);
      if (stripeCredits.get(stripeOffset(stripe)) != 0L
          && dirtyStripes.compareAndSet(stripe, 0, 1)) {
        dirtyStripeQueue.offer(token);
      }
    }
    return reclaimed;
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
      dirtyStripes.set(stripe, 0);
      stripeRefills.set(stripe, 0);
    }
    dirtyStripeQueue.clear();
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

  public boolean hasIdleCreditHint() {
    return !dirtyStripeQueue.isEmpty();
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
        int stripe = offset / CACHE_LINE_LONGS;
        if (dirtyStripes.compareAndSet(stripe, 0, 1)) {
          dirtyStripeQueue.offer(stripeTokens[stripe]);
        }
        return;
      }
      Thread.onSpinWait();
    }
  }

  private boolean tryAddStripeCredit(int offset, long bytes) {
    for (int attempt = 0; attempt < MAX_RESERVE_RETRIES; attempt++) {
      long credit = stripeCredits.get(offset);
      long updated = credit + bytes;
      if (updated < credit || updated > capacity) {
        throw new IllegalStateException(
            "native budget stripe overflow: " + updated + ">" + capacity);
      }
      if (stripeCredits.compareAndSet(offset, credit, updated)) {
        int stripe = offset / CACHE_LINE_LONGS;
        if (dirtyStripes.compareAndSet(stripe, 0, 1)) {
          dirtyStripeQueue.offer(stripeTokens[stripe]);
        }
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  private static final class StripeToken {
    private final int index;

    private StripeToken(int index) {
      this.index = index;
    }
  }
}
