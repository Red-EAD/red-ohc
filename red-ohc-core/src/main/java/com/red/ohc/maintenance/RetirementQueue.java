package com.red.ohc.maintenance;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/**
 * Fixed-stripe native MPSC retirement FIFO. Producers reserve records before making a pointer
 * unreachable, then publish the records after the mutation. The maintenance actor seals each record
 * with an epoch and returns the native slot only after every reader has crossed it.
 */
public final class RetirementQueue {
  private static final int RECORD_BYTES = 32;
  private static final long SEQUENCE = 0L;
  private static final long ADDRESS = 8L;
  private static final long ALLOCATION = 16L;
  private static final long EPOCH = 24L;

  private final NativeMemory.Memory memory;
  private final Stripe[] stripes;
  private final long capacityRecords;

  /** Only assigns a producer's first stripe; steady-state Reservations retain that assignment. */
  private final AtomicInteger nextPreferredStripe = new AtomicInteger();

  /** Actor-owned compact set of stripes with sealed records that still await QSBR reclaim. */
  private final Stripe[] activeStripes;

  /**
   * A coalescing producer-to-actor hint. It is deliberately a single boolean rather than a
   * per-record ready bitmap: one release store after a native sequence publication is enough to
   * make the actor scan once, and an actor clearing it races safely with a later producer.
   */
  private final AtomicBoolean readyHint = new AtomicBoolean();

  private int activeStripeCount;
  private int sealCursor;
  private int reclaimCursor;
  private long retiredBytes;
  private int retiredEntries;

  /** Published by the actor for deadline-driven QSBR reclaim scheduling. */
  private volatile boolean reclaimPending;

  /** Actor-published diagnostic snapshot; stats readers never traverse the actor-owned set. */
  private volatile long oldestRetireEpoch;

  public RetirementQueue(NativeMemory.Memory memory, int stripeCount, int recordsPerStripe) {
    if (Integer.bitCount(stripeCount) != 1) {
      throw new IllegalArgumentException("stripeCount must be a power of two");
    }
    if (Integer.bitCount(recordsPerStripe) != 1 || recordsPerStripe < 2) {
      throw new IllegalArgumentException("recordsPerStripe must be a power of two >= 2");
    }
    this.memory = memory;
    Stripe[] allocated = new Stripe[stripeCount];
    long stripeBytes = (long) recordsPerStripe * RECORD_BYTES;
    try {
      for (int index = 0; index < stripeCount; index++) {
        long address = memory.allocate(stripeBytes);
        try {
          allocated[index] = new Stripe(index, address, recordsPerStripe);
        } catch (Throwable failure) {
          memory.free(address, stripeBytes);
          throw failure;
        }
      }
    } catch (Throwable failure) {
      for (Stripe stripe : allocated) {
        if (stripe != null) {
          memory.free(stripe.address, stripeBytes);
        }
      }
      throw failure;
    }
    this.stripes = allocated;
    this.activeStripes = new Stripe[stripeCount];
    this.capacityRecords = Math.multiplyExact((long) stripeCount, recordsPerStripe);
  }

  public boolean reserve(Reservation reservation, int records) {
    if (records <= 0) {
      return true;
    }
    if (reservation.active()) {
      throw new IllegalStateException("retirement reservation is still active");
    }
    // A hot writer can temporarily fill one fixed stripe before the next epoch makes its
    // records reclaimable. Continue from its last successful fallback instead of scanning
    // every already-full stripe from the thread-id origin on each replacement.
    int start =
        reservation.preferredStripe >= 0
            ? reservation.preferredStripe
            : nextPreferredStripe.getAndIncrement() & (stripes.length - 1);
    for (int offset = 0; offset < stripes.length; offset++) {
      Stripe stripe = stripes[(start + offset) & (stripes.length - 1)];
      if (stripe.reserve(reservation, records)) {
        return true;
      }
    }
    return false;
  }

  /** Attempts one producer-side reservation without retrying a contended stripe. */
  public boolean tryReserve(Reservation reservation, int records) {
    if (records <= 0) {
      return true;
    }
    if (reservation.active()) {
      throw new IllegalStateException("retirement reservation is still active");
    }
    int start =
        reservation.preferredStripe >= 0
            ? reservation.preferredStripe
            : nextPreferredStripe.getAndIncrement() & (stripes.length - 1);
    for (int offset = 0; offset < stripes.length; offset++) {
      Stripe stripe = stripes[(start + offset) & (stripes.length - 1)];
      if (stripe.tryReserve(reservation, records)) {
        return true;
      }
    }
    return false;
  }

  public void append(Reservation reservation, long address, long allocation) {
    if (!reservation.active()) {
      throw new IllegalStateException("missing retirement reservation");
    }
    Stripe stripe = reservation.stripe;
    stripe.publish(reservation.nextIndex(), address, allocation);
    // Publish only after the native sequence release. If the actor clears the hint before
    // this store, the store remains visible for its next pass; if it clears afterwards, its
    // scan observes this already-published sequence. Either interleaving preserves progress.
    // This is the publication that makes an already-reserved retirement visible to the
    // event-loop's park decision. Use a volatile set: a lazy store can remain invisible to
    // the worker after it consumed the previous coalesced hint and is the only producer
    // notification for a retirement-only burst.
    if (!readyHint.get()) {
      readyHint.set(true);
    }
    reservation.written++;
    if (reservation.written == reservation.count) {
      reservation.clear();
    }
  }

  /** Publishes zero-address records so a failed pre-publication mutation cannot strand capacity. */
  public void cancel(Reservation reservation) {
    if (!reservation.active()) {
      return;
    }
    while (reservation.written < reservation.count) {
      append(reservation, 0L, 0L);
    }
  }

  /** Publishes tombstones for the next records of a still-active batch reservation. */
  public void cancelPrefix(Reservation reservation, int records) {
    if (records < 0 || !reservation.active() || records > reservation.count - reservation.written) {
      throw new IllegalArgumentException("invalid retirement cancellation prefix: " + records);
    }
    for (int index = 0; index < records; index++) {
      append(reservation, 0L, 0L);
    }
  }

  public int remaining(Reservation reservation) {
    return reservation.active() ? reservation.count - reservation.written : 0;
  }

  /** Seals at most {@code limit} producer-published records with the supplied actor epoch. */
  public int seal(int limit, long epoch) {
    int sealed = 0;
    int scanned = 0;
    while (sealed < limit && scanned < stripes.length) {
      Stripe stripe = stripes[sealCursor];
      int stripeSealed = stripe.seal(limit - sealed, epoch, this);
      sealed += stripeSealed;
      if (stripe.hasSealedRecords()) {
        activate(stripe);
        reclaimPending = true;
      }
      // Keep this stripe at the cursor until its bounded head is consumed. Otherwise
      // advance over it; the fixed scan is actor-only and producers do not touch a shared
      // ready bitmap for every replacement.
      if (stripe.hasReadyRecord()) {
        break;
      }
      sealCursor = (sealCursor + 1) & (stripes.length - 1);
      scanned++;
    }
    // A stripe can contain more published records than this bounded pass consumed. Keep
    // the actor required even when the pass stopped after exhausting one stripe rather than
    // exactly at the global limit; otherwise consuming the coalesced hint would strand the
    // remaining stripes until a new producer happens to publish another record.
    if (hasReadyRecord()) {
      readyHint.lazySet(true);
    }
    publishOldestRetireEpoch();
    return sealed;
  }

  private boolean hasReadyRecord() {
    for (Stripe stripe : stripes) {
      if (stripe.hasReadyRecord()) {
        return true;
      }
    }
    return false;
  }

  /** Reclaims at most {@code limit} sealed records whose readers are quiescent or newer. */
  public int reclaim(ReaderRegistry readers, int limit) {
    int reclaimed = 0;
    int remaining = activeStripeCount;
    while (remaining > 0 && reclaimed < limit) {
      if (reclaimCursor >= activeStripeCount) {
        reclaimCursor = 0;
      }
      Stripe stripe = activeStripes[reclaimCursor];
      reclaimed += stripe.reclaim(readers, limit - reclaimed, this);
      remaining--;
      if (stripe.hasSealedRecords()) {
        reclaimCursor = (reclaimCursor + 1) % activeStripeCount;
      } else {
        deactivate(stripe);
      }
    }
    reclaimPending = activeStripeCount != 0;
    publishOldestRetireEpoch();
    return reclaimed;
  }

  /**
   * Actor-only, consumes the coalesced producer hint before performing an expensive stripe scan.
   * The caller must re-request a hint if its bounded sealing budget is exhausted.
   */
  public boolean consumeReadyHint() {
    return readyHint.getAndSet(false);
  }

  /** Actor-only continuation after a bounded seal pass leaves work behind. */
  public void requestSeal() {
    readyHint.set(true);
  }

  /**
   * Lock-free approximate observation; physical capacity is enforced independently by every stripe.
   */
  public boolean hasReadyHint() {
    return readyHint.get();
  }

  /** A volatile reader-side hint; false means a quiescent reader need not signal the actor. */
  public boolean hasPendingReclaim() {
    return reclaimPending;
  }

  int activeStripeCount() {
    return activeStripeCount;
  }

  public long retiredBytes() {
    return retiredBytes;
  }

  public int retiredEntries() {
    return retiredEntries;
  }

  public long oldestEpoch() {
    return oldestRetireEpoch;
  }

  /** Actor-only traversal of the compact active set, published for lock-free diagnostics. */
  private void publishOldestRetireEpoch() {
    long oldest = Long.MAX_VALUE;
    for (int index = 0; index < activeStripeCount; index++) {
      Stripe stripe = activeStripes[index];
      long epoch = stripe.headEpoch();
      if (epoch != 0L && epoch < oldest) {
        oldest = epoch;
      }
    }
    oldestRetireEpoch = oldest == Long.MAX_VALUE ? 0L : oldest;
  }

  public long allocatedBytes() {
    return (long) stripes.length * stripes[0].capacity * RECORD_BYTES;
  }

  public long queuedRecords() {
    long total = 0L;
    for (Stripe stripe : stripes) {
      long outstanding = stripe.producer.get() - stripe.consumer;
      if (outstanding > 0L) {
        total = total > Long.MAX_VALUE - outstanding ? Long.MAX_VALUE : total + outstanding;
      }
    }
    return total;
  }

  public long capacityRecords() {
    return capacityRecords;
  }

  /** Called only after the close gate has stopped writers and all readers are quiescent. */
  public void freeAll() {
    for (Stripe stripe : stripes) {
      stripe.freeAll(this);
    }
    activeStripeCount = 0;
    sealCursor = 0;
    reclaimCursor = 0;
    retiredBytes = 0L;
    retiredEntries = 0;
    reclaimPending = false;
    oldestRetireEpoch = 0L;
    readyHint.set(false);
  }

  public void close() {
    for (Stripe stripe : stripes) {
      memory.free(stripe.address, (long) stripe.capacity * RECORD_BYTES);
    }
  }

  private void sealed(long allocation) {
    if (allocation == 0L) {
      return;
    }
    retiredBytes += WriterArena.allocationWeight(allocation);
    retiredEntries++;
  }

  private void reclaimed(long address, long allocation) {
    if (address != 0L) {
      memory.releaseEntry(address, allocation);
      retiredBytes -= WriterArena.allocationWeight(allocation);
      retiredEntries--;
    }
  }

  private void activate(Stripe stripe) {
    if (stripe.activeIndex >= 0) {
      return;
    }
    stripe.activeIndex = activeStripeCount;
    activeStripes[activeStripeCount++] = stripe;
  }

  private void deactivate(Stripe stripe) {
    int index = stripe.activeIndex;
    if (index < 0) {
      return;
    }
    int last = --activeStripeCount;
    Stripe replacement = activeStripes[last];
    activeStripes[last] = null;
    if (index != last) {
      activeStripes[index] = replacement;
      replacement.activeIndex = index;
    }
    stripe.activeIndex = -1;
    if (reclaimCursor >= activeStripeCount) {
      reclaimCursor = 0;
    }
  }

  public static final class Reservation {
    private Stripe stripe;

    /** Retained across reservations; it is producer-thread-owned through ThreadContext. */
    private int preferredStripe = -1;

    private long start;
    private int count;
    private int written;

    public boolean active() {
      return stripe != null;
    }

    private long nextIndex() {
      return start + written;
    }

    private void assign(Stripe stripe, long start, int count) {
      this.stripe = stripe;
      this.preferredStripe = stripe.index;
      this.start = start;
      this.count = count;
      this.written = 0;
    }

    private void clear() {
      stripe = null;
      start = 0L;
      count = 0;
      written = 0;
    }
  }

  private static final class Stripe {
    final int index;
    final long address;
    final int capacity;
    final int mask;
    final AtomicLong producer = new AtomicLong();
    volatile long consumer;
    long seal;
    int activeIndex = -1;

    Stripe(int index, long address, int capacity) {
      this.index = index;
      this.address = address;
      this.capacity = capacity;
      this.mask = capacity - 1;
      for (int slot = 0; slot < capacity; slot++) {
        NativeMemory.putLong(address(slot) + SEQUENCE, slot);
      }
    }

    boolean reserve(Reservation reservation, int records) {
      if (records > capacity) {
        return false;
      }
      while (true) {
        long start = producer.get();
        if (!available(start, records)) {
          return false;
        }
        if (producer.compareAndSet(start, start + records)) {
          reservation.assign(this, start, records);
          return true;
        }
      }
    }

    boolean tryReserve(Reservation reservation, int records) {
      if (records > capacity) {
        return false;
      }
      long start = producer.get();
      if (!available(start, records)
          || !producer.compareAndSet(start, start + records)) {
        return false;
      }
      reservation.assign(this, start, records);
      return true;
    }

    void publish(long index, long entryAddress, long allocation) {
      long record = address(index);
      NativeMemory.putLong(record + ADDRESS, entryAddress);
      NativeMemory.putLong(record + ALLOCATION, allocation);
      NativeMemory.putLong(record + EPOCH, 0L);
      NativeMemory.putLongRelease(record + SEQUENCE, index + 1L);
    }

    int seal(int limit, long epoch, RetirementQueue owner) {
      int sealed = 0;
      while (sealed < limit && seal < producer.get()) {
        long record = address(seal);
        if (NativeMemory.getLongVolatile(record + SEQUENCE) != seal + 1L) {
          break;
        }
        if (NativeMemory.getLong(record + EPOCH) == 0L) {
          long allocation = NativeMemory.getLong(record + ALLOCATION);
          NativeMemory.putLongRelease(record + EPOCH, epoch);
          owner.sealed(allocation);
          sealed++;
        }
        seal++;
      }
      return sealed;
    }

    int reclaim(ReaderRegistry readers, int limit, RetirementQueue owner) {
      int reclaimed = 0;
      long approvedEpoch = Long.MIN_VALUE;
      while (reclaimed < limit && consumer < seal) {
        long record = address(consumer);
        long epoch = NativeMemory.getLongVolatile(record + EPOCH);
        if (epoch == 0L) {
          break;
        }
        // Records are sealed FIFO, so a reader grace-period scan is sufficient for the
        // complete contiguous run carrying this epoch. A reader entering after the
        // scan cannot acquire the already-unpublished pointer; a pre-existing reader
        // had published its epoch before the pointer update and is included here.
        if (epoch != approvedEpoch) {
          if (!allQuiescentAfter(readers, epoch)) {
            break;
          }
          approvedEpoch = epoch;
        }
        long entryAddress = NativeMemory.getLong(record + ADDRESS);
        long allocation = NativeMemory.getLong(record + ALLOCATION);
        owner.reclaimed(entryAddress, allocation);
        NativeMemory.putLongRelease(record + SEQUENCE, consumer + capacity);
        consumer++;
        reclaimed++;
      }
      return reclaimed;
    }

    boolean hasReadyRecord() {
      return seal < producer.get()
          && NativeMemory.getLongVolatile(address(seal) + SEQUENCE) == seal + 1L;
    }

    boolean hasSealedRecords() {
      return consumer < seal;
    }

    long headEpoch() {
      if (consumer >= seal) {
        return 0L;
      }
      return NativeMemory.getLongVolatile(address(consumer) + EPOCH);
    }

    void freeAll(RetirementQueue owner) {
      while (consumer < producer.get()) {
        long record = address(consumer);
        if (NativeMemory.getLongVolatile(record + SEQUENCE) != consumer + 1L) {
          break;
        }
        long entryAddress = NativeMemory.getLong(record + ADDRESS);
        long allocation = NativeMemory.getLong(record + ALLOCATION);
        owner.reclaimed(entryAddress, allocation);
        NativeMemory.putLongRelease(record + SEQUENCE, consumer + capacity);
        consumer++;
      }
    }

    private boolean available(long start, int records) {
      for (int offset = 0; offset < records; offset++) {
        if (NativeMemory.getLongVolatile(address(start + offset) + SEQUENCE) != start + offset) {
          return false;
        }
      }
      return true;
    }

    private long address(long index) {
      return address + ((index & mask) * RECORD_BYTES);
    }

    private static boolean allQuiescentAfter(ReaderRegistry readers, long retireEpoch) {
      for (WeakReference<ReaderSlot> reference : readers.snapshot()) {
        ReaderSlot reader = reference.get();
        if (reader == null) {
          continue;
        }
        long activeEpoch = reader.epoch;
        if (activeEpoch != 0L && activeEpoch <= retireEpoch) {
          return false;
        }
      }
      return true;
    }
  }
}
