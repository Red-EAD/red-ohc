package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Exact native-resident admission budget with fixed CPU stripes. A normal writer consumes its
 * stripe-local credit; the globally contended balance is touched only when a stripe refills or
 * the single maintenance actor releases reclaimed resident memory.
 */
public final class Budget {
    private static final long MAX_REFILL_BYTES = 64L << 10;

    private final long capacity;
    /** Capacity that is neither resident nor temporarily assigned to a writer stripe. */
    private final AtomicLong available;
    private final Stripe[] credits;
    private final int stripeMask;
    private final long refillBytes;

    public Budget(long capacity) {
        if (capacity <= 0L) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
        int count = 1;
        int target = Math.max(1, Runtime.getRuntime().availableProcessors());
        while (count < target && count < (1 << 30)) count <<= 1;
        this.credits = new Stripe[count];
        for (int index = 0; index < count; index++) credits[index] = new Stripe();
        this.stripeMask = count - 1;
        this.available = new AtomicLong(capacity);
        long fairShare = Math.max(1L, capacity / count);
        this.refillBytes = Math.min(MAX_REFILL_BYTES, fairShare);
    }

    /** Atomically admits actual native resident weight; no allocation can exceed {@link #capacity}. */
    public boolean reserve(long bytes) {
        if (bytes <= 0L || bytes > capacity) return false;
        int stripe = stripeForCurrentThread();
        if (consumeCredit(stripe, bytes)) return true;
        return refillAndConsume(stripe, bytes);
    }

    /** The actor is the normal caller, so returning reclaimed bytes never creates a producer hotspot. */
    public void release(long bytes) {
        if (bytes <= 0L) return;
        long updated = available.addAndGet(bytes);
        if (updated <= capacity) return;
        available.addAndGet(-bytes);
        throw new IllegalStateException("native budget overflow: " + updated + ">" + capacity);
    }

    /** Returns a reservation whose allocated block was never published. */
    public void refund(long bytes) {
        release(bytes);
    }

    /** A stats-only snapshot; producers are not stopped, so a concurrently sampled value is approximate. */
    public long reserved() {
        long free = available.get();
        for (Stripe stripe : credits) free += stripe.credit;
        long resident = capacity - free;
        return resident <= 0L ? 0L : Math.min(capacity, resident);
    }

    public long capacity() { return capacity; }
    public int stripeCount() { return credits.length; }

    /** Returns unused fixed-stripe credits during shutdown; no writer may be active then. */
    public void returnUnusedCredits() {
        for (Stripe stripe : credits) {
            long credit = Stripe.CREDIT.getAndSet(stripe, 0L);
            if (credit != 0L) release(credit);
        }
    }

    public void clear() {
        for (Stripe stripe : credits) stripe.credit = 0L;
        available.set(capacity);
    }

    private boolean consumeCredit(int stripe, long bytes) {
        for (;;) {
            Stripe creditsForStripe = credits[stripe];
            long credit = creditsForStripe.credit;
            if (credit < bytes) return false;
            if (Stripe.CREDIT.compareAndSet(creditsForStripe, credit, credit - bytes)) return true;
        }
    }

    private boolean refillAndConsume(int stripe, long bytes) {
        for (;;) {
            if (consumeCredit(stripe, bytes)) return true;
            long requested = Math.max(bytes, refillBytes);
            if (borrowGlobalCredit(stripe, bytes, requested)) return true;
            if (!reclaimIdleCredits(stripe)) return false;
        }
    }

    /** Moves one bounded chunk into this stripe and consumes the requested part in one operation. */
    private boolean borrowGlobalCredit(int stripe, long bytes, long requested) {
        for (;;) {
            long free = available.get();
            if (free < bytes) return false;
            long grant = Math.min(free, requested);
            if (!available.compareAndSet(free, free - grant)) continue;
            Stripe.CREDIT.getAndAdd(credits[stripe], grant - bytes);
            return true;
        }
    }

    /**
     * Stripes retain at most a refill chunk. Under pressure a writer reclaims those idle chunks
     * before rejecting, so short-lived producer threads cannot pin cache capacity forever.
     */
    private boolean reclaimIdleCredits(int requestingStripe) {
        boolean reclaimed = false;
        for (int offset = 0; offset < credits.length; offset++) {
            int stripe = (requestingStripe + offset) & stripeMask;
            Stripe creditsForStripe = credits[stripe];
            for (;;) {
                long credit = creditsForStripe.credit;
                if (credit == 0L) break;
                if (Stripe.CREDIT.compareAndSet(creditsForStripe, credit, 0L)) {
                    available.addAndGet(credit);
                    reclaimed = true;
                    break;
                }
            }
        }
        return reclaimed;
    }

    private int stripeForCurrentThread() {
        return ((int) Thread.currentThread().getId()) & stripeMask;
    }

    /** Separate objects prevent different CPU stripes from sharing one AtomicLongArray cache line. */
    private static final class Stripe {
        private static final AtomicLongFieldUpdater<Stripe> CREDIT =
                AtomicLongFieldUpdater.newUpdater(Stripe.class, "credit");
        volatile long credit;
        @SuppressWarnings("unused") private long pad0, pad1, pad2, pad3, pad4, pad5, pad6;
    }
}
