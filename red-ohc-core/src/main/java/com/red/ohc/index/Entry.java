package com.red.ohc.index;

import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/** The one Java metadata object stored in the authoritative CHM as both key and value. */
public final class Entry {
    private static final long VALUE_TAG_MASK = 7L;
    private static final long VALUE_HAS_TTL = 1L;

    public static final int PENDING_ADD = 1;
    public static final int PENDING_UPDATE = 1 << 1;
    public static final int PENDING_REMOVE = 1 << 2;
    /** Returned to the maintenance actor when a writer owns the pending publication claim. */
    public static final int PENDING_BUSY = -1;
    public static final int POLICY_NONE = 0;
    public static final int POLICY_LRU = 1;
    public static final int POLICY_S3_SMALL = 2;
    public static final int POLICY_S3_MAIN = 3;
    public static final int POLICY_TINY_WINDOW = 4;
    public static final int POLICY_TINY_PROBATION = 5;
    public static final int POLICY_TINY_PROTECTED = 6;
    private static final int PENDING_MASK = PENDING_ADD | PENDING_UPDATE | PENDING_REMOVE;
    private static final int PENDING_CLAIMED = 1 << 3;
    private static final int PENDING_QUEUED = 1 << 4;
    private static final int PENDING_REPAIR = 1 << 5;
    private static final int PENDING_CLAIM_SHIFT = 6;
    private static final int PENDING_CLAIM_MASK = PENDING_MASK << PENDING_CLAIM_SHIFT;
    private static final int POLICY_STATE_SHIFT = 0;
    private static final int POLICY_ACCESS_SHIFT = 3;
    private static final int MUTATION_VERSION_SHIFT = 5;
    private static final int APPLIED_VERSION_SHIFT = 18;
    private static final int VERSION_MASK = (1 << 13) - 1;
    public static final long WRITER_LOCK = 1L << 63;
    private static final long STATE_SHIFT = 61L;
    private static final long STATE_MASK = 3L << STATE_SHIFT;
    private static final long ALIVE = 0L;
    private static final long RETIRED = 1L << STATE_SHIFT;
    private static final long DEAD = 2L << STATE_SHIFT;
    public static final long GENERATION_MASK = (1L << STATE_SHIFT) - 1L;

    private static final AtomicLongFieldUpdater<Entry> LIFECYCLE =
            AtomicLongFieldUpdater.newUpdater(Entry.class, "lifecycle");
    private static final AtomicIntegerFieldUpdater<Entry> PENDING =
            AtomicIntegerFieldUpdater.newUpdater(Entry.class, "pendingFlags");
    private static final AtomicIntegerFieldUpdater<Entry> POLICY =
            AtomicIntegerFieldUpdater.newUpdater(Entry.class, "policyMeta");

    public final long nativeKeyAddress;
    /** High 32 bits are serialized key length; low 32 bits are the CHM hash. */
    public final long keyMeta;
    /** Eight-byte aligned native value address; bit 0 marks a TTL-bearing value. */
    public volatile long valueAddress;
    public volatile long lifecycle;
    public Entry policyPrev;
    public Entry policyNext;
    public Entry timerPrev;
    public Entry timerNext;
    public short timerSlot;
    public int timerLevel;
    public long timerDeadlineTick;
    /** Actor-owned fields: readers and writers never mutate policy/timer links or counters. */
    public boolean timerScheduled;
    /** Actor-owned intrusive policy metadata. */
    private volatile int policyMeta;
    public long policyWeight;
    public volatile int pendingFlags;

    public Entry(long nativeKeyAddress, int keyLength, int chmHash, long valueAddress) {
        this(nativeKeyAddress, keyLength, chmHash, chmHash & 0xffffffffL, valueAddress);
    }

    public Entry(long nativeKeyAddress, int keyLength, int chmHash, long keyHash64, long valueAddress) {
        this.nativeKeyAddress = nativeKeyAddress;
        this.keyMeta = packKeyMeta(chmHash, keyLength);
        // Zero-address Entries exist only in policy/timer unit tests. Production Entries retain
        // the complete hash in their native key-block prefix without growing this hot object.
        if (nativeKeyAddress == 0L) this.timerDeadlineTick = keyHash64;
        this.valueAddress = valueAddress;
        this.pendingFlags = 0;
    }

    public static Entry bootstrap() {
        return new Entry(0L, 0, 0, 0L);
    }

    public static long packKeyMeta(int hash, int keyLength) {
        return ((long) keyLength << 32) | (hash & 0xffffffffL);
    }

    public static int keyHash(long keyMeta) {
        return (int) keyMeta;
    }

    public static int keyLength(long keyMeta) {
        return (int) (keyMeta >>> 32);
    }

    public int keyHash() { return keyHash(keyMeta); }
    public long keyHash64() {
        return nativeKeyAddress == 0L ? timerDeadlineTick : NativeMemory.getLong(nativeKeyAddress);
    }
    public int keyLength() { return keyLength(keyMeta); }
    public long nativeKeyAddress() { return nativeKeyAddress; }
    public long nativeKeyBytesAddress() { return nativeKeyAddress + Long.BYTES; }
    public long keyAllocationLength() { return Math.max(8L, CacheMath.roundUpTo8((long) keyLength() + Long.BYTES)); }
    public long rawValueAddress() { return rawValueAddress(valueAddress); }

    public static long tagValueAddress(long rawAddress, boolean hasTtl) {
        if (rawAddress == 0L) return 0L;
        if ((rawAddress & VALUE_TAG_MASK) != 0L) {
            throw new IllegalArgumentException("native value address must be 8-byte aligned: " + rawAddress);
        }
        return hasTtl ? rawAddress | VALUE_HAS_TTL : rawAddress;
    }

    public static long rawValueAddress(long taggedAddress) {
        return taggedAddress & ~VALUE_TAG_MASK;
    }

    public static boolean hasTtl(long taggedAddress) {
        return (taggedAddress & VALUE_HAS_TTL) != 0L;
    }

    @Override
    public int hashCode() {
        return keyHash();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Entry)) return false;
        Entry entry = (Entry) other;
        if (keyMeta != entry.keyMeta || keyHash64() != entry.keyHash64()) return false;
        return keyLength() == 0 || NativeMemory.equals(nativeKeyBytesAddress(), entry.nativeKeyBytesAddress(), keyLength());
    }

    public boolean claimWriter() {
        long current = lifecycle;
        return (current & (WRITER_LOCK | STATE_MASK)) == ALIVE
                && LIFECYCLE.compareAndSet(this, current, current | WRITER_LOCK);
    }

    public void finishWriter() {
        long current = lifecycle;
        if ((current & WRITER_LOCK) == 0L) throw new IllegalStateException("writer lock is not held");
        long generation = (current & GENERATION_MASK) + 1L;
        if (generation > GENERATION_MASK) generation = 1L;
        LIFECYCLE.lazySet(this, (current & STATE_MASK) | generation);
    }

    public long generation() {
        return lifecycle & GENERATION_MASK;
    }

    public boolean isAlive() {
        return (lifecycle & STATE_MASK) == ALIVE;
    }

    /** The maintenance actor must not consume a coalesced mutation during a writer publication. */
    public boolean isWriterLocked() {
        return (lifecycle & WRITER_LOCK) != 0L;
    }

    /** Must be called while holding the per-entry writer mutex. */
    public void markRetired() {
        long current = lifecycle;
        if ((current & WRITER_LOCK) == 0L) throw new IllegalStateException("writer lock is not held");
        LIFECYCLE.lazySet(this, (current & (WRITER_LOCK | GENERATION_MASK)) | RETIRED);
    }

    /** Restores ALIVE after a failed conditional CHM remove while still holding the writer mutex. */
    public void restoreAlive() {
        long current = lifecycle;
        if ((current & WRITER_LOCK) == 0L) throw new IllegalStateException("writer lock is not held");
        LIFECYCLE.lazySet(this, current & (WRITER_LOCK | GENERATION_MASK));
    }

    /** Used only for a private candidate that lost putIfAbsent before publication. */
    public void markDead() {
        long current = lifecycle;
        if ((current & WRITER_LOCK) != 0L) throw new IllegalStateException("writer lock is held");
        LIFECYCLE.lazySet(this, (current & GENERATION_MASK) | DEAD);
    }

    public boolean timerScheduled() {
        return timerScheduled;
    }

    public void timerScheduled(boolean scheduled) {
        timerScheduled = scheduled;
    }

    /** A negative timer level encodes the worker-owned overflow heap index. */
    public boolean timerInOverflowHeap() { return timerLevel < 0; }

    public int timerHeapIndex() {
        return timerLevel < 0 ? -timerLevel - 1 : -1;
    }

    public void timerHeapIndex(int index) {
        if (index < 0) timerLevel = 4;
        else timerLevel = -index - 1;
    }

    /**
     * Starts a writer-owned mutation publication. The claim remains held until the writer has
     * published its CHM/value-pointer change and calls {@link #completePendingClaim()}; the actor
     * therefore cannot consume a coalesced event between reservation and pointer publication.
     *
     * @return whether this is a new unique event that needs one dirty credit
     */
    public boolean beginPending(int flags) {
        if ((flags & ~PENDING_MASK) != 0 || flags == 0) {
            throw new IllegalArgumentException("invalid pending flags: " + flags);
        }
        int spins = 0;
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) != 0) {
                if (spins++ < 64) Thread.onSpinWait();
                else {
                    spins = 0;
                    LockSupport.parkNanos(this, 1_000L);
                }
                continue;
            }
            boolean needsCredit = (current & PENDING_MASK) == 0;
            // Keep the claim payload in the same atomic word. This avoids a separate int field
            // in the CHM node while preserving the claim-before-publication protocol.
            if (PENDING.compareAndSet(this, current,
                    current | PENDING_CLAIMED | (flags << PENDING_CLAIM_SHIFT))) {
                return needsCredit;
            }
        }
    }

    /** Releases a successful publication claim after the associated pointer or mapping is visible. */
    public void completePendingClaim() {
        if ((pendingFlags & PENDING_CLAIMED) == 0) {
            throw new IllegalStateException("missing pending claim");
        }
        incrementMutationVersion();
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) == 0) {
                throw new IllegalStateException("pending claim was lost");
            }
            int currentFlags = (current & PENDING_CLAIM_MASK) >>> PENDING_CLAIM_SHIFT;
            if (PENDING.compareAndSet(this, current,
                    (current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK)) | currentFlags)) return;
        }
    }

    /** Published after a writer has made its CHM/value mutation visible. */
    public long mutationVersion() {
        return Integer.toUnsignedLong((policyMeta >>> MUTATION_VERSION_SHIFT) & VERSION_MASK);
    }

    /** Published by the maintenance worker after applying the corresponding mutation. */
    public long appliedVersion() {
        return Integer.toUnsignedLong((policyMeta >>> APPLIED_VERSION_SHIFT) & VERSION_MASK);
    }

    private void incrementMutationVersion() {
        for (;;) {
            int current = policyMeta;
            int version = ((current >>> MUTATION_VERSION_SHIFT) + 1) & VERSION_MASK;
            int next = (current & ~(VERSION_MASK << MUTATION_VERSION_SHIFT))
                    | (version << MUTATION_VERSION_SHIFT);
            if (POLICY.compareAndSet(this, current, next)) return;
        }
    }

    /** Marks a maintenance snapshot applied only when no newer writer publication exists. */
    public boolean markAppliedVersion(long version) {
        int expected = (int) version & VERSION_MASK;
        for (;;) {
            int current = policyMeta;
            if (((current >>> MUTATION_VERSION_SHIFT) & VERSION_MASK) != expected) return false;
            int next = (current & ~(VERSION_MASK << APPLIED_VERSION_SHIFT))
                    | (expected << APPLIED_VERSION_SHIFT);
            if (POLICY.compareAndSet(this, current, next)) return true;
        }
    }

    /** Cancels a writer publication before its associated pointer or mapping becomes visible. */
    public boolean cancelPendingClaim() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) == 0) throw new IllegalStateException("missing pending claim");
            if (PENDING.compareAndSet(this, current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
                return true;
            }
        }
    }

    public boolean cancelPendingClaimIfPresent() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) == 0) return false;
            if (PENDING.compareAndSet(this, current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
                return true;
            }
        }
    }

    /** Returns true for the one producer responsible for putting this Entry in the transport. */
    public boolean publishPending() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) != 0) {
                throw new IllegalStateException("pending publication is still claimed");
            }
            if ((current & PENDING_MASK) == 0 || (current & PENDING_QUEUED) != 0) return false;
            if (PENDING.compareAndSet(this, current, current | PENDING_QUEUED)) return true;
        }
    }

    /** Clears the transport marker when an advisory queue offer was not accepted. */
    public boolean queueOfferFailed() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_QUEUED) == 0) return false;
            if ((current & PENDING_REPAIR) != 0) {
                if (PENDING.compareAndSet(this, current, current & ~PENDING_QUEUED)) return false;
            } else if (PENDING.compareAndSet(this, current, (current & ~PENDING_QUEUED) | PENDING_REPAIR)) return true;
        }
    }

    public boolean isPendingQueued() {
        return (pendingFlags & PENDING_QUEUED) != 0;
    }

    public boolean clearRepairMarker() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_REPAIR) == 0) return false;
            if (PENDING.compareAndSet(this, current, current & ~PENDING_REPAIR)) return true;
        }
    }

    public boolean isRepairMarked() {
        return (pendingFlags & PENDING_REPAIR) != 0;
    }

    public int takePending() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) != 0) return PENDING_BUSY;
            int next = current & ~(PENDING_MASK | PENDING_QUEUED);
            if (PENDING.compareAndSet(this, current, next)) return current & PENDING_MASK;
        }
    }

    public int policyState() { return (policyMeta >>> POLICY_STATE_SHIFT) & 7; }

    public void policyState(int state) {
        for (;;) {
            int current = policyMeta;
            int next = (current & ~7) | (state & 7);
            if (POLICY.compareAndSet(this, current, next)) return;
        }
    }

    public int policyAccessCount() {
        return (policyMeta >>> POLICY_ACCESS_SHIFT) & 3;
    }

    public void policyAccessCount(int count) {
        int value = Math.max(0, Math.min(3, count));
        for (;;) {
            int current = policyMeta;
            int next = (current & ~0x18) | (value << POLICY_ACCESS_SHIFT);
            if (POLICY.compareAndSet(this, current, next)) return;
        }
    }
}
