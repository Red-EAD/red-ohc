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
    /** Intrusive, bounded maintenance repair stack link; never used by the hot read path. */
    public volatile Entry repairNext;
    public short timerSlot;
    public int timerLevel;
    public long timerDeadlineTick;
    /** Actor-owned fields: readers and writers never mutate policy/timer links or counters. */
    public boolean timerScheduled;
    /** Actor-owned intrusive policy metadata. */
    private int policyMeta;
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
        if (generation == 0L || generation > GENERATION_MASK) generation = 1L;
        LIFECYCLE.lazySet(this, (current & STATE_MASK) | generation);
    }

    public long generation() {
        return lifecycle & GENERATION_MASK;
    }

    public boolean isAlive() {
        return (lifecycle & STATE_MASK) == ALIVE;
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

    /**
     * Claims a new unique mutation event. A true result means the caller must acquire a dirty
     * credit and then call {@link #commitPendingClaim(int)} before it publishes any visibility
     * change. A false result means an already queued event has absorbed {@code flags}.
     */
    public boolean beginPending(int flags) {
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
            if ((current & PENDING_MASK) != 0) {
                int next = current | flags;
                if (next == current || PENDING.compareAndSet(this, current, next)) return false;
                continue;
            }
            if (PENDING.compareAndSet(this, current, current | PENDING_CLAIMED)) return true;
        }
    }

    /** Completes a successful dirty-credit reservation. */
    public void commitPendingClaim(int flags) {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) == 0) throw new IllegalStateException("missing pending claim");
            int next = (current & ~PENDING_CLAIMED) | flags;
            if (PENDING.compareAndSet(this, current, next)) return;
        }
    }

    /** Cancels an uncommitted reservation before its mutation becomes visible. */
    public void cancelPendingClaim() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_CLAIMED) == 0) return;
            if (PENDING.compareAndSet(this, current, current & ~PENDING_CLAIMED)) return;
        }
    }

    /** Returns true for the one producer responsible for putting this Entry in the transport. */
    public boolean publishPending() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_MASK) == 0
                    || (current & (PENDING_QUEUED | PENDING_REPAIR)) != 0) return false;
            if (PENDING.compareAndSet(this, current, current | PENDING_QUEUED)) return true;
        }
    }

    /** Moves a failed bounded-queue publication to the intrusive repair transport. */
    public boolean moveToRepair() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_MASK) == 0 || (current & PENDING_REPAIR) != 0) return false;
            if ((current & PENDING_QUEUED) == 0) return false;
            int next = (current & ~PENDING_QUEUED) | PENDING_REPAIR;
            if (PENDING.compareAndSet(this, current, next)) return true;
        }
    }

    /** Cancels a committed but not yet queued mutation reservation. */
    public boolean cancelPending() {
        for (;;) {
            int current = pendingFlags;
            if ((current & PENDING_QUEUED) != 0) return false;
            if ((current & PENDING_MASK) == 0) return true;
            if (PENDING.compareAndSet(this, current, current & ~PENDING_MASK)) return true;
        }
    }

    public int takePending() {
        for (;;) {
            int current = pendingFlags;
            int next = current & ~(PENDING_MASK | PENDING_QUEUED | PENDING_REPAIR);
            if (PENDING.compareAndSet(this, current, next)) return current & PENDING_MASK;
        }
    }

    public int policyState() { return policyMeta & 7; }

    public void policyState(int state) {
        policyMeta = (policyMeta & ~7) | (state & 7);
    }

    public int policyAccessCount() {
        return (policyMeta >>> 3) & 3;
    }

    public void policyAccessCount(int count) {
        policyMeta = (policyMeta & 7) | (Math.max(0, Math.min(3, count)) << 3);
    }
}
