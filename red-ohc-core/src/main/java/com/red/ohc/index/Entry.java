package com.red.ohc.index;

import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/** The one Java metadata object stored in the authoritative CHM as both key and value. */
public final class Entry {
    private static final long VALUE_TAG_MASK = 7L;
    private static final long VALUE_HAS_TTL = 1L;

    public static final int PENDING_UPDATE = 1;
    public static final int PENDING_REMOVE = 1 << 1;
    private static final int PENDING_MASK = PENDING_UPDATE | PENDING_REMOVE;
    public static final int POLICY_MAIN = 1 << 2;
    private static final int POLICY_ACCESS_SHIFT = 3;
    private static final int POLICY_ACCESS_MASK = 3 << POLICY_ACCESS_SHIFT;
    private static final int MAPPED = 1 << 5;
    public static final long WRITER_LOCK = 1L << 63;
    public static final long TIMER_SCHEDULED = 1L << 62;
    public static final long GENERATION_MASK = TIMER_SCHEDULED - 1L;

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
    public int timerSlot;
    public volatile int pendingFlags;

    public Entry(long nativeKeyAddress, int keyLength, int chmHash, long valueAddress) {
        this.nativeKeyAddress = nativeKeyAddress;
        this.keyMeta = packKeyMeta(chmHash, keyLength);
        this.valueAddress = valueAddress;
        this.pendingFlags = MAPPED;
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
    public int keyLength() { return keyLength(keyMeta); }
    public long nativeKeyAddress() { return nativeKeyAddress; }
    public long keyAllocationLength() { return Math.max(8L, CacheMath.roundUpTo8(keyLength())); }
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
        if (keyMeta != entry.keyMeta) return false;
        return keyLength() == 0 || NativeMemory.equals(nativeKeyAddress, entry.nativeKeyAddress, keyLength());
    }

    public boolean claimWriter() {
        long current = lifecycle;
        return (current & WRITER_LOCK) == 0L
                && LIFECYCLE.compareAndSet(this, current, current | WRITER_LOCK);
    }

    public void finishWriter() {
        for (;;) {
            long current = lifecycle;
            long generation = (current & GENERATION_MASK) + 1L;
            if (generation == 0L || generation > GENERATION_MASK) generation = 1L;
            long next = (current & TIMER_SCHEDULED) | generation;
            if (LIFECYCLE.compareAndSet(this, current, next)) return;
        }
    }

    public long generation() {
        return lifecycle & GENERATION_MASK;
    }

    public boolean timerScheduled() {
        return (lifecycle & TIMER_SCHEDULED) != 0L;
    }

    public void timerScheduled(boolean scheduled) {
        for (;;) {
            long current = lifecycle;
            long next = scheduled ? current | TIMER_SCHEDULED : current & ~TIMER_SCHEDULED;
            if (current == next || LIFECYCLE.compareAndSet(this, current, next)) return;
        }
    }

    public boolean markPending(int flags) {
        for (;;) {
            int current = pendingFlags;
            int next = current | flags;
            if ((current & flags) == flags) return false;
            if (PENDING.compareAndSet(this, current, next)) return (current & PENDING_MASK) == 0;
        }
    }

    public int takePending() {
        for (;;) {
            int current = pendingFlags;
            int next = current & ~PENDING_MASK;
            if (PENDING.compareAndSet(this, current, next)) return current & PENDING_MASK;
        }
    }

    public boolean policyMain() {
        return (pendingFlags & POLICY_MAIN) != 0;
    }

    public void policyMain(boolean main) {
        for (;;) {
            int current = pendingFlags;
            int next = main ? current | POLICY_MAIN : current & ~POLICY_MAIN;
            if (current == next || PENDING.compareAndSet(this, current, next)) return;
        }
    }

    public int policyAccessCount() {
        return (pendingFlags & POLICY_ACCESS_MASK) >>> POLICY_ACCESS_SHIFT;
    }

    public void policyAccessCount(int count) {
        int value = Math.max(0, Math.min(3, count)) << POLICY_ACCESS_SHIFT;
        for (;;) {
            int current = pendingFlags;
            int next = (current & ~POLICY_ACCESS_MASK) | value;
            if (current == next || PENDING.compareAndSet(this, current, next)) return;
        }
    }

    /** Returns the logical CHM mapping state without comparing native key bytes. */
    public boolean isMapped() {
        return (pendingFlags & MAPPED) != 0;
    }

    public void markMapped() {
        updateMapped(true);
    }

    public void markUnmapped() {
        updateMapped(false);
    }

    private void updateMapped(boolean mapped) {
        for (;;) {
            int current = pendingFlags;
            int next = mapped ? current | MAPPED : current & ~MAPPED;
            if (current == next || PENDING.compareAndSet(this, current, next)) return;
        }
    }
}
