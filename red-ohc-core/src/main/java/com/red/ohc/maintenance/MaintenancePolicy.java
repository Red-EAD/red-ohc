package com.red.ohc.maintenance;

import com.red.ohc.Eviction;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;

/**
 * Maintenance-actor-only eviction policy state. Every list is intrusive in {@link Entry}; no
 * producer or reader mutates a policy link.
 */
public final class MaintenancePolicy {
    private static final double HILL_INITIAL_STEP_PERCENT = 0.0625d;
    private static final double HILL_STEP_DECAY = 0.98d;
    private static final double HILL_RESTART_THRESHOLD = 0.05d;
    private static final long HILL_MIN_STEP = 2L;
    private final Eviction eviction;
    private final long capacity;
    private final FrequencySketch sketch;
    private final EntryDeque lru = new EntryDeque();
    private final EntryDeque small = new EntryDeque();
    private final EntryDeque main = new EntryDeque();
    private final EntryDeque window = new EntryDeque();
    private final EntryDeque probation = new EntryDeque();
    private final EntryDeque protectedQueue = new EntryDeque();
    private final Long2IntLinkedOpenHashMap ghost;
    private final int ghostLimit;
    private final long smallMaximum;
    private long windowMaximum;
    private long protectedMaximum;

    private long weightedSize;
    private long smallWeight;
    private long mainWeight;
    private long windowWeight;
    private long probationWeight;
    private long protectedWeight;
    private long evictions;
    private long ghostHits;
    private long hitsInSample;
    private long missesInSample;
    private double previousSampleHitRate;
    private double hillStep;
    /** Actor-visible work consumed while finding the latest victim. */
    private int lastVictimScanCount;

    public MaintenancePolicy(Eviction eviction, long capacity) {
        this.eviction = eviction;
        this.capacity = capacity;
        long plannedEntries = Math.max(256L, capacity / 128L);
        this.sketch = eviction == Eviction.W_TINY_LFU ? new FrequencySketch(plannedEntries) : null;
        this.smallMaximum = Math.max(1L, capacity / 10L);
        this.windowMaximum = Math.max(1L, capacity / 100L);
        long mainMaximum = mainMaximum();
        this.protectedMaximum = Math.max(1L, mainMaximum * 80L / 100L);
        this.hillStep = -Math.max(HILL_MIN_STEP, capacity * HILL_INITIAL_STEP_PERCENT);
        this.ghostLimit = (int) Math.min(1_048_576L, Math.max(256L, (plannedEntries * 9L + 9L) / 10L));
        this.ghost = eviction == Eviction.S3_FIFO ? new Long2IntLinkedOpenHashMap(ghostLimit) : null;
    }

    public void add(Entry entry) {
        if (entry.policyState() != Entry.POLICY_NONE) {
            updateWeight(entry);
            access(entry);
            return;
        }
        entry.policyWeight = weightOf(entry);
        weightedSize += entry.policyWeight;
        switch (eviction) {
            case S3_FIFO:
                if (ghost.containsKey(entry.keyHash64())) {
                    ghost.remove(entry.keyHash64());
                    ghostHits++;
                    link(main, entry, Entry.POLICY_S3_MAIN);
                    mainWeight += entry.policyWeight;
                } else {
                    link(small, entry, Entry.POLICY_S3_SMALL);
                    smallWeight += entry.policyWeight;
                }
                entry.policyAccessCount(0);
                break;
            case W_TINY_LFU:
                link(window, entry, Entry.POLICY_TINY_WINDOW);
                windowWeight += entry.policyWeight;
                sketch.increment(entry.keyHash64());
                break;
            default:
                link(lru, entry, Entry.POLICY_LRU);
        }
    }

    public void access(Entry entry) {
        switch (entry.policyState()) {
            case Entry.POLICY_LRU:
                lru.moveToHead(entry);
                break;
            case Entry.POLICY_S3_SMALL:
            case Entry.POLICY_S3_MAIN:
                entry.policyAccessCount(entry.policyAccessCount() + 1);
                break;
            case Entry.POLICY_TINY_WINDOW:
                sketch.increment(entry.keyHash64());
                window.moveToHead(entry);
                break;
            case Entry.POLICY_TINY_PROBATION:
                sketch.increment(entry.keyHash64());
                unlink(probation, entry);
                probationWeight -= entry.policyWeight;
                link(protectedQueue, entry, Entry.POLICY_TINY_PROTECTED);
                protectedWeight += entry.policyWeight;
                demoteProtected();
                break;
            case Entry.POLICY_TINY_PROTECTED:
                sketch.increment(entry.keyHash64());
                protectedQueue.moveToHead(entry);
                break;
            default:
        }
    }

    public void remove(Entry entry, boolean eviction) {
        int state = entry.policyState();
        if (state == Entry.POLICY_NONE) return;
        switch (state) {
            case Entry.POLICY_LRU:
                unlink(lru, entry);
                break;
            case Entry.POLICY_S3_SMALL:
                unlink(small, entry);
                smallWeight -= entry.policyWeight;
                break;
            case Entry.POLICY_S3_MAIN:
                unlink(main, entry);
                mainWeight -= entry.policyWeight;
                if (eviction) addGhost(entry);
                break;
            case Entry.POLICY_TINY_WINDOW:
                unlink(window, entry);
                windowWeight -= entry.policyWeight;
                break;
            case Entry.POLICY_TINY_PROBATION:
                unlink(probation, entry);
                probationWeight -= entry.policyWeight;
                break;
            case Entry.POLICY_TINY_PROTECTED:
                unlink(protectedQueue, entry);
                protectedWeight -= entry.policyWeight;
                break;
            default:
        }
        weightedSize -= entry.policyWeight;
        entry.policyState(Entry.POLICY_NONE);
        entry.policyWeight = 0L;
        entry.policyAccessCount(0);
        if (eviction) evictions++;
    }

    /** Selects one victim. A bounded scan prevents hot FIFO entries monopolising one actor pass. */
    public Entry victim() {
        return victim(Integer.MAX_VALUE);
    }

    /**
     * Returns a removable candidate or {@code null} after {@code scanLimit} actor-owned list
     * operations. The caller can yield and let other maintenance classes make progress before
     * asking again.
     */
    public Entry victim(int scanLimit) {
        if (scanLimit <= 0) {
            lastVictimScanCount = 0;
            return null;
        }
        switch (eviction) {
            case S3_FIFO:
                return s3Victim(scanLimit);
            case W_TINY_LFU:
                lastVictimScanCount = 1;
                return tinyLfuVictim();
            default:
                lastVictimScanCount = lru.tail == null ? 0 : 1;
                return lru.tail;
        }
    }

    /** Rotates a candidate whose per-entry writer mutex is currently held by a business writer. */
    public void skipLocked(Entry entry) {
        switch (entry.policyState()) {
            case Entry.POLICY_LRU:
                lru.moveToHead(entry);
                break;
            case Entry.POLICY_S3_SMALL:
                small.moveToHead(entry);
                break;
            case Entry.POLICY_S3_MAIN:
                main.moveToHead(entry);
                break;
            case Entry.POLICY_TINY_WINDOW:
                window.moveToHead(entry);
                break;
            case Entry.POLICY_TINY_PROBATION:
                probation.moveToHead(entry);
                break;
            case Entry.POLICY_TINY_PROTECTED:
                protectedQueue.moveToHead(entry);
                break;
            default:
        }
    }

    long usedBytes() { return weightedSize; }
    long evictions() { return evictions; }
    long sketchBytes() { return sketch == null ? 0L : sketch.bytes(); }
    long ghostHeapBytes() { return ghost == null ? 0L : (long) ghost.size() * (Long.BYTES + Integer.BYTES); }
    long ghostHits() { return ghostHits; }

    /** Actor-owned sampled read statistics feed Caffeine's window/main hill climber. */
    public void recordHits(long count) {
        if (sketch == null || count <= 0L) return;
        hitsInSample = saturatedAdd(hitsInSample, count);
        climb();
    }

    public void recordMisses(long count) {
        if (sketch == null || count <= 0L) return;
        missesInSample = saturatedAdd(missesInSample, count);
        climb();
    }

    long sampleSize() { return sketch == null ? 0L : sketch.sampleSize(); }
    long windowMaximum() { return windowMaximum; }
    public int lastVictimScanCount() { return lastVictimScanCount; }

    boolean containsEntry(Entry entry) { return entry.policyState() != Entry.POLICY_NONE; }

    private Entry s3Victim(int scanLimit) {
        lastVictimScanCount = 0;
        while (lastVictimScanCount < scanLimit) {
            if (small.tail != null && (smallWeight > smallMaximum || main.tail == null)) {
                Entry candidate = small.tail;
                lastVictimScanCount++;
                if (candidate.policyAccessCount() >= 1) {
                    unlink(small, candidate);
                    smallWeight -= candidate.policyWeight;
                    candidate.policyAccessCount(0);
                    link(main, candidate, Entry.POLICY_S3_MAIN);
                    mainWeight += candidate.policyWeight;
                    continue;
                }
                return candidate;
            }
            Entry candidate = main.tail;
            if (candidate == null) {
                candidate = small.tail;
                if (candidate != null) lastVictimScanCount++;
                return candidate;
            }
            lastVictimScanCount++;
            if (candidate.policyAccessCount() > 0) {
                candidate.policyAccessCount(candidate.policyAccessCount() - 1);
                main.moveToHead(candidate);
                continue;
            }
            return candidate;
        }
        return null;
    }

    private Entry tinyLfuVictim() {
        demoteProtected();
        Entry candidate = window.tail;
        if (candidate != null && windowWeight > windowMaximum) {
            Entry victim = probation.tail;
            if (victim == null || probationWeight + protectedWeight < mainMaximum()) {
                promoteWindow(candidate);
            } else if (sketch.frequency(candidate.keyHash64()) > sketch.frequency(victim.keyHash64())) {
                promoteWindow(candidate);
                return victim;
            } else {
                return candidate;
            }
        }
        Entry fallback = probation.tail;
        if (fallback != null) return fallback;
        if (window.tail != null) return window.tail;
        return protectedQueue.tail;
    }

    private void promoteWindow(Entry candidate) {
        unlink(window, candidate);
        windowWeight -= candidate.policyWeight;
        link(probation, candidate, Entry.POLICY_TINY_PROBATION);
        probationWeight += candidate.policyWeight;
    }

    private void demoteProtected() {
        while (protectedWeight > protectedMaximum && protectedQueue.tail != null) {
            Entry candidate = protectedQueue.tail;
            unlink(protectedQueue, candidate);
            protectedWeight -= candidate.policyWeight;
            link(probation, candidate, Entry.POLICY_TINY_PROBATION);
            probationWeight += candidate.policyWeight;
        }
    }

    /** Caffeine-compatible step sizing: 6.25% initial, min 2, 5% restart, 0.98 decay. */
    private void climb() {
        long requests = hitsInSample + missesInSample;
        if (requests < sketch.sampleSize()) return;
        double hitRate = (double) hitsInSample / requests;
        double change = hitRate - previousSampleHitRate;
        long adjustment = (long) (change >= 0d ? hillStep : -hillStep);
        if (adjustment > 0L) increaseWindow(adjustment);
        else if (adjustment < 0L) decreaseWindow(-adjustment);

        double nextMagnitude = Math.abs(change) >= HILL_RESTART_THRESHOLD
                ? Math.max(HILL_MIN_STEP, capacity * HILL_INITIAL_STEP_PERCENT)
                : Math.max(1d, Math.abs(hillStep) * HILL_STEP_DECAY);
        hillStep = Math.copySign(nextMagnitude, adjustment == 0L ? hillStep : adjustment);
        previousSampleHitRate = hitRate;
        hitsInSample = 0L;
        missesInSample = 0L;
    }

    private void increaseWindow(long amount) {
        long quota = Math.min(amount, Math.max(0L, capacity - windowMaximum));
        if (quota == 0L) return;
        windowMaximum += quota;
        protectedMaximum = Math.max(1L, protectedMaximum - quota);
        demoteProtected();
    }

    private void decreaseWindow(long amount) {
        long quota = Math.min(amount, Math.max(0L, windowMaximum - 1L));
        if (quota == 0L) return;
        windowMaximum -= quota;
        protectedMaximum += quota;
    }

    private long mainMaximum() {
        return Math.max(1L, capacity - windowMaximum);
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private void updateWeight(Entry entry) {
        long updated = weightOf(entry);
        long delta = updated - entry.policyWeight;
        if (delta == 0L) return;
        weightedSize += delta;
        switch (entry.policyState()) {
            case Entry.POLICY_S3_SMALL: smallWeight += delta; break;
            case Entry.POLICY_S3_MAIN: mainWeight += delta; break;
            case Entry.POLICY_TINY_WINDOW: windowWeight += delta; break;
            case Entry.POLICY_TINY_PROBATION: probationWeight += delta; break;
            case Entry.POLICY_TINY_PROTECTED: protectedWeight += delta; break;
            default:
        }
        entry.policyWeight = updated;
    }

    private void addGhost(Entry entry) {
        long fingerprint = entry.keyHash64();
        ghost.putAndMoveToFirst(fingerprint, 1);
        while (ghost.size() > ghostLimit) ghost.removeLastInt();
    }

    private static long weightOf(Entry entry) {
        long key = WriterArena.allocationWeight(entry.keyAllocationLength());
        long valueAddress = Entry.rawValueAddress(entry.valueAddress);
        if (valueAddress == 0L) return key;
        return key + WriterArena.allocationWeight(ValueBlock.allocationLength(ValueBlock.length(valueAddress)));
    }

    private static void link(EntryDeque deque, Entry entry, int state) {
        deque.linkHead(entry);
        entry.policyState(state);
    }

    private static void unlink(EntryDeque deque, Entry entry) {
        deque.unlink(entry);
    }

    private static final class EntryDeque {
        Entry head;
        Entry tail;

        void linkHead(Entry entry) {
            entry.policyPrev = null;
            entry.policyNext = head;
            if (head == null) tail = entry; else head.policyPrev = entry;
            head = entry;
        }

        void unlink(Entry entry) {
            Entry previous = entry.policyPrev;
            Entry next = entry.policyNext;
            if (previous == null) head = next; else previous.policyNext = next;
            if (next == null) tail = previous; else next.policyPrev = previous;
            entry.policyPrev = null;
            entry.policyNext = null;
        }

        void moveToHead(Entry entry) {
            if (head == entry) return;
            unlink(entry);
            linkHead(entry);
        }
    }
}
