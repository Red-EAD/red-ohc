package com.red.ohc.maintenance;

import com.red.ohc.Eviction;
import com.red.ohc.index.Entry;
import java.util.concurrent.atomic.AtomicLong;

/** Worker-owned eviction links. Java Entry references avoid native pointer chasing. */
public final class MaintenancePolicy {
    private final Eviction eviction;
    private final AtomicLong liveBytes;
    private final FrequencySketch sketch;

    // LRU uses head/tail. S3-FIFO uses the same Entry link fields for its small and main queues.
    private Entry head;
    private Entry tail;
    private Entry windowHead;
    private Entry windowTail;
    private Entry mainHead;
    private Entry mainTail;
    private long evictions;
    private Entry repairCursor;
    private int repairList;

    public MaintenancePolicy(Eviction eviction, AtomicLong liveBytes) {
        this.eviction = eviction;
        this.liveBytes = liveBytes;
        this.sketch = eviction == Eviction.W_TINY_LFU ? new FrequencySketch() : null;
    }

    public void add(Entry entry) {
        if (contains(entry)) {
            access(entry);
            return;
        }
        if (eviction == Eviction.S3_FIFO) {
            entry.policyMain(false);
            entry.policyAccessCount(1);
            linkWindowHead(entry);
        } else {
            entry.policyMain(false);
            entry.policyAccessCount(0);
            linkLruHead(entry);
        }
        if (sketch != null) sketch.increment(entry.keyHash());
    }

    public void access(Entry entry) {
        if (!contains(entry)) return;
        if (eviction == Eviction.S3_FIFO) {
            int count = entry.policyAccessCount();
            entry.policyAccessCount(count + 1);
            if (entry.policyMain()) moveMainToHead(entry);
            return;
        }
        if (sketch != null) sketch.increment(entry.keyHash());
        if (head != entry) {
            unlinkLru(entry);
            linkLruHead(entry);
        }
    }

    public void remove(Entry entry, boolean eviction) {
        if (!contains(entry)) return;
        if (this.eviction == Eviction.S3_FIFO) {
            if (entry.policyMain()) unlinkMain(entry); else unlinkWindow(entry);
        } else {
            unlinkLru(entry);
        }
        entry.policyMain(false);
        entry.policyAccessCount(0);
        if (eviction) evictions++;
    }

    public Entry victim() {
        if (eviction == Eviction.W_TINY_LFU) return tinyLfuVictim();
        if (eviction != Eviction.S3_FIFO) return tail;

        // S3-FIFO's victim selection is intentionally worker-only. A hot Small entry is promoted
        // and a referenced Main entry is reinserted with a decayed counter before another victim
        // is considered. This keeps all policy mutation off the synchronous get path.
        for (;;) {
            if (windowTail != null) {
                Entry candidate = windowTail;
                if (candidate.policyAccessCount() >= 2) {
                    unlinkWindow(candidate);
                    candidate.policyMain(true);
                    linkMainHead(candidate);
                    continue;
                }
                return candidate;
            }
            if (mainTail == null) return null;
            Entry candidate = mainTail;
            int count = candidate.policyAccessCount();
            if (count > 0) {
                candidate.policyAccessCount(count - 1);
                moveMainToHead(candidate);
                continue;
            }
            return candidate;
        }
    }

    long usedBytes() { return liveBytes.get(); }
    long evictions() { return evictions; }
    long sketchBytes() { return sketch == null ? 0L : sketch.bytes(); }

    boolean containsEntry(Entry entry) {
        return contains(entry);
    }

    boolean repair(int limit) {
        int inspected = 0;
        while (inspected < limit) {
            if (repairCursor == null) {
                repairCursor = repairList == 0 ? (eviction == Eviction.S3_FIFO ? windowHead : head)
                        : (repairList == 1 ? mainHead : null);
                if (repairCursor == null) {
                    if (eviction == Eviction.S3_FIFO && repairList == 0) {
                        repairList = 1;
                        continue;
                    }
                    repairList = 0;
                    return true;
                }
            }
            Entry current = repairCursor;
            repairCursor = current.policyNext;
            inspected++;
            if (current.valueAddress == 0L || !current.isMapped()) remove(current, false);
            if (repairCursor == null) {
                if (eviction == Eviction.S3_FIFO && repairList == 0) {
                    repairList = 1;
                } else {
                    repairList = 0;
                    return true;
                }
            }
        }
        return false;
    }

    private Entry tinyLfuVictim() {
        Entry selected = tail;
        if (selected == null) return null;
        int selectedFrequency = sketch.frequency(selected.keyHash());
        Entry cursor = selected.policyPrev;
        for (int inspected = 0; cursor != null && inspected < 7; inspected++) {
            int frequency = sketch.frequency(cursor.keyHash());
            if (frequency < selectedFrequency) {
                selected = cursor;
                selectedFrequency = frequency;
            }
            cursor = cursor.policyPrev;
        }
        return selected;
    }

    private boolean contains(Entry entry) {
        if (eviction == Eviction.S3_FIFO) {
            return entry == windowHead || entry == windowTail || entry == mainHead || entry == mainTail
                    || entry.policyPrev != null || entry.policyNext != null;
        }
        return entry == head || entry == tail || entry.policyPrev != null || entry.policyNext != null;
    }

    private void linkLruHead(Entry entry) {
        entry.policyPrev = null;
        entry.policyNext = head;
        if (head == null) tail = entry; else head.policyPrev = entry;
        head = entry;
    }

    private void unlinkLru(Entry entry) {
        Entry previous = entry.policyPrev;
        Entry next = entry.policyNext;
        if (previous == null) head = next; else previous.policyNext = next;
        if (next == null) tail = previous; else next.policyPrev = previous;
        entry.policyPrev = null;
        entry.policyNext = null;
    }

    private void linkWindowHead(Entry entry) {
        entry.policyPrev = null;
        entry.policyNext = windowHead;
        if (windowHead == null) windowTail = entry; else windowHead.policyPrev = entry;
        windowHead = entry;
    }

    private void unlinkWindow(Entry entry) {
        Entry previous = entry.policyPrev;
        Entry next = entry.policyNext;
        if (previous == null) windowHead = next; else previous.policyNext = next;
        if (next == null) windowTail = previous; else next.policyPrev = previous;
        entry.policyPrev = null;
        entry.policyNext = null;
    }

    private void linkMainHead(Entry entry) {
        entry.policyPrev = null;
        entry.policyNext = mainHead;
        if (mainHead == null) mainTail = entry; else mainHead.policyPrev = entry;
        mainHead = entry;
    }

    private void unlinkMain(Entry entry) {
        Entry previous = entry.policyPrev;
        Entry next = entry.policyNext;
        if (previous == null) mainHead = next; else previous.policyNext = next;
        if (next == null) mainTail = previous; else next.policyPrev = previous;
        entry.policyPrev = null;
        entry.policyNext = null;
    }

    private void moveMainToHead(Entry entry) {
        if (mainHead == entry) return;
        unlinkMain(entry);
        linkMainHead(entry);
    }
}
