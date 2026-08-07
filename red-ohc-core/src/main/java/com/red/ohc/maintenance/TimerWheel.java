package com.red.ohc.maintenance;

import com.red.ohc.index.Entry;
import com.red.ohc.index.FlatConcurrentMap;
import com.red.ohc.storage.ValueBlock;

/** A worker-owned 1024-slot time wheel. Stale slots are harmless and are re-bucketed. */
public final class TimerWheel {
    private static final long TICK_MILLIS = 64L;
    private static final int SLOT_COUNT = 1024;
    private final Entry[] heads = new Entry[SLOT_COUNT];
    private long tick;
    private long scheduled;
    private int repairSlot;
    private Entry repairEntry;

    public TimerWheel(long nowMillis) {
        tick = nowMillis / TICK_MILLIS;
    }

    long bytes() { return (long) SLOT_COUNT * Long.BYTES; }
    long scheduled() { return scheduled; }
    public boolean hasPending() { return scheduled != 0L; }

    public void add(Entry entry, long expireAtMillis) {
        if (expireAtMillis <= 0L || entry.timerScheduled()) return;
        long target = expireAtMillis / TICK_MILLIS;
        if (expireAtMillis % TICK_MILLIS != 0L && target != Long.MAX_VALUE) target++;
        target = Math.max(tick + 1L, target);
        int slot = (int) target & (SLOT_COUNT - 1);
        Entry old = heads[slot];
        entry.timerPrev = null;
        entry.timerNext = old;
        entry.timerSlot = slot;
        if (old != null) old.timerPrev = entry;
        heads[slot] = entry;
        entry.timerScheduled(true);
        scheduled++;
    }

    void reschedule(Entry entry, long expireAtMillis) {
        remove(entry);
        add(entry, expireAtMillis);
    }

    void remove(Entry entry) {
        if (!entry.timerScheduled()) return;
        unlink(entry.timerSlot, entry);
    }

    public int advance(long nowMillis, TimerConsumer consumer) {
        long target = nowMillis / TICK_MILLIS;
        long steps = Math.min(4096L, Math.max(0L, target - tick));
        int work = 0;
        while (steps-- > 0L) {
            tick++;
            int slot = (int) tick & (SLOT_COUNT - 1);
            Entry entry = heads[slot];
            heads[slot] = null;
            while (entry != null) {
                Entry next = entry.timerNext;
                entry.timerPrev = null;
                entry.timerNext = null;
                entry.timerScheduled(false);
                if (scheduled > 0L) scheduled--;
                long taggedAddress = entry.valueAddress;
                long address = Entry.rawValueAddress(taggedAddress);
                long generation = entry.generation();
                if (address != 0L && Entry.hasTtl(taggedAddress) && ValueBlock.expired(address, nowMillis)) {
                    consumer.expire(entry, generation, taggedAddress);
                } else if (address != 0L && Entry.hasTtl(taggedAddress)) {
                    add(entry, ValueBlock.expireAtMillis(address));
                }
                entry = next;
                work++;
            }
        }
        return work;
    }

    long nextDelayNanos(long nowMillis) {
        if (scheduled == 0L) return Long.MAX_VALUE;
        long nextMillis = (tick + 1L) * TICK_MILLIS;
        if (nextMillis <= nowMillis) return 1L;
        long delayMillis = nextMillis - nowMillis;
        return delayMillis > Long.MAX_VALUE / 1_000_000L
                ? Long.MAX_VALUE : delayMillis * 1_000_000L;
    }

    boolean repair(FlatConcurrentMap data, int limit) {
        int inspected = 0;
        while (inspected < limit) {
            if (repairEntry == null) {
                if (repairSlot == SLOT_COUNT) {
                    repairSlot = 0;
                    return true;
                }
                repairEntry = heads[repairSlot];
                if (repairEntry == null) {
                    repairSlot++;
                    continue;
                }
            }
            Entry current = repairEntry;
            repairEntry = current.timerNext;
            inspected++;
            if (current.valueAddress == 0L || !data.isCurrent(current)) remove(current);
            if (repairEntry == null) repairSlot++;
        }
        return repairSlot == SLOT_COUNT;
    }

    private void unlink(int slot, Entry entry) {
        Entry previous = entry.timerPrev;
        Entry next = entry.timerNext;
        if (previous == null) heads[slot] = next; else previous.timerNext = next;
        if (next != null) next.timerPrev = previous;
        entry.timerPrev = null;
        entry.timerNext = null;
        entry.timerSlot = 0;
        entry.timerScheduled(false);
        if (scheduled > 0L) scheduled--;
    }

    @FunctionalInterface
    public interface TimerConsumer { void expire(Entry entry, long expectedGeneration, long expectedValueAddress); }
}
