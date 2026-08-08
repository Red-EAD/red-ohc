package com.red.ohc.maintenance;

import java.util.Comparator;
import java.util.PriorityQueue;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;

/**
 * Worker-owned hierarchical TTL wheel. Long deadlines sleep until their next cascade boundary
 * instead of forcing a 64ms wakeup. A stale node is harmless because generation and value pointer
 * are checked by the maintenance consumer.
 */
public final class TimerWheel {
    private static final long TICK_MILLIS = 64L;
    private static final int L0_SIZE = 1024;
    private static final int L1_SIZE = 64;
    private static final int L2_SIZE = 64;
    private static final int L3_SIZE = 32;
    private static final long L0_SPAN = L0_SIZE;
    private static final long L1_SPAN = L0_SPAN * L1_SIZE;
    private static final long L2_SPAN = L1_SPAN * L2_SIZE;
    private static final long L3_SPAN = L2_SPAN * L3_SIZE;

    private final Entry[] level0 = new Entry[L0_SIZE];
    private final Entry[] level1 = new Entry[L1_SIZE];
    private final Entry[] level2 = new Entry[L2_SIZE];
    private final Entry[] level3 = new Entry[L3_SIZE];
    private final long[] level0Occupied = new long[L0_SIZE >>> 6];
    private long level1Occupied;
    private long level2Occupied;
    private int level3Occupied;
    private final PriorityQueue<Entry> overflow = new PriorityQueue<>(Comparator.comparingLong(entry -> entry.timerDeadlineTick));
    private long tick;
    private long scheduled;
    /** A bucket that exceeded the caller's expiry budget. It is resumed before advancing time. */
    private int pendingExpirySlot = -1;

    public TimerWheel(long nowMillis) {
        tick = Math.max(0L, nowMillis / TICK_MILLIS);
    }

    long bytes() {
        return (long) (L0_SIZE + L1_SIZE + L2_SIZE + L3_SIZE) * Long.BYTES
                + (long) level0Occupied.length * Long.BYTES + Long.BYTES * 2L + Integer.BYTES;
    }

    long scheduled() { return scheduled; }
    public boolean hasPending() { return scheduled != 0L; }

    public void add(Entry entry, long expireAtMillis) {
        if (expireAtMillis <= 0L || entry.timerScheduled()) return;
        long target = ceilTick(expireAtMillis);
        if (target <= tick) target = tick + 1L;
        entry.timerDeadlineTick = target;
        link(entry, target);
        scheduled++;
    }

    void reschedule(Entry entry, long expireAtMillis) {
        remove(entry);
        add(entry, expireAtMillis);
    }

    void remove(Entry entry) {
        if (!entry.timerScheduled()) return;
        if (entry.timerLevel == 4) {
            overflow.remove(entry);
            entry.timerScheduled(false);
            if (scheduled > 0L) scheduled--;
            return;
        }
        unlink(entry.timerLevel, entry.timerSlot, entry);
        entry.timerScheduled(false);
        if (scheduled > 0L) scheduled--;
    }

    public int advance(long nowMillis, TimerConsumer consumer) {
        return advance(nowMillis, Integer.MAX_VALUE, consumer);
    }

    /**
     * Advances time while processing at most {@code expiryLimit} entries from L0. A storm in one
     * bucket is resumed on the next actor pass instead of monopolising the event loop or waiting
     * for the bucket to wrap around again.
     */
    public int advance(long nowMillis, int expiryLimit, TimerConsumer consumer) {
        if (expiryLimit <= 0) return 0;
        promoteOverflow();
        long target = Math.max(tick, nowMillis / TICK_MILLIS);
        long steps = Math.min(4096L, target - tick);
        int work = 0;
        if (pendingExpirySlot >= 0) {
            work += expireLevel0(pendingExpirySlot, nowMillis, expiryLimit, consumer);
            if (pendingExpirySlot >= 0 || work == expiryLimit) return work;
        }
        while (steps-- > 0L && work < expiryLimit) {
            tick++;
            if ((tick & (L0_SIZE - 1L)) == 0L) cascade(1, (int) ((tick >>> 10) & (L1_SIZE - 1)));
            if ((tick & (L1_SPAN - 1L)) == 0L) cascade(2, (int) ((tick >>> 16) & (L2_SIZE - 1)));
            if ((tick & (L2_SPAN - 1L)) == 0L) cascade(3, (int) ((tick >>> 22) & (L3_SIZE - 1)));
            promoteOverflow();
            work += expireLevel0((int) tick & (L0_SIZE - 1), nowMillis, expiryLimit - work, consumer);
            if (pendingExpirySlot >= 0) break;
        }
        return work;
    }

    long nextDelayNanos(long nowMillis) {
        if (scheduled == 0L) return Long.MAX_VALUE;
        long next = nextWakeTick();
        if (next <= tick) next = tick + 1L;
        long nextMillis = multiplySaturated(next, TICK_MILLIS);
        if (nextMillis <= nowMillis) return 1L;
        long delayMillis = nextMillis - nowMillis;
        return delayMillis > Long.MAX_VALUE / 1_000_000L ? Long.MAX_VALUE : delayMillis * 1_000_000L;
    }

    private void cascade(int level, int slot) {
        Entry entry = detach(level, slot);
        while (entry != null) {
            Entry next = entry.timerNext;
            clearLinks(entry);
            entry.timerScheduled(false);
            if (scheduled > 0L) scheduled--;
            link(entry, entry.timerDeadlineTick);
            scheduled++;
            entry = next;
        }
    }

    private int expireLevel0(int slot, long nowMillis, int limit, TimerConsumer consumer) {
        int work = 0;
        while (work < limit) {
            Entry entry = level0[slot];
            if (entry == null) {
                pendingExpirySlot = -1;
                return work;
            }
            unlink(0, slot, entry);
            entry.timerScheduled(false);
            if (scheduled > 0L) scheduled--;
            long taggedAddress = entry.valueAddress;
            long address = Entry.rawValueAddress(taggedAddress);
            long generation = entry.generation();
            if (entry.isAlive() && address != 0L && Entry.hasTtl(taggedAddress)
                    && ValueBlock.expired(address, nowMillis)) {
                consumer.expire(entry, generation, taggedAddress);
            } else if (entry.isAlive() && address != 0L && Entry.hasTtl(taggedAddress)) {
                add(entry, ValueBlock.expireAtMillis(address));
            }
            work++;
        }
        pendingExpirySlot = level0[slot] == null ? -1 : slot;
        return work;
    }

    private void promoteOverflow() {
        while (!overflow.isEmpty()) {
            Entry entry = overflow.peek();
            if (entry.timerDeadlineTick - tick >= L3_SPAN) return;
            overflow.poll();
            entry.timerScheduled(false);
            if (scheduled > 0L) scheduled--;
            link(entry, entry.timerDeadlineTick);
            scheduled++;
        }
    }

    private void link(Entry entry, long target) {
        long distance = target - tick;
        if (distance < L0_SPAN) {
            link(0, (int) target & (L0_SIZE - 1), entry);
        } else if (distance < L1_SPAN) {
            link(1, (int) (target >>> 10) & (L1_SIZE - 1), entry);
        } else if (distance < L2_SPAN) {
            link(2, (int) (target >>> 16) & (L2_SIZE - 1), entry);
        } else if (distance < L3_SPAN) {
            link(3, (int) (target >>> 22) & (L3_SIZE - 1), entry);
        } else {
            entry.timerLevel = 4;
            entry.timerSlot = 0;
            entry.timerPrev = null;
            entry.timerNext = null;
            entry.timerScheduled(true);
            overflow.offer(entry);
        }
    }

    private void link(int level, int slot, Entry entry) {
        Entry[] heads = heads(level);
        Entry head = heads[slot];
        entry.timerLevel = level;
        entry.timerSlot = (short) slot;
        entry.timerPrev = null;
        entry.timerNext = head;
        if (head != null) head.timerPrev = entry;
        heads[slot] = entry;
        entry.timerScheduled(true);
        setOccupied(level, slot);
    }

    private void unlink(int level, int slot, Entry entry) {
        Entry[] heads = heads(level);
        Entry previous = entry.timerPrev;
        Entry next = entry.timerNext;
        if (previous == null) heads[slot] = next; else previous.timerNext = next;
        if (next != null) next.timerPrev = previous;
        clearLinks(entry);
        if (heads[slot] == null) clearOccupied(level, slot);
    }

    private Entry detach(int level, int slot) {
        Entry[] heads = heads(level);
        Entry head = heads[slot];
        heads[slot] = null;
        clearOccupied(level, slot);
        return head;
    }

    private Entry[] heads(int level) {
        switch (level) {
            case 0: return level0;
            case 1: return level1;
            case 2: return level2;
            case 3: return level3;
            default: throw new IllegalArgumentException("invalid timer level: " + level);
        }
    }

    private void setOccupied(int level, int slot) {
        if (level == 0) level0Occupied[slot >>> 6] |= 1L << (slot & 63);
        else if (level == 1) level1Occupied |= 1L << slot;
        else if (level == 2) level2Occupied |= 1L << slot;
        else level3Occupied |= 1 << slot;
    }

    private void clearOccupied(int level, int slot) {
        if (level == 0) level0Occupied[slot >>> 6] &= ~(1L << (slot & 63));
        else if (level == 1) level1Occupied &= ~(1L << slot);
        else if (level == 2) level2Occupied &= ~(1L << slot);
        else level3Occupied &= ~(1 << slot);
    }

    private long nextWakeTick() {
        if (pendingExpirySlot >= 0) return tick + 1L;
        long next = Long.MAX_VALUE;
        next = firstLevel0WakeTick();
        next = Math.min(next, nextCascade(level1Occupied, tick >>> 10, 10, L1_SIZE));
        next = Math.min(next, nextCascade(level2Occupied, tick >>> 16, 16, L2_SIZE));
        next = Math.min(next, nextCascade(level3Occupied & 0xffffffffL, tick >>> 22, 22, L3_SIZE));
        Entry entry = overflow.peek();
        if (entry != null) {
            long promote = entry.timerDeadlineTick - L3_SPAN + 1L;
            next = Math.min(next, Math.max(tick + 1L, promote));
        }
        return next;
    }

    /** Finds the next L0 occupancy in at most sixteen bitmap words, never 1024 individual slots. */
    private long firstLevel0WakeTick() {
        int firstSlot = (int) (tick + 1L) & (L0_SIZE - 1);
        int firstWord = firstSlot >>> 6;
        int firstBit = firstSlot & 63;
        long firstOccupied = level0Occupied[firstWord];
        long occupied = firstOccupied & (-1L << firstBit);
        if (occupied != 0L) return wakeTickFor(firstWord, occupied, firstSlot);
        for (int wordOffset = 1; wordOffset < level0Occupied.length; wordOffset++) {
            int word = (firstWord + wordOffset) & (level0Occupied.length - 1);
            occupied = level0Occupied[word];
            if (occupied != 0L) return wakeTickFor(word, occupied, firstSlot);
        }
        // The physical word wraps too. Its lower bits are strictly later than firstSlot after
        // one complete L0 rotation, so they must be examined after every intervening word.
        occupied = firstOccupied & ((1L << firstBit) - 1L);
        if (occupied != 0L) return wakeTickFor(firstWord, occupied, firstSlot);
        return Long.MAX_VALUE;
    }

    private long wakeTickFor(int word, long occupied, int firstSlot) {
        int slot = (word << 6) + Long.numberOfTrailingZeros(occupied);
        long offset = (slot - firstSlot) & (L0_SIZE - 1L);
        return tick + 1L + offset;
    }

    private static long nextCascade(long occupied, long cycle, int shift, int size) {
        for (int offset = 1; offset <= size; offset++) {
            int slot = (int) (cycle + offset) & (size - 1);
            if ((occupied & (1L << slot)) != 0L) return (cycle + offset) << shift;
        }
        return Long.MAX_VALUE;
    }

    private static long ceilTick(long millis) {
        long quotient = millis / TICK_MILLIS;
        return millis % TICK_MILLIS == 0L ? quotient : quotient + 1L;
    }

    private static long multiplySaturated(long left, long right) {
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private static void clearLinks(Entry entry) {
        entry.timerPrev = null;
        entry.timerNext = null;
        entry.timerSlot = 0;
    }

    @FunctionalInterface
    public interface TimerConsumer { void expire(Entry entry, long expectedGeneration, long expectedValueAddress); }
}
