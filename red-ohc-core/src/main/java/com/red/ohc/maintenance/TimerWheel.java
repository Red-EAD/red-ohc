package com.red.ohc.maintenance;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;

/**
 * Worker-owned hierarchical TTL wheel. Occupancy bitmaps let an opportunistic maintenance pass
 * skip empty ticks while preserving each cascade boundary. A stale node is harmless because
 * generation and value pointer are checked by the maintenance consumer.
 */
public final class TimerWheel implements AutoCloseable {
  static final long TICK_NANOS = 64_000_000L;
  private static final long NO_DEADLINE = Long.MIN_VALUE;
  private static final int L0_SIZE = 1024;
  private static final int L1_SIZE = 64;
  private static final int L2_SIZE = 64;
  private static final int L3_SIZE = 32;
  private static final long L0_SPAN = L0_SIZE;
  private static final long L1_SPAN = L0_SPAN * L1_SIZE;
  private static final long L2_SPAN = L1_SPAN * L2_SIZE;
  private static final long L3_SPAN = L2_SPAN * L3_SIZE;
  private static final int TIMER_UNSCHEDULED = -1;
  private static final int TIMER_HEAP_BASE = -2;
  private static final int TIMER_SLOT_BITS = 10;
  private static final int TIMER_SLOT_MASK = (1 << TIMER_SLOT_BITS) - 1;

  private final Entry[] level0 = new Entry[L0_SIZE];
  private final Entry[] level1 = new Entry[L1_SIZE];
  private final Entry[] level2 = new Entry[L2_SIZE];
  private final Entry[] level3 = new Entry[L3_SIZE];
  private final long[] level0Occupied = new long[L0_SIZE >>> 6];
  private final EntryLinks links;
  private final boolean ownsLinks;
  private long level1Occupied;
  private long level2Occupied;
  private int level3Occupied;
  private Entry[] overflow = new Entry[16];
  private int overflowSize;
  private long tick;
  private long scheduled;

  /** A bucket that exceeded the caller's expiry budget. It is resumed before advancing time. */
  private int pendingExpirySlot = -1;

  public TimerWheel(long nowNanos) {
    this(nowNanos, new EntryLinks(), true);
  }

  TimerWheel(long nowNanos, EntryLinks links) {
    this(nowNanos, links, false);
  }

  private TimerWheel(long nowNanos, EntryLinks links, boolean ownsLinks) {
    if (links == null) {
      throw new NullPointerException("links");
    }
    this.links = links;
    this.ownsLinks = ownsLinks;
    tick = Math.max(0L, nowNanos / TICK_NANOS);
  }

  @Override
  public void close() {
    if (ownsLinks) {
      links.close();
    }
  }

  long bytes() {
    return (long) (L0_SIZE + L1_SIZE + L2_SIZE + L3_SIZE) * Long.BYTES
        + (long) level0Occupied.length * Long.BYTES
        + Long.BYTES * 2L
        + Integer.BYTES
        + (long) overflow.length * Long.BYTES
        + Integer.BYTES;
  }

  long scheduled() {
    return scheduled;
  }

  public boolean hasPending() {
    return scheduled != 0L;
  }

  boolean hasPendingExpiry() {
    return pendingExpirySlot >= 0;
  }

  /**
   * Returns whether an already-installed timer can represent the supplied live deadline.
   * Callers must separately prove that the old deadline is still live and only being extended.
   */
  boolean hasSameScheduledSlot(Entry entry, long deadlineNanos) {
    return deadlineNanos != NO_DEADLINE
        && entry.timerScheduled()
        && entry.timerDeadlineTick() == ceilTick(deadlineNanos);
  }

  public void add(Entry entry, long deadlineNanos) {
    add(entry, deadlineNanos, entry.policyLinkId());
  }

  void add(Entry entry, long deadlineNanos, int linkId) {
    if (deadlineNanos == NO_DEADLINE || entry.timerScheduled()) {
      return;
    }
    long target = targetTick(deadlineNanos);
    entry.timerDeadlineTick(target);
    link(entry, target, linkId);
    scheduled++;
  }

  void reschedule(Entry entry, long deadlineNanos) {
    reschedule(entry, deadlineNanos, entry.policyLinkId());
  }

  void reschedule(Entry entry, long deadlineNanos, int linkId) {
    long target = deadlineNanos == NO_DEADLINE ? NO_DEADLINE : targetTick(deadlineNanos);
    if (deadlineNanos != NO_DEADLINE
        && entry.timerScheduled()
        && entry.timerDeadlineTick() == target) {
      return;
    }
    remove(entry, linkId);
    // Removing the last maintenance link may release its record. Refresh the id before a new
    // timer link is installed; an applyEntry call that still owns policy state keeps the same id.
    add(entry, deadlineNanos, entry.policyLinkId(), target);
  }

  private void add(Entry entry, long deadlineNanos, int linkId, long target) {
    if (deadlineNanos == NO_DEADLINE || entry.timerScheduled()) {
      return;
    }
    entry.timerDeadlineTick(target);
    link(entry, target, linkId);
    scheduled++;
  }

  void remove(Entry entry) {
    remove(entry, entry.policyLinkId());
  }

  void remove(Entry entry, int linkId) {
    int location = entry.timerLocation();
    if (location == TIMER_UNSCHEDULED) {
      return;
    }
    if (location <= TIMER_HEAP_BASE) {
      heapRemove(entry);
      entry.timerLocation(TIMER_UNSCHEDULED);
      links.maybeRelease(entry);
      if (scheduled > 0L) {
        scheduled--;
      }
      return;
    }
    unlink(location >>> TIMER_SLOT_BITS, location & TIMER_SLOT_MASK, entry, linkId);
    entry.timerLocation(TIMER_UNSCHEDULED);
    links.maybeRelease(entry);
    if (scheduled > 0L) {
      scheduled--;
    }
  }

  public int advance(long nowNanos, TimerConsumer consumer) {
    return advance(nowNanos, Integer.MAX_VALUE, consumer);
  }

  /**
   * Advances time while processing at most {@code expiryLimit} entries from L0. A storm in one
   * bucket is resumed on the next actor pass instead of monopolising the event loop or waiting for
   * the bucket to wrap around again.
   */
  public int advance(long nowNanos, int expiryLimit, TimerConsumer consumer) {
    if (expiryLimit <= 0) {
      return 0;
    }
    long target = Math.max(tick, nowNanos / TICK_NANOS);
    if (pendingExpirySlot < 0 && target == tick) {
      return 0;
    }
    promoteOverflow();
    int work = 0;
    if (pendingExpirySlot >= 0) {
      work += expireLevel0(pendingExpirySlot, nowNanos, expiryLimit, consumer);
      if (pendingExpirySlot >= 0 || work == expiryLimit) {
        return work;
      }
    }
    while (tick < target && work < expiryLimit) {
      long next = nextWakeTick();
      if (next == Long.MAX_VALUE || next > target) {
        tick = target;
        break;
      }
      tick = Math.max(tick + 1L, next);
      if ((tick & (L0_SIZE - 1L)) == 0L) {
        cascade(1, (int) ((tick >>> 10) & (L1_SIZE - 1)));
      }
      if ((tick & (L1_SPAN - 1L)) == 0L) {
        cascade(2, (int) ((tick >>> 16) & (L2_SIZE - 1)));
      }
      if ((tick & (L2_SPAN - 1L)) == 0L) {
        cascade(3, (int) ((tick >>> 22) & (L3_SIZE - 1)));
      }
      promoteOverflow();
      work += expireLevel0((int) tick & (L0_SIZE - 1), nowNanos, expiryLimit - work, consumer);
      if (pendingExpirySlot >= 0) {
        break;
      }
    }
    return work;
  }

  private void cascade(int level, int slot) {
    Entry entry = detach(level, slot);
    while (entry != null) {
      int linkId = entry.policyLinkId();
      Entry next = links.timerNextEntry(linkId);
      clearLinks(entry, linkId);
      entry.timerScheduled(false);
      if (scheduled > 0L) {
        scheduled--;
      }
      link(entry, entry.timerDeadlineTick(), linkId);
      scheduled++;
      entry = next;
    }
  }

  private int expireLevel0(int slot, long nowNanos, int limit, TimerConsumer consumer) {
    int work = 0;
    while (work < limit) {
      Entry entry = level0[slot];
      if (entry == null) {
        pendingExpirySlot = -1;
        return work;
      }
      int linkId = entry.policyLinkId();
      unlink(0, slot, entry, linkId);
      entry.timerScheduled(false);
      if (scheduled > 0L) {
        scheduled--;
      }
      long taggedAddress = entry.valueAddress;
      if (!entry.isAlive()) {
        // The lifecycle tag is heap-resident and remains readable after a stale timer sample.
        // Do not load the native generation or value header for a retired/recycled Entry.
        work++;
        continue;
      }
      long address = Entry.rawValueAddress(taggedAddress);
      long generation = entry.generation();
      if (address != 0L
          && Entry.hasTtl(taggedAddress)
          && ValueBlock.expired(address, nowNanos)) {
        consumer.expire(entry, generation, taggedAddress);
      } else {
        if (address != 0L && Entry.hasTtl(taggedAddress)) {
          add(entry, ValueBlock.deadlineNanos(address), linkId);
        }
      }
      work++;
    }
    pendingExpirySlot = level0[slot] == null ? -1 : slot;
    return work;
  }

  private void promoteOverflow() {
    while (overflowSize != 0) {
      Entry entry = overflow[0];
      if (entry.timerDeadlineTick() - tick >= L3_SPAN) {
        return;
      }
      heapPoll();
      entry.timerScheduled(false);
      if (scheduled > 0L) {
        scheduled--;
      }
      link(entry, entry.timerDeadlineTick(), entry.policyLinkId());
      scheduled++;
    }
  }

  private void link(Entry entry, long target, int linkId) {
    long distance = target - tick;
    if (distance < L0_SPAN) {
      link(0, (int) target & (L0_SIZE - 1), entry, linkId);
    } else {
      if (distance < L1_SPAN) {
        link(1, (int) (target >>> 10) & (L1_SIZE - 1), entry, linkId);
      } else {
        if (distance < L2_SPAN) {
          link(2, (int) (target >>> 16) & (L2_SIZE - 1), entry, linkId);
        } else {
          if (distance < L3_SPAN) {
            link(3, (int) (target >>> 22) & (L3_SIZE - 1), entry, linkId);
          } else {
            if (linkId == 0) {
              linkId = links.ensure(entry);
            }
            entry.timerHeapIndex(-1);
            clearLinks(entry, linkId);
            heapOffer(entry);
          }
        }
      }
    }
  }

  private void link(int level, int slot, Entry entry, int linkId) {
    Entry[] heads = heads(level);
    Entry head = heads[slot];
    if (linkId == 0) {
      linkId = links.ensure(entry);
    }
    entry.timerLocation(level, slot);
    links.linkHead(
        linkId,
        head,
        EntryLinks.TIMER_PREVIOUS_OFFSET,
        EntryLinks.TIMER_NEXT_OFFSET);
    heads[slot] = entry;
    setOccupied(level, slot);
  }

  private void unlink(int level, int slot, Entry entry, int linkId) {
    Entry[] heads = heads(level);
    long neighbors =
        links.unlink(
            linkId,
            EntryLinks.TIMER_PREVIOUS_OFFSET,
            EntryLinks.TIMER_NEXT_OFFSET);
    if ((int) (neighbors >>> 32) == 0) {
      heads[slot] = links.entry((int) neighbors);
    }
    if (heads[slot] == null) {
      clearOccupied(level, slot);
    }
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
      case 0:
        return level0;
      case 1:
        return level1;
      case 2:
        return level2;
      case 3:
        return level3;
      default:
        throw new IllegalArgumentException("invalid timer level: " + level);
    }
  }

  private void setOccupied(int level, int slot) {
    if (level == 0) {
      level0Occupied[slot >>> 6] |= 1L << (slot & 63);
    } else {
      if (level == 1) {
        level1Occupied |= 1L << slot;
      } else {
        if (level == 2) {
          level2Occupied |= 1L << slot;
        } else {
          level3Occupied |= 1 << slot;
        }
      }
    }
  }

  private void clearOccupied(int level, int slot) {
    if (level == 0) {
      level0Occupied[slot >>> 6] &= ~(1L << (slot & 63));
    } else {
      if (level == 1) {
        level1Occupied &= ~(1L << slot);
      } else {
        if (level == 2) {
          level2Occupied &= ~(1L << slot);
        } else {
          level3Occupied &= ~(1 << slot);
        }
      }
    }
  }

  /** Returns the next actor-owned tick that can require timer work. */
  long nextWakeTick() {
    if (pendingExpirySlot >= 0) {
      return tick + 1L;
    }
    long next = Long.MAX_VALUE;
    next = firstLevel0WakeTick();
    next = Math.min(next, nextCascade(level1Occupied, tick >>> 10, 10, L1_SIZE));
    next = Math.min(next, nextCascade(level2Occupied, tick >>> 16, 16, L2_SIZE));
    next = Math.min(next, nextCascade(level3Occupied & 0xffffffffL, tick >>> 22, 22, L3_SIZE));
    Entry entry = overflowSize == 0 ? null : overflow[0];
    if (entry != null) {
      long promote = entry.timerDeadlineTick() - L3_SPAN + 1L;
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
    if (occupied != 0L) {
      return wakeTickFor(firstWord, occupied, firstSlot);
    }
    for (int wordOffset = 1; wordOffset < level0Occupied.length; wordOffset++) {
      int word = (firstWord + wordOffset) & (level0Occupied.length - 1);
      occupied = level0Occupied[word];
      if (occupied != 0L) {
        return wakeTickFor(word, occupied, firstSlot);
      }
    }
    // The physical word wraps too. Its lower bits are strictly later than firstSlot after
    // one complete L0 rotation, so they must be examined after every intervening word.
    occupied = firstOccupied & ((1L << firstBit) - 1L);
    if (occupied != 0L) {
      return wakeTickFor(firstWord, occupied, firstSlot);
    }
    return Long.MAX_VALUE;
  }

  private long wakeTickFor(int word, long occupied, int firstSlot) {
    int slot = (word << 6) + Long.numberOfTrailingZeros(occupied);
    long offset = (slot - firstSlot) & (L0_SIZE - 1L);
    return tick + 1L + offset;
  }

  static long nextCascade(long occupied, long cycle, int shift, int size) {
    if (occupied == 0L) {
      return Long.MAX_VALUE;
    }
    int firstSlot = (int) (cycle + 1L) & (size - 1);
    int distance;
    if (size == Long.SIZE) {
      distance = Long.numberOfTrailingZeros(Long.rotateRight(occupied, firstSlot));
    } else if (size == Integer.SIZE) {
      distance =
          Integer.numberOfTrailingZeros(Integer.rotateRight((int) occupied, firstSlot));
    } else {
      throw new IllegalArgumentException("unsupported cascade size: " + size);
    }
    return (cycle + 1L + distance) << shift;
  }

  private static long ceilTick(long nanos) {
    long quotient = nanos / TICK_NANOS;
    return nanos % TICK_NANOS == 0L ? quotient : quotient + 1L;
  }

  private long targetTick(long deadlineNanos) {
    long target = ceilTick(deadlineNanos);
    return target <= tick ? tick + 1L : target;
  }

  private void clearLinks(Entry entry, int linkId) {
    links.clear(linkId, EntryLinks.TIMER_PREVIOUS_OFFSET, EntryLinks.TIMER_NEXT_OFFSET);
  }

  private void heapOffer(Entry entry) {
    if (overflowSize == overflow.length) {
      Entry[] expanded = new Entry[overflow.length << 1];
      System.arraycopy(overflow, 0, expanded, 0, overflow.length);
      overflow = expanded;
    }
    int index = overflowSize++;
    overflow[index] = entry;
    entry.timerHeapIndex(index);
    siftUp(index);
  }

  private void heapPoll() {
    Entry removed = overflow[0];
    int last = --overflowSize;
    overflow[0] = overflow[last];
    overflow[last] = null;
    removed.timerHeapIndex(-1);
    if (last != 0) {
      overflow[0].timerHeapIndex(0);
      siftDown(0);
    }
  }

  private void heapRemove(Entry entry) {
    int index = entry.timerHeapIndex();
    if (index < 0 || index >= overflowSize || overflow[index] != entry) {
      return;
    }
    int last = --overflowSize;
    overflow[index] = overflow[last];
    overflow[last] = null;
    entry.timerHeapIndex(-1);
    if (index != last) {
      overflow[index].timerHeapIndex(index);
      if (index != 0 && before(overflow[index], overflow[(index - 1) >>> 1])) {
        siftUp(index);
      } else {
        siftDown(index);
      }
    }
  }

  private void siftUp(int index) {
    Entry value = overflow[index];
    while (index != 0) {
      int parent = (index - 1) >>> 1;
      Entry previous = overflow[parent];
      if (!before(value, previous)) {
        break;
      }
      overflow[index] = previous;
      previous.timerHeapIndex(index);
      index = parent;
    }
    overflow[index] = value;
    value.timerHeapIndex(index);
  }

  private void siftDown(int index) {
    Entry value = overflow[index];
    int half = overflowSize >>> 1;
    while (index < half) {
      int child = (index << 1) + 1;
      Entry candidate = overflow[child];
      int right = child + 1;
      if (right < overflowSize && before(overflow[right], candidate)) {
        child = right;
        candidate = overflow[right];
      }
      if (!before(candidate, value)) {
        break;
      }
      overflow[index] = candidate;
      candidate.timerHeapIndex(index);
      index = child;
    }
    overflow[index] = value;
    value.timerHeapIndex(index);
  }

  private static boolean before(Entry left, Entry right) {
    return left.timerDeadlineTick() < right.timerDeadlineTick();
  }

  @FunctionalInterface
  public interface TimerConsumer {
    void expire(Entry entry, long expectedGeneration, long expectedValueAddress);
  }
}
