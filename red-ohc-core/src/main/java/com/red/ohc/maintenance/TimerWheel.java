package com.red.ohc.maintenance;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;

/**
 * Worker-owned hierarchical TTL wheel. Buckets and overflow storage retain native link ids, while
 * the authoritative CHM resolves an Entry only when expiry needs to touch its heap state.
 */
public final class TimerWheel implements AutoCloseable {
  static final long TICK_NANOS = 64_000_000L;
  static final int MAX_TURN_WORK = 1_024;
  private static final long NO_DEADLINE = Long.MIN_VALUE;
  private static final int L0_SIZE = 1_024;
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
  private static final int PHASE_IDLE = 0;
  private static final int PHASE_CASCADE_L1 = 1;
  private static final int PHASE_CASCADE_L2 = 2;
  private static final int PHASE_CASCADE_L3 = 3;
  private static final int PHASE_OVERFLOW = 4;
  private static final int PHASE_EXPIRE_L0 = 5;

  private final int[] level0 = new int[L0_SIZE];
  private final int[] level1 = new int[L1_SIZE];
  private final int[] level2 = new int[L2_SIZE];
  private final int[] level3 = new int[L3_SIZE];
  private final long[] level0Occupied = new long[L0_SIZE >>> 6];
  private final EntryLinks links;
  private final boolean ownsLinks;
  private long level1Occupied;
  private long level2Occupied;
  private int level3Occupied;
  private int[] overflow = new int[16];
  private int overflowSize;
  private long tick;
  private long scheduled;

  /** A bucket that exceeded the turn budget is resumed before advancing time. */
  private int pendingExpirySlot = -1;
  /** 0=between ticks, 1..3=cascades, 4=overflow promotion, 5=L0 expiry. */
  private int tickPhase;

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
    return (long) (L0_SIZE + L1_SIZE + L2_SIZE + L3_SIZE) * Integer.BYTES
        + (long) level0Occupied.length * Long.BYTES
        + Long.BYTES * 2L
        + Integer.BYTES
        + (long) overflow.length * Integer.BYTES
        + Integer.BYTES;
  }

  long scheduled() {
    return scheduled;
  }

  public boolean hasPending() {
    return scheduled != 0L;
  }

  boolean hasPendingExpiry() {
    return pendingExpirySlot >= 0 || tickPhase != PHASE_IDLE;
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
    if (deadlineNanos == NO_DEADLINE) {
      return;
    }
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      linkId = links.ensure(entry);
    }
    add(linkId, deadlineNanos, targetTick(deadlineNanos));
  }

  void add(Entry entry, long deadlineNanos, int linkId) {
    if (deadlineNanos == NO_DEADLINE) {
      return;
    }
    if (linkId == 0) {
      linkId = links.ensure(entry);
    }
    add(linkId, deadlineNanos, targetTick(deadlineNanos));
  }

  private void add(int linkId, long deadlineNanos, long target) {
    if (links.timerLocation(linkId) != TIMER_UNSCHEDULED) {
      return;
    }
    links.timerDeadlineTick(linkId, target);
    link(linkId, target);
    scheduled++;
  }

  void reschedule(Entry entry, long deadlineNanos) {
    reschedule(entry, deadlineNanos, entry.policyLinkId());
  }

  void reschedule(Entry entry, long deadlineNanos, int linkId) {
    long target = deadlineNanos == NO_DEADLINE ? NO_DEADLINE : targetTick(deadlineNanos);
    if (deadlineNanos != NO_DEADLINE
        && linkId != 0
        && links.timerLocation(linkId) != TIMER_UNSCHEDULED
        && links.timerDeadlineTick(linkId) == target) {
      return;
    }
    remove(entry, linkId);
    if (deadlineNanos != NO_DEADLINE) {
      int currentLinkId = entry.policyLinkId();
      if (currentLinkId == 0) {
        currentLinkId = links.ensure(entry);
      }
      add(currentLinkId, deadlineNanos, target);
    }
  }

  void remove(Entry entry) {
    remove(entry, entry.policyLinkId());
  }

  void remove(Entry entry, int linkId) {
    if (linkId == 0) {
      return;
    }
    int location = links.timerLocation(linkId);
    if (location == TIMER_UNSCHEDULED) {
      return;
    }
    if (location <= TIMER_HEAP_BASE) {
      heapRemove(linkId);
    } else {
      unlink(location >>> TIMER_SLOT_BITS, location & TIMER_SLOT_MASK, linkId);
    }
    links.timerLocation(linkId, TIMER_UNSCHEDULED);
    decrementScheduled();
    links.maybeRelease(entry);
  }

  public int advance(long nowNanos, TimerConsumer consumer) {
    return advance(nowNanos, MAX_TURN_WORK, consumer);
  }

  /**
   * Advances the wheel using one shared work budget for cascades, heap promotion and expiry.
   * Partial buckets stay attached to their current phase and resume on the next actor turn.
   */
  public int advance(long nowNanos, int workLimit, TimerConsumer consumer) {
    if (workLimit <= 0) {
      return 0;
    }
    int budget = Math.min(workLimit, MAX_TURN_WORK);
    int work = 0;
    int expired = 0;
    long target = Math.max(tick, nowNanos / TICK_NANOS);
    while (work < budget) {
      if (tickPhase == PHASE_IDLE) {
        if (target == tick) {
          return expired;
        }
        long next = nextWakeTick();
        if (next == Long.MAX_VALUE || next > target) {
          tick = target;
          return expired;
        }
        tick = Math.max(tick + 1L, next);
        tickPhase = PHASE_CASCADE_L1;
      }

      if (tickPhase == PHASE_CASCADE_L1) {
        if ((tick & (L0_SIZE - 1L)) == 0L) {
          work += cascade(1, (int) ((tick >>> 10) & (L1_SIZE - 1)), budget - work);
          if (bucketHead(1, (int) ((tick >>> 10) & (L1_SIZE - 1))) != 0) {
            return expired;
          }
        }
        tickPhase = PHASE_CASCADE_L2;
      }
      if (work == budget) {
        return expired;
      }

      if (tickPhase == PHASE_CASCADE_L2) {
        if ((tick & (L1_SPAN - 1L)) == 0L) {
          work += cascade(2, (int) ((tick >>> 16) & (L2_SIZE - 1)), budget - work);
          if (bucketHead(2, (int) ((tick >>> 16) & (L2_SIZE - 1))) != 0) {
            return expired;
          }
        }
        tickPhase = PHASE_CASCADE_L3;
      }
      if (work == budget) {
        return expired;
      }

      if (tickPhase == PHASE_CASCADE_L3) {
        if ((tick & (L2_SPAN - 1L)) == 0L) {
          work += cascade(3, (int) ((tick >>> 22) & (L3_SIZE - 1)), budget - work);
          if (bucketHead(3, (int) ((tick >>> 22) & (L3_SIZE - 1))) != 0) {
            return expired;
          }
        }
        tickPhase = PHASE_OVERFLOW;
      }
      if (work == budget) {
        return expired;
      }

      if (tickPhase == PHASE_OVERFLOW) {
        work += promoteOverflow(budget - work);
        if (overflowNeedsPromotion()) {
          return expired;
        }
        tickPhase = PHASE_EXPIRE_L0;
      }
      if (work == budget) {
        return expired;
      }

      if (tickPhase == PHASE_EXPIRE_L0) {
        int slot = (int) tick & (L0_SIZE - 1);
        int processed = expireLevel0(slot, nowNanos, budget - work, consumer);
        work += processed;
        expired += processed;
        if (bucketHead(0, slot) != 0) {
          pendingExpirySlot = slot;
          return expired;
        }
        pendingExpirySlot = -1;
        tickPhase = PHASE_IDLE;
      }
    }
    return expired;
  }

  private int cascade(int level, int slot, int limit) {
    int work = 0;
    while (work < limit) {
      int linkId = popHead(level, slot);
      if (linkId == 0) {
        return work;
      }
      long deadline = links.timerDeadlineTick(linkId);
      links.timerLocation(linkId, TIMER_UNSCHEDULED);
      decrementScheduled();
      link(linkId, deadline);
      scheduled++;
      work++;
    }
    return work;
  }

  private int expireLevel0(int slot, long nowNanos, int limit, TimerConsumer consumer) {
    int work = 0;
    while (work < limit) {
      int linkId = popHead(0, slot);
      if (linkId == 0) {
        return work;
      }
      links.timerLocation(linkId, TIMER_UNSCHEDULED);
      decrementScheduled();
      Entry entry = links.entry(linkId);
      if (entry != null && entry.isAlive()) {
        long taggedAddress = entry.valueAddress;
        long address = Entry.rawValueAddress(taggedAddress);
        long generation = entry.generation();
        if (address != 0L
            && Entry.hasTtl(taggedAddress)
            && ValueBlock.expired(address, nowNanos)) {
          consumer.expire(entry, generation, taggedAddress);
        } else if (address != 0L && Entry.hasTtl(taggedAddress)) {
          long deadlineNanos = ValueBlock.deadlineNanos(address);
          add(linkId, deadlineNanos, targetTick(deadlineNanos));
        }
      }
      work++;
    }
    return work;
  }

  private int promoteOverflow(int limit) {
    int work = 0;
    while (work < limit && overflowSize != 0) {
      int linkId = overflow[0];
      long deadline = links.timerDeadlineTick(linkId);
      if (deadline - tick >= L3_SPAN) {
        break;
      }
      heapPoll();
      links.timerLocation(linkId, TIMER_UNSCHEDULED);
      decrementScheduled();
      link(linkId, deadline);
      scheduled++;
      work++;
    }
    return work;
  }

  private boolean overflowNeedsPromotion() {
    return overflowSize != 0
        && links.timerDeadlineTick(overflow[0]) - tick < L3_SPAN;
  }

  private void link(int linkId, long target) {
    long distance = target - tick;
    if (distance < L0_SPAN) {
      link(0, (int) target & (L0_SIZE - 1), linkId);
    } else if (distance < L1_SPAN) {
      link(1, (int) (target >>> 10) & (L1_SIZE - 1), linkId);
    } else if (distance < L2_SPAN) {
      link(2, (int) (target >>> 16) & (L2_SIZE - 1), linkId);
    } else if (distance < L3_SPAN) {
      link(3, (int) (target >>> 22) & (L3_SIZE - 1), linkId);
    } else {
      links.timerHeapIndex(linkId, -1);
      heapOffer(linkId);
    }
  }

  private void link(int level, int slot, int linkId) {
    int head = bucketHead(level, slot);
    links.timerLocation(linkId, (level << TIMER_SLOT_BITS) | (slot & TIMER_SLOT_MASK));
    links.linkHead(
        linkId,
        head,
        EntryLinks.TIMER_PREVIOUS_OFFSET,
        EntryLinks.TIMER_NEXT_OFFSET);
    setBucketHead(level, slot, linkId);
    setOccupied(level, slot);
  }

  private void unlink(int level, int slot, int linkId) {
    long neighbors =
        links.unlink(
            linkId,
            EntryLinks.TIMER_PREVIOUS_OFFSET,
            EntryLinks.TIMER_NEXT_OFFSET);
    if ((int) (neighbors >>> 32) == 0) {
      setBucketHead(level, slot, (int) neighbors);
    }
    if (bucketHead(level, slot) == 0) {
      clearOccupied(level, slot);
    }
  }

  private int popHead(int level, int slot) {
    int linkId = bucketHead(level, slot);
    if (linkId == 0) {
      return 0;
    }
    int nextId = links.timerNext(linkId);
    setBucketHead(level, slot, nextId);
    if (nextId != 0) {
      links.timerPrev(nextId, 0);
    }
    links.clear(
        linkId,
        EntryLinks.TIMER_PREVIOUS_OFFSET,
        EntryLinks.TIMER_NEXT_OFFSET);
    if (nextId == 0) {
      clearOccupied(level, slot);
    }
    return linkId;
  }

  private int bucketHead(int level, int slot) {
    switch (level) {
      case 0:
        return level0[slot];
      case 1:
        return level1[slot];
      case 2:
        return level2[slot];
      case 3:
        return level3[slot];
      default:
        throw new IllegalArgumentException("invalid timer level: " + level);
    }
  }

  private void setBucketHead(int level, int slot, int head) {
    switch (level) {
      case 0:
        level0[slot] = head;
        break;
      case 1:
        level1[slot] = head;
        break;
      case 2:
        level2[slot] = head;
        break;
      case 3:
        level3[slot] = head;
        break;
      default:
        throw new IllegalArgumentException("invalid timer level: " + level);
    }
  }

  private void setOccupied(int level, int slot) {
    if (level == 0) {
      level0Occupied[slot >>> 6] |= 1L << (slot & 63);
    } else if (level == 1) {
      level1Occupied |= 1L << slot;
    } else if (level == 2) {
      level2Occupied |= 1L << slot;
    } else {
      level3Occupied |= 1 << slot;
    }
  }

  private void clearOccupied(int level, int slot) {
    if (level == 0) {
      level0Occupied[slot >>> 6] &= ~(1L << (slot & 63));
    } else if (level == 1) {
      level1Occupied &= ~(1L << slot);
    } else if (level == 2) {
      level2Occupied &= ~(1L << slot);
    } else {
      level3Occupied &= ~(1 << slot);
    }
  }

  /** Returns the next actor-owned tick that can require timer work. */
  long nextWakeTick() {
    if (tickPhase != PHASE_IDLE || pendingExpirySlot >= 0) {
      return tick;
    }
    long next = firstLevel0WakeTick();
    next = Math.min(next, nextCascade(level1Occupied, tick >>> 10, 10, L1_SIZE));
    next = Math.min(next, nextCascade(level2Occupied, tick >>> 16, 16, L2_SIZE));
    next = Math.min(
        next, nextCascade(level3Occupied & 0xffff_ffffL, tick >>> 22, 22, L3_SIZE));
    if (overflowSize != 0) {
      long promote = links.timerDeadlineTick(overflow[0]) - L3_SPAN + 1L;
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

  public static long ceilTick(long nanos) {
    long quotient = nanos / TICK_NANOS;
    return nanos % TICK_NANOS == 0L ? quotient : quotient + 1L;
  }

  private long targetTick(long deadlineNanos) {
    long target = ceilTick(deadlineNanos);
    return target <= tick ? tick + 1L : target;
  }

  private void heapOffer(int linkId) {
    if (overflowSize == overflow.length) {
      int[] expanded = new int[overflow.length << 1];
      System.arraycopy(overflow, 0, expanded, 0, overflow.length);
      overflow = expanded;
    }
    int index = overflowSize++;
    overflow[index] = linkId;
    links.timerHeapIndex(linkId, index);
    siftUp(index);
  }

  private void heapPoll() {
    int removed = overflow[0];
    int last = --overflowSize;
    overflow[0] = overflow[last];
    overflow[last] = 0;
    links.timerHeapIndex(removed, -1);
    if (last != 0) {
      links.timerHeapIndex(overflow[0], 0);
      siftDown(0);
    }
  }

  private void heapRemove(int linkId) {
    int index = links.timerHeapIndex(linkId);
    if (index < 0 || index >= overflowSize || overflow[index] != linkId) {
      return;
    }
    int last = --overflowSize;
    overflow[index] = overflow[last];
    overflow[last] = 0;
    links.timerHeapIndex(linkId, -1);
    if (index != last) {
      links.timerHeapIndex(overflow[index], index);
      if (index != 0 && before(overflow[index], overflow[(index - 1) >>> 1])) {
        siftUp(index);
      } else {
        siftDown(index);
      }
    }
  }

  private void siftUp(int index) {
    int value = overflow[index];
    while (index != 0) {
      int parent = (index - 1) >>> 1;
      int previous = overflow[parent];
      if (!before(value, previous)) {
        break;
      }
      overflow[index] = previous;
      links.timerHeapIndex(previous, index);
      index = parent;
    }
    overflow[index] = value;
    links.timerHeapIndex(value, index);
  }

  private void siftDown(int index) {
    int value = overflow[index];
    int half = overflowSize >>> 1;
    while (index < half) {
      int child = (index << 1) + 1;
      int candidate = overflow[child];
      int right = child + 1;
      if (right < overflowSize && before(overflow[right], candidate)) {
        child = right;
        candidate = overflow[right];
      }
      if (!before(candidate, value)) {
        break;
      }
      overflow[index] = candidate;
      links.timerHeapIndex(candidate, index);
      index = child;
    }
    overflow[index] = value;
    links.timerHeapIndex(value, index);
  }

  private boolean before(int leftId, int rightId) {
    return links.timerDeadlineTick(leftId) < links.timerDeadlineTick(rightId);
  }

  private void decrementScheduled() {
    if (scheduled > 0L) {
      scheduled--;
    }
  }

  @FunctionalInterface
  public interface TimerConsumer {
    void expire(Entry entry, long expectedGeneration, long expectedValueAddress);
  }

}
