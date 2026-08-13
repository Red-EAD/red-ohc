package com.red.ohc.maintenance;

import it.unimi.dsi.fastutil.longs.Long2LongLinkedOpenHashMap;

import com.red.ohc.api.Eviction;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/**
 * Maintenance-actor-only eviction policy state. Every list is intrusive in {@link Entry}; no
 * producer or reader mutates a policy link.
 */
public final class MaintenancePolicy {
  private static final double HILL_INITIAL_STEP_PERCENT = 0.0625d;
  private static final double HILL_STEP_DECAY = 0.98d;
  private static final double HILL_RESTART_THRESHOLD = 0.05d;
  private static final long HILL_MIN_STEP = 2L;

  /**
   * Matches Caffeine's ADMIT_HASHDOS_THRESHOLD: only warm candidates use the 1/128 escape hatch.
   */
  private static final int HASHDOS_ADMISSION_THRESHOLD = 6;

  private final Eviction eviction;
  private final long capacity;
  private final boolean countBounded;
  private final FrequencySketch sketch;
  private final EntryDeque lru = new EntryDeque();
  private final EntryDeque small = new EntryDeque();
  private final EntryDeque main = new EntryDeque();
  private final EntryDeque window = new EntryDeque();
  private final EntryDeque probation = new EntryDeque();
  private final EntryDeque protectedQueue = new EntryDeque();
  private final GhostMap ghost;
  private final long ghostMaximum;
  private final long smallMaximum;
  private long windowMaximum;
  private long protectedMaximum;

  private long weightedSize;
  private long liveBytes;
  private long smallWeight;
  private long mainWeight;
  private long windowWeight;
  private long probationWeight;
  private long protectedWeight;
  private volatile long evictions;
  private long ghostWeight;
  private long hitsInSample;
  private long missesInSample;
  private double previousSampleHitRate;
  private double hillStep;

  /** First Window entry moved to probation and not yet processed by main admission. */
  private Entry tinyCandidate;

  private long admissionSequence;
  private final Selection selection = new Selection();

  /** Actor-visible work consumed while finding the latest victim. */
  private int lastVictimScanCount;

  public MaintenancePolicy(Eviction eviction, long capacity) {
    this(eviction, capacity, false);
  }

  public MaintenancePolicy(Eviction eviction, long capacity, boolean countBounded) {
    this.eviction = eviction;
    this.capacity = capacity;
    this.countBounded = countBounded;
    long plannedEntries = Math.max(256L, capacity / 128L);
    this.sketch = eviction == Eviction.W_TINY_LFU ? new FrequencySketch(plannedEntries) : null;
    this.smallMaximum = Math.max(1L, capacity / 10L);
    this.windowMaximum = Math.max(1L, capacity / 100L);
    long mainMaximum = mainMaximum();
    this.protectedMaximum = Math.max(1L, mainMaximum * 80L / 100L);
    this.hillStep = -Math.max(HILL_MIN_STEP, capacity * HILL_INITIAL_STEP_PERCENT);
    this.ghostMaximum = Math.max(1L, capacity - smallMaximum);
    if (eviction == Eviction.S3_FIFO) {
      this.ghost = new GhostMap();
      this.ghost.defaultReturnValue(Long.MIN_VALUE);
    } else {
      this.ghost = null;
    }
  }

  public void add(Entry entry) {
    if (entry.policyState() != Entry.POLICY_NONE) {
      updateWeight(entry);
      return;
    }
    long bytes = byteWeightOf(entry);
    long weight = countBounded ? 1L : bytes;
    entry.policyWeight = weight;
    weightedSize += weight;
    entry.policyByteWeight = bytes;
    liveBytes += bytes;
    switch (eviction) {
      case S3_FIFO:
        long hash = entry.keyHash64();
        long ghostEntryWeight = ghost.remove(hash);
        if (ghostEntryWeight != Long.MIN_VALUE) {
          ghostWeight -= ghostEntryWeight;
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
        recordWriteMiss();
        drainWindow();
        break;
      default:
        link(lru, entry, Entry.POLICY_LRU);
    }
  }

  /**
   * Candidates only have meaning for the mutations drained in one actor batch. A batch that stayed
   * within capacity must not let its old Window candidate compete with a later write.
   */
  void beginWriteBatch() {
    if (eviction == Eviction.W_TINY_LFU) {
      tinyCandidate = null;
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
        advanceTinyCandidate(entry);
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
    remove(entry, eviction, 0L, false);
  }

  /** Removes an entry, reusing a hash already loaded by the eviction selector when provided. */
  void remove(Entry entry, boolean eviction, long keyHash64) {
    remove(entry, eviction, keyHash64, true);
  }

  private void remove(Entry entry, boolean eviction, long keyHash64, boolean hashProvided) {
    int state = entry.policyState();
    if (state == Entry.POLICY_NONE) {
      entry.policyWeight = 0L;
      entry.policyByteWeight = 0L;
      return;
    }
    switch (state) {
      case Entry.POLICY_LRU:
        unlink(lru, entry);
        break;
      case Entry.POLICY_S3_SMALL:
        unlink(small, entry);
        smallWeight -= entry.policyWeight;
        if (eviction) {
          addGhost(entry, hashProvided ? keyHash64 : entry.keyHash64());
        }
        break;
      case Entry.POLICY_S3_MAIN:
        unlink(main, entry);
        mainWeight -= entry.policyWeight;
        break;
      case Entry.POLICY_TINY_WINDOW:
        unlink(window, entry);
        windowWeight -= entry.policyWeight;
        break;
      case Entry.POLICY_TINY_PROBATION:
        advanceTinyCandidate(entry);
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
    liveBytes -= entry.policyByteWeight;
    entry.policyState(Entry.POLICY_NONE);
    entry.policyWeight = 0L;
    entry.policyByteWeight = 0L;
    entry.policyAccessCount(0);
    if (eviction) {
      evictions++;
    }
  }

  /** Selects one production eviction outcome; scan exhaustion is distinct from an empty policy. */
  public Selection selectVictim(int scanLimit) {
    selection.reset();
    if (scanLimit <= 0) {
      lastVictimScanCount = 0;
      return selection.scanExhausted();
    }
    switch (eviction) {
      case S3_FIFO:
        return s3Victim(scanLimit);
      case W_TINY_LFU:
        lastVictimScanCount = 1;
        return tinyLfuVictim();
      default:
        lastVictimScanCount = lru.tail == null ? 0 : 1;
        return lru.tail == null ? selection.none() : selection.entry(lru.tail);
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

  long usedWeight() {
    return weightedSize;
  }

  long usedBytes() {
    return liveBytes;
  }

  long evictions() {
    return evictions;
  }

  long sketchBytes() {
    return sketch == null ? 0L : sketch.bytes();
  }

  long ghostHeapBytes() {
    return ghost == null ? 0L : ghost.heapBytes();
  }

  /** A successfully consumed access event, not a delayed global cache-stat delta. */
  public void recordAccessHit() {
    if (sketch == null) {
      return;
    }
    hitsInSample = saturatedAdd(hitsInSample, 1L);
    climb();
  }

  /** An actor-applied ADD is one policy miss. */
  void recordWriteMiss() {
    missesInSample = saturatedAdd(missesInSample, 1L);
    climb();
  }

  long sampleSize() {
    return sketch == null ? 0L : sketch.sampleSize();
  }

  long windowMaximum() {
    return windowMaximum;
  }

  public int lastVictimScanCount() {
    return lastVictimScanCount;
  }

  private Selection s3Victim(int scanLimit) {
    lastVictimScanCount = 0;
    while (lastVictimScanCount < scanLimit) {
      if (small.tail != null && (smallWeight >= smallMaximum || main.tail == null)) {
        Entry candidate = small.tail;
        lastVictimScanCount++;
        if (candidate.policyAccessCount() > 1) {
          unlink(small, candidate);
          smallWeight -= candidate.policyWeight;
          candidate.policyAccessCount(0);
          link(main, candidate, Entry.POLICY_S3_MAIN);
          mainWeight += candidate.policyWeight;
          continue;
        }
        return selection.entry(candidate);
      }
      Entry candidate = main.tail;
      if (candidate == null) {
        candidate = small.tail;
        if (candidate != null) {
          lastVictimScanCount++;
        }
        return candidate == null ? selection.none() : selection.entry(candidate);
      }
      lastVictimScanCount++;
      if (candidate.policyAccessCount() > 0) {
        candidate.policyAccessCount(candidate.policyAccessCount() - 1);
        main.moveToHead(candidate);
        continue;
      }
      return selection.entry(candidate);
    }
    return selection.scanExhausted();
  }

  private Selection tinyLfuVictim() {
    demoteProtected();
    drainWindow();
    Entry candidate = tinyCandidate;
    if (candidate != null && candidate.policyState() != Entry.POLICY_TINY_PROBATION) {
      tinyCandidate = null;
      candidate = null;
    }
    Entry victim = probation.tail;
    if (candidate != null && victim == candidate) {
      advanceTinyCandidate(candidate);
      return selection.entry(candidate);
    }
    if (candidate != null && victim != null) {
      advanceTinyCandidate(candidate);
      return selection.entry(admit(candidate, victim) ? victim : candidate);
    }
    if (victim != null) {
      return selection.entry(victim);
    }
    if (candidate != null) {
      advanceTinyCandidate(candidate);
      return selection.entry(candidate);
    }
    if (protectedQueue.tail != null) {
      return selection.entry(protectedQueue.tail);
    }
    return selection.none();
  }

  private boolean admit(Entry candidate, Entry victim) {
    long candidateHash = candidate.keyHash64();
    long victimHash = victim.keyHash64();
    int candidateFrequency = sketch.frequency(candidateHash);
    int victimFrequency = sketch.frequency(victimHash);
    if (candidateFrequency > victimFrequency) {
      return true;
    }
    if (candidateFrequency < HASHDOS_ADMISSION_THRESHOLD) {
      return false;
    }
    // Caffeine's 1/128 HashDoS escape hatch, made deterministic because the actor owns the
    // sequence and policy results must not depend on a producer thread's RNG state.
    return ((candidateHash + ++admissionSequence) & 127L) == 0L;
  }

  private void promoteWindow(Entry candidate) {
    unlink(window, candidate);
    windowWeight -= candidate.policyWeight;
    link(probation, candidate, Entry.POLICY_TINY_PROBATION);
    probationWeight += candidate.policyWeight;
    if (tinyCandidate == null) {
      tinyCandidate = candidate;
    }
  }

  private void drainWindow() {
    while (windowWeight > windowMaximum && window.tail != null) {
      Entry candidate = window.tail;
      promoteWindow(candidate);
    }
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

  /** Candidate traversal is oldest-to-newest, opposite to the intrusive deque's head links. */
  private void advanceTinyCandidate(Entry entry) {
    if (entry == tinyCandidate) {
      tinyCandidate = entry.policyPrev;
    }
  }

  /** Caffeine-compatible step sizing: 6.25% initial, min 2, 5% restart, 0.98 decay. */
  private void climb() {
    long requests = hitsInSample + missesInSample;
    if (requests < sketch.sampleSize()) {
      return;
    }
    double hitRate = (double) hitsInSample / requests;
    double change = hitRate - previousSampleHitRate;
    long adjustment = (long) (change >= 0d ? hillStep : -hillStep);
    if (adjustment > 0L) {
      increaseWindow(adjustment);
    } else {
      if (adjustment < 0L) {
        decreaseWindow(-adjustment);
      }
    }

    double nextMagnitude =
        Math.abs(change) >= HILL_RESTART_THRESHOLD
            ? Math.max(HILL_MIN_STEP, capacity * HILL_INITIAL_STEP_PERCENT)
            : Math.max(1d, Math.abs(hillStep) * HILL_STEP_DECAY);
    hillStep = Math.copySign(nextMagnitude, adjustment == 0L ? hillStep : adjustment);
    previousSampleHitRate = hitRate;
    hitsInSample = 0L;
    missesInSample = 0L;
  }

  private void increaseWindow(long amount) {
    long quota = Math.min(amount, Math.max(0L, capacity - windowMaximum));
    if (quota == 0L) {
      return;
    }
    windowMaximum += quota;
    protectedMaximum = Math.max(1L, protectedMaximum - quota);
    demoteProtected();
  }

  private void decreaseWindow(long amount) {
    long quota = Math.min(amount, Math.max(0L, windowMaximum - 1L));
    if (quota == 0L) {
      return;
    }
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
    long updatedBytes = byteWeightOf(entry);
    long updated = countBounded ? 1L : updatedBytes;
    long delta = updated - entry.policyWeight;
    long byteDelta = updatedBytes - entry.policyByteWeight;
    if (delta == 0L && byteDelta == 0L) {
      return;
    }
    weightedSize += delta;
    liveBytes += byteDelta;
    switch (entry.policyState()) {
      case Entry.POLICY_S3_SMALL:
        smallWeight += delta;
        break;
      case Entry.POLICY_S3_MAIN:
        mainWeight += delta;
        break;
      case Entry.POLICY_TINY_WINDOW:
        windowWeight += delta;
        break;
      case Entry.POLICY_TINY_PROBATION:
        probationWeight += delta;
        break;
      case Entry.POLICY_TINY_PROTECTED:
        protectedWeight += delta;
        break;
      default:
    }
    entry.policyWeight = updated;
    entry.policyByteWeight = updatedBytes;
  }

  private void addGhost(Entry entry, long fingerprint) {
    long previous = ghost.remove(fingerprint);
    if (previous != Long.MIN_VALUE) {
      ghostWeight -= previous;
    }
    ghost.put(fingerprint, entry.policyWeight);
    ghostWeight += entry.policyWeight;
    if (ghostWeight > ghostMaximum) {
      while (ghostWeight > ghostMaximum && !ghost.isEmpty()) {
        ghostWeight -= ghost.removeFirstLong();
      }
    }
  }

  public static final class Selection {
    public enum Kind {
      ENTRY,
      SCAN_EXHAUSTED,
      NONE
    }

    public Kind kind;
    public Entry entry;

    private Selection() {
      this.kind = Kind.NONE;
    }

    private void reset() {
      kind = Kind.NONE;
      entry = null;
    }

    private Selection entry(Entry value) {
      kind = Kind.ENTRY;
      entry = value;
      return this;
    }

    private Selection scanExhausted() {
      kind = Kind.SCAN_EXHAUSTED;
      entry = null;
      return this;
    }

    private Selection none() {
      kind = Kind.NONE;
      entry = null;
      return this;
    }
  }

  private static long byteWeightOf(Entry entry) {
    long key = WriterArena.allocationWeight(entry.keyAllocationLength());
    long valueAddress = Entry.rawValueAddress(entry.valueAddress);
    if (valueAddress == 0L) {
      return key;
    }
    return key
        + WriterArena.allocationWeight(
            ValueBlock.allocationLength(ValueBlock.length(valueAddress)));
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
      if (head == null) {
        tail = entry;
      } else {
        head.policyPrev = entry;
      }
      head = entry;
    }

    void unlink(Entry entry) {
      Entry previous = entry.policyPrev;
      Entry next = entry.policyNext;
      if (previous == null) {
        head = next;
      } else {
        previous.policyNext = next;
      }
      if (next == null) {
        tail = previous;
      } else {
        next.policyPrev = previous;
      }
      entry.policyPrev = null;
      entry.policyNext = null;
    }

    void moveToHead(Entry entry) {
      if (head == entry) {
        return;
      }
      unlink(entry);
      linkHead(entry);
    }
  }

  /**
   * Fastutil owns three primitive arrays for this linked map. Report their payload capacity rather
   * than the logical entry count; like the other internal-memory statistics, JVM object and array
   * headers are deliberately excluded because their sizes are VM-specific.
   */
  private static final class GhostMap extends Long2LongLinkedOpenHashMap {
    long heapBytes() {
      return ((long) key.length + value.length + link.length) * Long.BYTES;
    }
  }
}
