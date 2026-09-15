package com.red.ohc.maintenance;

import com.red.ohc.api.Eviction;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;

/**
 * Maintenance-actor-only eviction policy state. List links are stored in the actor-owned native
 * link arena; no producer or reader mutates a policy link.
 */
public final class MaintenancePolicy implements AutoCloseable {
  /**
   * Fixed S4-FIFO-lite profile applied to the existing S3_FIFO selector: rhoS=.10, kappa=.25,
   * tauS=2, tauG=1.
   */
  private static final long S3_SMALL_TARGET_DIVISOR = 10L;
  private static final long S3_SKIP_WEIGHT_DIVISOR = 4L;
  private static final int S3_SMALL_PROMOTION_HITS = 2;
  private static final int S3_GHOST_TO_MAIN_HITS = 1;
  private static final int GHOST_INSERT_TRIM_ATTEMPTS = 8;
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
  private final EntryLinks links;
  private final boolean ownsLinks;
  private final FrequencySketch sketch;
  private final EntryDeque lru;
  private final EntryDeque small;
  private final EntryDeque main;
  private final EntryDeque window;
  private final EntryDeque probation;
  private final EntryDeque protectedQueue;
  private final NativeS3GhostMap ghost;
  private final long ghostMaximum;
  private final long smallMaximum;
  private final long smallSkipMaximum;
  private long windowMaximum;
  private long protectedMaximum;

  private long weightedSize;
  private long liveBytes;
  private long smallWeight;
  private long smallSkipWeight;
  private Entry smallSkipTail;
  private long mainWeight;
  private long windowWeight;
  private long probationWeight;
  private long protectedWeight;
  private long ghostWeight;
  private long ghostAllocationTrims;
  private long ghostAllocationDrops;
  private long skipSuppressedAccesses;
  private long ghostDeferredPromotions;
  private long hitsInSample;
  private long missesInSample;
  private double previousSampleHitRate;
  private double hillStep;

  /** First Window entry moved to probation and not yet processed by main admission. */
  private Entry tinyCandidate;

  private long admissionSequence;
  private final Selection selection = new Selection();
  private final NativeS3GhostMap.Record ghostRecord = new NativeS3GhostMap.Record();

  /** Actor-visible work consumed while finding the latest victim. */
  private int lastVictimScanCount;

  public MaintenancePolicy(Eviction eviction, long capacity) {
    this(eviction, capacity, false, new EntryLinks(), true);
  }

  public MaintenancePolicy(Eviction eviction, long capacity, boolean countBounded) {
    this(eviction, capacity, countBounded, new EntryLinks(), true);
  }

  MaintenancePolicy(
      Eviction eviction, long capacity, boolean countBounded, EntryLinks links) {
    this(eviction, capacity, countBounded, links, false);
  }

  private MaintenancePolicy(
      Eviction eviction,
      long capacity,
      boolean countBounded,
      EntryLinks links,
      boolean ownsLinks) {
    this.eviction = eviction;
    this.capacity = capacity;
    this.countBounded = countBounded;
    this.links = links;
    this.ownsLinks = ownsLinks;
    this.lru = new EntryDeque();
    this.small = new EntryDeque();
    this.main = new EntryDeque();
    this.window = new EntryDeque();
    this.probation = new EntryDeque();
    this.protectedQueue = new EntryDeque();
    long plannedEntries = Math.max(256L, capacity / 128L);
    this.sketch = eviction == Eviction.W_TINY_LFU ? new FrequencySketch(plannedEntries) : null;
    this.smallMaximum = Math.max(1L, capacity / S3_SMALL_TARGET_DIVISOR);
    this.smallSkipMaximum = smallMaximum / S3_SKIP_WEIGHT_DIVISOR;
    this.windowMaximum = Math.max(1L, capacity / 100L);
    long mainMaximum = mainMaximum();
    this.protectedMaximum = Math.max(1L, mainMaximum * 80L / 100L);
    this.hillStep = -Math.max(HILL_MIN_STEP, capacity * HILL_INITIAL_STEP_PERCENT);
    this.ghostMaximum = Math.max(1L, capacity - smallMaximum);
    if (eviction == Eviction.S3_FIFO) {
      this.ghost = new NativeS3GhostMap(links.memory());
    } else {
      this.ghost = null;
    }
  }

  @Override
  public void close() {
    if (ghost != null) {
      ghost.close();
    }
    if (ownsLinks) {
      links.close();
    }
  }

  public void add(Entry entry) {
    long valueAddress = Entry.rawValueAddress(entry.valueAddress);
    long valueAllocation =
        valueAddress == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(valueAddress));
    add(entry, valueAllocation, entry.keyHash64());
  }

  /** Applies an actor-captured value allocation and immutable hash seed. */
  void add(Entry entry, long valueAllocation, long keyHash64) {
    int linkId = entry.policyLinkId();
    int state = linkId == 0 ? Entry.POLICY_NONE : links.policyState(linkId);
    if (state != Entry.POLICY_NONE) {
      updateWeight(entry, valueAllocation);
      return;
    }
    long bytes = normalizedByteWeight(
        CacheMath.logicalEntryBytes(entry.keyAllocationLength(), valueAllocation));
    long weight = countBounded ? 1L : bytes;
    linkId = links.ensure(entry);
    links.keyHash64(linkId, keyHash64);
    setPolicyByteWeight(entry, bytes);
    setPolicyAccessCount(entry, 0);
    weightedSize += weight;
    liveBytes += bytes;
    switch (eviction) {
      case S3_FIFO:
        if (ghost.observe(keyHash64, ghostRecord)) {
          long ghostEntryWeight =
              ghostRecord.frequency >= S3_GHOST_TO_MAIN_HITS
                  ? ghost.remove(keyHash64)
                  : Long.MIN_VALUE;
          if (ghostEntryWeight != Long.MIN_VALUE) {
            ghostWeight -= ghostEntryWeight;
            link(main, entry, Entry.POLICY_S3_MAIN);
            mainWeight += weight;
          } else {
            ghostDeferredPromotions++;
            addSmall(entry, weight);
          }
        } else {
          addSmall(entry, weight);
        }
        break;
      case W_TINY_LFU:
        link(window, entry, Entry.POLICY_TINY_WINDOW);
        windowWeight += weight;
        sketch.increment(keyHash64);
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
    access(entry, stateOf(entry));
  }

  /** Applies a hit using the policy-state snapshot captured by the business-thread sampler. */
  void access(Entry entry, int observedPolicyState) {
    if (eviction == Eviction.S3_FIFO && observedPolicyState == Entry.POLICY_S4_SKIP) {
      skipSuppressedAccesses++;
      return;
    }
    switch (stateOf(entry)) {
      case Entry.POLICY_LRU:
        lru.moveToHead(entry);
        break;
      case Entry.POLICY_S3_SMALL:
      case Entry.POLICY_S3_MAIN:
        setPolicyAccessCount(entry, accessCountOf(entry) + 1);
        break;
      case Entry.POLICY_S4_SKIP:
        skipSuppressedAccesses++;
        break;
      case Entry.POLICY_TINY_WINDOW:
        sketch.increment(keyHashOf(entry));
        window.moveToHead(entry);
        break;
      case Entry.POLICY_TINY_PROBATION:
        sketch.increment(keyHashOf(entry));
        advanceTinyCandidate(entry);
        unlink(probation, entry);
        long weight = weightOf(entry);
        probationWeight -= weight;
        link(protectedQueue, entry, Entry.POLICY_TINY_PROTECTED);
        protectedWeight += weight;
        demoteProtected();
        break;
      case Entry.POLICY_TINY_PROTECTED:
        sketch.increment(keyHashOf(entry));
        protectedQueue.moveToHead(entry);
        break;
      default:
    }
  }

  public void remove(Entry entry, boolean eviction) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      clearUnlinkedPolicyMetadata(entry);
      return;
    }
    int state = links.policyState(linkId);
    long removedLogicalBytes = links.policyByteWeight(linkId);
    int removedAccessCount = links.policyAccessCount(linkId);
    long keyHash64 = links.keyHash64(linkId);
    if (state == Entry.POLICY_NONE) {
      clearPolicyMetadata(entry);
      links.maybeRelease(entry);
      return;
    }
    long removedWeight = countBounded ? 1L : removedLogicalBytes;
    switch (state) {
      case Entry.POLICY_LRU:
        unlink(lru, entry);
        break;
      case Entry.POLICY_S3_SMALL:
      case Entry.POLICY_S4_SKIP:
        removeSmallWeight(entry, removedWeight);
        unlink(small, entry);
        if (eviction) {
          addGhost(keyHash64, removedWeight, removedAccessCount);
        } else {
          clearGhostEvidence(keyHash64);
        }
        break;
      case Entry.POLICY_S3_MAIN:
        unlink(main, entry);
        mainWeight -= removedWeight;
        break;
      case Entry.POLICY_TINY_WINDOW:
        unlink(window, entry);
        windowWeight -= removedWeight;
        break;
      case Entry.POLICY_TINY_PROBATION:
        advanceTinyCandidate(entry);
        unlink(probation, entry);
        probationWeight -= removedWeight;
        break;
      case Entry.POLICY_TINY_PROTECTED:
        unlink(protectedQueue, entry);
        protectedWeight -= removedWeight;
        break;
      default:
    }
    weightedSize -= removedWeight;
    liveBytes -= removedLogicalBytes;
    clearPolicyMetadata(entry);
    links.maybeRelease(entry);
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
    switch (stateOf(entry)) {
      case Entry.POLICY_LRU:
        lru.moveToHead(entry);
        break;
      case Entry.POLICY_S3_SMALL:
      case Entry.POLICY_S4_SKIP:
        moveSmallToHeadAsSkip(entry);
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

  long sketchBytes() {
    return sketch == null ? 0L : sketch.bytes();
  }

  long ghostNativeBytes() {
    return ghost == null ? 0L : ghost.nativeBytes();
  }

  long ghostAllocationTrims() {
    return ghostAllocationTrims;
  }

  long ghostAllocationDrops() {
    return ghostAllocationDrops;
  }

  long skipSuppressedAccesses() {
    return skipSuppressedAccesses;
  }

  long ghostDeferredPromotions() {
    return ghostDeferredPromotions;
  }

  boolean ghostRehashPending() {
    return ghost != null && ghost.rehashPending();
  }

  int advanceGhostRehash(int budget) {
    return ghost == null ? 0 : ghost.advanceRehash(budget);
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
        if (accessCountOf(candidate) >= S3_SMALL_PROMOTION_HITS) {
          long candidateWeight = weightOf(candidate);
          promoteSmallToMain(candidate, candidateWeight);
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
      int accessCount = accessCountOf(candidate);
      if (accessCount > 0) {
        setPolicyAccessCount(candidate, accessCount - 1);
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
    if (candidate != null && stateOf(candidate) != Entry.POLICY_TINY_PROBATION) {
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
    long candidateHash = keyHashOf(candidate);
    long victimHash = keyHashOf(victim);
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
    long weight = weightOf(candidate);
    unlink(window, candidate);
    windowWeight -= weight;
    link(probation, candidate, Entry.POLICY_TINY_PROBATION);
    probationWeight += weight;
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
      long weight = weightOf(candidate);
      unlink(protectedQueue, candidate);
      protectedWeight -= weight;
      link(probation, candidate, Entry.POLICY_TINY_PROBATION);
      probationWeight += weight;
    }
  }

  /** Candidate traversal is oldest-to-newest, opposite to the intrusive deque's head links. */
  private void advanceTinyCandidate(Entry entry) {
    if (entry == tinyCandidate) {
      tinyCandidate = links.policyPrevEntry(entry);
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

  private void updateWeight(Entry entry, long valueAllocation) {
    long updatedBytes = normalizedByteWeight(
        CacheMath.logicalEntryBytes(entry.keyAllocationLength(), valueAllocation));
    long oldBytes = byteWeightOf(entry);
    long updatedWeight = countBounded ? 1L : updatedBytes;
    long oldWeight = countBounded ? 1L : oldBytes;
    long delta = updatedWeight - oldWeight;
    long byteDelta = updatedBytes - oldBytes;
    if (delta == 0L && byteDelta == 0L) {
      return;
    }
    weightedSize += delta;
    liveBytes += byteDelta;
    int state = stateOf(entry);
    switch (state) {
      case Entry.POLICY_S3_SMALL:
      case Entry.POLICY_S4_SKIP:
        smallWeight += delta;
        if (state == Entry.POLICY_S4_SKIP) {
          smallSkipWeight += delta;
        }
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
    setPolicyByteWeight(entry, updatedBytes);
    if (state == Entry.POLICY_S4_SKIP) {
      trimSmallSkip();
    }
  }

  private void addGhost(long fingerprint, long weight, int smallHits) {
    boolean inserted = ghost.refresh(fingerprint, weight, smallHits, ghostRecord);
    int frequency = ghostRecord.found ? Math.max(ghostRecord.frequency, smallHits) : smallHits;
    if (ghostRecord.found) {
      ghostWeight -= ghostRecord.weight;
    }
    for (int attempt = 0; !inserted && attempt < GHOST_INSERT_TRIM_ATTEMPTS; attempt++) {
      long removed = ghost.removeFirst();
      if (removed == Long.MIN_VALUE) {
        break;
      }
      ghostWeight -= removed;
      ghostAllocationTrims++;
      inserted = ghost.put(fingerprint, weight, frequency);
    }
    if (!inserted) {
      ghostAllocationDrops++;
      return;
    }
    ghostWeight += weight;
    if (ghostWeight > ghostMaximum) {
      while (ghostWeight > ghostMaximum && !ghost.isEmpty()) {
        ghostWeight -= ghost.removeFirst();
      }
    }
  }

  private void addSmall(Entry entry, long weight) {
    link(small, entry, Entry.POLICY_S4_SKIP);
    smallWeight += weight;
    smallSkipWeight += weight;
    if (smallSkipTail == null) {
      smallSkipTail = entry;
    }
    trimSmallSkip();
  }

  /** Converts the oldest virtual Skip entries into ordinary Small entries at the fixed 25% mark. */
  private void trimSmallSkip() {
    while (smallSkipWeight > smallSkipMaximum && smallSkipTail != null) {
      Entry candidate = smallSkipTail;
      Entry previous = links.policyPrevEntry(candidate);
      smallSkipTail = previous;
      if (stateOf(candidate) == Entry.POLICY_S4_SKIP) {
        smallSkipWeight -= weightOf(candidate);
        setPolicyState(candidate, Entry.POLICY_S3_SMALL);
      }
    }
  }

  /** Removes an entry from Small accounting while its intrusive links still identify its neighbors. */
  private void removeSmallWeight(Entry entry, long weight) {
    if (stateOf(entry) == Entry.POLICY_S4_SKIP) {
      if (smallSkipTail == entry) {
        smallSkipTail = links.policyPrevEntry(entry);
      }
      smallSkipWeight -= weight;
    }
    smallWeight -= weight;
  }

  /** Writer contention rotates the entry as the newest Small item, therefore back into Skip. */
  private void moveSmallToHeadAsSkip(Entry entry) {
    long weight = weightOf(entry);
    boolean wasSkip = stateOf(entry) == Entry.POLICY_S4_SKIP;
    if (wasSkip) {
      if (smallSkipTail == entry) {
        smallSkipTail = links.policyPrevEntry(entry);
      }
      smallSkipWeight -= weight;
    } else {
      setPolicyAccessCount(entry, 0);
    }
    small.unlink(entry);
    link(small, entry, Entry.POLICY_S4_SKIP);
    smallSkipWeight += weight;
    if (smallSkipTail == null) {
      smallSkipTail = entry;
    }
    trimSmallSkip();
  }

  private void promoteSmallToMain(Entry entry, long weight) {
    removeSmallWeight(entry, weight);
    unlink(small, entry);
    clearGhostEvidence(keyHashOf(entry));
    setPolicyAccessCount(entry, 0);
    link(main, entry, Entry.POLICY_S3_MAIN);
    mainWeight += weight;
  }

  private void clearGhostEvidence(long fingerprint) {
    if (ghost == null) {
      return;
    }
    long removed = ghost.remove(fingerprint);
    if (removed != Long.MIN_VALUE) {
      ghostWeight -= removed;
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

  private long weightOf(Entry entry) {
    return countBounded ? 1L : byteWeightOf(entry);
  }

  private int stateOf(Entry entry) {
    int linkId = entry.policyLinkId();
    return linkId == 0 ? Entry.POLICY_NONE : links.policyState(linkId);
  }

  private int accessCountOf(Entry entry) {
    return links.policyAccessCount(requireLinkId(entry));
  }

  private long keyHashOf(Entry entry) {
    return links.keyHash64(requireLinkId(entry));
  }

  private long byteWeightOf(Entry entry) {
    return links.policyByteWeight(requireLinkId(entry));
  }

  private int requireLinkId(Entry entry) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      throw new IllegalStateException("policy entry has no native link record");
    }
    return linkId;
  }

  /** Publishes policy state to the reader-visible Entry before its actor mirror. */
  private void setPolicyState(Entry entry, int state) {
    int linkId = requireLinkId(entry);
    entry.policyState(state);
    links.policyState(linkId, state);
  }

  /** Publishes policy access count to the reader-visible Entry before its actor mirror. */
  private void setPolicyAccessCount(Entry entry, int count) {
    int linkId = requireLinkId(entry);
    entry.policyAccessCount(count);
    links.policyAccessCount(linkId, count);
  }

  /** Publishes policy weight to the reader-visible Entry before its actor mirror. */
  private void setPolicyByteWeight(Entry entry, long bytes) {
    long normalizedBytes = normalizedByteWeight(bytes);
    int linkId = requireLinkId(entry);
    int storedBytes = (int) normalizedBytes;
    entry.policyByteWeight(storedBytes);
    links.policyByteWeight(linkId, storedBytes);
  }

  private static long normalizedByteWeight(long bytes) {
    if (bytes < 0L) {
      throw new IllegalArgumentException("policy byte weight must fit in a non-negative int");
    }
    return Math.min(bytes, (long) Integer.MAX_VALUE);
  }

  private void clearPolicyMetadata(Entry entry) {
    int linkId = requireLinkId(entry);
    entry.policyState(Entry.POLICY_NONE);
    links.policyState(linkId, Entry.POLICY_NONE);
    entry.policyAccessCount(0);
    links.policyAccessCount(linkId, 0);
    entry.policyByteWeight(0);
    links.policyByteWeight(linkId, 0);
  }

  private static void clearUnlinkedPolicyMetadata(Entry entry) {
    entry.policyState(Entry.POLICY_NONE);
    entry.policyAccessCount(0);
    entry.policyByteWeight(0L);
  }

  private void link(EntryDeque deque, Entry entry, int state) {
    // Publish actor ownership before exposing the entry through either neighbor pointer. The
    // writer admission policy may remove an entry concurrently; EntryLinks must not reclaim its
    // record in the interval where the actor deque is already linking it but policyState is still
    // NONE.
    int linkId;
    synchronized (links) {
      linkId = links.ensure(entry);
      setPolicyState(entry, state);
    }
    deque.linkHead(entry, linkId);
  }

  private static void unlink(EntryDeque deque, Entry entry) {
    deque.unlink(entry);
  }

  private final class EntryDeque {
    Entry head;
    Entry tail;

    void linkHead(Entry entry) {
      linkHead(entry, entry.policyLinkId());
    }

    void linkHead(Entry entry, int linkId) {
      links.linkHead(
          linkId,
          head,
          EntryLinks.POLICY_PREVIOUS_OFFSET,
          EntryLinks.POLICY_NEXT_OFFSET);
      if (head == null) {
        tail = entry;
      }
      head = entry;
    }

    void unlink(Entry entry) {
      unlink(entry, entry.policyLinkId());
    }

    void unlink(Entry entry, int linkId) {
      long neighbors =
          links.unlink(
              linkId,
              EntryLinks.POLICY_PREVIOUS_OFFSET,
              EntryLinks.POLICY_NEXT_OFFSET);
      int previousId = (int) (neighbors >>> 32);
      int nextId = (int) neighbors;
      if (previousId == 0) {
        head = links.entry(nextId);
      }
      if (nextId == 0) {
        tail = links.entry(previousId);
      }
    }

    void moveToHead(Entry entry) {
      if (head == entry) {
        return;
      }
      unlink(entry);
      linkHead(entry);
    }
  }

}
