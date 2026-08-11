package com.red.ohc.index;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;

/** The one Java metadata object stored in the authoritative CHM as both key and value. */
public final class Entry {
  private static final long VALUE_TAG_MASK = 7L;
  private static final long VALUE_HAS_TTL = 1L;

  public static final int PENDING_ADD = 1;
  public static final int PENDING_UPDATE = 1 << 1;
  public static final int PENDING_REMOVE = 1 << 2;

  /** Returned to the maintenance actor when a reliable removal owns the pending claim. */
  public static final int PENDING_BUSY = -1;

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
  private static final int PENDING_CLAIM_SHIFT = 6;
  private static final int PENDING_CLAIM_MASK = PENDING_MASK << PENDING_CLAIM_SHIFT;
  private static final int PENDING_ROLLOVER = 1 << 9;
  private static final int PENDING_ROLLOVER_CLAIMED = 1 << 10;
  private static final int TIMER_UNSCHEDULED = -1;
  private static final int TIMER_HEAP_BASE = -2;
  private static final int TIMER_SLOT_BITS = 10;
  private static final int TIMER_SLOT_MASK = (1 << TIMER_SLOT_BITS) - 1;
  private static final int POLICY_STATE_SHIFT = 0;
  private static final int POLICY_ACCESS_SHIFT = 3;
  private static final long MUTATION_VERSION_SHIFT = 32L;
  private static final long VERSION_MASK = 0xffffffffL;
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
  private static final AtomicIntegerFieldUpdater<Entry> POLICY =
      AtomicIntegerFieldUpdater.newUpdater(Entry.class, "policyMeta");
  private static final AtomicLongFieldUpdater<Entry> MAINTENANCE =
      AtomicLongFieldUpdater.newUpdater(Entry.class, "maintenanceMeta");
  private static final AtomicReferenceFieldUpdater<Entry, ValueState> VALUE_STATE =
      AtomicReferenceFieldUpdater.newUpdater(Entry.class, ValueState.class, "valueState");
  private static final ValueState EMPTY_VALUE_STATE = new ValueState(0L, null);

  public final long nativeKeyAddress;

  /** High 32 bits are serialized key length; low 32 bits are the CHM hash. */
  public final long keyMeta;

  /** Eight-byte aligned native value address; bit 0 marks a TTL-bearing value. */
  public volatile long valueAddress;

  /** Optional weak Java value bound to one immutable publication of the native value. */
  private volatile ValueState valueState;

  public volatile long lifecycle;
  public Entry policyPrev;
  public Entry policyNext;
  public Entry timerPrev;
  public Entry timerNext;

  /** Packed timer location: wheel level/slot, or a negative overflow-heap index. */
  public int timerLocation = TIMER_UNSCHEDULED;

  public long timerDeadlineTick;

  /** Actor-owned fields: readers and writers never mutate policy/timer links or counters. */
  /** Actor-owned intrusive policy metadata. */
  private volatile int policyMeta;

  /** High 32 bits are the latest published mutation; low 32 bits are the applied version. */
  public volatile long maintenanceMeta;

  public long policyWeight;
  public long policyByteWeight;
  public volatile int pendingFlags;

  public Entry(long nativeKeyAddress, int keyLength, int chmHash, long valueAddress) {
    this(nativeKeyAddress, keyLength, chmHash, chmHash & 0xffffffffL, valueAddress);
  }

  public Entry(
      long nativeKeyAddress, int keyLength, int chmHash, long keyHash64, long valueAddress) {
    this.nativeKeyAddress = nativeKeyAddress;
    this.keyMeta = packKeyMeta(chmHash, keyLength);
    // Zero-address Entries exist only in policy/timer unit tests. Production Entries retain
    // the complete hash in their native key-block prefix without growing this hot object.
    if (nativeKeyAddress == 0L) {
      this.timerDeadlineTick = keyHash64;
    }
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

  public int keyHash() {
    return keyHash(keyMeta);
  }

  public long keyHash64() {
    return nativeKeyAddress == 0L ? timerDeadlineTick : NativeMemory.getLong(nativeKeyAddress);
  }

  public int keyLength() {
    return keyLength(keyMeta);
  }

  public long nativeKeyAddress() {
    return nativeKeyAddress;
  }

  public long nativeKeyBytesAddress() {
    return nativeKeyAddress + Long.BYTES;
  }

  public long keyAllocationLength() {
    return Math.max(8L, CacheMath.roundUpTo8((long) keyLength() + Long.BYTES));
  }

  public long rawValueAddress() {
    return rawValueAddress(valueAddress);
  }

  public WeakValueSlot weakValueSlot() {
    ValueState state = valueState;
    return state == null ? null : state.weakValue;
  }

  public boolean compareAndSetWeakValueSlot(WeakValueSlot expected, WeakValueSlot update) {
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      ValueState current = valueState;
      WeakValueSlot currentValue = current == null ? null : current.weakValue;
      if (currentValue != expected) {
        return false;
      }
      ValueState next =
          current == null
              ? new ValueState(valueAddress, update)
              : new ValueState(current.taggedValueAddress, update);
      if (VALUE_STATE.compareAndSet(this, current, next)) {
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  public void clearWeakValueSlot() {
    ValueState current = valueState;
    while (current != null) {
      ValueState next = new ValueState(current.taggedValueAddress, null);
      if (valueAddress != current.taggedValueAddress) {
        return;
      }
      if (VALUE_STATE.compareAndSet(this, current, next)) {
        return;
      }
      current = valueState;
    }
  }

  public void setWeakValueSlot(WeakValueSlot slot) {
    if (slot == null && valueState == null) {
      return;
    }
    long taggedValueAddress = valueAddress;
    if (taggedValueAddress == 0L && slot != null) {
      throw new IllegalArgumentException("weak value requires a native value address");
    }
    valueState = new ValueState(taggedValueAddress, slot);
  }

  /** Initializes weak-value publication metadata without allocating it for disabled caches. */
  public void initializeValueState(long taggedValueAddress, WeakValueSlot slot) {
    valueState = new ValueState(taggedValueAddress, slot);
  }

  /** Publishes a new native value and its weak-value binding as one immutable logical state. */
  public void publishValueState(long taggedValueAddress, WeakValueSlot slot) {
    ValueState next = new ValueState(taggedValueAddress, slot);
    valueAddress = taggedValueAddress;
    valueState = next;
  }

  /** Publishes a native value while preserving the no-metadata fast path for strong caches. */
  public void publishValue(long taggedValueAddress, WeakValueSlot slot) {
    if (valueState == null && slot == null) {
      valueAddress = taggedValueAddress;
      return;
    }
    publishValueState(taggedValueAddress, slot);
  }

  /** Clears the published value before native retirement, if weak-value state is enabled. */
  public void clearValueState() {
    if (valueState != null) {
      valueAddress = 0L;
      valueState = EMPTY_VALUE_STATE;
      return;
    }
    valueAddress = 0L;
  }

  /** Clears the native value and weak binding, preserving the strong-cache no-metadata path. */
  public void clearValue() {
    if (valueState != null) {
      clearValueState();
    } else {
      valueAddress = 0L;
    }
  }

  public ValueState valueState() {
    return valueState;
  }

  public boolean compareAndSetValueState(ValueState expected, ValueState update) {
    if (expected == null || update == null) {
      return false;
    }
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      if (valueState != expected || valueAddress != expected.taggedValueAddress) {
        return false;
      }
      if (VALUE_STATE.compareAndSet(this, expected, update)) {
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  /** Immutable token joining a native value address and its optional weak Java object. */
  public static final class ValueState {
    private final long taggedValueAddress;
    private final WeakValueSlot weakValue;

    public ValueState(long taggedValueAddress, WeakValueSlot weakValue) {
      if (taggedValueAddress == 0L && weakValue != null) {
        throw new IllegalArgumentException("weak value requires a native value address");
      }
      this.taggedValueAddress = taggedValueAddress;
      this.weakValue = weakValue;
    }

    public long taggedValueAddress() {
      return taggedValueAddress;
    }

    public WeakValueSlot weakValue() {
      return weakValue;
    }
  }

  /** Weak value metadata bound to one exact tagged native value address. */
  public static final class WeakValueSlot {
    private final WeakReference<Object> reference;
    private final long taggedValueAddress;

    public WeakValueSlot(Object value, long taggedValueAddress) {
      if (value == null) {
        throw new NullPointerException("value");
      }
      if (taggedValueAddress == 0L) {
        throw new IllegalArgumentException("weak value requires a native value address");
      }
      this.reference = new WeakReference<>(value);
      this.taggedValueAddress = taggedValueAddress;
    }

    public Object get() {
      return reference.get();
    }

    public long taggedValueAddress() {
      return taggedValueAddress;
    }
  }

  public static long tagValueAddress(long rawAddress, boolean hasTtl) {
    if (rawAddress == 0L) {
      return 0L;
    }
    if ((rawAddress & VALUE_TAG_MASK) != 0L) {
      throw new IllegalArgumentException(
          "native value address must be 8-byte aligned: " + rawAddress);
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
    if (this == other) {
      return true;
    }
    if (!(other instanceof Entry)) {
      return false;
    }
    Entry entry = (Entry) other;
    if (keyMeta != entry.keyMeta || keyHash64() != entry.keyHash64()) {
      return false;
    }
    return keyLength() == 0
        || NativeMemory.equals(nativeKeyBytesAddress(), entry.nativeKeyBytesAddress(), keyLength());
  }

  public boolean claimWriter() {
    long current = lifecycle;
    return (current & (WRITER_LOCK | STATE_MASK)) == ALIVE
        && LIFECYCLE.compareAndSet(this, current, current | WRITER_LOCK);
  }

  public void finishWriter() {
    long current = lifecycle;
    if ((current & WRITER_LOCK) == 0L) {
      throw new IllegalStateException("writer lock is not held");
    }
    long generation = (current & GENERATION_MASK) + 1L;
    if (generation > GENERATION_MASK) {
      generation = 1L;
    }
    LIFECYCLE.lazySet(this, (current & STATE_MASK) | generation);
  }

  public long generation() {
    return lifecycle & GENERATION_MASK;
  }

  public boolean isAlive() {
    return (lifecycle & STATE_MASK) == ALIVE;
  }

  /** The maintenance actor must not consume a coalesced mutation during a writer publication. */
  public boolean isWriterLocked() {
    return (lifecycle & WRITER_LOCK) != 0L;
  }

  /** Must be called while holding the per-entry writer mutex. */
  public void markRetired() {
    long current = lifecycle;
    if ((current & WRITER_LOCK) == 0L) {
      throw new IllegalStateException("writer lock is not held");
    }
    LIFECYCLE.lazySet(this, (current & (WRITER_LOCK | GENERATION_MASK)) | RETIRED);
  }

  /** Restores ALIVE after a failed conditional CHM remove while still holding the writer mutex. */
  public void restoreAlive() {
    long current = lifecycle;
    if ((current & WRITER_LOCK) == 0L) {
      throw new IllegalStateException("writer lock is not held");
    }
    LIFECYCLE.lazySet(this, current & (WRITER_LOCK | GENERATION_MASK));
  }

  /** Used only for a private candidate that lost putIfAbsent before publication. */
  public void markDead() {
    long current = lifecycle;
    if ((current & WRITER_LOCK) != 0L) {
      throw new IllegalStateException("writer lock is held");
    }
    LIFECYCLE.lazySet(this, (current & GENERATION_MASK) | DEAD);
  }

  public boolean timerScheduled() {
    return timerLocation != TIMER_UNSCHEDULED;
  }

  public void timerScheduled(boolean scheduled) {
    if (!scheduled) {
      timerLocation = TIMER_UNSCHEDULED;
    }
  }

  /** A negative timer level encodes the worker-owned overflow heap index. */
  public boolean timerInOverflowHeap() {
    return timerLocation <= TIMER_HEAP_BASE;
  }

  public int timerHeapIndex() {
    return timerInOverflowHeap() ? -timerLocation - 2 : -1;
  }

  public void timerHeapIndex(int index) {
    if (index < 0) {
      timerLocation = TIMER_HEAP_BASE;
    } else {
      timerLocation = -index - 2;
    }
  }

  public int timerLevel() {
    return timerLocation >= 0 ? timerLocation >>> TIMER_SLOT_BITS : -1;
  }

  public void timerLevel(int level) {
    timerLocation = (level << TIMER_SLOT_BITS) | (timerLocation & TIMER_SLOT_MASK);
  }

  public int timerSlot() {
    return timerLocation & TIMER_SLOT_MASK;
  }

  public void timerSlot(int slot) {
    timerLocation = (timerLocation & ~TIMER_SLOT_MASK) | (slot & TIMER_SLOT_MASK);
  }

  /** Attempts a reliable-removal claim once; callers must not wait for another producer. */
  public boolean tryBeginPending(int flags) {
    if (flags != PENDING_REMOVE) {
      throw new IllegalArgumentException("invalid pending flags: " + flags);
    }
    int current = pendingFlags;
    return (current & PENDING_CLAIMED) == 0
        && PENDING.compareAndSet(
            this, current, current | PENDING_CLAIMED | (flags << PENDING_CLAIM_SHIFT));
  }

  /** Releases a successful reliable removal claim after its retirement records are visible. */
  public void completePendingClaim() {
    if ((pendingFlags & PENDING_CLAIMED) == 0) {
      throw new IllegalStateException("missing pending claim");
    }
    tryIncrementMutationVersion();
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_CLAIMED) == 0) {
        throw new IllegalStateException("pending claim was lost");
      }
      int currentFlags = (current & PENDING_CLAIM_MASK) >>> PENDING_CLAIM_SHIFT;
      if (PENDING.compareAndSet(
          this, current, (current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK)) | currentFlags)) {
        return;
      }
    }
  }

  /** Published after a writer has made its CHM/value mutation visible. */
  public long mutationVersion() {
    return (maintenanceMeta >>> MUTATION_VERSION_SHIFT) & VERSION_MASK;
  }

  /** Published by the maintenance worker after applying the corresponding mutation. */
  public long appliedVersion() {
    return maintenanceMeta & VERSION_MASK;
  }

  /** Best-effort version publication for producer paths; it never waits for the actor. */
  private boolean tryIncrementMutationVersion() {
    int flags = pendingFlags;
    if ((flags & (PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED)) != 0) {
      return false;
    }
    long current = maintenanceMeta;
    long currentVersion = current >>> MUTATION_VERSION_SHIFT;
    if (currentVersion == VERSION_MASK) {
      int nextFlags = flags | PENDING_ROLLOVER;
      if ((flags & PENDING_ROLLOVER) == 0
          && (flags & PENDING_ROLLOVER_CLAIMED) == 0) {
        PENDING.compareAndSet(this, flags, nextFlags);
      }
      return false;
    }
    long next = (current & VERSION_MASK) | ((currentVersion + 1L) << MUTATION_VERSION_SHIFT);
    return MAINTENANCE.compareAndSet(this, current, next);
  }

  /** Clears a fully applied final version. Called by the single maintenance owner. */
  public boolean tryRolloverMaintenanceVersion() {
    if (isWriterLocked()) {
      return false;
    }
    int currentFlags = pendingFlags;
    if ((currentFlags
                & (PENDING_ROLLOVER
                    | PENDING_MASK
                    | PENDING_CLAIMED
                    | PENDING_QUEUED
                    | PENDING_REPAIR))
            != PENDING_ROLLOVER
        || !PENDING.compareAndSet(
            this, PENDING_ROLLOVER, PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED)) {
      return false;
    }
    long current = maintenanceMeta;
    if ((current >>> MUTATION_VERSION_SHIFT) != VERSION_MASK
        || (current & VERSION_MASK) != VERSION_MASK) {
      PENDING.compareAndSet(this, PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED, PENDING_ROLLOVER);
      return false;
    }
    if (!MAINTENANCE.compareAndSet(this, current, 0L)) {
      PENDING.compareAndSet(this, PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED, PENDING_ROLLOVER);
      return false;
    }
    if (!PENDING.compareAndSet(this, PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED, 0)) {
      throw new IllegalStateException("maintenance rollover fence was modified");
    }
    return true;
  }

  /** Marks a maintenance snapshot applied only when no newer writer publication exists. */
  public boolean markAppliedVersion(long version) {
    long expected = version & VERSION_MASK;
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      long current = maintenanceMeta;
      if ((current >>> MUTATION_VERSION_SHIFT) != expected) {
        return false;
      }
      long next = (current & ~VERSION_MASK) | expected;
      if (MAINTENANCE.compareAndSet(this, current, next)) {
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  /**
   * Publishes an ADD/UPDATE maintenance hint after the authoritative CHM/value mutation is visible.
   * It never claims the writer mutex and never waits on another producer.
   *
   * @return true for the producer that must submit this Entry to the transport queue
   */
  public boolean publishMutation(int flags) {
    if ((flags & ~(PENDING_ADD | PENDING_UPDATE)) != 0 || flags == 0) {
      throw new IllegalArgumentException("invalid advisory mutation flags: " + flags);
    }
    tryIncrementMutationVersion();
    while (true) {
      int current = pendingFlags;
      int next = current | flags;
      if ((current & PENDING_QUEUED) != 0) {
        if (PENDING.compareAndSet(this, current, next)) {
          return false;
        }
        continue;
      }
      if (PENDING.compareAndSet(this, current, next | PENDING_QUEUED)) {
        return true;
      }
    }
  }

  /** Re-queues an already published mutation when a worker observed an older version. */
  public boolean requeueMutation(int flags) {
    if ((flags & ~(PENDING_ADD | PENDING_UPDATE)) != 0 || flags == 0) {
      throw new IllegalArgumentException("invalid advisory mutation flags: " + flags);
    }
    while (true) {
      int current = pendingFlags;
      int next = current | flags;
      if ((current & PENDING_QUEUED) != 0) {
        if (PENDING.compareAndSet(this, current, next)) {
          return false;
        }
        continue;
      }
      if (PENDING.compareAndSet(this, current, next | PENDING_QUEUED)) {
        return true;
      }
    }
  }

  /** Cancels a reliable removal claim before its associated mapping becomes invisible. */
  public boolean cancelPendingClaim() {
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_CLAIMED) == 0) {
        throw new IllegalStateException("missing pending claim");
      }
      if (PENDING.compareAndSet(this, current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
        return true;
      }
    }
  }

  public boolean cancelPendingClaimIfPresent() {
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_CLAIMED) == 0) {
        return false;
      }
      if (PENDING.compareAndSet(this, current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
        return true;
      }
    }
  }

  /** Clears the transport marker when an advisory queue offer was not accepted. */
  public boolean queueOfferFailed() {
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_QUEUED) == 0) {
        return false;
      }
      if ((current & PENDING_REPAIR) != 0) {
        if (PENDING.compareAndSet(this, current, current & ~PENDING_QUEUED)) {
          return false;
        }
      } else {
        if (PENDING.compareAndSet(this, current, (current & ~PENDING_QUEUED) | PENDING_REPAIR)) {
          return true;
        }
      }
    }
  }

  public boolean isPendingQueued() {
    return (pendingFlags & PENDING_QUEUED) != 0;
  }

  public boolean clearRepairMarker() {
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_REPAIR) == 0) {
        return false;
      }
      if (PENDING.compareAndSet(this, current, current & ~PENDING_REPAIR)) {
        return true;
      }
    }
  }

  public boolean isRepairMarked() {
    return (pendingFlags & PENDING_REPAIR) != 0;
  }

  public int takePending() {
    while (true) {
      int current = pendingFlags;
      if ((current & PENDING_CLAIMED) != 0) {
        return PENDING_BUSY;
      }
      int next = current & ~(PENDING_MASK | PENDING_QUEUED);
      if (PENDING.compareAndSet(this, current, next)) {
        return current & PENDING_MASK;
      }
    }
  }

  public int policyState() {
    return (policyMeta >>> POLICY_STATE_SHIFT) & 7;
  }

  public void policyState(int state) {
    while (true) {
      int current = policyMeta;
      int next = (current & ~7) | (state & 7);
      if (POLICY.compareAndSet(this, current, next)) {
        return;
      }
    }
  }

  public int policyAccessCount() {
    return (policyMeta >>> POLICY_ACCESS_SHIFT) & 3;
  }

  public void policyAccessCount(int count) {
    int value = Math.max(0, Math.min(3, count));
    while (true) {
      int current = policyMeta;
      int next = (current & ~0x18) | (value << POLICY_ACCESS_SHIFT);
      if (POLICY.compareAndSet(this, current, next)) {
        return;
      }
    }
  }
}
