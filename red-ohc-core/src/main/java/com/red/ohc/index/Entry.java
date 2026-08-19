package com.red.ohc.index;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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
  /** Heap-resident, volatile hint used by the business writer to avoid reading policy metadata. */
  private static final int POLICY_PRESENT = 1 << 11;
  private static final int TIMER_UNSCHEDULED = -1;
  private static final int TIMER_HEAP_BASE = -2;
  private static final int TIMER_SLOT_BITS = 10;
  private static final int TIMER_SLOT_MASK = (1 << TIMER_SLOT_BITS) - 1;
  private static final int POLICY_STATE_SHIFT = 0;
  private static final int POLICY_ACCESS_SHIFT = 3;
  private static final long POLICY_META_OFFSET = 0L;
  private static final long TIMER_LOCATION_OFFSET = 4L;
  private static final long TIMER_DEADLINE_OFFSET = 8L;
  private static final long MAINTENANCE_META_OFFSET = 16L;
  private static final long POLICY_BYTE_WEIGHT_OFFSET = 24L;
  public static final long NATIVE_METADATA_BYTES = 32L;
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

  public volatile int pendingFlags;

  public Entry(long nativeKeyAddress, int keyLength, int chmHash, long valueAddress) {
    this(nativeKeyAddress, keyLength, chmHash, chmHash & 0xffffffffL, valueAddress);
  }

  public Entry(
      long nativeKeyAddress, int keyLength, int chmHash, long keyHash64, long valueAddress) {
    this.nativeKeyAddress = nativeKeyAddress;
    this.keyMeta = packKeyMeta(chmHash, keyLength);
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
    return nativeKeyAddress == 0L ? 0L : NativeMemory.getLong(nativeKeyAddress);
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

  public static long keyDataAllocationLength(int keyLength) {
    if (keyLength < 0) {
      throw new IllegalArgumentException("keyLength must be non-negative");
    }
    return Math.max(8L, CacheMath.roundUpTo8((long) keyLength + Long.BYTES));
  }

  public static long keyAllocationLengthForKeyLength(int keyLength) {
    return keyDataAllocationLength(keyLength) + NATIVE_METADATA_BYTES;
  }

  public long keyAllocationLength() {
    return keyAllocationLengthForKeyLength(keyLength());
  }

  public long rawValueAddress() {
    return rawValueAddress(valueAddress);
  }

  private long nativeMetadataAddress() {
    if (nativeKeyAddress == 0L) {
      throw new IllegalStateException("Entry has no native key block");
    }
    return nativeKeyAddress + keyDataAllocationLength(keyLength());
  }

  public void initializeNativeMetadata() {
    long metadata = nativeMetadataAddress();
    NativeMemory.setMemory(metadata, NATIVE_METADATA_BYTES, (byte) 0);
    NativeMemory.putInt(metadata + TIMER_LOCATION_OFFSET, TIMER_UNSCHEDULED);
  }

  /** Immutable token joining a native value address and its optional weak Java object. */
  public static final class ValueState {
    private static final long FINGERPRINT_UNINITIALIZED = 0L;
    private static final long FINGERPRINT_DISABLED = 1L;
    private static final long FINGERPRINT_READY_MARKER = 2L;
    private static final AtomicLongFieldUpdater<ValueState> FINGERPRINT_STATE =
        AtomicLongFieldUpdater.newUpdater(ValueState.class, "fingerprintState");

    private final long taggedValueAddress;
    private final WeakValueSlot weakValue;
    private final ValueState publication;
    private volatile long fingerprintState;

    public ValueState(long taggedValueAddress, WeakValueSlot weakValue) {
      if (taggedValueAddress == 0L && weakValue != null) {
        throw new IllegalArgumentException("weak value requires a native value address");
      }
      this.taggedValueAddress = taggedValueAddress;
      this.weakValue = weakValue;
      this.publication = this;
      this.fingerprintState = FINGERPRINT_UNINITIALIZED;
    }

    private ValueState(long taggedValueAddress, WeakValueSlot weakValue, ValueState publication) {
      if (taggedValueAddress == 0L && weakValue != null) {
        throw new IllegalArgumentException("weak value requires a native value address");
      }
      this.taggedValueAddress = taggedValueAddress;
      this.weakValue = weakValue;
      this.publication = publication;
      this.fingerprintState = FINGERPRINT_UNINITIALIZED;
    }

    public long taggedValueAddress() {
      return taggedValueAddress;
    }

    public WeakValueSlot weakValue() {
      return weakValue;
    }

    public ValueState publication() {
      return publication;
    }

    public ValueState withWeakValue(WeakValueSlot replacement) {
      return new ValueState(taggedValueAddress, replacement, publication);
    }

    public long fingerprintState() {
      return publication.fingerprintState;
    }

    public boolean fingerprintReady() {
      return (publication.fingerprintState & 0xffffffffL) == FINGERPRINT_READY_MARKER;
    }

    public boolean fingerprintDisabled() {
      return publication.fingerprintState == FINGERPRINT_DISABLED;
    }

    public long fingerprint() {
      if (!fingerprintReady()) {
        throw new IllegalStateException("fingerprint is not ready");
      }
      return publication.fingerprintState >>> 32;
    }

    public boolean tryPublishFingerprint(long fingerprint) {
      long packed = (fingerprint << 32) | FINGERPRINT_READY_MARKER;
      return FINGERPRINT_STATE.compareAndSet(
          publication, FINGERPRINT_UNINITIALIZED, packed);
    }

    void disableFingerprint() {
      for (; ; ) {
        long current = publication.fingerprintState;
        if (current == FINGERPRINT_DISABLED
            || FINGERPRINT_STATE.compareAndSet(publication, current, FINGERPRINT_DISABLED)) {
          return;
        }
      }
    }
  }

  /** Weak value metadata bound to one exact tagged native value address. */
  public static final class WeakValueSlot extends WeakReference<Object> {
    private final Entry owner;
    private final long taggedValueAddress;

    public WeakValueSlot(Object value, long taggedValueAddress) {
      this(value, taggedValueAddress, null, null);
    }

    public WeakValueSlot(
        Object value,
        long taggedValueAddress,
        Entry owner,
        ReferenceQueue<Object> queue) {
      super(value, queue);
      if (value == null) {
        throw new NullPointerException("value");
      }
      if (taggedValueAddress == 0L) {
        throw new IllegalArgumentException("weak value requires a native value address");
      }
      this.owner = owner;
      this.taggedValueAddress = taggedValueAddress;
    }

    public Entry owner() {
      return owner;
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
    // keyMeta already fuses chmHash + keyLength, so it rejects both spread collisions and
    // length mismatches from the heap cache line. The hash64 native read is dropped for the
    // same reason as LookupKey.equals: a hit always matches it, and the memcmp below is
    // authoritative while reading the same native key block.
    if (keyMeta != entry.keyMeta) {
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
    return timerLocation() != TIMER_UNSCHEDULED;
  }

  public void timerScheduled(boolean scheduled) {
    if (!scheduled) {
      timerLocation(TIMER_UNSCHEDULED);
    }
  }

  /** A negative timer level encodes the worker-owned overflow heap index. */
  public boolean timerInOverflowHeap() {
    return timerLocation() <= TIMER_HEAP_BASE;
  }

  public int timerHeapIndex() {
    int location = timerLocation();
    return location <= TIMER_HEAP_BASE ? -location - 2 : -1;
  }

  public void timerHeapIndex(int index) {
    if (index < 0) {
      timerLocation(TIMER_HEAP_BASE);
    } else {
      timerLocation(-index - 2);
    }
  }

  public int timerLevel() {
    int location = timerLocation();
    return location >= 0 ? location >>> TIMER_SLOT_BITS : -1;
  }

  public void timerLevel(int level) {
    int location = timerLocation();
    timerLocation((level << TIMER_SLOT_BITS) | (location & TIMER_SLOT_MASK));
  }

  public int timerSlot() {
    return timerLocation() & TIMER_SLOT_MASK;
  }

  public void timerSlot(int slot) {
    int location = timerLocation();
    timerLocation((location & ~TIMER_SLOT_MASK) | (slot & TIMER_SLOT_MASK));
  }

  public long timerDeadlineTick() {
    return NativeMemory.getLong(nativeMetadataAddress() + TIMER_DEADLINE_OFFSET);
  }

  public void timerDeadlineTick(long tick) {
    NativeMemory.putLong(nativeMetadataAddress() + TIMER_DEADLINE_OFFSET, tick);
  }

  private int timerLocation() {
    return NativeMemory.getInt(nativeMetadataAddress() + TIMER_LOCATION_OFFSET);
  }

  private void timerLocation(int location) {
    NativeMemory.putInt(nativeMetadataAddress() + TIMER_LOCATION_OFFSET, location);
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
    return (maintenanceMeta() >>> MUTATION_VERSION_SHIFT) & VERSION_MASK;
  }

  /** Published by the maintenance worker after applying the corresponding mutation. */
  public long appliedVersion() {
    return maintenanceMeta() & VERSION_MASK;
  }

  /** Best-effort version publication for producer paths; it never waits for the actor. */
  private boolean tryIncrementMutationVersion() {
    int flags = pendingFlags;
    if ((flags & (PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED)) != 0) {
      return false;
    }
    long current = maintenanceMeta();
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
    return NativeMemory.compareAndSwapLong(
        nativeMetadataAddress() + MAINTENANCE_META_OFFSET, current, next);
  }

  /** Clears a fully applied final version. Called by the single maintenance owner. */
  public boolean tryRolloverMaintenanceVersion() {
    if (isWriterLocked()) {
      return false;
    }
    int currentFlags = pendingFlags;
    int rolloverFlags =
        PENDING_ROLLOVER | PENDING_MASK | PENDING_CLAIMED | PENDING_QUEUED | PENDING_REPAIR;
    if ((currentFlags & rolloverFlags) != PENDING_ROLLOVER) {
      return false;
    }
    int claimedFlags = currentFlags | PENDING_ROLLOVER_CLAIMED;
    if (!PENDING.compareAndSet(this, currentFlags, claimedFlags)) {
      return false;
    }
    long current = maintenanceMeta();
    if ((current >>> MUTATION_VERSION_SHIFT) != VERSION_MASK
        || (current & VERSION_MASK) != VERSION_MASK) {
      PENDING.compareAndSet(this, claimedFlags, currentFlags);
      return false;
    }
    if (!NativeMemory.compareAndSwapLong(
        nativeMetadataAddress() + MAINTENANCE_META_OFFSET, current, 0L)) {
      PENDING.compareAndSet(this, claimedFlags, currentFlags);
      return false;
    }
    if (!PENDING.compareAndSet(this, claimedFlags, currentFlags & ~PENDING_ROLLOVER)) {
      throw new IllegalStateException("maintenance rollover fence was modified");
    }
    return true;
  }

  /** Marks a maintenance snapshot applied only when no newer writer publication exists. */
  public boolean markAppliedVersion(long version) {
    long expected = version & VERSION_MASK;
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      long current = maintenanceMeta();
      if ((current >>> MUTATION_VERSION_SHIFT) != expected) {
        return false;
      }
      long next = (current & ~VERSION_MASK) | expected;
      if (NativeMemory.compareAndSwapLong(
          nativeMetadataAddress() + MAINTENANCE_META_OFFSET, current, next)) {
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
    return (policyMeta() >>> POLICY_STATE_SHIFT) & 7;
  }

  public void policyState(int state) {
    int normalizedState = state & 7;
    int current = policyMeta();
    NativeMemory.putInt(
        nativeMetadataAddress() + POLICY_META_OFFSET, (current & ~7) | normalizedState);
    setPolicyPresent(normalizedState != POLICY_NONE);
  }

  /** True when the maintenance actor has this entry linked into an eviction policy. */
  public boolean policyPresent() {
    return (pendingFlags & POLICY_PRESENT) != 0;
  }

  public int policyAccessCount() {
    return (policyMeta() >>> POLICY_ACCESS_SHIFT) & 3;
  }

  public void policyAccessCount(int count) {
    int value = Math.max(0, Math.min(3, count));
    int current = policyMeta();
    NativeMemory.putInt(
        nativeMetadataAddress() + POLICY_META_OFFSET, (current & ~0x18) | (value << POLICY_ACCESS_SHIFT));
  }

  public long policyByteWeight() {
    return NativeMemory.getLong(nativeMetadataAddress() + POLICY_BYTE_WEIGHT_OFFSET);
  }

  public void policyByteWeight(long bytes) {
    NativeMemory.putLong(nativeMetadataAddress() + POLICY_BYTE_WEIGHT_OFFSET, bytes);
  }

  private int policyMeta() {
    return NativeMemory.getInt(nativeMetadataAddress() + POLICY_META_OFFSET);
  }

  private void setPolicyPresent(boolean present) {
    while (true) {
      int current = pendingFlags;
      int next = present ? current | POLICY_PRESENT : current & ~POLICY_PRESENT;
      if (current == next || PENDING.compareAndSet(this, current, next)) {
        return;
      }
    }
  }

  private long maintenanceMeta() {
    return NativeMemory.getLongVolatile(nativeMetadataAddress() + MAINTENANCE_META_OFFSET);
  }
}
