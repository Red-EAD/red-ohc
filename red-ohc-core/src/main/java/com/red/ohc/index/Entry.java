package com.red.ohc.index;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;

/** The one Java metadata object stored in the authoritative CHM as both key and value. */
public final class Entry {
  private static final long VALUE_TAG_MASK = 7L;
  private static final long VALUE_HAS_TTL = 1L;
  private static final long VALUE_RETIRED = 1L << 1;
  private static final long VALUE_DEAD = 1L << 2;
  private static final long VALUE_LIFECYCLE_MASK = VALUE_RETIRED | VALUE_DEAD;

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
  /** Virtual S4-FIFO-lite Skip region; it shares the existing three-bit policy-state field. */
  public static final int POLICY_S4_SKIP = 7;
  private static final int PENDING_MASK = PENDING_ADD | PENDING_UPDATE | PENDING_REMOVE;
  private static final int PENDING_CLAIMED = 1 << 3;
  private static final int PENDING_QUEUED = 1 << 4;
  private static final int PENDING_RETRY = 1 << 5;
  private static final int PENDING_CLAIM_SHIFT = 6;
  private static final int PENDING_CLAIM_MASK = PENDING_MASK << PENDING_CLAIM_SHIFT;
  private static final int PENDING_ROLLOVER = 1 << 9;
  private static final int PENDING_ROLLOVER_CLAIMED = 1 << 10;
  /** Native state-word hint used by the business writer to avoid reading policy metadata. */
  private static final int POLICY_PRESENT = 1 << 11;
  private static final int TIMER_UNSCHEDULED = -1;
  private static final int TIMER_HEAP_BASE = -2;
  private static final int TIMER_SLOT_BITS = 10;
  private static final int TIMER_SLOT_MASK = (1 << TIMER_SLOT_BITS) - 1;
  private static final int POLICY_STATE_SHIFT = 0;
  private static final int POLICY_ACCESS_SHIFT = 3;
  private static final int POLICY_LINK_ID_SHIFT = 5;
  private static final int POLICY_LINK_ID_MASK = 0x07ff_ffff;
  private static final long STATE_WORD_OFFSET = 0L;
  private static final long CURRENT_VALUE_ALLOCATION_OFFSET = 8L;
  private static final long MAINTENANCE_META_OFFSET = 16L;
  private static final long POLICY_META_OFFSET = 24L;
  private static final long TIMER_LOCATION_OFFSET = 28L;
  private static final long TIMER_DEADLINE_OFFSET = 32L;
  private static final long POLICY_BYTE_WEIGHT_OFFSET = 40L;
  /** Unified native header: allocator metadata occupies +48 and +56. */
  public static final long NATIVE_METADATA_BYTES = 64L;
  private static final long MUTATION_VERSION_SHIFT = 32L;
  private static final long VERSION_MASK = 0xffffffffL;
  public static final long WRITER_LOCK = 1L << 63;
  private static final int PENDING_STATE_SHIFT = 51;
  private static final long PENDING_STATE_MASK = 0xfffL << PENDING_STATE_SHIFT;
  private static final long WRITER_WAITER = 1L << 50;
  public static final long GENERATION_MASK = WRITER_WAITER - 1L;

  private static final AtomicLongFieldUpdater<Entry> VALUE_ADDRESS =
      AtomicLongFieldUpdater.newUpdater(Entry.class, "valueAddress");
  /** High bit reserved in the native allocation word for the logical mapping state. */
  private static final long LOGICAL_ABSENT = Long.MIN_VALUE;
  private static final long VALUE_ALLOCATION_MASK = Long.MAX_VALUE;

  public final long nativeKeyAddress;
  /** Packed key identity: 24-bit hash above the low 8 length bits. */
  public final int keyIndex;

  /** Eight-byte aligned native value address; low bits carry TTL/lifecycle tags. */
  public volatile long valueAddress;

  public Entry(long nativeKeyAddress, int keyIndex, long valueAddress) {
    this.nativeKeyAddress = nativeKeyAddress;
    this.keyIndex = keyIndex;
    this.valueAddress = valueAddress;
  }

  /** Packed key identity: 24-bit hash in the high bits, key length in the low 8. */
  public int keyIndex() {
    return keyIndex;
  }

  public int keyHash() {
    return keyIndex >>> 8;
  }

  public int keyLength() {
    return keyIndex & 0xff;
  }

  public long nativeKeyAddress() {
    return nativeKeyAddress;
  }

  public long nativeKeyBytesAddress() {
    return nativeKeyAddress;
  }

  public static long keyDataAllocationLength(int keyLength) {
    if (keyLength < 0 || keyLength > 0xff) {
      throw new IllegalArgumentException("keyLength must be in [0, 255]");
    }
    return Math.max(8L, CacheMath.roundUpTo8((long) keyLength));
  }

  public static long keyAllocationLengthForKeyLength(int keyLength) {
    return keyDataAllocationLength(keyLength) + NATIVE_METADATA_BYTES;
  }

  /** Physical bytes after the allocator prefix: key hash and serialized key only. */
  public static long keyPhysicalAllocationLengthForKeyLength(int keyLength) {
    return keyDataAllocationLength(keyLength);
  }

  public long keyAllocationLength() {
    return keyAllocationLengthForKeyLength(keyLength());
  }

  public long nativeKeyAllocationLength() {
    return keyPhysicalAllocationLengthForKeyLength(keyLength());
  }

  public long rawValueAddress() {
    return rawValueAddress(valueAddress);
  }

  private long nativeMetadataAddress() {
    if (nativeKeyAddress == 0L) {
      throw new IllegalStateException("Entry has no native key block");
    }
    return nativeKeyAddress - NATIVE_METADATA_BYTES;
  }

  private long stateWordAddress() {
    return nativeMetadataAddress() + STATE_WORD_OFFSET;
  }

  private static int pendingFlags(long stateWord) {
    return (int) ((stateWord & PENDING_STATE_MASK) >>> PENDING_STATE_SHIFT);
  }

  /** Snapshot of pending flags stored in the native state word. */
  public int pendingFlags() {
    return pendingFlags(NativeMemory.getLongVolatile(stateWordAddress()));
  }

  private boolean compareAndSetPending(int expected, int update) {
    long address = stateWordAddress();
    long current = NativeMemory.getLongVolatile(address);
    if (pendingFlags(current) != expected) {
      return false;
    }
    long next = (current & ~PENDING_STATE_MASK) | ((long) update << PENDING_STATE_SHIFT);
    return NativeMemory.compareAndSwapLong(address, current, next);
  }

  public void initializeNativeMetadata() {
    long metadata = nativeMetadataAddress();
    NativeMemory.putLong(metadata + STATE_WORD_OFFSET, 0L);
    NativeMemory.putInt(metadata + TIMER_LOCATION_OFFSET, TIMER_UNSCHEDULED);
    NativeMemory.putInt(metadata + POLICY_META_OFFSET, 0);
    NativeMemory.putLong(metadata + CURRENT_VALUE_ALLOCATION_OFFSET, LOGICAL_ABSENT);
    NativeMemory.putLong(metadata + MAINTENANCE_META_OFFSET, 0L);
    NativeMemory.putLong(metadata + TIMER_DEADLINE_OFFSET, 0L);
    NativeMemory.putLong(metadata + POLICY_BYTE_WEIGHT_OFFSET, 0L);
  }

  /** Allocation length paired with the currently published value pointer. */
  public long currentValueAllocation() {
    return NativeMemory.getLongVolatile(nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET)
        & VALUE_ALLOCATION_MASK;
  }

  /** Must be published before the matching value pointer becomes visible. */
  public void currentValueAllocation(long allocation) {
    if (allocation < 0L) {
      throw new IllegalArgumentException("negative value allocation");
    }
    long address = nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET;
    // Value writers own the Entry claim whenever they update the allocation. Preserve the
    // logical marker observed under that claim and publish the combined word with one release
    // store; logical present/absent transitions retain their CAS helpers below because they can
    // be initiated by the reader/actor side.
    long current = NativeMemory.getLong(address);
    NativeMemory.putLongRelease(
        address, (allocation & VALUE_ALLOCATION_MASK) | (current & LOGICAL_ABSENT));
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
    // Entry-to-Entry equality is also used by CHM.remove(key, value). Keep that value contract
    // distinct from LookupKey.equals: a zero-length key must not make every candidate value equal.
    if (keyIndex != entry.keyIndex) {
      return false;
    }
    return keyLength() == 0
        || NativeMemory.equals(nativeKeyBytesAddress(), entry.nativeKeyBytesAddress(), keyLength());
  }

  public boolean claimWriter() {
    return claimWriterStateWord() != 0L;
  }

  /** Claims the writer bit and reports the post-claim state word; 0 means the claim failed. */
  public long claimWriterStateWord() {
    if (!isAlive()) {
      return 0L;
    }
    long address = stateWordAddress();
    long current = NativeMemory.getLongVolatile(address);
    if ((current & WRITER_LOCK) == 0L
        && NativeMemory.compareAndSwapLong(address, current, current | WRITER_LOCK)) {
      return current | WRITER_LOCK;
    }
    return 0L;
  }

  public boolean markWriterWaiter() {
    long address = stateWordAddress();
    while (true) {
      long current = NativeMemory.getLongVolatile(address);
      if ((current & WRITER_LOCK) == 0L) {
        return false;
      }
      if ((current & WRITER_WAITER) != 0L) {
        return true;
      }
      if (NativeMemory.compareAndSwapLong(address, current, current | WRITER_WAITER)) {
        return true;
      }
    }
  }

  public void finishWriter() {
    long address = stateWordAddress();
    while (true) {
      long current = NativeMemory.getLongVolatile(address);
      if ((current & WRITER_LOCK) == 0L) {
        throw new IllegalStateException("writer lock is not held");
      }
      long generation = (current & GENERATION_MASK) + 1L;
      if (generation > GENERATION_MASK) {
        generation = 1L;
      }
      long next = (current & PENDING_STATE_MASK) | generation;
      if (NativeMemory.compareAndSwapLong(address, current, next)) {
        if ((current & WRITER_WAITER) != 0L) {
          synchronized (this) {
            notifyAll();
          }
        }
        return;
      }
    }
  }

  public long generation() {
    return NativeMemory.getLongVolatile(stateWordAddress()) & GENERATION_MASK;
  }

  /** One volatile read of the state word; callers extract the writer bit and the generation. */
  public long writerClaimStateWord() {
    return NativeMemory.getLongVolatile(stateWordAddress());
  }

  public static long generationOfStateWord(long stateWord) {
    return stateWord & GENERATION_MASK;
  }

  public boolean isAlive() {
    return (valueAddress & VALUE_LIFECYCLE_MASK) == 0L;
  }

  /** Lifecycle check on an already-loaded tagged pointer; avoids a second volatile field read. */
  public static boolean isAliveTagged(long taggedValue) {
    return (taggedValue & VALUE_LIFECYCLE_MASK) == 0L;
  }

  /** Marks this entry as logically present and reports whether the counter needs an increment. */
  public boolean markLogicallyPresent() {
    long address = nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET;
    for (;;) {
      long current = NativeMemory.getLongVolatile(address);
      if ((current & LOGICAL_ABSENT) == 0L) {
        return false;
      }
      if (NativeMemory.compareAndSwapLong(address, current, current & VALUE_ALLOCATION_MASK)) {
        return true;
      }
    }
  }

  /** Returns whether this entry has already been excluded from logical Map accounting. */
  public boolean isLogicallyAbsent() {
    long address = nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET;
    return (NativeMemory.getLongVolatile(address) & LOGICAL_ABSENT) != 0L;
  }

  /** Single volatile read yielding allocation + absent from the metadata word. */
  public long currentValueAllocationAndAbsent() {
    return NativeMemory.getLongVolatile(nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET);
  }

  public static long allocationOfMetadataWord(long metadataWord) {
    return metadataWord & VALUE_ALLOCATION_MASK;
  }

  public static boolean absentOfMetadataWord(long metadataWord) {
    return (metadataWord & LOGICAL_ABSENT) != 0L;
  }

  /** Marks this entry as logically absent and reports whether the counter needs a decrement. */
  public boolean markLogicallyAbsent() {
    long address = nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET;
    for (;;) {
      long current = NativeMemory.getLongVolatile(address);
      if ((current & LOGICAL_ABSENT) != 0L) {
        return false;
      }
      if (NativeMemory.compareAndSwapLong(address, current, current | LOGICAL_ABSENT)) {
        return true;
      }
    }
  }

  /**
   * Marks this entry absent while the caller owns the Entry writer claim. The writer claim makes
   * the allocation word stable, so the actor can use one ordered store instead of a CAS loop.
   */
  public boolean markLogicallyAbsentAfterWriterClaim() {
    if (!isWriterLocked()) {
      throw new IllegalStateException("writer claim is required for the actor absent transition");
    }
    long address = nativeMetadataAddress() + CURRENT_VALUE_ALLOCATION_OFFSET;
    long current = NativeMemory.getLongVolatile(address);
    if ((current & LOGICAL_ABSENT) != 0L) {
      return false;
    }
    NativeMemory.putLongRelease(address, current | LOGICAL_ABSENT);
    return true;
  }

  /** The maintenance actor must not consume a coalesced mutation during a writer publication. */
  public boolean isWriterLocked() {
    return (NativeMemory.getLongVolatile(stateWordAddress()) & WRITER_LOCK) != 0L;
  }

  /** Must be called while holding the per-entry writer mutex. */
  public void markRetired() {
    long current = NativeMemory.getLongVolatile(stateWordAddress());
    if ((current & WRITER_LOCK) == 0L) {
      throw new IllegalStateException("writer lock is not held");
    }
    setValueLifecycle(VALUE_RETIRED);
  }

  /** Restores ALIVE after a failed conditional CHM remove while still holding the writer mutex. */
  public void restoreAlive() {
    long current = NativeMemory.getLongVolatile(stateWordAddress());
    if ((current & WRITER_LOCK) == 0L) {
      throw new IllegalStateException("writer lock is not held");
    }
    setValueLifecycle(0L);
  }

  /** Used only for a private candidate that lost putIfAbsent before publication. */
  public void markDead() {
    long current = NativeMemory.getLongVolatile(stateWordAddress());
    if ((current & WRITER_LOCK) != 0L) {
      throw new IllegalStateException("writer lock is held");
    }
    setValueLifecycle(VALUE_DEAD);
  }

  /** Clears only the native value pointer while retaining the retired/dead lifecycle tag. */
  public void clearValue() {
    while (true) {
      long current = valueAddress;
      long next = current & VALUE_LIFECYCLE_MASK;
      if (current == next || VALUE_ADDRESS.compareAndSet(this, current, next)) {
        return;
      }
    }
  }

  private void setValueLifecycle(long lifecycle) {
    while (true) {
      long current = valueAddress;
      long next = (current & ~VALUE_LIFECYCLE_MASK) | lifecycle;
      if (VALUE_ADDRESS.compareAndSet(this, current, next)) {
        return;
      }
    }
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
    return NativeMemory.getLongVolatile(nativeMetadataAddress() + TIMER_DEADLINE_OFFSET);
  }

  public void timerDeadlineTick(long tick) {
    NativeMemory.putLongRelease(nativeMetadataAddress() + TIMER_DEADLINE_OFFSET, tick);
  }

  /** Actor-owned packed timer location; exposed for one-snapshot maintenance paths. */
  public int timerLocation() {
    return NativeMemory.getIntVolatile(nativeMetadataAddress() + TIMER_LOCATION_OFFSET);
  }

  /** Publishes an already encoded timer location. */
  public void timerLocation(int location) {
    NativeMemory.putIntVolatile(nativeMetadataAddress() + TIMER_LOCATION_OFFSET, location);
  }

  /** Publishes a normal timer location with one volatile write. */
  public void timerLocation(int level, int slot) {
    timerLocation((level << TIMER_SLOT_BITS) | (slot & TIMER_SLOT_MASK));
  }

  /** Attempts a reliable-removal claim once; callers must not wait for another producer. */
  public boolean tryBeginPending(int flags) {
    if (flags != PENDING_REMOVE) {
      throw new IllegalArgumentException("invalid pending flags: " + flags);
    }
    int current = pendingFlags();
    return (current & PENDING_CLAIMED) == 0
        && compareAndSetPending(
            current, current | PENDING_CLAIMED | (flags << PENDING_CLAIM_SHIFT));
  }

  /** Releases a successful reliable removal claim after its retirement records are visible. */
  public void completePendingClaim() {
    if ((pendingFlags() & PENDING_CLAIMED) == 0) {
      throw new IllegalStateException("missing pending claim");
    }
    tryIncrementMutationVersion();
    while (true) {
      int current = pendingFlags();
      if ((current & PENDING_CLAIMED) == 0) {
        throw new IllegalStateException("pending claim was lost");
      }
      int currentFlags = (current & PENDING_CLAIM_MASK) >>> PENDING_CLAIM_SHIFT;
      if (compareAndSetPending(
          current, (current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK)) | currentFlags)) {
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

  /** Advances every unsaturated publication without waiting for the actor's rollover fence. */
  private boolean tryIncrementMutationVersion() {
    for (;;) {
      int flags = pendingFlags();
      if ((flags & (PENDING_ROLLOVER | PENDING_ROLLOVER_CLAIMED)) != 0) {
        return false;
      }
      long current = maintenanceMeta();
      long currentVersion = current >>> MUTATION_VERSION_SHIFT;
      if (currentVersion == VERSION_MASK) {
        compareAndSetPending(flags, flags | PENDING_ROLLOVER);
        return false;
      }
      long next = (current & VERSION_MASK) | ((currentVersion + 1L) << MUTATION_VERSION_SHIFT);
      if (NativeMemory.compareAndSwapLong(
          nativeMetadataAddress() + MAINTENANCE_META_OFFSET, current, next)) {
        return true;
      }
      // The actor can update appliedVersion in the same word. Dropping a failed increment would
      // let a later value share the version of an older queued allocation seed.
      Thread.onSpinWait();
    }
  }

  /** Clears a fully applied final version. Called by the single maintenance owner. */
  public boolean tryRolloverMaintenanceVersion() {
    if (isWriterLocked()) {
      return false;
    }
    int currentFlags = pendingFlags();
    int rolloverFlags =
        PENDING_ROLLOVER | PENDING_MASK | PENDING_CLAIMED | PENDING_QUEUED | PENDING_RETRY;
    if ((currentFlags & rolloverFlags) != PENDING_ROLLOVER) {
      return false;
    }
    int claimedFlags = currentFlags | PENDING_ROLLOVER_CLAIMED;
    if (!compareAndSetPending(currentFlags, claimedFlags)) {
      return false;
    }
    long current = maintenanceMeta();
    if ((current >>> MUTATION_VERSION_SHIFT) != VERSION_MASK
        || (current & VERSION_MASK) != VERSION_MASK) {
      compareAndSetPending(claimedFlags, currentFlags);
      return false;
    }
    if (!NativeMemory.compareAndSwapLong(
        nativeMetadataAddress() + MAINTENANCE_META_OFFSET, current, 0L)) {
      compareAndSetPending(claimedFlags, currentFlags);
      return false;
    }
    if (!compareAndSetPending(claimedFlags, currentFlags & ~PENDING_ROLLOVER)) {
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
      int current = pendingFlags();
      int next = (current | flags) & ~PENDING_RETRY;
      if ((current & PENDING_QUEUED) != 0) {
        if (compareAndSetPending(current, next)) {
          return false;
        }
        continue;
      }
      if (compareAndSetPending(current, next | PENDING_QUEUED)) {
        return true;
      }
    }
  }

  /** Cancels a reliable removal claim before its associated mapping becomes invisible. */
  public boolean cancelPendingClaim() {
    while (true) {
      int current = pendingFlags();
      if ((current & PENDING_CLAIMED) == 0) {
        throw new IllegalStateException("missing pending claim");
      }
      if (compareAndSetPending(current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
        return true;
      }
    }
  }

  public boolean cancelPendingClaimIfPresent() {
    while (true) {
      int current = pendingFlags();
      if ((current & PENDING_CLAIMED) == 0) {
        return false;
      }
      if (compareAndSetPending(current, current & ~(PENDING_CLAIMED | PENDING_CLAIM_MASK))) {
        return true;
      }
    }
  }

  /** Retains an advisory mutation for the next publication after the actor observed a lock. */
  public boolean requestMutationRetry(int flags) {
    if ((flags & ~(PENDING_ADD | PENDING_UPDATE)) != 0 || flags == 0) {
      throw new IllegalArgumentException("invalid advisory retry flags: " + flags);
    }
    while (true) {
      int current = pendingFlags();
      int next = current | flags | PENDING_RETRY;
      if (next == current) {
        return false;
      }
      if (compareAndSetPending(current, next)) {
        return true;
      }
    }
  }

  /**
   * Claims the single transport record required by an actor retry. The actor and the writer may
   * race this handoff after the writer lock clears; exactly one of them may publish the record.
   */
  public boolean claimMutationRetry() {
    while (true) {
      int current = pendingFlags();
      if ((current & PENDING_RETRY) == 0) {
        return false;
      }
      int next = current & ~PENDING_RETRY;
      boolean publish = (current & PENDING_QUEUED) == 0;
      if (publish) {
        next |= PENDING_QUEUED;
      }
      if (compareAndSetPending(current, next)) {
        return publish;
      }
    }
  }

  public boolean isPendingQueued() {
    return (pendingFlags() & PENDING_QUEUED) != 0;
  }

  public boolean isMutationRetryRequested() {
    return (pendingFlags() & PENDING_RETRY) != 0;
  }

  /**
   * Clears advisory state before the native key block is retired.
   *
   * <p>This method deliberately changes only the pending-state slice of the native word. It does
   * not clear the writer bit, generation, or policy-presence bit. The maintenance lifecycle owner
   * calls it after applying a terminal removal and before retiring the Entry's native key block;
   * stale advisory queue nodes can then be discarded without keeping the link record alive.
   */
  public void clearStalePendingFlags() {
    long address = stateWordAddress();
    int clearMask =
        PENDING_MASK
            | PENDING_CLAIMED
            | PENDING_CLAIM_MASK
            | PENDING_QUEUED
            | PENDING_RETRY
            | PENDING_ROLLOVER
            | PENDING_ROLLOVER_CLAIMED;
    for (;;) {
      long current = NativeMemory.getLongVolatile(address);
      int flags = pendingFlags(current);
      int nextFlags = flags & ~clearMask;
      if (flags == nextFlags) {
        return;
      }
      long next = (current & ~PENDING_STATE_MASK) | ((long) nextFlags << PENDING_STATE_SHIFT);
      if (NativeMemory.compareAndSwapLong(address, current, next)) {
        return;
      }
    }
  }

  public int takePending() {
    while (true) {
      int current = pendingFlags();
      if ((current & PENDING_CLAIMED) != 0) {
        return PENDING_BUSY;
      }
      int next = current & ~(PENDING_MASK | PENDING_QUEUED | PENDING_RETRY);
      if (compareAndSetPending(current, next)) {
        return current & PENDING_MASK;
      }
    }
  }

  public int policyState() {
    return (policyMetaVolatile() >>> POLICY_STATE_SHIFT) & 7;
  }

  public void policyState(int state) {
    int normalizedState = state & 7;
    long address = nativeMetadataAddress() + POLICY_META_OFFSET;
    int current = NativeMemory.getIntVolatile(address);
    NativeMemory.putIntVolatile(address, (current & ~7) | normalizedState);
    setPolicyPresent(normalizedState != POLICY_NONE);
  }

  /** True when the maintenance actor has this entry linked into an eviction policy. */
  public boolean policyPresent() {
    return (pendingFlags() & POLICY_PRESENT) != 0;
  }

  /** Clears a stale actor hint without touching it while the actor still owns a live policy. */
  public boolean clearPolicyPresentIfNone() {
    if (policyState() != POLICY_NONE) {
      return false;
    }
    long address = stateWordAddress();
    for (;;) {
      if (policyState() != POLICY_NONE) {
        return false;
      }
      long current = NativeMemory.getLongVolatile(address);
      int flags = pendingFlags(current);
      if ((flags & POLICY_PRESENT) == 0) {
        return false;
      }
      int nextFlags = flags & ~POLICY_PRESENT;
      long next = (current & ~PENDING_STATE_MASK) | ((long) nextFlags << PENDING_STATE_SHIFT);
      if (NativeMemory.compareAndSwapLong(address, current, next)) {
        return true;
      }
    }
  }

  public int policyAccessCount() {
    return (policyMetaVolatile() >>> POLICY_ACCESS_SHIFT) & 3;
  }

  public void policyAccessCount(int count) {
    int value = Math.max(0, Math.min(3, count));
    long address = nativeMetadataAddress() + POLICY_META_OFFSET;
    int current = NativeMemory.getIntVolatile(address);
    NativeMemory.putIntVolatile(address, (current & ~0x18) | (value << POLICY_ACCESS_SHIFT));
  }

  /** Actor-owned native link record id; zero means the entry has no maintenance links. */
  public int policyLinkId() {
    return (policyMetaVolatile() >>> POLICY_LINK_ID_SHIFT) & POLICY_LINK_ID_MASK;
  }

  public void policyLinkId(int id) {
    if ((id & ~POLICY_LINK_ID_MASK) != 0) {
      throw new IllegalArgumentException("link id out of range: " + id);
    }
    long address = nativeMetadataAddress() + POLICY_META_OFFSET;
    int current = NativeMemory.getIntVolatile(address);
    NativeMemory.putIntVolatile(address, (current & 0x1f) | (id << POLICY_LINK_ID_SHIFT));
  }

  public long policyByteWeight() {
    return NativeMemory.getLong(nativeMetadataAddress() + POLICY_BYTE_WEIGHT_OFFSET);
  }

  public void policyByteWeight(long bytes) {
    NativeMemory.putLong(nativeMetadataAddress() + POLICY_BYTE_WEIGHT_OFFSET, bytes);
  }

  private int policyMetaVolatile() {
    return NativeMemory.getIntVolatile(nativeMetadataAddress() + POLICY_META_OFFSET);
  }

  private void setPolicyPresent(boolean present) {
    while (true) {
      int current = pendingFlags();
      int next = present ? current | POLICY_PRESENT : current & ~POLICY_PRESENT;
      if (current == next || compareAndSetPending(current, next)) {
        return;
      }
    }
  }

  private long maintenanceMeta() {
    return NativeMemory.getLongVolatile(nativeMetadataAddress() + MAINTENANCE_META_OFFSET);
  }
}
