package com.red.ohc.runtime;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;

import com.red.ohc.storage.NativeMemory;

/** Cache-local QSBR slot table with strong ownership and actor-driven lifecycle cleanup. */
public final class ReaderRegistry {
  /** High bit marks a reader that may dereference a replaceable native value payload. */
  public static final long VALUE_PROTECTION_BIT = Long.MIN_VALUE;
  /** Low 63 bits carry the reader epoch; all ones remains the scan sentinel. */
  public static final long EPOCH_MASK = Long.MAX_VALUE;
  public static final long MAX_READER_EPOCH = EPOCH_MASK - 1L;

  public static final int SLOT_CHUNK_SHIFT = 6;
  public static final int SLOT_CHUNK_SIZE = 1 << SLOT_CHUNK_SHIFT;
  private static final int SLOT_CHUNK_MASK = SLOT_CHUNK_SIZE - 1;
  private static final int INITIAL_SLOT_CAPACITY = SLOT_CHUNK_SIZE;
  /** Eight words reserve one 64-byte cache line per reader lane. */
  private static final int SLOT_STRIDE_WORDS = 8;
  /** Writer admission uses the second word and never shares the reader state RMW path. */
  private static final int WRITER_WORD_OFFSET = 1;
  private static final int WORDS_PER_CHUNK = SLOT_CHUNK_SIZE * SLOT_STRIDE_WORDS;
  private static final int SLOT_CHUNK_BYTES = WORDS_PER_CHUNK * Long.BYTES;
  private static final int SLOT_ALIGNMENT_BYTES = 64;
  private static final VarHandle LONG_ARRAY_HANDLE =
      MethodHandles.arrayElementVarHandle(long[].class);
  private static final VarHandle READER_NOTIFICATION_HANDLE;

  static {
    try {
      READER_NOTIFICATION_HANDLE =
          MethodHandles.lookup().findVarHandle(ReaderSlot.class, "readerNotification", int.class);
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private final NativeMemory.Memory memory;
  private final Object registrationLock = new Object();
  /** Volatile root; growth copies only the root directories and appends new chunks. */
  private volatile SlotStorage storage;
  /** Primitive free-index stack. Only registration and actor lifecycle cleanup mutate it. */
  private int[] freeSlotStack = new int[INITIAL_SLOT_CAPACITY];
  private int freeSlotCount;
  private int nextSlot;
  private volatile int registeredCount;
  private volatile boolean closed;
  private boolean nativeClosed;

  public ReaderRegistry(NativeMemory.Memory memory) {
    this.memory = Objects.requireNonNull(memory, "memory");
    this.storage = SlotStorage.initial(memory);
  }

  public int register(ReaderSlot slot) {
    if (slot == null) {
      throw new NullPointerException("slot");
    }
    synchronized (registrationLock) {
      if (closed) {
        throw new IllegalStateException("reader registry is closed");
      }
      if (slot.registry == this) {
        int index = slot.registryIndex;
        if (index >= 0 && isLiveLocked(index, slot)) {
          return index;
        }
        throw new IllegalStateException("reader slot registration is stale");
      }
      if (slot.registry != null) {
        throw new IllegalStateException("reader slot is already registered");
      }

      int index;
      if (freeSlotCount != 0) {
        index = freeSlotStack[--freeSlotCount];
      } else {
        index = nextSlot;
        if (index == storage.slotCapacity) {
          growLocked();
        }
        nextSlot = index + 1;
      }

      SlotStorage current = storage;
      int chunkIndex = index >> SLOT_CHUNK_SHIFT;
      int offset = index & SLOT_CHUNK_MASK;
      long stateAddress = stateAddress(current, index);
      setWordVolatile(stateAddress, 0L);
      setWordVolatile(stateAddress + WRITER_WORD_OFFSET * Long.BYTES, 0L);
      READER_NOTIFICATION_HANDLE.setVolatile(slot, 0);
      current.consumedHitChunks[chunkIndex][offset] = 0L;
      current.consumedMissChunks[chunkIndex][offset] = 0L;
      current.slotChunks[chunkIndex][offset] = slot;

      slot.owner = Thread.currentThread();
      slot.writerResource = null;
      slot.readerStateAddress = stateAddress;
      slot.registryIndex = index;
      slot.registry = this;
      setLiveBitRelease(current.liveBitmaps, chunkIndex, offset, true);
      registeredCount++;
      return index;
    }
  }

  /** Associates a strong reader identity with its first-write resource handle. */
  public void attachWriterResource(ReaderSlot slot, WriterResource resource) {
    if (slot == null || resource == null) {
      throw new NullPointerException("writer resource attachment");
    }
    synchronized (registrationLock) {
      requireRegisteredLocked(slot);
      if (slot.writerResource != null && slot.writerResource != resource) {
        throw new IllegalStateException("reader slot already owns another writer resource");
      }
      slot.writerResource = resource;
    }
  }

  /** Returns the current root's number of 64-slot chunks. */
  public int slotChunkCount() {
    return storage.liveBitmaps.length;
  }

  public int slotCapacity() {
    return storage.slotCapacity;
  }

  public boolean hasRegisteredSlots() {
    return registeredCount != 0;
  }

  /** Captures the current append-only slot-table root without allocating. */
  public SlotTableSnapshot slotTableSnapshot() {
    SlotStorage current = storage;
    return current.snapshot;
  }

  /** Returns the first live index in the half-open range, using only the registration bitmaps. */
  public int firstLiveSlot(int startInclusive, int endExclusive) {
    SlotStorage current = storage;
    int start = Math.max(0, startInclusive);
    int end = Math.min(current.slotCapacity, Math.max(start, endExclusive));
    if (start >= end) {
      return -1;
    }
    int firstChunk = start >> SLOT_CHUNK_SHIFT;
    int lastChunk = (end - 1) >> SLOT_CHUNK_SHIFT;
    for (int chunkIndex = firstChunk; chunkIndex <= lastChunk; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      int firstOffset = chunkIndex == firstChunk ? start & SLOT_CHUNK_MASK : 0;
      int lastOffset =
          chunkIndex == lastChunk ? ((end - 1) & SLOT_CHUNK_MASK) : SLOT_CHUNK_MASK;
      bits &= -1L << firstOffset;
      if (lastOffset != SLOT_CHUNK_MASK) {
        bits &= (1L << (lastOffset + 1)) - 1L;
      }
      if (bits != 0L) {
        return (chunkIndex << SLOT_CHUNK_SHIFT) + Long.numberOfTrailingZeros(bits);
      }
    }
    return -1;
  }

  /** Acquire-loads one 64-slot registration bitmap for actor scans. */
  public long liveBitmap(int chunkIndex) {
    SlotStorage current = storage;
    if (chunkIndex < 0 || chunkIndex >= current.liveBitmaps.length) {
      return 0L;
    }
    return (long) LONG_ARRAY_HANDLE.getAcquire(current.liveBitmaps, chunkIndex);
  }

  /** Returns the strong metadata slot at a stable index, or null for an unused index. */
  public ReaderSlot slotAt(int index) {
    SlotStorage current = storage;
    if (index < 0 || index >= current.slotCapacity) {
      return null;
    }
    return current.slotChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK];
  }

  public boolean isLive(int index) {
    SlotStorage current = storage;
    return index >= 0
        && index < current.slotCapacity
        && isLive(current.liveBitmaps, index >> SLOT_CHUNK_SHIFT, index & SLOT_CHUNK_MASK);
  }

  public boolean isTerminated(ReaderSlot slot) {
    if (slot == null || slot.registry != this) {
      return false;
    }
    Thread owner = slot.owner;
    return owner != null && !owner.isAlive();
  }

  /**
   * Detaches a dead owner after the actor has consumed its final counters and access ring. The
   * returned resource remains outside the pool until the actor requests its retirement.
   */
  public WriterResource detachTerminated(int index) {
    synchronized (registrationLock) {
      SlotStorage current = storage;
      if (index < 0 || index >= current.slotCapacity) {
        return null;
      }
      int chunkIndex = index >> SLOT_CHUNK_SHIFT;
      int offset = index & SLOT_CHUNK_MASK;
      if (!isLive(current.liveBitmaps, chunkIndex, offset)) {
        return null;
      }
      ReaderSlot slot = current.slotChunks[chunkIndex][offset];
      if (slot == null || slot.registry != this || !isTerminated(slot)) {
        return null;
      }
      WriterResource resource = slot.writerResource;
      long stateAddress = slot.readerStateAddress;
      setWordVolatile(stateAddress, 0L);
      setWordVolatile(stateAddress + WRITER_WORD_OFFSET * Long.BYTES, 0L);
      READER_NOTIFICATION_HANDLE.setVolatile(slot, 0);
      current.consumedHitChunks[chunkIndex][offset] = 0L;
      current.consumedMissChunks[chunkIndex][offset] = 0L;
      setLiveBitRelease(current.liveBitmaps, chunkIndex, offset, false);
      slot.setAccessSignal(null);
      slot.registry = null;
      slot.registryIndex = -1;
      slot.readerStateAddress = 0L;
      slot.owner = null;
      slot.writerResource = null;
      current.slotChunks[chunkIndex][offset] = null;
      freeSlotLocked(index);
      registeredCount--;
      return resource;
    }
  }

  /** Publishes one reader epoch without touching the independently published writer admission. */
  public void setEpoch(int index, long epoch) {
    validateEpoch(epoch);
    SlotStorage current = storage;
    setEpoch(stateAddress(current, index), epoch);
  }

  public void setEpoch(ReaderSlot slot, long epoch) {
    validateEpoch(epoch);
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    setEpoch(stateAddress, epoch);
  }

  /** Publishes the value-protection bit in the one reader QSBR state word. */
  public void setValueEpoch(int index, long epoch) {
    validateEpoch(epoch);
    SlotStorage current = storage;
    setValueEpoch(stateAddress(current, index), epoch);
  }

  public void setValueEpoch(ReaderSlot slot, long epoch) {
    validateEpoch(epoch);
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    setValueEpoch(stateAddress, epoch);
  }

  /** Publishes writer admission in its own word in the reader's cache-line lane. */
  public void setWriterActive(int index, boolean active) {
    SlotStorage current = storage;
    setWriterAdmission(stateAddress(current, index), active);
  }

  public void setWriterAdmission(ReaderSlot slot, boolean active) {
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    setWriterAdmission(stateAddress, active);
  }

  public long epoch(int index) {
    return readerState(index) & EPOCH_MASK;
  }

  /** Returns the one volatile-loaded reader word: epoch plus value protection only. */
  public long readerState(int index) {
    SlotStorage current = storage;
    return getWordVolatile(stateAddress(current, index));
  }

  public long readerState(ReaderSlot slot) {
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    return getWordVolatileTrusted(stateAddress);
  }

  /** Publishes an active reader with StoreLoad ordering and a clear with release ordering. */
  public void setReaderState(int index, long state) {
    validateReaderState(state);
    SlotStorage current = storage;
    setReaderBits(stateAddress(current, index), state);
  }

  public void setReaderState(ReaderSlot slot, long state) {
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    validateReaderState(state);
    setReaderBitsTrusted(stateAddress, state);
  }

  /** Publishes a validated reader state without touching writer admission. */
  public void setReaderStateKnownEpoch(ReaderSlot slot, long state) {
    long stateAddress = slot == null ? 0L : slot.readerStateAddress;
    if (slot == null || slot.registry != this || stateAddress == 0L) {
      throw new IllegalArgumentException("reader slot is not registered with this registry");
    }
    // Close is quiesced-only; no teardown can race this publication.
    assertOwner(slot);
    setReaderBitsTrusted(stateAddress, state);
  }

  /** Clears a reader state when shutdown may have concurrently unbound the slot. */
  public boolean clearReaderStateIfRegistered(ReaderSlot slot) {
    if (slot == null) {
      return false;
    }
    synchronized (registrationLock) {
      if (slot.registry != this || slot.readerStateAddress == 0L) {
        return false;
      }
      setReaderBits(slot.readerStateAddress, 0L);
      return true;
    }
  }

  private static void validateReaderState(long state) {
    long epoch = state & EPOCH_MASK;
    if (epoch > MAX_READER_EPOCH) {
      throw new IllegalArgumentException("reader state epoch must be in [0, Long.MAX_VALUE - 1]");
    }
    if (epoch == 0L && (state & VALUE_PROTECTION_BIT) != 0L) {
      throw new IllegalArgumentException("value protection must carry a non-zero epoch");
    }
  }

  private static void validateEpoch(long epoch) {
    if (epoch < 0L || epoch > MAX_READER_EPOCH) {
      throw new IllegalArgumentException("epoch must be in [0, Long.MAX_VALUE - 1]");
    }
  }

  private void requireRegisteredLocked(ReaderSlot slot) {
    if (slot.registry != this || slot.readerStateAddress == 0L) {
      throw new IllegalArgumentException("reader slot is not registered with this registry");
    }
  }

  private boolean isLiveLocked(int index, ReaderSlot expected) {
    SlotStorage current = storage;
    if (index < 0 || index >= current.slotCapacity) {
      return false;
    }
    return isLive(current.liveBitmaps, index >> SLOT_CHUNK_SHIFT, index & SLOT_CHUNK_MASK)
        && current.slotChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK] == expected;
  }

  public boolean hasActiveReader() {
    SlotStorage current = storage;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        long state = getWordVolatile(stateAddress(current, chunkIndex, offset));
        if ((state & EPOCH_MASK) != 0L) {
          return true;
        }
        bits &= bits - 1L;
      }
    }
    return false;
  }

  /** Marks every currently active reader before the actor performs a blocking QSBR scan. */
  public int markActiveReaderNotifications() {
    return markActiveReaderNotifications(Long.MAX_VALUE);
  }

  /** Marks active readers admitted before the supplied epoch; newer readers cannot block its cut. */
  public int markActiveReaderNotifications(long epochExclusive) {
    SlotStorage current = storage;
    int marked = 0;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        ReaderSlot slot = current.slotChunks[chunkIndex][offset];
        if (slot != null) {
          long state = getWordVolatile(stateAddress(current, chunkIndex, offset));
          long readerEpoch = state & EPOCH_MASK;
          if (readerEpoch != 0L && readerEpoch < epochExclusive) {
            READER_NOTIFICATION_HANDLE.setRelease(slot, 1);
            marked++;
          }
        }
        bits &= bits - 1L;
      }
    }
    if (marked != 0) {
      // The actor's confirmation scan follows this arm. A release store alone does not provide
      // the required arm-to-rescan StoreLoad edge on every Java 11 VarHandle implementation.
      VarHandle.fullFence();
    }
    return marked;
  }

  /** Consumes the actor's marker without entering the retirement path on a reader thread. */
  public boolean consumeReaderNotification(ReaderSlot slot) {
    if (slot == null || slot.registry != this) {
      return false;
    }
    if ((int) READER_NOTIFICATION_HANDLE.getVolatile(slot) == 0) {
      return false;
    }
    return READER_NOTIFICATION_HANDLE.compareAndSet(slot, 1, 0);
  }

  /** Strongly consistent only with respect to the actor's current scan; used for diagnostics. */
  public int activeReaderCount() {
    SlotStorage current = storage;
    int active = 0;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        long state = getWordVolatile(stateAddress(current, chunkIndex, offset));
        if ((state & EPOCH_MASK) != 0L) {
          active++;
        }
        bits &= bits - 1L;
      }
    }
    return active;
  }

  /** Returns the oldest active reader epoch from a weakly consistent lock-free scan. */
  public long minActiveEpoch() {
    return minimumEpoch(false);
  }

  /** Returns the oldest reader that may dereference a native value payload. */
  public long minActiveValueEpoch() {
    return minimumEpoch(true);
  }

  /** Writes both minimum epochs without taking the cold registration lock or allocating. */
  public void minActiveEpochs(long[] output) {
    if (output == null || output.length < 2) {
      throw new IllegalArgumentException("epoch output must contain lookup and value entries");
    }
    SlotStorage current = storage;
    long minimumLookup = Long.MAX_VALUE;
    long minimumValue = Long.MAX_VALUE;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        long state = getWordVolatile(stateAddress(current, chunkIndex, offset));
        long lookupEpoch = state & EPOCH_MASK;
        if (lookupEpoch != 0L && lookupEpoch < minimumLookup) {
          minimumLookup = lookupEpoch;
        }
        if ((state & VALUE_PROTECTION_BIT) != 0L && lookupEpoch < minimumValue) {
          minimumValue = lookupEpoch;
        }
        bits &= bits - 1L;
      }
    }
    output[0] = minimumLookup;
    output[1] = minimumValue;
  }

  private long minimumEpoch(boolean values) {
    SlotStorage current = storage;
    long minimum = Long.MAX_VALUE;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        long state = getWordVolatile(stateAddress(current, chunkIndex, offset));
        long epoch = state & EPOCH_MASK;
        if ((!values || (state & VALUE_PROTECTION_BIT) != 0L)
            && epoch != 0L
            && epoch < minimum) {
          minimum = epoch;
        }
        bits &= bits - 1L;
      }
    }
    return minimum;
  }

  public boolean hasActiveWriter() {
    SlotStorage current = storage;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        if (getWordVolatile(
                stateAddress(current, chunkIndex, offset) + WRITER_WORD_OFFSET * Long.BYTES)
            != 0L) {
          return true;
        }
        bits &= bits - 1L;
      }
    }
    return false;
  }

  public int activeWriterCount() {
    SlotStorage current = storage;
    int count = 0;
    for (int chunkIndex = 0; chunkIndex < current.liveBitmaps.length; chunkIndex++) {
      long bits = liveBits(current.liveBitmaps, chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        if (getWordVolatile(
                stateAddress(current, chunkIndex, offset) + WRITER_WORD_OFFSET * Long.BYTES)
            != 0L) {
          count++;
        }
        bits &= bits - 1L;
      }
    }
    return count;
  }

  public void clear() {
    synchronized (registrationLock) {
      closed = true;
      SlotStorage current = storage;
      for (int chunkIndex = 0; chunkIndex < current.slotChunks.length; chunkIndex++) {
        long bits = liveBits(current.liveBitmaps, chunkIndex);
        while (bits != 0L) {
          int offset = Long.numberOfTrailingZeros(bits);
          ReaderSlot slot = current.slotChunks[chunkIndex][offset];
          if (slot != null) {
            slot.setAccessSignal(null);
            slot.registry = null;
            slot.registryIndex = -1;
            slot.readerStateAddress = 0L;
            slot.owner = null;
            slot.writerResource = null;
            READER_NOTIFICATION_HANDLE.setVolatile(slot, 0);
            current.slotChunks[chunkIndex][offset] = null;
          }
          long stateAddress = stateAddress(current, chunkIndex, offset);
          setWordVolatile(stateAddress, 0L);
          setWordVolatile(stateAddress + WRITER_WORD_OFFSET * Long.BYTES, 0L);
          current.consumedHitChunks[chunkIndex][offset] = 0L;
          current.consumedMissChunks[chunkIndex][offset] = 0L;
          bits &= bits - 1L;
        }
        LONG_ARRAY_HANDLE.setVolatile(current.liveBitmaps, chunkIndex, 0L);
      }
      registeredCount = 0;
      nextSlot = 0;
      freeSlotCount = 0;
    }
  }

  /** Releases the append-only native slot chunks after the actor and all publishers have stopped. */
  public void close() {
    synchronized (registrationLock) {
      if (nativeClosed) {
        return;
      }
      if (registeredCount != 0) {
        throw new IllegalStateException("reader registry still has registered slots");
      }
      closed = true;
      storage.close(memory);
      nativeClosed = true;
    }
  }

  int registeredCount() {
    return registeredCount;
  }

  private static long liveBits(long[] bitmaps, int chunkIndex) {
    return (long) LONG_ARRAY_HANDLE.getAcquire(bitmaps, chunkIndex);
  }

  private static boolean isLive(long[] bitmaps, int chunkIndex, int offset) {
    return (liveBits(bitmaps, chunkIndex) & (1L << offset)) != 0L;
  }

  private static void setLiveBitRelease(long[] bitmaps, int chunkIndex, int offset, boolean live) {
    long current = liveBits(bitmaps, chunkIndex);
    long mask = 1L << offset;
    LONG_ARRAY_HANDLE.setRelease(bitmaps, chunkIndex, live ? current | mask : current & ~mask);
  }

  private void freeSlotLocked(int index) {
    if (freeSlotCount == freeSlotStack.length) {
      freeSlotStack = Arrays.copyOf(freeSlotStack, freeSlotStack.length << 1);
    }
    freeSlotStack[freeSlotCount++] = index;
  }

  private void growLocked() {
    int newCapacity = storage.slotCapacity << 1;
    storage = storage.grow(memory, newCapacity);
  }

  private static void setEpoch(long address, long epoch) {
    long current = getWordVolatile(address);
    long readerState = epoch == 0L ? 0L : (current & VALUE_PROTECTION_BIT) | epoch;
    setReaderPublication(address, readerState);
  }

  private static void setValueEpoch(long address, long epoch) {
    long current = getWordVolatile(address);
    long readerState = epoch == 0L ? current & EPOCH_MASK : VALUE_PROTECTION_BIT | epoch;
    setReaderPublication(address, readerState);
  }

  private static void setReaderBits(long address, long state) {
    setReaderPublication(address, state);
  }

  /** Same publication protocol for a slot whose registration fields were already validated. */
  private static void setReaderBitsTrusted(long address, long state) {
    setReaderPublicationTrusted(address, state);
  }

  private static void setWriterAdmission(long address, boolean active) {
    long writerAddress = address + WRITER_WORD_OFFSET * Long.BYTES;
    if (active) {
      setWordVolatile(writerAddress, 1L);
    } else {
      setWordRelease(writerAddress, 0L);
    }
  }

  /**
   * Entry publication must order the following close/epoch load after the state becomes visible.
   * Quiescent exits only need release ordering, so they stay on the cheaper store path.
   */
  private static void setReaderPublication(long address, long state) {
    if (state == 0L) {
      setWordRelease(address, state);
    } else {
      setWordVolatile(address, state);
    }
  }

  private static void setReaderPublicationTrusted(long address, long state) {
    if (state == 0L) {
      setWordReleaseTrusted(address, state);
    } else {
      setWordVolatileTrusted(address, state);
    }
  }

  private static void assertOwner(ReaderSlot slot) {
    // Close may unbind a slot after validation. Load its owner once, and only with -ea,
    // so shutdown follows the existing invalid-word/closing path without adding a hot-path read.
    assert slot == null || isOwnerOrUnbound(slot.owner)
        : "reader state must be published by its owner";
  }

  private static void assertOwner(SlotStorage storage, int index) {
    assertOwner(storage.slotChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK]);
  }

  private static boolean isOwnerOrUnbound(Thread owner) {
    return owner == null || owner == Thread.currentThread();
  }

  private static long stateAddress(SlotStorage storage, int index) {
    if (index < 0 || index >= storage.slotCapacity) {
      throw new IndexOutOfBoundsException("reader slot index: " + index);
    }
    return stateAddress(storage, index >> SLOT_CHUNK_SHIFT, index & SLOT_CHUNK_MASK);
  }

  private static long stateAddress(SlotStorage storage, int chunkIndex, int offset) {
    if (chunkIndex < 0
        || chunkIndex >= storage.stateChunkAddresses.length
        || offset < 0
        || offset >= SLOT_CHUNK_SIZE) {
      throw new IllegalArgumentException("invalid reader slot word");
    }
    return storage.stateChunkAddresses[chunkIndex] + (long) offset * SLOT_STRIDE_WORDS * Long.BYTES;
  }

  private static void setWordVolatile(long address, long value) {
    validateAddress(address);
    NativeMemory.putLongVolatile(address, value);
  }

  private static void setWordVolatileTrusted(long address, long value) {
    NativeMemory.putLongVolatile(address, value);
  }

  private static void setWordRelease(long address, long value) {
    validateAddress(address);
    NativeMemory.putLongRelease(address, value);
  }

  private static void setWordReleaseTrusted(long address, long value) {
    NativeMemory.putLongRelease(address, value);
  }

  private static long getWordVolatile(long address) {
    validateAddress(address);
    return NativeMemory.getLongVolatile(address);
  }

  private static long getWordVolatileTrusted(long address) {
    return NativeMemory.getLongVolatile(address);
  }

  private static void validateAddress(long address) {
    if (address == 0L) {
      throw new IllegalArgumentException("invalid reader slot word");
    }
  }

  private static final class SlotStorage {
    private final long[] stateChunkAddresses;
    private final ReaderSlot[][] slotChunks;
    private final long[] liveBitmaps;
    private final long[][] consumedHitChunks;
    private final long[][] consumedMissChunks;
    private final int slotCapacity;
    private final SlotTableSnapshot snapshot;

    private SlotStorage(
        long[] stateChunkAddresses,
        ReaderSlot[][] slotChunks,
        long[] liveBitmaps,
        long[][] consumedHitChunks,
        long[][] consumedMissChunks,
        int slotCapacity) {
      this.stateChunkAddresses = stateChunkAddresses;
      this.slotChunks = slotChunks;
      this.liveBitmaps = liveBitmaps;
      this.consumedHitChunks = consumedHitChunks;
      this.consumedMissChunks = consumedMissChunks;
      this.slotCapacity = slotCapacity;
      this.snapshot = new SlotTableSnapshot(this);
    }

    private static SlotStorage initial(NativeMemory.Memory memory) {
      return new SlotStorage(
          new long[] {allocateChunk(memory)},
          new ReaderSlot[][] {new ReaderSlot[SLOT_CHUNK_SIZE]},
          new long[1],
          new long[][] {new long[SLOT_CHUNK_SIZE]},
          new long[][] {new long[SLOT_CHUNK_SIZE]},
          INITIAL_SLOT_CAPACITY);
    }

    private static long allocateChunk(NativeMemory.Memory memory) {
      return memory.allocateAligned(SLOT_CHUNK_BYTES, SLOT_ALIGNMENT_BYTES);
    }

    private SlotStorage grow(NativeMemory.Memory memory, int newSlotCapacity) {
      int requiredChunks =
          (newSlotCapacity + SLOT_CHUNK_SIZE - 1) >> SLOT_CHUNK_SHIFT;
      long[] expandedState = Arrays.copyOf(stateChunkAddresses, requiredChunks);
      ReaderSlot[][] expandedSlots = Arrays.copyOf(slotChunks, requiredChunks);
      long[][] expandedHitChunks = Arrays.copyOf(consumedHitChunks, requiredChunks);
      long[][] expandedMissChunks = Arrays.copyOf(consumedMissChunks, requiredChunks);
      int firstNewChunk = stateChunkAddresses.length;
      int allocatedChunks = 0;
      try {
        for (int index = firstNewChunk; index < requiredChunks; index++) {
          expandedState[index] = allocateChunk(memory);
          allocatedChunks++;
          expandedSlots[index] = new ReaderSlot[SLOT_CHUNK_SIZE];
          expandedHitChunks[index] = new long[SLOT_CHUNK_SIZE];
          expandedMissChunks[index] = new long[SLOT_CHUNK_SIZE];
        }
      } catch (Throwable failure) {
        for (int index = firstNewChunk; index < firstNewChunk + allocatedChunks; index++) {
          memory.freeAligned(expandedState[index], SLOT_CHUNK_BYTES, SLOT_ALIGNMENT_BYTES);
        }
        throw failure;
      }
      return new SlotStorage(
          expandedState,
          expandedSlots,
          Arrays.copyOf(liveBitmaps, requiredChunks),
          expandedHitChunks,
          expandedMissChunks,
          newSlotCapacity);
    }

    private void close(NativeMemory.Memory memory) {
      for (int index = 0; index < stateChunkAddresses.length; index++) {
        long address = stateChunkAddresses[index];
        if (address != 0L) {
          memory.freeAligned(address, SLOT_CHUNK_BYTES, SLOT_ALIGNMENT_BYTES);
          stateChunkAddresses[index] = 0L;
        }
      }
    }
  }

  /** Immutable structural view used by one actor scan while the registry root may grow. */
  public static final class SlotTableSnapshot {
    private final SlotStorage storage;

    private SlotTableSnapshot(SlotStorage storage) {
      this.storage = storage;
    }

    public int slotCapacity() {
      return storage.slotCapacity;
    }

    public int slotChunkCount() {
      return storage.liveBitmaps.length;
    }

    public long liveBitmap(int chunkIndex) {
      if (chunkIndex < 0 || chunkIndex >= storage.liveBitmaps.length) {
        return 0L;
      }
      return liveBits(storage.liveBitmaps, chunkIndex);
    }

    public ReaderSlot slotAt(int index) {
      if (index < 0 || index >= storage.slotCapacity) {
        return null;
      }
      return storage.slotChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK];
    }

    public int firstLiveSlot(int startInclusive, int endExclusive) {
      int start = Math.max(0, startInclusive);
      int end = Math.min(storage.slotCapacity, Math.max(start, endExclusive));
      if (start >= end) {
        return -1;
      }
      int firstChunk = start >> SLOT_CHUNK_SHIFT;
      int lastChunk = (end - 1) >> SLOT_CHUNK_SHIFT;
      for (int chunkIndex = firstChunk; chunkIndex <= lastChunk; chunkIndex++) {
        long bits = liveBitmap(chunkIndex);
        int firstOffset = chunkIndex == firstChunk ? start & SLOT_CHUNK_MASK : 0;
        int lastOffset =
            chunkIndex == lastChunk ? ((end - 1) & SLOT_CHUNK_MASK) : SLOT_CHUNK_MASK;
        bits &= -1L << firstOffset;
        if (lastOffset != SLOT_CHUNK_MASK) {
          bits &= (1L << (lastOffset + 1)) - 1L;
        }
        if (bits != 0L) {
          return (chunkIndex << SLOT_CHUNK_SHIFT) + Long.numberOfTrailingZeros(bits);
        }
      }
      return -1;
    }

    public long consumedHits(int index) {
      checkIndex(index);
      return storage.consumedHitChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK];
    }

    public void consumedHits(int index, long value) {
      checkIndex(index);
      storage.consumedHitChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK] = value;
    }

    public long consumedMisses(int index) {
      checkIndex(index);
      return storage.consumedMissChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK];
    }

    public void consumedMisses(int index, long value) {
      checkIndex(index);
      storage.consumedMissChunks[index >> SLOT_CHUNK_SHIFT][index & SLOT_CHUNK_MASK] = value;
    }

    private void checkIndex(int index) {
      if (index < 0 || index >= storage.slotCapacity) {
        throw new IndexOutOfBoundsException("reader slot index: " + index);
      }
    }
  }
}
