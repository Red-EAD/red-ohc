package com.red.ohc.maintenance;

import java.util.Arrays;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Intrusive link storage owned by the maintenance actor. */
public final class EntryLinks implements AutoCloseable {
  /**
   * Native metadata shared by the actor policy and timer wheel.
   *
   * <p>The record is intentionally a power-of-two size so that a link lookup is one shift, one
   * mask, and one native address addition.
   */
  public static final int RECORD_BYTES = 32;
  public static final int LOGICAL_PAGE_BYTES = 4 << 10;
  public static final int SUPERPAGE_BYTES = 64 << 10;
  public static final int RECORDS_PER_LOGICAL_PAGE = LOGICAL_PAGE_BYTES / RECORD_BYTES;
  private static final int RECORDS_PER_SUPERPAGE = SUPERPAGE_BYTES / RECORD_BYTES;
  private static final int RECORD_SHIFT = Integer.numberOfTrailingZeros(RECORDS_PER_SUPERPAGE);
  private static final int PAGE_MASK = RECORDS_PER_SUPERPAGE - 1;
  private static final int LINK_ID_MASK = 0x07ff_ffff;
  private static final int MAX_LINK_ID = LINK_ID_MASK;
  private static final int POLICY_STATE_SHIFT = 0;
  private static final int POLICY_STATE_MASK = 0x7;
  private static final int POLICY_ACCESS_SHIFT = 3;
  private static final int POLICY_ACCESS_MASK = 0x3 << POLICY_ACCESS_SHIFT;
  private static final long ALLOCATION_BYTES = SUPERPAGE_BYTES + LOGICAL_PAGE_BYTES - 1L;
  static final int POLICY_PREVIOUS_OFFSET = 0;
  static final int POLICY_NEXT_OFFSET = 4;
  static final int TIMER_PREVIOUS_OFFSET = 8;
  static final int TIMER_NEXT_OFFSET = 12;
  private static final int POLICY_STATE_ACCESS_OFFSET = 16;
  private static final int POLICY_BYTE_WEIGHT_OFFSET = 20;
  private static final int KEY_HASH_OFFSET = 24;

  private final NativeMemory.Memory memory;
  private final boolean ownsMemory;
  private volatile long[] rawSuperpages = new long[2];
  private volatile long[] alignedSuperpages = new long[2];
  private volatile Entry[] registry = new Entry[1024];
  private int freeHead;
  private int nextId = 1;
  private boolean closed;

  /** Production constructor; all reservations are charged to the cache's native hard limit. */
  public EntryLinks(NativeMemory.Memory memory) {
    if (memory == null) {
      throw new NullPointerException("memory");
    }
    this.memory = memory;
    this.ownsMemory = false;
  }

  /** Test-only convenience constructor with an independently owned native budget. */
  EntryLinks() {
    this(new NativeMemory.Memory(AllocatorType.JNA), true);
  }

  NativeMemory.Memory memory() {
    return memory;
  }

  private EntryLinks(NativeMemory.Memory memory, boolean ownsMemory) {
    this.memory = memory;
    this.ownsMemory = ownsMemory;
  }

  /** Returns the native record id, allocating one lazily on the first policy/timer link. */
  synchronized int ensure(Entry entry) {
    checkOpen();
    int id = entry.policyLinkId();
    if (id != 0) {
      Entry[] entries = registry;
      if (id >= entries.length || entries[id] != entry) {
        throw new IllegalStateException("link registry mismatch for id " + id);
      }
      return id;
    }
    id = freeHead;
    if (id != 0) {
      freeHead = NativeMemory.getInt(recordAddress(id)) & LINK_ID_MASK;
    } else {
      id = nextId++;
      if (id <= 0 || id > MAX_LINK_ID) {
        throw new IllegalStateException("native link id exhausted");
      }
    }
    ensureRegistryCapacity(id);
    ensureSuperpage(id);
    registry[id] = entry;
    long address = recordAddress(id);
    NativeMemory.setMemory(address, RECORD_BYTES, (byte) 0);
    entry.policyLinkId(id);
    return id;
  }

  Entry entry(int id) {
    Entry[] entries = registry;
    if (id <= 0 || id >= entries.length) {
      return null;
    }
    return entries[id];
  }

  int policyPrev(Entry entry) {
    return get(entry, 0);
  }

  int policyPrev(int linkId) {
    return get(linkId, 0);
  }

  int policyNext(Entry entry) {
    return get(entry, 4);
  }

  int policyNext(int linkId) {
    return get(linkId, 4);
  }

  int timerPrev(Entry entry) {
    return get(entry, 8);
  }

  int timerPrev(int linkId) {
    return get(linkId, 8);
  }

  int timerNext(Entry entry) {
    return get(entry, 12);
  }

  int timerNext(int linkId) {
    return get(linkId, 12);
  }

  /** Actor-only packed policy state mirror; the native Entry remains the reader-visible source. */
  int policyState(int linkId) {
    return rawPolicyWord(linkId) & POLICY_STATE_MASK;
  }

  /** Actor-only packed policy state mirror. */
  void policyState(int linkId, int state) {
    int normalizedState = state & 7;
    long address = writableRecordAddress(linkId) + POLICY_STATE_ACCESS_OFFSET;
    int current = NativeMemory.getInt(address);
    NativeMemory.putInt(
        address, (current & ~POLICY_STATE_MASK) | (normalizedState << POLICY_STATE_SHIFT));
  }

  /** Actor-only packed access counter mirror. */
  int policyAccessCount(int linkId) {
    return (rawPolicyWord(linkId) & POLICY_ACCESS_MASK) >>> POLICY_ACCESS_SHIFT;
  }

  /** Actor-only packed access counter mirror. */
  void policyAccessCount(int linkId, int count) {
    int normalizedCount = Math.max(0, Math.min(3, count));
    long address = writableRecordAddress(linkId) + POLICY_STATE_ACCESS_OFFSET;
    int current = NativeMemory.getInt(address);
    NativeMemory.putInt(
        address, (current & ~POLICY_ACCESS_MASK) | (normalizedCount << POLICY_ACCESS_SHIFT));
  }

  /** Actor-only immutable key hash mirror. */
  long keyHash64(int linkId) {
    return linkId == 0 ? 0L : NativeMemory.getLong(recordAddress(linkId) + KEY_HASH_OFFSET);
  }

  /** Actor-only immutable key hash mirror initializer. */
  void keyHash64(int linkId, long hash64) {
    NativeMemory.putLong(writableRecordAddress(linkId) + KEY_HASH_OFFSET, hash64);
  }

  /** Actor-only policy byte-weight mirror. */
  int policyByteWeight(int linkId) {
    return linkId == 0
        ? 0
        : NativeMemory.getInt(recordAddress(linkId) + POLICY_BYTE_WEIGHT_OFFSET);
  }

  /** Actor-only policy byte-weight mirror. */
  void policyByteWeight(int linkId, int bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("policy byte weight must be non-negative");
    }
    NativeMemory.putInt(writableRecordAddress(linkId) + POLICY_BYTE_WEIGHT_OFFSET, bytes);
  }

  Entry policyPrevEntry(Entry entry) {
    return entry(policyPrev(entry));
  }

  Entry policyPrevEntry(int linkId) {
    return entry(policyPrev(linkId));
  }

  Entry policyNextEntry(Entry entry) {
    return entry(policyNext(entry));
  }

  Entry policyNextEntry(int linkId) {
    return entry(policyNext(linkId));
  }

  Entry timerPrevEntry(Entry entry) {
    return entry(timerPrev(entry));
  }

  Entry timerPrevEntry(int linkId) {
    return entry(timerPrev(linkId));
  }

  Entry timerNextEntry(Entry entry) {
    return entry(timerNext(entry));
  }

  Entry timerNextEntry(int linkId) {
    return entry(timerNext(linkId));
  }

  void policyPrev(Entry entry, Entry previous) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      linkId = ensure(entry);
    }
    policyPrev(linkId, previous);
  }

  void policyPrev(int linkId, Entry previous) {
    put(linkId, 0, idOf(previous));
  }

  void policyNext(Entry entry, Entry next) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      linkId = ensure(entry);
    }
    policyNext(linkId, next);
  }

  void policyNext(int linkId, Entry next) {
    put(linkId, 4, idOf(next));
  }

  void timerPrev(Entry entry, Entry previous) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      linkId = ensure(entry);
    }
    timerPrev(linkId, previous);
  }

  void timerPrev(int linkId, Entry previous) {
    put(linkId, 8, idOf(previous));
  }

  void timerNext(Entry entry, Entry next) {
    int linkId = entry.policyLinkId();
    if (linkId == 0) {
      linkId = ensure(entry);
    }
    timerNext(linkId, next);
  }

  void timerNext(int linkId, Entry next) {
    put(linkId, 12, idOf(next));
  }

  /** Links one actor-owned list head while resolving the two involved records once. */
  void linkHead(int linkId, Entry head, int previousOffset, int nextOffset) {
    long linkAddress = writableRecordAddress(linkId);
    int headId = idOf(head);
    putLink(linkAddress, previousOffset, 0);
    putLink(linkAddress, nextOffset, headId);
    if (headId != 0) {
      putLink(writableRecordAddress(headId), previousOffset, linkId);
    }
  }

  /** Unlinks one actor-owned list node and returns its previous/next ids as one primitive value. */
  long unlink(int linkId, int previousOffset, int nextOffset) {
    long linkAddress = writableRecordAddress(linkId);
    int previousId = getLink(linkAddress, previousOffset);
    int nextId = getLink(linkAddress, nextOffset);
    if (previousId != 0) {
      putLink(writableRecordAddress(previousId), nextOffset, nextId);
    }
    if (nextId != 0) {
      putLink(writableRecordAddress(nextId), previousOffset, previousId);
    }
    putLink(linkAddress, previousOffset, 0);
    putLink(linkAddress, nextOffset, 0);
    return ((long) previousId << 32) | (nextId & 0xffff_ffffL);
  }

  /** Clears one link pair while resolving its native record only once. */
  void clear(int linkId, int previousOffset, int nextOffset) {
    long linkAddress = writableRecordAddress(linkId);
    putLink(linkAddress, previousOffset, 0);
    putLink(linkAddress, nextOffset, 0);
  }

  /** Releases the record only after both policy and timer ownership have been removed. */
  synchronized void maybeRelease(Entry entry) {
    int id = entry.policyLinkId();
    if (id == 0) {
      return;
    }
    if (entry.policyState() != Entry.POLICY_NONE
        || policyState(id) != Entry.POLICY_NONE
        || entry.timerScheduled()
        || policyPrev(id) != 0
        || policyNext(id) != 0
        || timerPrev(id) != 0
        || timerNext(id) != 0) {
      return;
    }
    entry.clearPolicyPresentIfNone();
    if (entry.policyState() != Entry.POLICY_NONE) {
      return;
    }
    int pendingFlags = entry.pendingFlags();
    if (pendingFlags != 0) {
      if (entry.isAlive()) {
        return;
      }
      // A terminal entry cannot receive a new mutation. Its pending bits belong to the old
      // generation and must not keep an otherwise unowned native link record alive forever.
      entry.clearStalePendingFlags();
      if (entry.pendingFlags() != 0) {
        return;
      }
    }
    if (registry[id] != entry) {
      throw new IllegalStateException("link registry mismatch while releasing id " + id);
    }
    long address = recordAddress(id);
    NativeMemory.setMemory(address, RECORD_BYTES, (byte) 0);
    registry[id] = null;
    NativeMemory.putInt(address, freeHead);
    freeHead = id;
    entry.policyLinkId(0);
  }

  int activeLinkCount() {
    return nextId - 1 - freeCount();
  }

  long nativeBytes() {
    return (long) allocatedSuperpageCount() * ALLOCATION_BYTES;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    Arrays.fill(registry, null);
    for (int index = 0; index < rawSuperpages.length; index++) {
      long raw = rawSuperpages[index];
      if (raw != 0L) {
        memory.free(raw, ALLOCATION_BYTES);
        rawSuperpages[index] = 0L;
        alignedSuperpages[index] = 0L;
      }
    }
    if (ownsMemory) {
      memory.closeArenas();
    }
  }

  private int idOf(Entry entry) {
    if (entry == null) {
      return 0;
    }
    int id = entry.policyLinkId();
    Entry[] entries = registry;
    if (id == 0 || id >= entries.length || entries[id] != entry) {
      throw new IllegalStateException("entry has no active native link record");
    }
    return id;
  }

  private int get(Entry entry, int offset) {
    return get(entry.policyLinkId(), offset);
  }

  private int get(int id, int offset) {
    if (id == 0) {
      return 0;
    }
    return getLink(recordAddress(id), offset);
  }

  private void put(int id, int offset, int value) {
    putLink(writableRecordAddress(id), offset, value);
  }

  private int rawPolicyWord(int linkId) {
    if (linkId == 0) {
      return 0;
    }
    return NativeMemory.getInt(recordAddress(linkId) + POLICY_STATE_ACCESS_OFFSET);
  }

  private int getLink(long recordAddress, int offset) {
    int value = NativeMemory.getInt(recordAddress + offset);
    return offset == POLICY_PREVIOUS_OFFSET ? value & LINK_ID_MASK : value;
  }

  private void putLink(long recordAddress, int offset, int value) {
    if (offset == POLICY_PREVIOUS_OFFSET) {
      NativeMemory.putInt(recordAddress, value & LINK_ID_MASK);
    } else {
      NativeMemory.putInt(recordAddress + offset, value);
    }
  }

  private long writableRecordAddress(int id) {
    Entry[] entries = registry;
    if (id <= 0 || id >= entries.length || entries[id] == null) {
      throw new IllegalStateException("link registry mismatch for id " + id);
    }
    return recordAddress(id);
  }

  private long recordAddress(int id) {
    int zeroBased = id - 1;
    int superpage = zeroBased >>> RECORD_SHIFT;
    int slot = zeroBased & PAGE_MASK;
    long[] pages = alignedSuperpages;
    if (superpage >= pages.length) {
      throw new IllegalStateException("native link page is not allocated for id " + id);
    }
    long address = pages[superpage];
    if (address == 0L) {
      throw new IllegalStateException("native link page is not allocated for id " + id);
    }
    return address + (long) slot * RECORD_BYTES;
  }

  private void ensureSuperpage(int id) {
    int superpage = (id - 1) >>> RECORD_SHIFT;
    long[] pages = alignedSuperpages;
    if (superpage < pages.length && pages[superpage] != 0L) {
      return;
    }
    ensureSuperpageCapacity(superpage);
    long raw = memory.allocateRaw(ALLOCATION_BYTES);
    long aligned = (raw + LOGICAL_PAGE_BYTES - 1L) & -((long) LOGICAL_PAGE_BYTES);
    NativeMemory.setMemory(aligned, SUPERPAGE_BYTES, (byte) 0);
    rawSuperpages[superpage] = raw;
    alignedSuperpages[superpage] = aligned;
  }

  private void ensureSuperpageCapacity(int superpage) {
    if (superpage < alignedSuperpages.length) {
      return;
    }
    int length = alignedSuperpages.length;
    while (length <= superpage) {
      length <<= 1;
    }
    rawSuperpages = Arrays.copyOf(rawSuperpages, length);
    alignedSuperpages = Arrays.copyOf(alignedSuperpages, length);
  }

  private void ensureRegistryCapacity(int id) {
    if (id < registry.length) {
      return;
    }
    int length = registry.length;
    while (length <= id) {
      length <<= 1;
    }
    registry = Arrays.copyOf(registry, length);
  }

  private int allocatedSuperpageCount() {
    int count = 0;
    for (long address : alignedSuperpages) {
      if (address != 0L) {
        count++;
      }
    }
    return count;
  }

  private int freeCount() {
    int count = 0;
    for (int id = freeHead; id != 0; id = NativeMemory.getInt(recordAddress(id)) & LINK_ID_MASK) {
      count++;
    }
    return count;
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("entry links are closed");
    }
  }
}
