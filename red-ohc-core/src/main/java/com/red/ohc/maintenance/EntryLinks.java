package com.red.ohc.maintenance;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

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
  private static final int TIMER_UNSCHEDULED = -1;
  private static final int TIMER_HEAP_BASE = -2;
  private static final long ALLOCATION_BYTES = SUPERPAGE_BYTES + LOGICAL_PAGE_BYTES - 1L;
  static final int POLICY_PREVIOUS_OFFSET = 0;
  static final int POLICY_NEXT_OFFSET = 4;
  static final int TIMER_PREVIOUS_OFFSET = 8;
  static final int TIMER_NEXT_OFFSET = 12;
  private static final int POLICY_STATE_ACCESS_OFFSET = 16;
  private static final int POLICY_BYTE_WEIGHT_OFFSET = 20;
  private static final int KEY_HASH_OFFSET = 24;

  private final NativeMemory.Memory memory;
  private final ConcurrentHashMap<Entry, Entry> data;
  private final boolean ownsMemory;
  private final boolean ownsData;
  private volatile long[] rawSuperpages = new long[2];
  private volatile long[] alignedSuperpages = new long[2];
  private volatile long[][] nativeKeyAddressPages = new long[2][];
  private final EntryResolver resolver;
  private int activeCount;
  private int freeHead;
  private int nextId = 1;
  private boolean closed;

  /** Production constructor; all reservations are charged to the cache's native hard limit. */
  public EntryLinks(
      NativeMemory.Memory memory, ConcurrentHashMap<Entry, Entry> data) {
    this(memory, data, false, false);
  }

  /** Test-only constructor with an independently owned index and native budget. */
  EntryLinks(NativeMemory.Memory memory) {
    this(memory, new ConcurrentHashMap<>(), false, true);
  }

  /** Test-only convenience constructor with an independently owned native budget. */
  EntryLinks() {
    this(new NativeMemory.Memory(), new ConcurrentHashMap<>(), true, true);
  }

  private EntryLinks(
      NativeMemory.Memory memory,
      ConcurrentHashMap<Entry, Entry> data,
      boolean ownsMemory,
      boolean ownsData) {
    if (memory == null) {
      throw new NullPointerException("memory");
    }
    if (data == null) {
      throw new NullPointerException("data");
    }
    this.memory = memory;
    this.data = data;
    this.ownsMemory = ownsMemory;
    this.ownsData = ownsData;
    this.resolver = new EntryResolver(data);
  }

  NativeMemory.Memory memory() {
    return memory;
  }

  /** Returns the native record id, allocating one lazily on the first policy/timer link. */
  int ensure(Entry entry) {
    checkOpen();
    int id = entry.policyLinkId();
    if (id != 0) {
      if (nativeKeyAddress(id) != entry.nativeKeyAddress) {
        throw new IllegalStateException("link identity mismatch for id " + id);
      }
      return id;
    }
    if (ownsData) {
      indexEntry(entry);
    }
    boolean fromFreeList = false;
    id = freeHead;
    if (id != 0) {
      // Reused records are pre-zeroed except the free-list successor stored at offset 0; clear
      // that one word (maybeRelease's policyPrev gate would otherwise refuse the release).
      fromFreeList = true;
      long reusedAddress = recordAddress(id);
      freeHead = NativeMemory.getInt(reusedAddress) & LINK_ID_MASK;
      NativeMemory.putInt(reusedAddress + POLICY_PREVIOUS_OFFSET, 0);
    } else {
      id = nextId++;
      if (id <= 0 || id > MAX_LINK_ID) {
        throw new IllegalStateException("native link id exhausted");
      }
    }
    ensureSuperpage(id);
    setNativeKeyAddress(id, entry.nativeKeyAddress);
    if (!fromFreeList) {
      NativeMemory.setMemory(recordAddress(id), RECORD_BYTES, (byte) 0);
    }
    entry.policyLinkId(id);
    activeCount++;
    return id;
  }

  Entry entry(int id) {
    long nativeKeyAddress = nativeKeyAddress(id);
    if (nativeKeyAddress == 0L) {
      return null;
    }
    return resolver.resolve(nativeKeyAddress, Entry.keyHashAt(nativeKeyAddress));
  }

  int timerLocation(int linkId) {
    return linkId == 0 ? TIMER_UNSCHEDULED : Entry.timerLocationAt(nativeKeyAddress(linkId));
  }

  void timerLocation(int linkId, int location) {
    Entry.timerLocationAt(nativeKeyAddress(linkId), location);
  }

  long timerDeadlineTick(int linkId) {
    return Entry.timerDeadlineTickAt(nativeKeyAddress(linkId));
  }

  void timerDeadlineTick(int linkId, long tick) {
    Entry.timerDeadlineTickAt(nativeKeyAddress(linkId), tick);
  }

  int timerHeapIndex(int linkId) {
    int location = timerLocation(linkId);
    return location <= TIMER_HEAP_BASE ? -location - 2 : -1;
  }

  void timerHeapIndex(int linkId, int index) {
    timerLocation(linkId, index < 0 ? TIMER_HEAP_BASE : -index - 2);
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
  int keyHash(int linkId) {
    return linkId == 0 ? 0 : NativeMemory.getInt(recordAddress(linkId) + KEY_HASH_OFFSET);
  }

  /** Actor-only immutable key hash mirror initializer. */
  void keyHash(int linkId, int keyHash) {
    NativeMemory.putInt(writableRecordAddress(linkId) + KEY_HASH_OFFSET, keyHash);
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

  void timerPrev(int linkId, int previousId) {
    put(linkId, 8, previousId);
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
    linkHead(linkId, idOf(head), previousOffset, nextOffset);
  }

  /** Links one actor-owned list head using only native record identities. */
  void linkHead(int linkId, int headId, int previousOffset, int nextOffset) {
    long linkAddress = writableRecordAddress(linkId);
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
  void maybeRelease(Entry entry) {
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
    if (nativeKeyAddress(id) != entry.nativeKeyAddress) {
      throw new IllegalStateException("link identity mismatch while releasing id " + id);
    }
    long address = recordAddress(id);
    NativeMemory.setMemory(address, RECORD_BYTES, (byte) 0);
    setNativeKeyAddress(id, 0L);
    NativeMemory.putInt(address, freeHead);
    freeHead = id;
    entry.policyLinkId(0);
    activeCount--;
    if (ownsData) {
      data.remove(entry, entry);
    }
  }

  int activeLinkCount() {
    return activeCount;
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
    Arrays.fill(nativeKeyAddressPages, null);
    if (ownsData) {
      data.clear();
    }
    activeCount = 0;
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
    if (id == 0 || nativeKeyAddress(id) != entry.nativeKeyAddress) {
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
    if (nativeKeyAddress(id) == 0L) {
      throw new IllegalStateException("link identity is not active for id " + id);
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
    long[] identities = new long[RECORDS_PER_SUPERPAGE];
    long raw = memory.allocateRaw(ALLOCATION_BYTES);
    long aligned = (raw + LOGICAL_PAGE_BYTES - 1L) & -((long) LOGICAL_PAGE_BYTES);
    NativeMemory.setMemory(aligned, SUPERPAGE_BYTES, (byte) 0);
    rawSuperpages[superpage] = raw;
    alignedSuperpages[superpage] = aligned;
    nativeKeyAddressPages[superpage] = identities;
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
    nativeKeyAddressPages = Arrays.copyOf(nativeKeyAddressPages, length);
  }

  private long nativeKeyAddress(int id) {
    if (id <= 0) {
      return 0L;
    }
    int zeroBased = id - 1;
    int superpage = zeroBased >>> RECORD_SHIFT;
    int slot = zeroBased & PAGE_MASK;
    long[][] pages = nativeKeyAddressPages;
    if (superpage >= pages.length || pages[superpage] == null) {
      return 0L;
    }
    return pages[superpage][slot];
  }

  private void setNativeKeyAddress(int id, long nativeKeyAddress) {
    int zeroBased = id - 1;
    int superpage = zeroBased >>> RECORD_SHIFT;
    int slot = zeroBased & PAGE_MASK;
    nativeKeyAddressPages[superpage][slot] = nativeKeyAddress;
  }

  private void indexEntry(Entry entry) {
    if (entry == null) {
      throw new NullPointerException("entry");
    }
    Entry current = data.get(entry);
    if (ownsData) {
      if (current == null) {
        current = data.putIfAbsent(entry, entry);
      }
      if (current != null && current != entry) {
        throw new IllegalStateException("standalone maintenance index contains another Entry");
      }
    } else if (current != entry) {
      throw new IllegalStateException("entry is not the authoritative CHM value");
    }
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

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("entry links are closed");
    }
  }

  /** Reusable actor-confined CHM probe; native address is the maintenance identity. */
  private static final class EntryResolver extends Entry {
    private final ConcurrentHashMap<Entry, Entry> data;
    private long nativeKeyAddress;
    private int keyHash;

    private EntryResolver(ConcurrentHashMap<Entry, Entry> data) {
      super(0L, 0, 0L);
      this.data = data;
    }

    private Entry resolve(long nativeKeyAddress, int keyHash) {
      this.nativeKeyAddress = nativeKeyAddress;
      this.keyHash = keyHash;
      return data.get(this);
    }

    @Override
    public int hashCode() {
      return keyHash;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Entry && ((Entry) other).nativeKeyAddress == nativeKeyAddress;
    }
  }
}
