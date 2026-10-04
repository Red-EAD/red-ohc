package com.red.ohc.runtime;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.RetirementSegment;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
  static final int READER_EXITED_LOOKUP = 1;
  static final int READER_EXITED_VALUES = 1 << 1;
  static final long ACCESS_SAMPLE_MASK = 15L;
  private static final long NO_WRITE_TIMESTAMP = Long.MIN_VALUE;
  public static final int RESIDENCE_SAMPLE_INTERVAL = 1_024;
  private static final int MAX_REUSABLE_BULK_KEYS = 4_096;
  private static final int RELEASE_PAGE_MEMO_CAPACITY = 256;
  public byte[] keyBytes = new byte[64];
  public ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
  public LookupKey lookupKey = new LookupKey();
  public final ReaderSlot slot = new ReaderSlot();
  private DirectValueView topLevelDirectView;
  private DirectValueView[] directViews;
  private int directViewDepth;
  private int readerDepth;
  /** Number of active reader scopes that remain inside the value-protection region. */
  private int valueProtectionDepth;
  private long readerSeq;
  private int userCallbackDepth;
  private ByteBuffer writableValueBuffer;
  private ByteBuffer[] readOnlyValueBuffers;
  private int readOnlyValueDepth;
  private Set<Entry>[] bulkEntrySets;
  private int[] bulkEntrySetCapacities;
  private boolean[] reusableBulkEntrySets;
  private int bulkEntrySetDepth;

  private WriterState writerState;
  private long readSequence;
  private long accessSequence;
  private int bulkReadDepth;
  private boolean bulkReadChanged;
  private int replacementSampleCounter;
  private long writeCreatedAtMillis = NO_WRITE_TIMESTAMP;
  private long writeMonotonicNowNanos = NO_WRITE_TIMESTAMP;
  private final IdentityRemoval identityRemoval = new IdentityRemoval();
  private RetirementSegment.Reservation retirementReservation;
  private long[] releaseGroupKeys;
  private int[] releaseGroupCounts;
  private int[] releaseGroupOffsets;
  private int[] releaseGroupPositions;
  private int[] releaseGroupSlots;
  private int[] releaseGroupIndexes;
  private long[] releaseGroupBytes;
  private int[] releaseEntryGroupSlots;
  // Allocator-private Page references; NativeMemory clears them even when a batch fails.
  private Object[] releaseGroupPages;
  private int[] releaseEntrySlots;
  private int releaseBatchRecords;
  private long releaseBatchBytes;
  private ReleasePageMemo releasePageMemo;
  // Turn-level merged release scratch: per-record provenance across one reclaim wave plus
  // per-segment progress outputs. Indexed by wave-relative record/segment positions.
  private int[] releaseTurnSegmentIndexes;
  private long[] releaseTurnEntryWeights;
  private int[] releaseTurnClearedRecords;
  private long[] releaseTurnClearedBytes;
  private int[] releaseTurnPreZeroRecords;
  /** Opaque per-thread reclaim-wave bookkeeping owned by the retirement journal. */
  private Object releaseTurnWave;
  /** Per-thread register spill for the whole-page fast path's slot mask (8 bitmap words). */
  private final long[] releaseFreeMask = new long[8];
  /** Direct-mapped pageKey→group-slot cache for the grouped release decode. */
  private long[] releaseGroupCacheKeys;
  private int[] releaseGroupCacheSlots;

  public ThreadContext(WriterArena writerArena) {
    if (writerArena != null) {
      throw new IllegalArgumentException("writer resources must be registry-owned");
    }
  }

  /** Removes a CHM mapping only when its value is the exact Entry observed by this writer. */
  public boolean removeEntryIfSame(
      ConcurrentHashMap<Entry, Entry> data, Entry expected) {
    return identityRemoval.remove(data, expected);
  }

  /** Reusable CHM remapping function; one instance is retained by each writer context. */
  public static final class IdentityRemoval implements BiFunction<Entry, Entry, Entry> {
    private Entry expected;
    private boolean removed;

    public boolean remove(ConcurrentHashMap<Entry, Entry> data, Entry expected) {
      if (data == null || expected == null) {
        throw new NullPointerException();
      }
      this.expected = expected;
      this.removed = false;
      try {
        data.compute(expected, this);
        return removed;
      } finally {
        this.expected = null;
      }
    }

    @Override
    public Entry apply(Entry ignoredKey, Entry current) {
      if (current == expected) {
        removed = true;
        return null;
      }
      return current;
    }
  }

  private static final class WriterState {
    private WriterResource resource;
    private boolean writerEntered;
  }




  public static void verifyNativeByteBufferSupported() {
    NativeByteBuffer.verifySupported();
  }

  public void ensureKey(int length) {
    if (keyBytes.length < length) {
      keyBytes = new byte[round(length)];
      keyBuffer = ByteBuffer.wrap(keyBytes);
    }
  }




  public ByteBuffer keyBuffer(int length) {
    keyBuffer.clear();
    keyBuffer.limit(length);
    return keyBuffer;
  }

  /** Reuses the direct-buffer shell while each invocation still invalidates it before returning. */
  public ByteBuffer writableValueBuffer(long address, int length) {
    if (writableValueBuffer == null) {
      writableValueBuffer = NativeByteBuffer.writable(address, length);
    } else {
      NativeByteBuffer.writableTrusted(writableValueBuffer, address, length);
    }
    return writableValueBuffer;
  }

  /** Reuses read-only native views while preserving nested deserialize calls by depth. */
  public ByteBuffer readOnlyValueBuffer(long address, int length) {
    if (readOnlyValueBuffers == null) {
      readOnlyValueBuffers = new ByteBuffer[4];
    }
    if (readOnlyValueDepth == readOnlyValueBuffers.length) {
      ByteBuffer[] expanded = new ByteBuffer[readOnlyValueBuffers.length << 1];
      System.arraycopy(readOnlyValueBuffers, 0, expanded, 0, readOnlyValueBuffers.length);
      readOnlyValueBuffers = expanded;
    }
    ByteBuffer buffer = readOnlyValueBuffers[readOnlyValueDepth];
    if (buffer == null) {
      buffer = NativeByteBuffer.readOnly(address, length);
      readOnlyValueBuffers[readOnlyValueDepth] = buffer;
    } else {
      NativeByteBuffer.readOnlyTrusted(buffer, address, length);
    }
    readOnlyValueDepth++;
    return buffer;
  }

  public void releaseReadOnlyValueBuffer() {
    if (readOnlyValueDepth <= 0) {
      throw new IllegalStateException("read-only value buffer is not entered");
    }
    ByteBuffer buffer = readOnlyValueBuffers[--readOnlyValueDepth];
    NativeByteBuffer.invalidateTrusted(buffer);
  }

  public void invalidateWritableValueBuffer(ByteBuffer buffer) {
    NativeByteBuffer.invalidateTrusted(buffer);
  }

  public WriterArena writer() {
    WriterState state = writerState;
    if (state == null || state.resource == null) {
      throw new IllegalStateException("writer resources are not bound");
    }
    return state.resource.arena();
  }

  /** Reusable writer-side retirement reservation; no allocation occurs on replacement. */
  public RetirementSegment.Reservation retirementReservation() {
    if (retirementReservation == null) {
      retirementReservation = new RetirementSegment.Reservation();
    }
    return retirementReservation;
  }

  /** Ensures reusable grouping storage for actor-side page batch release. */
  public void ensureReleaseBatchScratch(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("release batch scratch capacity must be positive");
    }
    int tableCapacity = 1;
    while (tableCapacity < capacity * 2 && tableCapacity < (1 << 30)) {
      tableCapacity <<= 1;
    }
    if (releaseGroupKeys != null
        && releaseGroupKeys.length >= tableCapacity
        && releaseGroupIndexes.length >= capacity
        && releaseGroupBytes.length >= tableCapacity
        && releaseEntryGroupSlots.length >= capacity) {
      return;
    }
    releaseGroupKeys = new long[tableCapacity];
    releaseGroupCounts = new int[tableCapacity];
    releaseGroupOffsets = new int[tableCapacity];
    releaseGroupPositions = new int[tableCapacity];
    releaseGroupSlots = new int[capacity];
    releaseGroupIndexes = new int[capacity];
    releaseGroupBytes = new long[tableCapacity];
    releaseEntryGroupSlots = new int[capacity];
    releaseGroupPages = new Object[tableCapacity];
    releaseEntrySlots = new int[capacity];
  }

  public long[] releaseGroupKeys() {
    return releaseGroupKeys;
  }

  public int[] releaseGroupCounts() {
    return releaseGroupCounts;
  }

  public int[] releaseGroupOffsets() {
    return releaseGroupOffsets;
  }

  public int[] releaseGroupPositions() {
    return releaseGroupPositions;
  }

  public int[] releaseGroupSlots() {
    return releaseGroupSlots;
  }

  public int[] releaseGroupIndexes() {
    return releaseGroupIndexes;
  }

  public long[] releaseGroupBytes() {
    return releaseGroupBytes;
  }

  public int[] releaseEntryGroupSlots() {
    return releaseEntryGroupSlots;
  }

  /** Opaque allocator references scoped to one release call, never retained between batches. */
  public Object[] releaseGroupPages() {
    return releaseGroupPages;
  }

  public int[] releaseEntrySlots() {
    return releaseEntrySlots;
  }

  /** Resets the primitive progress counters used by an idempotent grouped native release. */
  public void resetReleaseBatchProgress() {
    releaseBatchRecords = 0;
    releaseBatchBytes = 0L;
  }

  /** Records a group after its native slots have been accepted by the allocator. */
  public void recordReleaseBatchProgress(int records, long bytes) {
    if (records < 0 || bytes < 0L) {
      throw new IllegalArgumentException("invalid release batch progress");
    }
    releaseBatchRecords += records;
    releaseBatchBytes += bytes;
  }

  public int releaseBatchRecords() {
    return releaseBatchRecords;
  }

  public long releaseBatchBytes() {
    return releaseBatchBytes;
  }

  /** Begins one actor reclaim scope for the bounded allocator-page descriptor memo. */
  public void beginReleasePageMemo() {
    ReleasePageMemo memo = releasePageMemo;
    if (memo == null) {
      memo = new ReleasePageMemo();
      releasePageMemo = memo;
    }
    memo.begin();
  }

  /**
   * Ensures reusable storage for one merged turn release wave: {@code recordCapacity} decode
   * slots (grouping scratch plus per-record provenance) and {@code segmentCapacity} per-segment
   * progress slots. Sizing follows {@link #ensureReleaseBatchScratch}; arrays only ever grow.
   */
  public void ensureReleaseTurnScratch(int recordCapacity, int segmentCapacity) {
    if (recordCapacity <= 0 || segmentCapacity <= 0) {
      throw new IllegalArgumentException("release turn scratch capacities must be positive");
    }
    ensureReleaseBatchScratch(recordCapacity);
    if (releaseTurnSegmentIndexes != null
        && releaseTurnSegmentIndexes.length >= recordCapacity
        && releaseTurnEntryWeights.length >= recordCapacity
        && releaseTurnClearedRecords != null
        && releaseTurnClearedRecords.length >= segmentCapacity
        && releaseTurnClearedBytes.length >= segmentCapacity
        && releaseTurnPreZeroRecords.length >= segmentCapacity) {
      return;
    }
    if (releaseTurnSegmentIndexes == null || releaseTurnSegmentIndexes.length < recordCapacity) {
      releaseTurnSegmentIndexes = new int[recordCapacity];
      releaseTurnEntryWeights = new long[recordCapacity];
    }
    if (releaseTurnClearedRecords == null || releaseTurnClearedRecords.length < segmentCapacity) {
      releaseTurnClearedRecords = new int[segmentCapacity];
      releaseTurnClearedBytes = new long[segmentCapacity];
      releaseTurnPreZeroRecords = new int[segmentCapacity];
    }
  }

  /** Zeroes the wave's per-segment progress outputs; self-ensuring (fresh arrays are zeroed). */
  public void resetReleaseTurnProgress(int segmentCount) {
    if (releaseTurnClearedRecords == null || releaseTurnClearedRecords.length < segmentCount) {
      releaseTurnClearedRecords = new int[segmentCount];
      releaseTurnClearedBytes = new long[segmentCount];
      releaseTurnPreZeroRecords = new int[segmentCount];
      return;
    }
    for (int index = 0; index < segmentCount; index++) {
      releaseTurnClearedRecords[index] = 0;
      releaseTurnClearedBytes[index] = 0L;
      releaseTurnPreZeroRecords[index] = 0;
    }
  }

  public int[] releaseTurnSegmentIndexes() {
    return releaseTurnSegmentIndexes;
  }

  public long[] releaseTurnEntryWeights() {
    return releaseTurnEntryWeights;
  }

  public int[] releaseTurnClearedRecords() {
    return releaseTurnClearedRecords;
  }

  public long[] releaseTurnClearedBytes() {
    return releaseTurnClearedBytes;
  }

  public int[] releaseTurnPreZeroRecords() {
    return releaseTurnPreZeroRecords;
  }

  /** Opaque, journal-owned claim bookkeeping for the merged turn release; never inspected here. */
  public Object releaseTurnWave() {
    return releaseTurnWave;
  }

  /** Reusable 8-word slot-mask spill for {@code Page.freeValidatedSlotsTurn}. */
  public long[] releaseFreeMask() {
    return releaseFreeMask;
  }

  /** Number of release group-cache slots; a power of two, indexed by mixed page key. */
  public static final int RELEASE_GROUP_CACHE_ENTRIES = 256;

  /** Clears the page-group cache at the start of a grouped release call. */
  public void resetReleaseGroupCache() {
    if (releaseGroupCacheKeys == null) {
      releaseGroupCacheKeys = new long[RELEASE_GROUP_CACHE_ENTRIES];
      releaseGroupCacheSlots = new int[RELEASE_GROUP_CACHE_ENTRIES];
      return;
    }
    java.util.Arrays.fill(releaseGroupCacheKeys, 0L);
    java.util.Arrays.fill(releaseGroupCacheSlots, 0);
  }

  public long[] releaseGroupCacheKeys() {
    return releaseGroupCacheKeys;
  }

  public int[] releaseGroupCacheSlots() {
    return releaseGroupCacheSlots;
  }

  public void releaseTurnWave(Object wave) {
    this.releaseTurnWave = wave;
  }

  /** Returns an opaque Page only when the full page key matches in the current reclaim scope. */
  public Object releasePageMemoLookup(long pageKey) {
    ReleasePageMemo memo = releasePageMemo;
    return memo == null ? null : memo.lookup(pageKey);
  }

  /** Remembers one fully validated Page; outside a reclaim scope this is intentionally a no-op. */
  public void releasePageMemoRemember(long pageKey, Object page) {
    ReleasePageMemo memo = releasePageMemo;
    if (memo != null) {
      memo.remember(pageKey, page);
    }
  }

  /** Invalidates the exact descriptor before a page can be republished as available. */
  public void releasePageMemoInvalidate(long pageKey, Object page) {
    ReleasePageMemo memo = releasePageMemo;
    if (memo != null) {
      memo.invalidate(pageKey, page);
    }
  }

  /** Clears every Page reference touched by the current reclaim scope. */
  public void endReleasePageMemo() {
    ReleasePageMemo memo = releasePageMemo;
    if (memo != null) {
      memo.end();
    }
  }

  private static final class ReleasePageMemo {
    private final long[] pageKeys = new long[RELEASE_PAGE_MEMO_CAPACITY];
    private final Object[] pages = new Object[RELEASE_PAGE_MEMO_CAPACITY];
    private final int[] touchedSlots = new int[RELEASE_PAGE_MEMO_CAPACITY];
    private final boolean[] touched = new boolean[RELEASE_PAGE_MEMO_CAPACITY];
    private int touchedCount;
    private boolean active;

    private void begin() {
      if (active) {
        throw new IllegalStateException("release page memo is already active");
      }
      active = true;
    }

    private Object lookup(long pageKey) {
      if (!active) {
        return null;
      }
      int slot = slot(pageKey);
      return pageKeys[slot] == pageKey ? pages[slot] : null;
    }

    private void remember(long pageKey, Object page) {
      if (!active) {
        return;
      }
      if (pageKey == 0L || page == null) {
        throw new IllegalArgumentException("release page memo requires a page key and descriptor");
      }
      int slot = slot(pageKey);
      if (!touched[slot]) {
        touched[slot] = true;
        touchedSlots[touchedCount++] = slot;
      }
      pageKeys[slot] = pageKey;
      pages[slot] = page;
    }

    private void invalidate(long pageKey, Object page) {
      if (!active) {
        return;
      }
      int slot = slot(pageKey);
      if (pageKeys[slot] == pageKey && pages[slot] == page) {
        pageKeys[slot] = 0L;
        pages[slot] = null;
      }
    }

    private void end() {
      if (!active) {
        return;
      }
      for (int index = 0; index < touchedCount; index++) {
        int slot = touchedSlots[index];
        pageKeys[slot] = 0L;
        pages[slot] = null;
        touched[slot] = false;
      }
      touchedCount = 0;
      active = false;
    }

    private static int slot(long pageKey) {
      return Long.hashCode(pageKey) & (RELEASE_PAGE_MEMO_CAPACITY - 1);
    }
  }

  /** Returns true once per fixed replacement interval at publication time. */
  public boolean sampleNextReplacement() {
    replacementSampleCounter++;
    if (replacementSampleCounter < RESIDENCE_SAMPLE_INTERVAL) {
      return false;
    }
    replacementSampleCounter = 0;
    return true;
  }

  public boolean hasWriterResources() {
    WriterState state = writerState;
    return state != null && state.resource != null;
  }

  public WriterLifecycleLane lifecycleLane() {
    WriterState state = writerState;
    if (state == null || state.resource == null) {
      throw new IllegalStateException("writer lifecycle lane is not bound");
    }
    return state.resource.lifecycleLane();
  }

  public RetirementJournal.Lane retirementLane() {
    WriterState state = writerState;
    if (state == null || state.resource == null) {
      throw new IllegalStateException("writer retirement lane is not bound");
    }
    return state.resource.retirementLane();
  }

  public void bindWriterResource(WriterResource resource) {
    if (resource == null) {
      throw new IllegalArgumentException("writer resource is required");
    }
    WriterState state = ensureWriterState();
    if (state.resource != null) {
      if (state.resource != resource) {
        throw new IllegalStateException("writer resources are already bound");
      }
      return;
    }
    state.resource = resource;
  }

  public WriterResource writerResource() {
    WriterState state = writerState;
    if (state == null || state.resource == null) {
      throw new IllegalStateException("writer resource is not bound");
    }
    return state.resource;
  }

  public boolean tryEnterWriter() {
    WriterState state = ensureWriterState();
    if (state.writerEntered) {
      return false;
    }
    state.writerEntered = true;
    return true;
  }

  public void exitWriter() {
    WriterState state = writerState;
    if (state == null || !state.writerEntered) {
      throw new IllegalStateException("writer is not entered");
    }
    state.writerEntered = false;
  }

  public boolean isWriterEntered() {
    WriterState state = writerState;
    return state != null && state.writerEntered;
  }

  /** Reuses the wall-clock sample taken while resolving the current write deadline. */
  public void writeCreatedAtMillis(long createdAtMillis) {
    writeCreatedAtMillis = createdAtMillis;
  }

  public long writeCreatedAtMillis() {
    return writeCreatedAtMillis;
  }

  /** Reuses the monotonic sample taken while resolving the current write deadline. */
  public void writeMonotonicNowNanos(long nowNanos) {
    writeMonotonicNowNanos = nowNanos;
  }

  public long writeMonotonicNowNanos() {
    return writeMonotonicNowNanos;
  }

  public boolean isRegistered() {
    return slot.registry != null;
  }


  public int readerDepth() {
    return readerDepth;
  }

  /** Depth-0 entry: one fenced odd-seq store; the fence orders it before this op's native loads. */
  void beginReaderOp(boolean protectsValues) {
    readerSeq = (readerSeq + 1L) | 1L;
    NativeMemory.putLongVolatile(
        slot.seqAddress,
        protectsValues ? readerSeq | ReaderRegistry.VALUE_PROTECTION_BIT : readerSeq);
  }

  /** Nested value entry inside an already-odd op: re-store the same odd value with the bit. */
  void upgradeReaderOpValueBit() {
    NativeMemory.putLongVolatile(
        slot.seqAddress, readerSeq | ReaderRegistry.VALUE_PROTECTION_BIT);
  }

  /** Depth-0 exit: plain even store, then the actor's wake marker is consumed by the guard. */
  void endReaderOp() {
    readerSeq = (readerSeq + 2L) & ~1L;
    NativeMemory.putLongRelease(slot.seqAddress, readerSeq);
  }

  public boolean isUserCallbackActive() {
    return userCallbackDepth != 0;
  }

  public void enterUserCallback() {
    userCallbackDepth++;
  }

  public void exitUserCallback() {
    if (userCallbackDepth <= 0) {
      throw new IllegalStateException("cache callback is not active");
    }
    userCallbackDepth--;
  }

  /** Enters a nested lookup guard and returns whether value protection became newly active. */
  public boolean enterReader(boolean protectsValues) {
    readerDepth++;
    if (protectsValues || valueProtectionDepth != 0) {
      return valueProtectionDepth++ == 0;
    }
    return false;
  }

  public int exitReader() {
    if (readerDepth <= 0) {
      throw new IllegalStateException("reader guard is not entered");
    }
    boolean protectedValues = valueProtectionDepth != 0;
    readerDepth--;
    if (protectedValues) {
      valueProtectionDepth--;
    }
    int state = 0;
    if (protectedValues && valueProtectionDepth == 0) {
      state |= READER_EXITED_VALUES;
    }
    if (readerDepth == 0) {
      state |= READER_EXITED_LOOKUP;
    }
    return state;
  }

  public DirectValueView pushDirectView(long address, int length) {
    DirectValueView view;
    if (directViewDepth == 0) {
      view = topLevelDirectView;
      if (view == null) {
        view = new DirectValueView();
        topLevelDirectView = view;
      }
    } else {
      int nestedDepth = directViewDepth - 1;
      if (directViews == null) {
        directViews = new DirectValueView[4];
      }
      if (nestedDepth == directViews.length) {
        DirectValueView[] expanded = new DirectValueView[directViews.length << 1];
        System.arraycopy(directViews, 0, expanded, 0, directViews.length);
        directViews = expanded;
      }
      view = directViews[nestedDepth];
      if (view == null) {
        view = new DirectValueView();
        directViews[nestedDepth] = view;
      }
    }
    directViewDepth++;
    view.reset(address, length, this);
    return view;
  }

  public void popDirectView() {
    if (directViewDepth <= 0) {
      throw new IllegalStateException("direct view is not entered");
    }
    DirectValueView view;
    if (directViewDepth == 1) {
      directViewDepth = 0;
      view = topLevelDirectView;
    } else {
      directViewDepth--;
      view = directViews[directViewDepth - 1];
    }
    try {
      if (view.hasMaterializedByteBuffer()) {
        releaseReadOnlyValueBuffer();
      }
    } finally {
      view.reset(0L, 0, null);
    }
  }

  /** Acquires per-depth hit-Entry identity tracking without retaining a huge table. */
  public Set<Entry> acquireBulkEntries(int expected) {
    if (expected < 0) {
      throw new IllegalArgumentException("negative bulk entry count");
    }
    ensureBulkEntryDepth();
    int depth = bulkEntrySetDepth++;
    boolean reusable = expected <= MAX_REUSABLE_BULK_KEYS;
    Set<Entry> entries = bulkEntrySets[depth];
    if (!reusable || entries == null || bulkEntrySetCapacities[depth] < expected) {
      entries =
          Collections.newSetFromMap(
              new IdentityHashMap<>(Math.min(expected, MAX_REUSABLE_BULK_KEYS)));
      bulkEntrySets[depth] = entries;
      bulkEntrySetCapacities[depth] = reusable ? expected : 0;
    } else {
      entries.clear();
    }
    reusableBulkEntrySets[depth] = reusable;
    return entries;
  }

  /** Releases a duplicate table while preserving nested bulk-call isolation. */
  public void releaseBulkEntries(Set<Entry> entries) {
    if (bulkEntrySetDepth <= 0) {
      throw new IllegalStateException("bulk entry set is not acquired");
    }
    int depth = --bulkEntrySetDepth;
    if (bulkEntrySets[depth] != entries) {
      throw new IllegalStateException("bulk entry set release order mismatch");
    }
    entries.clear();
    if (!reusableBulkEntrySets[depth]) {
      bulkEntrySets[depth] = null;
      bulkEntrySetCapacities[depth] = 0;
    }
    reusableBulkEntrySets[depth] = false;
  }

  private void ensureBulkEntryDepth() {
    if (bulkEntrySets == null) {
      bulkEntrySets = new Set[2];
      bulkEntrySetCapacities = new int[2];
      reusableBulkEntrySets = new boolean[2];
    }
    if (bulkEntrySetDepth < bulkEntrySets.length) {
      return;
    }
    int expandedLength = bulkEntrySets.length << 1;
    Set<Entry>[] expanded = new Set[expandedLength];
    int[] expandedCapacities = new int[expandedLength];
    boolean[] expandedReusable = new boolean[expandedLength];
    System.arraycopy(bulkEntrySets, 0, expanded, 0, bulkEntrySets.length);
    System.arraycopy(bulkEntrySetCapacities, 0, expandedCapacities, 0, bulkEntrySetCapacities.length);
    System.arraycopy(reusableBulkEntrySets, 0, expandedReusable, 0, reusableBulkEntrySets.length);
    bulkEntrySets = expanded;
    bulkEntrySetCapacities = expandedCapacities;
    reusableBulkEntrySets = expandedReusable;
  }

  public long hit() {
    slot.localHits++;
    return ++readSequence;
  }

  public long miss() {
    slot.localMisses++;
    return ++readSequence;
  }

  public void beginBulkRead() {
    if (bulkReadDepth == 0) {
      bulkReadChanged = false;
    }
    bulkReadDepth++;
  }

  public void bulkHit(Entry entry) {
    slot.localHits++;
    readSequence++;
    if ((++accessSequence & ACCESS_SAMPLE_MASK) == 0L) {
      long observedValueAddress = entry.valueAddress;
      long observedGeneration = entry.generation();
      int observedPolicyState = entry.policyState();
      accessRing().offer(entry, observedValueAddress, observedGeneration, observedPolicyState);
    }
    bulkReadChanged = true;
  }

  public void bulkMiss() {
    slot.localMisses++;
    readSequence++;
    bulkReadChanged = true;
  }

  /** Publishes one batch of read counters instead of one per key. */
  public void finishBulkRead() {
    if (bulkReadDepth == 0) {
      return;
    }
    if (--bulkReadDepth != 0) {
      return;
    }
    if (bulkReadChanged) {
      publish();
    }
    bulkReadChanged = false;
  }

  /** Delivers every sampled hit to this thread's actor-owned access ring. */

  /** Delivers every sampled hit to this thread's actor-owned access ring. */
  public void access(Entry entry) {
    access(entry, entry == null ? 0L : entry.valueAddress);
  }

  /** Delivers a sampled hit with the value token observed by the lookup. */
  public void access(Entry entry, long observedValueAddress) {
    if ((++accessSequence & ACCESS_SAMPLE_MASK) != 0L) {
      return;
    }
    publishSampledAccess(entry, observedValueAddress);
  }

  private void publishSampledAccess(Entry entry, long observedValueAddress) {
    long observedGeneration = entry.generation();
    int observedPolicyState = entry.policyState();
    accessRing().offer(entry, observedValueAddress, observedGeneration, observedPolicyState);
  }

  public void finishRead(long sequence) {
    if ((sequence & 1023L) != 0L) {
      return;
    }
    publishReadCounters();
  }

  private void publishReadCounters() {
    publish();
  }

  /** Flushes sub-threshold read counters when a control-plane barrier is requested. */
  public void flushRead() {
    publish();
  }

  public void publish() {
    slot.publishedHits = slot.localHits;
    slot.publishedMisses = slot.localMisses;
  }

  private AccessRing accessRing() {
    AccessRing ring = slot.access;
    if (ring == null) {
      ring = new AccessRing(slot::signalAccess);
      slot.access = ring;
    }
    return ring;
  }

  private WriterState ensureWriterState() {
    WriterState state = writerState;
    if (state == null) {
      state = new WriterState();
      writerState = state;
    }
    return state;
  }

  private static int round(int value) {
    if (value < 0 || value > (1 << 30)) {
      throw new IllegalArgumentException("serialized key is too large: " + value);
    }
    long size = 64L;
    while (size < value) {
      size <<= 1;
    }
    return (int) size;
  }

}
