package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

import sun.misc.Unsafe;

import com.red.ohc.runtime.ThreadContext;

/** Native allocation and primitive access. */
public final class NativeMemory {
  static final Unsafe U;
  static final long BYTE_ARRAY_BASE;
  /** Cached once so bounded CAS paths never query the runtime on their hot path. */
  public static final int LOGICAL_CPU_COUNT =
      Math.max(1, Runtime.getRuntime().availableProcessors());

  /** Start block-wise comparison only when the key is large enough to amortize aggregation. */
  static final int BULK_EQUALS_THRESHOLD = 128;

  /** Eight machine words form one comparison block (64 bytes). */
  static final int BULK_EQUALS_LONGS = 8;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      U = (Unsafe) field.get(null);
      BYTE_ARRAY_BASE = U.arrayBaseOffset(byte[].class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final long OBJECT_ARRAY_BASE = U.arrayBaseOffset(Object[].class);
  private static final long OBJECT_ARRAY_SCALE = U.arrayIndexScale(Object[].class);

  /** Volatile-access element offset valid for any reference array. */
  private static long objectArrayOffset(int index) {
    return OBJECT_ARRAY_BASE + (long) index * OBJECT_ARRAY_SCALE;
  }

  private NativeMemory() {}

  public static final class Memory {
    private static final long ALIGNMENT_PADDING_BYTES = 128L;
    /** Merged-release marker: direct-class record decoded, free deferred to the publish pass. */
    private static final int DIRECT_RELEASE_PENDING = -2;
    private final NativeAllocator allocator;
    private final AtomicLong allocated = new AtomicLong();
    private final AtomicLong rawAllocations = new AtomicLong();
    private final AtomicLong entryAllocations = new AtomicLong();
    private final AtomicLong pageAllocatedCount = new AtomicLong();
    private final LongAdder pageReusedCount = new LongAdder();
    private final LongAdder retainedPageCount = new LongAdder();
    private final LongAdder retainedPageConsumedCount = new LongAdder();
    private final LongAdder[] pageReadyCounts = newReadyPageCounts();
    private final AtomicLong pageTrimmedCount = new AtomicLong();
    private final AtomicLong trimmedBytesTotal = new AtomicLong();
    private final AtomicLong smallAllocationFallbacks = new AtomicLong();
    private final AtomicLong directEntryAllocations = new AtomicLong();
    private final AtomicInteger nextArenaId = new AtomicInteger(1);
    private final AtomicBoolean trimOwner = new AtomicBoolean();
    private static final int READY_STACK_STRIDE = 8;
    /**
     * Packed ABA-safe page-id heads; each independently mutated size-class head owns one 64-byte
     * lane so unrelated page traffic cannot bounce the same cache line.
     */
    private final AtomicLongArray readyPageStacks =
        new AtomicLongArray(SizeClasses.count() * READY_STACK_STRIDE);
    private static final int PAGE_CHUNK_BITS = 16;
    private static final int PAGE_CHUNK_SIZE = 1 << PAGE_CHUNK_BITS;
    private static final int PAGE_CHUNK_COUNT = 1 << (WriterArena.HANDLE_PAGE_BITS - PAGE_CHUNK_BITS);
    private static final int PAGE_VERSION_MASK =
        (1 << WriterArena.HANDLE_GENERATION_BITS) - 1;
    private final AtomicLong nextPageId = new AtomicLong(1L);

    /**
     * Sparse page-id free stack; nodes are indices in fixed primitive arrays, never Page objects.
     * The low 32 bits are the page id; the high 32 bits advance on every stack mutation.
     */
    private final AtomicLong freePageIdHead = new AtomicLong();

    private final IntChunkTable freePageIdNext = new IntChunkTable();
    private final IntChunkTable readyPageNext = new IntChunkTable();
    private final IntChunkTable pageVersions = new IntChunkTable();
    private final WriterArena.Page[][] pageChunks = new WriterArena.Page[PAGE_CHUNK_COUNT][];
    private final AtomicInteger pooledPageCount = new AtomicInteger();
    /** Each directly mapped page is one VMA; stay far below the default vm.max_map_count. */
    private static final int PAGE_MAPPING_BUDGET = 45_000;
    private final AtomicLong directMappedPageCount = new AtomicLong();

    public Memory() {
      this(new NativeAllocator());
    }

    Memory(NativeAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    private static LongAdder[] newReadyPageCounts() {
      LongAdder[] counts = new LongAdder[SizeClasses.count()];
      for (int sizeClass = 0; sizeClass < counts.length; sizeClass++) {
        counts[sizeClass] = new LongAdder();
      }
      return counts;
    }

    public long allocate(long bytes) {
      long address = allocateRaw(bytes);
      U.setMemory(address, bytes, (byte) 0);
      return address;
    }

    /** Allocates a stable aligned range while charging the complete allocator-owned block. */
    public long allocateAligned(long bytes, long alignment) {
      long rawBytes = alignedAllocationBytes(bytes, alignment);
      long rawAddress = allocateRaw(rawBytes);
      try {
        long candidate = Math.addExact(rawAddress, Long.BYTES);
        long alignedAddress =
            Math.addExact(candidate, alignment - 1L) & ~(alignment - 1L);
        U.putLong(alignedAddress - Long.BYTES, rawAddress);
        return alignedAddress;
      } catch (Throwable failure) {
        free(rawAddress, rawBytes);
        throw failure;
      }
    }

    /** Frees a range returned by {@link #allocateAligned(long, long)}. */
    public void freeAligned(long address, long bytes, long alignment) {
      if (address == 0L) {
        return;
      }
      long rawBytes = alignedAllocationBytes(bytes, alignment);
      long rawAddress = U.getLong(address - Long.BYTES);
      if (rawAddress == 0L) {
        throw new IllegalStateException("aligned allocation has no raw base");
      }
      free(rawAddress, rawBytes);
    }

    private static long alignedAllocationBytes(long bytes, long alignment) {
      if (bytes <= 0L) {
        throw new IllegalArgumentException("bytes must be positive");
      }
      if (alignment <= 0L || (alignment & (alignment - 1L)) != 0L) {
        throw new IllegalArgumentException("alignment must be a positive power of two");
      }
      try {
        return Math.addExact(Math.addExact(bytes, alignment), Long.BYTES);
      } catch (ArithmeticException overflow) {
        throw new IllegalArgumentException("aligned allocation is too large", overflow);
      }
    }

    public long allocateRaw(long bytes) {
      if (bytes <= 0L) {
        throw new IllegalArgumentException("bytes must be positive");
      }
      allocated.getAndAdd(bytes);
      try {
        long address = allocator.allocate(bytes);
        if (address == 0L) {
          throw new IllegalStateException("native allocator returned address 0");
        }
        rawAllocations.incrementAndGet();
        return address;
      } catch (Throwable failure) {
        allocated.addAndGet(-bytes);
        throw failure;
      }
    }

    public long tryAllocateRaw(long bytes) {
      if (bytes <= 0L) {
        throw new IllegalArgumentException("bytes must be positive");
      }
      allocated.getAndAdd(bytes);
      try {
        long address = allocator.allocate(bytes);
        if (address == 0L) {
          allocated.addAndGet(-bytes);
          return 0L;
        }
        rawAllocations.incrementAndGet();
        return address;
      } catch (OutOfMemoryError exhausted) {
        allocated.addAndGet(-bytes);
        return 0L;
      } catch (Throwable failure) {
        allocated.addAndGet(-bytes);
        throw failure;
      }
    }

    public void free(long address, long bytes) {
      if (address == 0L) {
        return;
      }
      allocator.free(address);
      allocated.addAndGet(-bytes);
    }

    public long allocated() {
      return allocated.get();
    }


    public long rawAllocationCount() {
      return rawAllocations.get();
    }


    public long pageAllocatedCount() {
      return pageAllocatedCount.get();
    }

    public long pageReusedCount() {
      return pageReusedCount.sum();
    }

    public long retainedPageCount() {
      return retainedPageCount.sum();
    }

    public long retainedPageConsumedCount() {
      return retainedPageConsumedCount.sum();
    }

    public long pageReadyCount() {
      long ready = 0L;
      for (LongAdder count : pageReadyCounts) {
        ready += count.sum();
      }
      return ready;
    }

    public long pageReadyCount(int sizeClass) {
      return pageReadyCounts[sizeClass].sum();
    }

    public int pooledPageCount() {
      return pooledPageCount.get();
    }

    public long retainedPagesInUse() {
      return retainedPageCount.sum() - retainedPageConsumedCount.sum();
    }

    public long trimmedBytesTotal() {
      return trimmedBytesTotal.get();
    }

    public long pageTrimmedCount() {
      return pageTrimmedCount.get();
    }

    /**
     * Accumulates per-class in-use page usage for up to maxPages registered pages starting at the
     * given cursor. Returns the next cursor, or -1 once the whole page table has been visited.
     * Slot-steps are bounded so a sparse table cannot stall one call.
     */
    public long auditPageUsage(
        long cursor, int maxPages, long[] pages, long[] allocated, long[] freed) {
      long next = cursor;
      int visited = 0;
      int maxSteps = maxPages * 4;
      for (int steps = 0; visited < maxPages && steps < maxSteps; steps++) {
        int chunkIndex = (int) (next >>> 32);
        if (chunkIndex >= pageChunks.length) {
          return -1L;
        }
        WriterArena.Page[] chunk =
            (WriterArena.Page[]) U.getObjectVolatile(pageChunks, objectArrayOffset(chunkIndex));
        if (chunk == null) {
          next = ((long) (chunkIndex + 1)) << 32;
          continue;
        }
        int slotIndex = (int) next;
        if (slotIndex >= PAGE_CHUNK_SIZE) {
          next = ((long) (chunkIndex + 1)) << 32;
          continue;
        }
        WriterArena.Page page =
            (WriterArena.Page) U.getObjectVolatile(chunk, objectArrayOffset(slotIndex));
        if (page == null) {
          next++;
          continue;
        }
        if (page.sizeClass < pages.length) {
          pages[page.sizeClass]++;
          allocated[page.sizeClass] += page.auditAllocatedSlots();
          freed[page.sizeClass] += page.freedSlots;
        }
        visited++;
        next++;
      }
      return next;
    }

    public long smallAllocationFallbackCount() {
      return smallAllocationFallbacks.get();
    }

    public long directEntryAllocationCount() {
      return directEntryAllocations.get();
    }

    private long allocateEntryPage(int pageBytes) {
      long rawAddress = allocateRaw(pageBytes + ALIGNMENT_PADDING_BYTES);
      long address = (rawAddress + 64L) & ~63L;
      // The word immediately before the visible page is outside every slot and retains the raw
      // allocator base needed to free the aligned page.
      U.putLong(address - Long.BYTES, rawAddress);
      entryAllocations.incrementAndGet();
      pageAllocatedCount.incrementAndGet();
      return address;
    }

    private long allocateDirectMappedPage(int pageBytes) {
      long address = allocator.mapPage(pageBytes);
      allocated.addAndGet(pageBytes);
      rawAllocations.incrementAndGet();
      entryAllocations.incrementAndGet();
      pageAllocatedCount.incrementAndGet();
      return address;
    }

    public long allocateDirectEntry(long bytes) {
      long rawAddress = allocateRaw(bytes);
      long address = (rawAddress + 64L) & ~63L;
      U.putLong(address + 56L, rawAddress);
      entryAllocations.incrementAndGet();
      return address;
    }

    /** Returns the stable pooled-slot handle, or zero for direct allocations. */
    public static long entryAllocatorHandle(long entryAddress, long entryBytes) {
      if (entryAddress == 0L || SizeClasses.indexForEntry(entryBytes) < 0) {
        return 0L;
      }
      return U.getLong(entryAddress - WriterArena.PREFIX_BYTES + 56L);
    }

    private void freeDirectEntry(long block, long bytes) {
      long rawAddress = U.getLong(block + 56L);
      if (rawAddress == 0L) {
        throw new IllegalStateException("direct allocation has no raw base");
      }
      free(rawAddress, bytes);
    }

    private void freeEntryPageAllocation(WriterArena.Page page) {
      if (page.directMapped) {
        allocator.unmapPage(page.address, page.pageBytes);
        directMappedPageCount.decrementAndGet();
        allocated.addAndGet(-page.pageBytes);
        return;
      }
      long rawAddress = U.getLong(page.address - Long.BYTES);
      if (rawAddress == 0L) {
        throw new IllegalStateException("pooled page has no raw base");
      }
      free(rawAddress, page.pageBytes + ALIGNMENT_PADDING_BYTES);
    }

    public WriterArena newWriterArena() {
      int arenaId = nextArenaId.getAndIncrement();
      if (arenaId <= 0) {
        throw new IllegalStateException("writer arena id exhausted");
      }
      return new WriterArena(this);
    }

    WriterArena.Page tryStealAvailablePage(
        int sizeClass, WriterArena.SizeClassState newOwner) {
      for (;;) {
        WriterArena.Page page = popReadyPage(sizeClass);
        if (page == null) {
          return null;
        }
        if (page.sizeClass != sizeClass) {
          continue;
        }
        if (!isCurrentPage(page)) {
          continue;
        }
        WriterArena.SizeClassState expectedOwner = page.ownerClass;
        boolean acquired =
            expectedOwner == newOwner
                ? page.tryActivate(newOwner)
                : page.tryRebalance(expectedOwner, newOwner);
        if (acquired) {
          pageReusedCount.increment();
          return page;
        }
      }
    }

    /**
     * A queued page is only a candidate. The page table identity and generation must still match
     * before its owner/state CAS can make it active; this keeps a trimmed token from surviving a
     * page-id reuse.
     */
    private boolean isCurrentPage(WriterArena.Page page) {
      WriterArena.Page current = pageForId(page.id);
      return current == page && current.pageKey == page.pageKey;
    }

    void publishAvailablePage(WriterArena.Page page) {
      // Publish the diagnostic count before the token. A concurrent pop can observe the stack
      // immediately after the CAS, and must be able to balance the ready count.
      restoreReadyPage(page);
    }

    /** Publishes an owner-detached retained page directly to the shared ready stack. */
    void publishAvailablePageGlobal(WriterArena.Page page) {
      restoreReadyPage(page);
    }

    /** Cumulative observability events for the owner-private retention path. */
    void recordRetainedPage() {
      retainedPageCount.increment();
    }

    void recordRetainedPageConsumed() {
      retainedPageConsumedCount.increment();
    }

    void recordRetainedPageReleased() {
      retainedPageCount.decrement();
    }

    void recordPageReuse() {
      pageReusedCount.increment();
    }

    private void restoreReadyPage(WriterArena.Page page) {
      recordPageReady(page.sizeClass);
      try {
        pushReadyPage(page);
      } catch (Throwable failure) {
        pageReadyCounts[page.sizeClass].decrement();
        throw failure;
      }
    }

    private void pushReadyPage(WriterArena.Page page) {
      int sizeClass = page.sizeClass;
      int stackIndex = readyStackIndex(sizeClass);
      for (;;) {
        long current = readyPageStacks.get(stackIndex);
        int headId = readyPageId(current);
        readyPageNext.set(page.id, headId);
        long updated = nextReadyPageHead(current, page.id);
        if (readyPageStacks.compareAndSet(stackIndex, current, updated)) {
          return;
        }
      }
    }

    /** Pops one token from the per-size-class ABA-safe LIFO stack. */
    private WriterArena.Page popReadyPage(int sizeClass) {
      int stackIndex = readyStackIndex(sizeClass);
      for (;;) {
        long current = readyPageStacks.get(stackIndex);
        int headId = readyPageId(current);
        if (headId == 0) {
          return null;
        }
        int nextId = readyPageNext.get(headId);
        long updated = nextReadyPageHead(current, nextId);
        if (readyPageStacks.compareAndSet(stackIndex, current, updated)) {
          pageReadyCounts[sizeClass].decrement();
          WriterArena.Page page = pageForId(headId);
          if (page == null || page.id != headId) {
            continue;
          }
          return page;
        }
      }
    }

    private static int readyPageId(long head) {
      return (int) (head & WriterArena.PAGE_ID_MASK);
    }

    private static int readyStackIndex(int sizeClass) {
      return sizeClass * READY_STACK_STRIDE;
    }

    private static long nextReadyPageHead(long head, int pageId) {
      long stamp =
          ((head >>> WriterArena.PAGE_ID_BITS) + 1L)
              & (-1L >>> WriterArena.PAGE_ID_BITS);
      return (stamp << WriterArena.PAGE_ID_BITS) | (pageId & WriterArena.PAGE_ID_MASK);
    }

    public void releaseEntry(long entryAddress, long entryBytes) {
      if (entryAddress == 0L) {
        return;
      }
      long block = entryAddress - WriterArena.PREFIX_BYTES;
      int sizeClass = SizeClasses.indexForEntry(entryBytes);
      if (sizeClass < 0) {
        freeDirectEntry(block, WriterArena.directAllocationBytes(entryBytes));
        return;
      }
      long handle = U.getLong(block + 56L);
      WriterArena.Page page = pageForHandle(handle);
      if (page == null || page.sizeClass != sizeClass || !page.ownsEntry(block, handle)) {
        throw new IllegalStateException("unknown allocator slot handle " + handle);
      }
      int remaining = page.freeSlot(block, handle);
      page.completeRemoteFree(remaining);
    }

    /** Releases a reusable actor batch, grouping entries by page with context-owned scratch. */
    public void releaseEntryBatch(
        ThreadContext context,
        long[] entryAddresses,
        long[] entryBytes,
        long[] entryHandles,
        int count) {
      if (context == null) {
        throw new NullPointerException("context");
      }
      if (entryAddresses == null
          || entryBytes == null
          || entryHandles == null
          || count < 0
          || count > entryAddresses.length
          || count > entryBytes.length
          || count > entryHandles.length) {
        throw new IllegalArgumentException("invalid entry release batch");
      }
      if (count == 0) {
        return;
      }
      context.ensureReleaseBatchScratch(count);
      long[] groupKeys = context.releaseGroupKeys();
      int[] groupCounts = context.releaseGroupCounts();
      int[] groupOffsets = context.releaseGroupOffsets();
      int[] groupPositions = context.releaseGroupPositions();
      int[] groupSlots = context.releaseGroupSlots();
      int[] groupIndexes = context.releaseGroupIndexes();
      int groupMask = groupKeys.length - 1;
      int groupCount = 0;
      try {
        int index = 0;
        while (index < count) {
          entryHandles[index] = 0L;
          long entryAddress = entryAddresses[index];
          if (entryAddress == 0L) {
            index++;
            continue;
          }
          long block = entryAddress - WriterArena.PREFIX_BYTES;
          int sizeClass = SizeClasses.indexForEntry(entryBytes[index]);
          if (sizeClass < 0) {
            freeDirectEntry(block, WriterArena.directAllocationBytes(entryBytes[index]));
            index++;
            continue;
          }
          long handle = U.getLong(block + 56L);
          entryHandles[index] = handle;
          WriterArena.Page page = pageForHandle(handle);
          if (page == null || page.sizeClass != sizeClass || !page.ownsEntry(block, handle)) {
            throw new IllegalStateException("unknown allocator slot handle " + handle);
          }
          long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
          int groupSlot = groupSlot(pageKey, groupKeys, groupMask);
          if (groupKeys[groupSlot] == 0L) {
            groupKeys[groupSlot] = pageKey;
            groupCounts[groupSlot] = 0;
            groupSlots[groupCount++] = groupSlot;
          }
          groupCounts[groupSlot]++;
          index++;
        }

        int pooledCount = 0;
        for (index = 0; index < count; index++) {
          if (entryHandles[index] != 0L) {
            pooledCount++;
          }
        }
        int offset = 0;
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          groupOffsets[groupSlot] = offset;
          groupPositions[groupSlot] = offset;
          offset += groupCounts[groupSlot];
        }
        for (index = 0; index < count; index++) {
          long handle = entryHandles[index];
          if (handle == 0L) {
            continue;
          }
          long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
          int groupSlot = groupSlot(pageKey, groupKeys, groupMask);
          groupIndexes[groupPositions[groupSlot]++] = index;
        }
        if (offset != pooledCount) {
          throw new IllegalStateException("allocator release grouping lost a pooled entry");
        }
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          int firstIndex = groupIndexes[groupOffsets[groupSlot]];
          long firstHandle = entryHandles[firstIndex];
          int sizeClass = SizeClasses.indexForEntry(entryBytes[firstIndex]);
          WriterArena.Page page = pageForHandle(firstHandle);
          if (page == null || page.sizeClass != sizeClass) {
            throw new IllegalStateException("unknown allocator page in grouped release");
          }
          int remaining =
              page.freeEntries(
                  entryAddresses,
                  entryHandles,
                  groupIndexes,
                  groupOffsets[groupSlot],
                  groupCounts[groupSlot]);
          for (int groupIndex = 0; groupIndex < groupCounts[groupSlot]; groupIndex++) {
            int recordIndex = groupIndexes[groupOffsets[groupSlot] + groupIndex];
            // The release operation is the linearization point for this group. Clear the source
            // slots before the availability callback. If that callback fails after the allocator
            // has accepted the group, a retry must skip these records rather than free them again.
            entryAddresses[recordIndex] = 0L;
            entryHandles[recordIndex] = 0L;
          }
          page.completeRemoteFree(remaining);
        }
      } finally {
        for (int group = 0; group < groupCount; group++) {
          groupKeys[groupSlots[group]] = 0L;
        }
      }
    }

    /** Releases native retirement columns with an actor-prepared pooled-handle column. */
    public int releaseEntryBatch(
        ThreadContext context,
        long entryAddressesAddress,
        long entryAllocationsAddress,
        long entryHandlesAddress,
        int count) {
      if (context == null) {
        throw new NullPointerException("context");
      }
      if (entryAddressesAddress == 0L
          || entryAllocationsAddress == 0L
          || entryHandlesAddress == 0L
          || count < 0) {
        throw new IllegalArgumentException("invalid native entry release batch");
      }
      context.resetReleaseBatchProgress();
      if (count == 0) {
        return 0;
      }
      context.ensureReleaseBatchScratch(count);
      long[] groupKeys = context.releaseGroupKeys();
      int[] groupCounts = context.releaseGroupCounts();
      int[] groupOffsets = context.releaseGroupOffsets();
      int[] groupPositions = context.releaseGroupPositions();
      int[] groupSlots = context.releaseGroupSlots();
      int[] groupIndexes = context.releaseGroupIndexes();
      long[] groupBytes = context.releaseGroupBytes();
      int[] entryGroupSlots = context.releaseEntryGroupSlots();
      Object[] groupPages = context.releaseGroupPages();
      int[] entrySlots = context.releaseEntrySlots();
      int groupMask = groupKeys.length - 1;
      int groupCount = 0;
      int pooledCount = 0;
      context.resetReleaseGroupCache();
      long[] cacheKeys = context.releaseGroupCacheKeys();
      int[] cacheSlots = context.releaseGroupCacheSlots();
      int cacheMask = cacheKeys.length - 1;
      try {
        for (int index = 0; index < count; index++) {
          entryGroupSlots[index] = -1;
          long entryAddress = NativeMemory.getLong(entryAddressesAddress + (long) index * 8L);
          if (entryAddress == 0L) {
            continue;
          }
          long allocation = NativeMemory.getLong(entryAllocationsAddress + (long) index * 8L);
          long block = entryAddress - WriterArena.PREFIX_BYTES;
          long handle =
              NativeMemory.getLong(entryHandlesAddress + (long) index * Long.BYTES);
          int expectedClass = SizeClasses.indexForEntry(allocation);
          int sizeClass = expectedClass < 0 ? WriterArena.DIRECT_CLASS : expectedClass;
          if (sizeClass == WriterArena.DIRECT_CLASS) {
            freeDirectEntry(block, WriterArena.directAllocationBytes(allocation));
            // Address is the sole liveness marker. The other columns are dead once it is cleared
            // and will be overwritten before this segment slot is reused.
            NativeMemory.putLong(entryAddressesAddress + (long) index * 8L, 0L);
            context.recordReleaseBatchProgress(1, WriterArena.directAllocationBytes(allocation));
            continue;
          }
          if (handle == 0L) {
            throw new IllegalStateException("pooled retirement record has no allocator handle");
          }
          long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
          int slot = WriterArena.Page.slotOf(handle);
          // Direct-mapped pageKey->groupSlot cache (probe runs once per page).
          int cacheIndex = (int) ((pageKey ^ (pageKey >>> 32)) & cacheMask);
          int groupSlot =
              cacheKeys[cacheIndex] == pageKey
                  ? cacheSlots[cacheIndex]
                  : groupSlot(pageKey, groupKeys, groupMask);
          cacheKeys[cacheIndex] = pageKey;
          cacheSlots[cacheIndex] = groupSlot;
          WriterArena.Page page;
          if (groupKeys[groupSlot] == 0L) {
            // The unreleased retirement slot is still included in allocatedSlots-freedSlots, so
            // beginTrimming cannot reach its equality gate while this actor is about to free it.
            // The descriptor state check below is defense-in-depth for a stale memo entry; it is
            // not relied upon as a read-then-act pin.
            Object cached = context.releasePageMemoLookup(pageKey);
            page = cached instanceof WriterArena.Page ? (WriterArena.Page) cached : null;
            if (page != null && !page.matchesRelease(pageKey, sizeClass, block, slot)) {
              context.releasePageMemoInvalidate(pageKey, cached);
              page = null;
            }
            if (page == null) {
              page = pageForHandle(handle);
            }
            if (page == null || !page.matchesRelease(pageKey, sizeClass, block, slot)) {
              throw new IllegalStateException("unknown allocator slot handle " + handle);
            }
            context.releasePageMemoRemember(pageKey, page);
            groupKeys[groupSlot] = pageKey;
            groupPages[groupSlot] = page;
            groupCounts[groupSlot] = 0;
            groupBytes[groupSlot] = 0L;
            groupSlots[groupCount++] = groupSlot;
          } else {
            page = (WriterArena.Page) groupPages[groupSlot];
            // Reusing a segment-local group never bypasses per-record decoded ownership.
            if (page.sizeClass != sizeClass || !page.ownsEntry(pageKey, block, slot)) {
              throw new IllegalStateException("unknown allocator slot handle " + handle);
            }
          }
          entrySlots[index] = slot;
          entryGroupSlots[index] = groupSlot;
          groupCounts[groupSlot]++;
          groupBytes[groupSlot] += WriterArena.allocationWeight(allocation);
          pooledCount++;
        }

        int offset = 0;
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          groupOffsets[groupSlot] = offset;
          groupPositions[groupSlot] = offset;
          offset += groupCounts[groupSlot];
        }
        for (int index = 0; index < count; index++) {
          int groupSlot = entryGroupSlots[index];
          if (groupSlot < 0) {
            continue;
          }
          groupIndexes[groupPositions[groupSlot]++] = index;
        }
        if (offset != pooledCount) {
          throw new IllegalStateException("allocator release grouping lost a pooled entry");
        }
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          WriterArena.Page page = (WriterArena.Page) groupPages[groupSlot];
          int remaining =
              page.freeValidatedSlotsTurn(
                  entrySlots,
                  groupIndexes,
                  groupOffsets[groupSlot],
                  groupCounts[groupSlot],
                  context.releaseFreeMask());
          boolean recycled = remaining == WriterArena.Page.PAGE_RECYCLED_PENDING_PUBLICATION;
          if (recycled) {
            remaining = 0;
          }
          if (remaining == 0) {
            context.releasePageMemoInvalidate(groupKeys[groupSlot], page);
          }
          for (int index = 0; index < groupCounts[groupSlot]; index++) {
            int recordIndex = groupIndexes[groupOffsets[groupSlot] + index];
            // Grouped reclaim can be retried after another group has already succeeded. Address
            // is the sole liveness marker and must be cleared before progress/callback publication.
            NativeMemory.putLong(
                entryAddressesAddress + (long) recordIndex * Long.BYTES, 0L);
          }
          // Record the group before the availability callback. If that callback fails, the cleared
          // records are already durable and the retry must account only for the remaining groups.
          context.recordReleaseBatchProgress(groupCounts[groupSlot], groupBytes[groupSlot]);
          if (recycled) {
            // The page was reset while undiscoverable; publishing is the single availability
            // transition, replacing completeRemoteFree for this group.
            page.publishRecycledPage();
          } else {
            page.completeRemoteFree(remaining);
          }
        }
        return context.releaseBatchRecords();
      } finally {
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          groupKeys[groupSlot] = 0L;
          groupPages[groupSlot] = null;
        }
      }
    }

    private static int groupSlot(long pageKey, long[] groupKeys, int groupMask) {
      int slot = (int) ((pageKey ^ (pageKey >>> 32)) & groupMask);
      while (groupKeys[slot] != 0L && groupKeys[slot] != pageKey) {
        slot = (slot + 1) & groupMask;
      }
      return slot;
    }

    /**
     * Merged release for one reclaim wave: decodes every claimed segment's columns into one
     * shared page grouping, then publishes each page exactly once. Decode is side-effect free
     * (a validation failure leaves the wave unreleased); per-segment progress lands in the
     * context's turn-progress arrays.
     */
    public void releaseEntryBatchTurn(
        ThreadContext context,
        long[] segmentAddressesColumns,
        long[] segmentAllocationsColumns,
        long[] segmentHandlesColumns,
        int[] segmentTails,
        int segmentCount) {
      if (context == null) {
        throw new NullPointerException("context");
      }
      if (segmentAddressesColumns == null
          || segmentAllocationsColumns == null
          || segmentHandlesColumns == null
          || segmentTails == null
          || segmentCount < 0
          || segmentCount > segmentTails.length
          || segmentCount > segmentAddressesColumns.length
          || segmentCount > segmentAllocationsColumns.length
          || segmentCount > segmentHandlesColumns.length) {
        throw new IllegalArgumentException("invalid merged native release wave");
      }
      int total = 0;
      int[] segmentBases = new int[segmentCount];
      for (int segment = 0; segment < segmentCount; segment++) {
        int tail = segmentTails[segment];
        if (tail < 0) {
          throw new IllegalArgumentException("invalid merged native release tail");
        }
        segmentBases[segment] = total;
        total += tail;
      }
      context.ensureReleaseTurnScratch(Math.max(1, total), segmentCount);
      context.resetReleaseTurnProgress(segmentCount);
      if (total == 0) {
        return;
      }
      long[] groupKeys = context.releaseGroupKeys();
      int[] groupCounts = context.releaseGroupCounts();
      int[] groupOffsets = context.releaseGroupOffsets();
      int[] groupPositions = context.releaseGroupPositions();
      int[] groupSlots = context.releaseGroupSlots();
      int[] groupIndexes = context.releaseGroupIndexes();
      long[] groupBytes = context.releaseGroupBytes();
      int[] entryGroupSlots = context.releaseEntryGroupSlots();
      Object[] groupPages = context.releaseGroupPages();
      int[] entrySlots = context.releaseEntrySlots();
      int[] entrySegments = context.releaseTurnSegmentIndexes();
      long[] entryWeights = context.releaseTurnEntryWeights();
      int[] clearedRecords = context.releaseTurnClearedRecords();
      long[] clearedBytes = context.releaseTurnClearedBytes();
      int[] preZeroRecords = context.releaseTurnPreZeroRecords();
      int groupMask = groupKeys.length - 1;
      int groupCount = 0;
      int pooledCount = 0;
      int directCount = 0;
      context.resetReleaseGroupCache();
      long[] cacheKeys = context.releaseGroupCacheKeys();
      int[] cacheSlots = context.releaseGroupCacheSlots();
      int cacheMask = cacheKeys.length - 1;
      try {
        // Phase A: decode every segment's columns into the shared grouping. No side effects.
        int record = 0;
        for (int segment = 0; segment < segmentCount; segment++) {
          int tail = segmentTails[segment];
          long addressesColumn = segmentAddressesColumns[segment];
          long allocationsColumn = segmentAllocationsColumns[segment];
          long handlesColumn = segmentHandlesColumns[segment];
          for (int index = 0; index < tail; index++, record++) {
            entryGroupSlots[record] = -1;
            long entryAddress = NativeMemory.getLong(addressesColumn + (long) index * 8L);
            if (entryAddress == 0L) {
              preZeroRecords[segment]++;
              continue;
            }
            long allocation =
                NativeMemory.getLong(allocationsColumn + (long) index * 8L);
            long block = entryAddress - WriterArena.PREFIX_BYTES;
            long handle =
                NativeMemory.getLong(handlesColumn + (long) index * Long.BYTES);
            int expectedClass = SizeClasses.indexForEntry(allocation);
            int sizeClass = expectedClass < 0 ? WriterArena.DIRECT_CLASS : expectedClass;
            long weight =
                expectedClass < 0
                    ? WriterArena.directAllocationBytes(allocation)
                    : SizeClasses.slotBytes(expectedClass);
            entrySegments[record] = segment;
            entryWeights[record] = weight;
            if (sizeClass == WriterArena.DIRECT_CLASS) {
              // Deferred to the publication pass so a decode failure stays side-effect free.
              entryGroupSlots[record] = DIRECT_RELEASE_PENDING;
              directCount++;
              continue;
            }
            if (handle == 0L) {
              throw new IllegalStateException("pooled retirement record has no allocator handle");
            }
            long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
            int slot = WriterArena.Page.slotOf(handle);
            // Direct-mapped pageKey->groupSlot cache: the probe runs once per page.
            int cacheIndex = (int) ((pageKey ^ (pageKey >>> 32)) & cacheMask);
            int groupSlot =
                cacheKeys[cacheIndex] == pageKey
                    ? cacheSlots[cacheIndex]
                    : groupSlot(pageKey, groupKeys, groupMask);
            cacheKeys[cacheIndex] = pageKey;
            cacheSlots[cacheIndex] = groupSlot;
            WriterArena.Page page;
            if (groupKeys[groupSlot] == 0L) {
              Object cached = context.releasePageMemoLookup(pageKey);
              page = cached instanceof WriterArena.Page ? (WriterArena.Page) cached : null;
              if (page != null && !page.matchesRelease(pageKey, sizeClass, block, slot)) {
                context.releasePageMemoInvalidate(pageKey, cached);
                page = null;
              }
              if (page == null) {
                page = pageForHandle(handle);
              }
              if (page == null || !page.matchesRelease(pageKey, sizeClass, block, slot)) {
                throw new IllegalStateException("unknown allocator slot handle " + handle);
              }
              context.releasePageMemoRemember(pageKey, page);
              groupKeys[groupSlot] = pageKey;
              groupPages[groupSlot] = page;
              groupCounts[groupSlot] = 0;
              groupBytes[groupSlot] = 0L;
              groupSlots[groupCount++] = groupSlot;
            } else {
              page = (WriterArena.Page) groupPages[groupSlot];
              if (page.sizeClass != sizeClass || !page.ownsEntry(pageKey, block, slot)) {
                throw new IllegalStateException("unknown allocator slot handle " + handle);
              }
            }
            entrySlots[record] = slot;
            entryGroupSlots[record] = groupSlot;
            groupCounts[groupSlot]++;
            groupBytes[groupSlot] += weight;
            pooledCount++;
          }
        }

        // Phase B1: direct entries, in record order; skipped when the wave decoded none.
        if (directCount != 0) {
          for (int index = 0; index < record; index++) {
            if (entryGroupSlots[index] != DIRECT_RELEASE_PENDING) {
              continue;
            }
            int segment = entrySegments[index];
            int localIndex = index - segmentRecordBase(segmentBases, segment);
            long entryAddress =
                NativeMemory.getLong(segmentAddressesColumns[segment] + (long) localIndex * 8L);
            long allocation =
                NativeMemory.getLong(segmentAllocationsColumns[segment] + (long) localIndex * 8L);
            freeDirectEntry(
                entryAddress - WriterArena.PREFIX_BYTES,
                WriterArena.directAllocationBytes(allocation));
            NativeMemory.putLong(
                segmentAddressesColumns[segment] + (long) localIndex * Long.BYTES, 0L);
            clearedRecords[segment]++;
            clearedBytes[segment] += entryWeights[index];
          }
        }

        // Phase B2: one publication round per page for the whole wave.
        int offset = 0;
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          groupOffsets[groupSlot] = offset;
          groupPositions[groupSlot] = offset;
          offset += groupCounts[groupSlot];
        }
        for (int index = 0; index < record; index++) {
          int groupSlot = entryGroupSlots[index];
          if (groupSlot < 0) {
            continue;
          }
          groupIndexes[groupPositions[groupSlot]++] = index;
        }
        if (offset != pooledCount) {
          throw new IllegalStateException("allocator release grouping lost a pooled entry");
        }
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          WriterArena.Page page = (WriterArena.Page) groupPages[groupSlot];
          int remaining =
              page.freeValidatedSlotsTurn(
                  entrySlots,
                  groupIndexes,
                  groupOffsets[groupSlot],
                  groupCounts[groupSlot],
                  context.releaseFreeMask());
          boolean recycled = remaining == WriterArena.Page.PAGE_RECYCLED_PENDING_PUBLICATION;
          if (recycled) {
            remaining = 0;
          }
          if (remaining == 0) {
            context.releasePageMemoInvalidate(groupKeys[groupSlot], page);
          }
          for (int index = 0; index < groupCounts[groupSlot]; index++) {
            int recordIndex = groupIndexes[groupOffsets[groupSlot] + index];
            int segment = entrySegments[recordIndex];
            int localIndex = recordIndex - segmentRecordBase(segmentBases, segment);
            // Address is the sole liveness marker: cleared only after the page accepted the
            // free, before the availability callback, so a callback failure keeps this group
            // durable and a retry accounts only the remaining groups.
            NativeMemory.putLong(
                segmentAddressesColumns[segment] + (long) localIndex * Long.BYTES, 0L);
            clearedRecords[segment]++;
            clearedBytes[segment] += entryWeights[recordIndex];
          }
          if (recycled) {
            // The page was reset while undiscoverable; publishing is the single availability
            // transition, replacing completeRemoteFree for this group.
            page.publishRecycledPage();
          } else {
            page.completeRemoteFree(remaining);
          }
        }
      } finally {
        for (int group = 0; group < groupCount; group++) {
          int groupSlot = groupSlots[group];
          groupKeys[groupSlot] = 0L;
          groupPages[groupSlot] = null;
        }
      }
    }

    /** Wave-relative record index of the first record of {@code segment}. */
    private static int segmentRecordBase(int[] segmentBases, int segment) {
      return segmentBases[segment];
    }

    public void closeArenas() {
      // Remove every ready token before invalidating the page table. This prevents a stale stack
      // node from retaining a freed Page descriptor after shutdown.
      for (int sizeClass = 0; sizeClass < SizeClasses.count(); sizeClass++) {
        while (popReadyPage(sizeClass) != null) {
          // The page-table pass below owns the physical free; popping only removes the token.
        }
      }
      for (int chunkIndex = 0; chunkIndex < pageChunks.length; chunkIndex++) {
        WriterArena.Page[] chunk =
            (WriterArena.Page[]) U.getObjectVolatile(pageChunks, objectArrayOffset(chunkIndex));
        if (chunk == null) {
          continue;
        }
        for (int slot = 0; slot < chunk.length; slot++) {
          WriterArena.Page page =
              (WriterArena.Page) U.getObjectVolatile(chunk, objectArrayOffset(slot));
          if (page != null) {
            freeEntryPage(page, true);
          }
        }
      }
      for (LongAdder count : pageReadyCounts) {
        count.reset();
      }
    }

    /** Allocation-failure cold path. Exactly one caller drains each ready LIFO once. */
    public long trimAvailablePages() {
      if (!trimOwner.compareAndSet(false, true)) {
        return 0L;
      }
      long trimmedBytes = 0L;
      try {
        for (int sizeClass = 0; sizeClass < SizeClasses.count(); sizeClass++) {
          long candidates = Math.max(0L, pageReadyCounts[sizeClass].sum());
          if (candidates == 0L) {
            continue;
          }
          int deferredHead = 0;
          try {
            for (long candidate = 0L; candidate < candidates; candidate++) {
              WriterArena.Page page = popReadyPage(sizeClass);
              if (page == null) {
                break;
              }
              if (!isCurrentPage(page)) {
                continue;
              }
              if (page.beginTrimming() && freeEntryPage(page, false)) {
                pageTrimmedCount.incrementAndGet();
                trimmedBytes += page.pageBytes;
                trimmedBytesTotal.addAndGet(page.pageBytes);
              } else {
                // Keep a page that cannot be trimmed off the stack until this pass has inspected
                // the rest of the snapshot; immediately pushing it back would pop the same page
                // again and starve a trimmable page below it.
                readyPageNext.set(page.id, deferredHead);
                deferredHead = page.id;
              }
            }
          } finally {
            while (deferredHead != 0) {
              int pageId = deferredHead;
              deferredHead = readyPageNext.get(pageId);
              WriterArena.Page page = pageForId(pageId);
              if (page != null && page.id == pageId) {
                restoreReadyPage(page);
              }
            }
          }
        }
      } finally {
        trimOwner.set(false);
      }
      return trimmedBytes;
    }

    WriterArena.Page tryAcquireEntryPage(
        int sizeClass, WriterArena.SizeClassState ownerClass) {
      pooledPageCount.incrementAndGet();
      long pageKey = 0L;
      long address = 0L;
      int pageBytes = SizeClasses.pageBytes(sizeClass);
      // Anonymous mappings keep page frees OS-visible; the budget bounds vm.max_map_count use.
      boolean directMapped =
          allocator.pageMappingsAvailable() && directMappedPageCount.get() < PAGE_MAPPING_BUDGET;
      try {
        pageKey = nextPageKey();
        if (pageKey == 0) {
          pooledPageCount.decrementAndGet();
          return null;
        }
        int pageId = (int) (pageKey & WriterArena.PAGE_ID_MASK);
        // Ready publication happens after the Page leaves its owner. Materialize the sparse link
        // chunk while acquisition can still unwind, so that publication itself cannot allocate.
        readyPageNext.prepare(pageId);
        address =
            directMapped ? allocateDirectMappedPage(pageBytes) : allocateEntryPage(pageBytes);
        if (address == 0L) {
          recyclePageId(pageId);
          pooledPageCount.decrementAndGet();
          return null;
        }
        WriterArena.Page page =
            new WriterArena.Page(
                ownerClass,
                pageId,
                pageKey,
                address,
                sizeClass,
                SizeClasses.slotBytes(sizeClass),
                pageBytes,
                directMapped);
        registerPage(page);
        return page;
      } catch (Throwable failure) {
        if (address != 0L) {
          if (directMapped) {
            allocator.unmapPage(address, pageBytes);
            directMappedPageCount.decrementAndGet();
            allocated.addAndGet(-pageBytes);
          } else {
            long rawAddress = U.getLong(address - Long.BYTES);
            free(rawAddress, pageBytes + ALIGNMENT_PADDING_BYTES);
          }
        }
        if (pageKey != 0) {
          recyclePageId((int) (pageKey & WriterArena.PAGE_ID_MASK));
        }
        pooledPageCount.decrementAndGet();
        throw failure;
      }
    }

    /** Test-only raw page acquisition; production allocation always supplies its sticky owner. */
    WriterArena.Page tryAcquireEntryPage(int sizeClass) {
      WriterArena arena = newWriterArena();
      return tryAcquireEntryPage(sizeClass, arena.sizeClassState(sizeClass));
    }

    void returnUnusedPage(WriterArena.Page page) {
      freeEntryPage(page, true);
    }

    void freeEntryPage(WriterArena.Page page) {
      freeEntryPage(page, true);
    }

    private boolean freeEntryPage(WriterArena.Page page, boolean force) {
      if (!page.freePhysical(force)) {
        return false;
      }
      unregisterPage(page);
      freeEntryPageAllocation(page);
      pooledPageCount.decrementAndGet();
      recyclePageId(page.id);
      return true;
    }

    void recordPageReady(int sizeClass) {
      pageReadyCounts[sizeClass].increment();
    }

    void recordSmallAllocationFallback() {
      smallAllocationFallbacks.incrementAndGet();
    }

    void recordDirectEntryAllocation() {
      directEntryAllocations.incrementAndGet();
    }

    long nextPageKey() {
      long pageId = takeFreePageId();
      if (pageId == 0) {
        pageId = nextPageId.getAndIncrement();
      }
      if (pageId <= 0L || pageId > WriterArena.PAGE_ID_MASK) {
        return 0;
      }
      int version = pageVersions.incrementAndGet((int) pageId) & PAGE_VERSION_MASK;
      return ((long) version << WriterArena.HANDLE_PAGE_BITS) | pageId;
    }

    void registerPage(WriterArena.Page page) {
      if ((page.id & ~WriterArena.PAGE_ID_MASK) != 0) {
        throw new IllegalStateException("page id outside the table namespace: " + page.id);
      }
      int chunkIndex = page.id >>> PAGE_CHUNK_BITS;
      WriterArena.Page[] chunk =
          (WriterArena.Page[]) U.getObjectVolatile(pageChunks, objectArrayOffset(chunkIndex));
      if (chunk == null) {
        WriterArena.Page[] created = new WriterArena.Page[PAGE_CHUNK_SIZE];
        if (!U.compareAndSwapObject(pageChunks, objectArrayOffset(chunkIndex), null, created)) {
          created =
              (WriterArena.Page[])
                  U.getObjectVolatile(pageChunks, objectArrayOffset(chunkIndex));
        }
        chunk = created;
      }
      if (!U.compareAndSwapObject(
          chunk, objectArrayOffset(page.id & (PAGE_CHUNK_SIZE - 1)), null, page)) {
        throw new IllegalStateException("duplicate native page id " + page.id);
      }
    }

    void unregisterPage(WriterArena.Page page) {
      WriterArena.Page[] chunk =
          (WriterArena.Page[])
              U.getObjectVolatile(pageChunks, objectArrayOffset(page.id >>> PAGE_CHUNK_BITS));
      if (chunk != null) {
        U.compareAndSwapObject(
            chunk, objectArrayOffset(page.id & (PAGE_CHUNK_SIZE - 1)), page, null);
      }
    }

    WriterArena.Page pageForHandle(long handle) {
      long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
      int pageId = (int) (pageKey & WriterArena.PAGE_ID_MASK);
      if (pageId == 0) {
        return null;
      }
      WriterArena.Page page = pageForId(pageId);
      return page != null && page.pageKey == pageKey ? page : null;
    }

    private WriterArena.Page pageForId(int pageId) {
      WriterArena.Page[] chunk =
          (WriterArena.Page[])
              U.getObjectVolatile(pageChunks, objectArrayOffset(pageId >>> PAGE_CHUNK_BITS));
      if (chunk == null) {
        return null;
      }
      return (WriterArena.Page)
          U.getObjectVolatile(chunk, objectArrayOffset(pageId & (PAGE_CHUNK_SIZE - 1)));
    }

    private long takeFreePageId() {
      while (true) {
        long head = freePageIdHead.get();
        int pageId = (int) head;
        if (pageId == 0) {
          return 0;
        }
        int next = freePageIdNext.get(pageId);
        long updated = packPageIdHead(((int) (head >>> 32)) + 1, next);
        if (freePageIdHead.compareAndSet(head, updated)) {
          return pageId & WriterArena.PAGE_ID_MASK;
        }
      }
    }

    private void recyclePageId(int pageId) {
      while (true) {
        long head = freePageIdHead.get();
        freePageIdNext.set(pageId, (int) head);
        long updated = packPageIdHead(((int) (head >>> 32)) + 1, pageId);
        if (freePageIdHead.compareAndSet(head, updated)) {
          return;
        }
      }
    }

    private static long packPageIdHead(int stamp, int pageId) {
      return ((long) stamp << 32) | (pageId & WriterArena.PAGE_ID_MASK);
    }

    /** Lazily allocated primitive chunks covering exactly the handle's page-id namespace. */
    private static final class IntChunkTable {
      private static final int CHUNK_BITS = 12;
      private static final int CHUNK_SIZE = 1 << CHUNK_BITS;
      private static final int CHUNK_COUNT =
          1 << (WriterArena.HANDLE_PAGE_BITS - CHUNK_BITS);
      private final AtomicIntegerArray[] chunks = new AtomicIntegerArray[CHUNK_COUNT];

      int get(int index) {
        AtomicIntegerArray chunk =
            (AtomicIntegerArray)
                U.getObjectVolatile(chunks, objectArrayOffset(index >>> CHUNK_BITS));
        return chunk == null ? 0 : chunk.get(index & (CHUNK_SIZE - 1));
      }

      void set(int index, int value) {
        ensureChunk(index).set(index & (CHUNK_SIZE - 1), value);
      }

      void prepare(int index) {
        ensureChunk(index);
      }

      int incrementAndGet(int index) {
        return ensureChunk(index).incrementAndGet(index & (CHUNK_SIZE - 1));
      }

      private AtomicIntegerArray ensureChunk(int index) {
        int chunkIndex = index >>> CHUNK_BITS;
        AtomicIntegerArray chunk =
            (AtomicIntegerArray) U.getObjectVolatile(chunks, objectArrayOffset(chunkIndex));
        if (chunk != null) {
          return chunk;
        }
        AtomicIntegerArray created = new AtomicIntegerArray(CHUNK_SIZE);
        if (!U.compareAndSwapObject(chunks, objectArrayOffset(chunkIndex), null, created)) {
          return (AtomicIntegerArray) U.getObjectVolatile(chunks, objectArrayOffset(chunkIndex));
        }
        return created;
      }
    }

  }

  public static long getLong(long address) {
    return U.getLong(address);
  }

  public static long getLong(byte[] bytes, int offset) {
    return U.getLong(bytes, BYTE_ARRAY_BASE + offset);
  }

  public static int getInt(byte[] bytes, int offset) {
    return U.getInt(bytes, BYTE_ARRAY_BASE + offset);
  }

  public static Unsafe unsafe() {
    return U;
  }


  /** The actual heap reference width used by this HotSpot process (compressed or wide OOPs). */

  public static long getLongVolatile(long address) {
    return U.getLongVolatile(null, address);
  }

  public static boolean compareAndSwapLong(long address, long expected, long update) {
    return U.compareAndSwapLong(null, address, expected, update);
  }

  public static void putLong(long address, long value) {
    U.putLong(address, value);
  }

  public static void putLongRelease(long address, long value) {
    U.putOrderedLong(null, address, value);
  }

  public static void putLongVolatile(long address, long value) {
    U.putLongVolatile(null, address, value);
  }

  public static int getInt(long address) {
    return U.getInt(address);
  }

  public static int getIntVolatile(long address) {
    return U.getIntVolatile(null, address);
  }


  public static void putInt(long address, int value) {
    U.putInt(address, value);
  }

  public static void putIntVolatile(long address, int value) {
    U.putIntVolatile(null, address, value);
  }

  public static byte getByte(long address) {
    return U.getByte(address);
  }

  public static void putByte(long address, byte value) {
    U.putByte(address, value);
  }

  /** Fills a contiguous native range with one byte value. */
  public static void setMemory(long address, long bytes, byte value) {
    U.setMemory(address, bytes, value);
  }

  public static void copy(byte[] source, int sourceOffset, long destination, long bytes) {
    U.copyMemory(source, BYTE_ARRAY_BASE + sourceOffset, null, destination, bytes);
  }

  public static void copy(long source, byte[] destination, int destinationOffset, long bytes) {
    U.copyMemory(null, source, destination, BYTE_ARRAY_BASE + destinationOffset, bytes);
  }

  public static boolean equals(long address, byte[] bytes, int offset, int length) {
    // The bulk block path lives in its own method so this hot entry stays inlinable.
    int i = 0;
    if (length >= BULK_EQUALS_THRESHOLD) {
      i = equalsBulkBlocks(address, bytes, offset, length);
      if (i < 0) {
        return false;
      }
    }
    for (; i + 8 <= length; i += 8) {
      if (U.getLong(address + i) != U.getLong(bytes, BYTE_ARRAY_BASE + offset + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (U.getByte(address + i) != bytes[offset + i]) {
        return false;
      }
    }
    return true;
  }

  /** Compares whole 64-byte blocks; returns the advanced index or -1 on the first mismatch. */
  private static int equalsBulkBlocks(long address, byte[] bytes, int offset, int length) {
    int blockBytes = BULK_EQUALS_LONGS * Long.BYTES;
    for (int i = 0; i + blockBytes <= length; i += blockBytes) {
      // Aggregate all word differences in the block before branching. This is a
      // portable SWAR-style optimization: it reduces branch frequency without
      // requiring the JVM to generate platform-specific SIMD instructions.
      long diff = 0L;
      diff |= U.getLong(address + i) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i);
      diff |= U.getLong(address + i + 8) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 8);
      diff |= U.getLong(address + i + 16) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 16);
      diff |= U.getLong(address + i + 24) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 24);
      diff |= U.getLong(address + i + 32) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 32);
      diff |= U.getLong(address + i + 40) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 40);
      diff |= U.getLong(address + i + 48) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 48);
      diff |= U.getLong(address + i + 56) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 56);
      if (diff != 0L) {
        return -1;
      }
    }
    return blockBytes * (length / blockBytes);
  }

  public static boolean equals(long left, long right, int length) {
    // Same split as the heap-array overload: the bulk path stays out of the hot entry.
    int i = 0;
    if (length >= BULK_EQUALS_THRESHOLD) {
      i = equalsBulkBlocks(left, right, length);
      if (i < 0) {
        return false;
      }
    }
    for (; i + 8 <= length; i += 8) {
      if (U.getLong(left + i) != U.getLong(right + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (U.getByte(left + i) != U.getByte(right + i)) {
        return false;
      }
    }
    return true;
  }

  /** Compares whole 64-byte blocks; returns the advanced index or -1 on the first mismatch. */
  private static int equalsBulkBlocks(long left, long right, int length) {
    int blockBytes = BULK_EQUALS_LONGS * Long.BYTES;
    for (int i = 0; i + blockBytes <= length; i += blockBytes) {
      // Keep the same block shape as the heap-array overload so both hot paths
      // have identical early-exit semantics and are easy to benchmark together.
      long diff = 0L;
      diff |= U.getLong(left + i) ^ U.getLong(right + i);
      diff |= U.getLong(left + i + 8) ^ U.getLong(right + i + 8);
      diff |= U.getLong(left + i + 16) ^ U.getLong(right + i + 16);
      diff |= U.getLong(left + i + 24) ^ U.getLong(right + i + 24);
      diff |= U.getLong(left + i + 32) ^ U.getLong(right + i + 32);
      diff |= U.getLong(left + i + 40) ^ U.getLong(right + i + 40);
      diff |= U.getLong(left + i + 48) ^ U.getLong(right + i + 48);
      diff |= U.getLong(left + i + 56) ^ U.getLong(right + i + 56);
      if (diff != 0L) {
        return -1;
      }
    }
    return blockBytes * (length / blockBytes);
  }
}
