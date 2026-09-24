package com.red.ohc.storage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * One writer-owned allocator. Allocation is single-consumer while native retirement may return
 * slots from any thread. The current page stays writer-exclusive; available pages may move to
 * another writer resource so skew cannot force unbounded native allocation.
 * Pages are retained at the workload high-water mark; only allocation pressure and close
 * physically release them.
 */
public final class WriterArena {
  public static final int PREFIX_BYTES = 64;
  public static final int DIRECT_CLASS = 0xffff;
  static final int HANDLE_SLOT_BITS = 14;
  static final int HANDLE_PAGE_BITS = 24;
  static final int HANDLE_GENERATION_BITS = 26;
  static final int PAGE_ID_BITS = HANDLE_PAGE_BITS;
  static final long PAGE_ID_MASK = (1L << PAGE_ID_BITS) - 1L;
  private static final long HANDLE_SLOT_MASK = (1L << HANDLE_SLOT_BITS) - 1L;
  private static final int HANDLE_PAGE_SHIFT = HANDLE_SLOT_BITS;
  private static final VarHandle SIZE_CLASS_STATE =
      MethodHandles.arrayElementVarHandle(SizeClassState[].class);

  /** Owner-private retention depth per size class before pages spill to the shared ready stack. */
  static final int RETAINED_PAGE_LIMIT = 4;

  /** Sentinel that permanently seals a retained slot after its owner detached. */
  private static final Page RETENTION_CLOSED_PAGE = new Page(null, 0, 0L, 0L, 0, 1, 1, false);

  private final NativeMemory.Memory memory;
  private volatile Runnable retirementHookForTest;
  /** Set by the actor's idle seal; cleared by the owner's next cold allocation. */
  private volatile boolean retentionSealed;

  /** Lazily populated by the exclusive writer; remote frees use Page.ownerClass instead. */
  private final SizeClassState[] sizeClasses = new SizeClassState[SizeClasses.count()];

  WriterArena(NativeMemory.Memory memory) {
    this.memory = memory;
  }

  public long allocate(long entryBytes) {
    int sizeClass = SizeClasses.indexForEntry(entryBytes);
    if (sizeClass < 0) {
      return allocateDirect(entryBytes);
    }
    return allocateSmall(sizeClass);
  }

  private long allocateSmall(int sizeClass) {
    SizeClassState ownerClass = sizeClassState(sizeClass);
    boolean pressureAttempted = false;
    OutOfMemoryError firstAllocationFailure = null;
    for (;;) {
      Page page = ownerClass.currentPage;
      if (page == null) {
        page = pollRetainedOwnerPage(ownerClass);
        if (page == null && retentionSealed) {
          retentionSealed = false;
          reopenRetention();
          page = pollRetainedOwnerPage(ownerClass);
        }
        if (page == null) {
          page = memory.tryStealAvailablePage(sizeClass, ownerClass);
        }
        if (page == null) {
          try {
            page = memory.tryAcquireEntryPage(sizeClass, ownerClass);
          } catch (OutOfMemoryError failure) {
            if (pressureAttempted) {
              if (firstAllocationFailure != null) {
                failure.addSuppressed(firstAllocationFailure);
              }
              throw failure;
            }
            firstAllocationFailure = failure;
          }
        }
        if (page == null && !pressureAttempted) {
          pressureAttempted = true;
          memory.trimAvailablePages();
          continue;
        }
        if (page == null) {
          if (firstAllocationFailure != null) {
            throw firstAllocationFailure;
          }
          return 0L;
        }
        ownerClass.currentPage = page;
      }
      long block = page.allocateSlot();
      if (block == 0L) {
        if (ownerClass.currentPage == page) {
          ownerClass.currentPage = null;
        }
        page.ownerExhausted();
        continue;
      }
      return block + PREFIX_BYTES;
    }
  }

  /** Takes an owner-retained available page without touching the shared ready stack. */
  private Page pollRetainedOwnerPage(SizeClassState ownerClass) {
    Page page = ownerClass.pollRetainedPage();
    if (page == null) {
      return null;
    }
    if (!page.tryActivate(ownerClass)) {
      // Only this owner can activate a retained page; reaching here means an invariant broke.
      throw new IllegalStateException("retained page is not activatable by its owner");
    }
    memory.recordPageReuse();
    return page;
  }

  /** QSBR reclaim returns a slot to the page's current owner. */
  public void remoteFree(long block, int sizeClass) {
    long handle = NativeMemory.getLong(block + 56L);
    if (handle == 0L) {
      throw new IllegalStateException("small allocation has no stable slot handle");
    }
    Page page = memory.pageForHandle(handle);
    if (page == null || page.sizeClass != sizeClass || !page.ownsEntry(block, handle)) {
      throw new IllegalStateException("unknown allocator slot handle " + handle);
    }
    int remaining = page.freeSlot(block, handle);
    page.completeRemoteFree(remaining);
  }

  void completeRemoteFree(Page page, int remaining) {
    // Publish before the optional test hook so a racing trim still sees the page.
    page.publishAvailableAfterRemoteFree();
    if (remaining == 0) {
      // A completely free page must not stay hidden in owner retention.
      page.releaseRetentionToSharedStack();
      Runnable hook = retirementHookForTest;
      if (hook != null) {
        hook.run();
      }
    }
  }

  void setRetirementHookForTest(Runnable hook) {
    retirementHookForTest = hook;
  }

  Runnable retirementHookForTest() {
    return retirementHookForTest;
  }

  /** Called only after the writer context is no longer active. */
  public void detach() {
    for (SizeClassState sizeClass : sizeClasses) {
      if (sizeClass == null) {
        continue;
      }
      Page page = sizeClass.currentPage;
      sizeClass.currentPage = null;
      if (page != null) {
        page.detachOwner();
      }
      // Seal every slot so no remote free can strand a page on a detached arena.
      sizeClass.closeRetention();
    }
  }

  /** Reopens retention after this arena's writer resource is re-activated from the pool. */
  public void reopenRetention() {
    for (SizeClassState sizeClass : sizeClasses) {
      if (sizeClass != null) {
        sizeClass.reopenRetention();
      }
    }
  }

  /**
   * Actor-side idle seal: publishes retained pages to the shared stack. Current pages and the
   * resource lifecycle stay untouched; the owner re-arms retention on its next cold allocation.
   */
  public void sealRetention() {
    for (SizeClassState sizeClass : sizeClasses) {
      if (sizeClass != null) {
        sizeClass.closeRetention();
      }
    }
    retentionSealed = true;
  }

  public static long directAllocationBytes(long entryBytes) {
    return SizeClasses.directBytes(entryBytes);
  }

  /** Native weight consumed by one allocation, including prefix and class rounding. */
  public static long allocationWeight(long entryBytes) {
    int sizeClass = SizeClasses.indexForEntry(entryBytes);
    return sizeClass < 0 ? directAllocationBytes(entryBytes) : SizeClasses.slotBytes(sizeClass);
  }

  private long allocateDirect(long entryBytes) {
    long block = memory.allocateDirectEntry(directAllocationBytes(entryBytes));
    if (block == 0L) {
      return 0L;
    }
    memory.recordDirectEntryAllocation();
    return block + PREFIX_BYTES;
  }

  SizeClassState sizeClassState(int sizeClass) {
    SizeClassState state = (SizeClassState) SIZE_CLASS_STATE.getAcquire(sizeClasses, sizeClass);
    if (state == null) {
      state = new SizeClassState(this, sizeClass);
      SIZE_CLASS_STATE.setRelease(sizeClasses, sizeClass, state);
    }
    return state;
  }

  static final class SizeClassState {
    final WriterArena arena;
    final int sizeClass;
    Page currentPage;
    /** Owner-private available pages; overflow still reaches the shared ready stack. */
    private final AtomicReferenceArray<Page> retainedPages =
        new AtomicReferenceArray<>(RETAINED_PAGE_LIMIT);

    SizeClassState(WriterArena arena, int sizeClass) {
      this.arena = arena;
      this.sizeClass = sizeClass;
    }

    /** Offers a just-published available page to the owner; false means spill to the shared stack. */
    boolean tryRetainPage(Page page) {
      for (int slot = 0; slot < RETAINED_PAGE_LIMIT; slot++) {
        if (retainedPages.compareAndSet(slot, null, page)) {
          arena.memory.recordRetainedPage();
          return true;
        }
      }
      return false;
    }

    /** Takes one retained page for the owner's next allocation; null when nothing is retained. */
    Page pollRetainedPage() {
      for (int slot = 0; slot < RETAINED_PAGE_LIMIT; slot++) {
        Page page = retainedPages.get(slot);
        if (page == null || page == RETENTION_CLOSED_PAGE) {
          continue;
        }
        if (retainedPages.compareAndSet(slot, page, null)) {
          arena.memory.recordRetainedPageConsumed();
          return page;
        }
      }
      return null;
    }

    /** Removes one specific page from retention so it becomes shared-stack visible again. */
    boolean tryReleaseRetainedPage(Page page) {
      for (int slot = 0; slot < RETAINED_PAGE_LIMIT; slot++) {
        if (retainedPages.get(slot) == page
            && retainedPages.compareAndSet(slot, page, null)) {
          arena.memory.recordRetainedPageReleased();
          return true;
        }
      }
      return false;
    }

    /** Clears the closed sentinel so a recycled arena's owner can retain pages again. */
    void reopenRetention() {
      for (int slot = 0; slot < RETAINED_PAGE_LIMIT; slot++) {
        retainedPages.compareAndSet(slot, RETENTION_CLOSED_PAGE, null);
      }
    }

    /** Seals every retained slot after the owner stopped allocating; claimed pages go shared. */
    void closeRetention() {
      for (int slot = 0; slot < RETAINED_PAGE_LIMIT; slot++) {
        for (;;) {
          Page page = retainedPages.get(slot);
          if (page == RETENTION_CLOSED_PAGE) {
            break;
          }
          if (page == null) {
            if (retainedPages.compareAndSet(slot, null, RETENTION_CLOSED_PAGE)) {
              break;
            }
            continue;
          }
          if (retainedPages.compareAndSet(slot, page, RETENTION_CLOSED_PAGE)) {
            arena.memory.recordRetainedPageReleased();
            arena.memory.publishAvailablePageGlobal(page);
            break;
          }
        }
      }
    }
  }

  /**
   * Owner-only allocation cursor and counter; padding keeps shared state out of this line. The
   * cursor is a long so it stays inside this line: the JVM lays int fields after the long group,
   * which would drop the cursor into the contended reclaimer region below.
   */
  static class PageOwnerLine {
    long nextSlot;
    long allocatedSlots;
    long ownerPadding0;
    long ownerPadding1;
    long ownerPadding2;
    long ownerPadding3;
    long ownerPadding4;
    long ownerPadding5;
    long ownerPadding6;
  }

  /** Reclaimer-shared bitmap, counter, state, and owner handoff fields. */
  static class PageSharedLine extends PageOwnerLine {
    // Actor-only cumulative freed count. Isolated on both sides so the writer-side allocation
    // read of freeSummary never shares a line with this counter's remote-free RMWs; with only
    // 8-byte object alignment, double-sided padding is the sole deterministic isolation.
    volatile long freedSlots;
    long freedPad0;
    long freedPad1;
    long freedPad2;
    long freedPad3;
    long freedPad4;
    long freedPad5;
    long freedPad6;
    long freedPad7;
    volatile long freeSummary;
    volatile long freeBits0;
    volatile long freeBits1;
    volatile long freeBits2;
    volatile long freeBits3;
    volatile long freeBits4;
    volatile long freeBits5;
    volatile long freeBits6;
    volatile long freeBits7;
    volatile int state;
    volatile SizeClassState ownerClass;
  }

  /**
   * A generation-stable physical page descriptor. Allocation is owner-only; free publication and
   * the cumulative freed count are contended by remote reclaimers. AVAILABLE is the sole
   * trimmable state.
   */
  static final class Page extends PageSharedLine {
    /** Whole-page fast-path sentinel: recycled in place, caller publishes after its epilogue. */
    static final int PAGE_RECYCLED_PENDING_PUBLICATION = -1;
    private static final int ACTIVE = 1;
    private static final int FULL = 2;
    private static final int AVAILABLE = 3;
    private static final int REBALANCING = 4;
    private static final int TRIMMING = 5;
    private static final int FREED = 6;
    private static final VarHandle ALLOCATED_SLOTS;
    private static final VarHandle FREED_SLOTS;
    private static final VarHandle FREE_SUMMARY;
    private static final VarHandle FREE_BITS_0;
    private static final VarHandle FREE_BITS_1;
    private static final VarHandle FREE_BITS_2;
    private static final VarHandle FREE_BITS_3;
    private static final VarHandle FREE_BITS_4;
    private static final VarHandle FREE_BITS_5;
    private static final VarHandle FREE_BITS_6;
    private static final VarHandle FREE_BITS_7;
    private static final VarHandle STATE;
    private static final int FREE_BITMAP_WORDS = 8;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ALLOCATED_SLOTS =
            lookup.findVarHandle(PageOwnerLine.class, "allocatedSlots", long.class);
        FREED_SLOTS = lookup.findVarHandle(PageSharedLine.class, "freedSlots", long.class);
        FREE_SUMMARY = lookup.findVarHandle(PageSharedLine.class, "freeSummary", long.class);
        FREE_BITS_0 = lookup.findVarHandle(PageSharedLine.class, "freeBits0", long.class);
        FREE_BITS_1 = lookup.findVarHandle(PageSharedLine.class, "freeBits1", long.class);
        FREE_BITS_2 = lookup.findVarHandle(PageSharedLine.class, "freeBits2", long.class);
        FREE_BITS_3 = lookup.findVarHandle(PageSharedLine.class, "freeBits3", long.class);
        FREE_BITS_4 = lookup.findVarHandle(PageSharedLine.class, "freeBits4", long.class);
        FREE_BITS_5 = lookup.findVarHandle(PageSharedLine.class, "freeBits5", long.class);
        FREE_BITS_6 = lookup.findVarHandle(PageSharedLine.class, "freeBits6", long.class);
        FREE_BITS_7 = lookup.findVarHandle(PageSharedLine.class, "freeBits7", long.class);
        STATE = lookup.findVarHandle(PageSharedLine.class, "state", int.class);
      } catch (ReflectiveOperationException failure) {
        throw new ExceptionInInitializerError(failure);
      }
    }

    final int id;
    final long pageKey;
    final long address;
    final int pageBytes;
    final int sizeClass;
    final int slotBytes;
    final int slotCount;
    final boolean directMapped;
    Page(
        SizeClassState ownerClass,
        int id,
        long pageKey,
        long address,
        int sizeClass,
        int slotBytes,
        int pageBytes,
        boolean directMapped) {
      this.ownerClass = ownerClass;
      this.id = id;
      this.pageKey = pageKey;
      this.address = address;
      this.pageBytes = pageBytes;
      this.sizeClass = sizeClass;
      this.slotBytes = slotBytes;
      this.slotCount = pageBytes / slotBytes;
      this.directMapped = directMapped;
      if (slotCount > FREE_BITMAP_WORDS * Long.SIZE) {
        throw new IllegalArgumentException("allocator page has too many slots: " + slotCount);
      }
      state = ACTIVE;
    }

    long allocateSlot() {
      int slot = popFreeSlot();
      if (slot < 0) {
        slot = (int) nextSlot;
        if (slot >= slotCount) {
          return 0L;
        }
        nextSlot = slot + 1;
      }
      long block = blockFor(slot);
      NativeMemory.putLong(block + 56L, handleAt(slot));
      ALLOCATED_SLOTS.setRelease(this, allocatedSlots + 1L);
      return block;
    }

    int freeSlot(long block, long handle) {
      publishFreeSlot(slotOf(handle));
      int remaining = recordFreedSlots(1);
      checkRemaining(remaining, handle, block);
      return remaining;
    }

    int freeEntries(long[] entries, long[] handles, int[] indexes, int offset, int count) {
      if (entries == null
          || handles == null
          || indexes == null
          || offset < 0
          || count <= 0
          || offset > entries.length - count
          || offset > handles.length - count
          || offset > indexes.length - count) {
        throw new IllegalArgumentException("invalid page free batch");
      }
      for (int index = 0; index < count; index++) {
        int recordIndex = indexes[offset + index];
        long handle = handles[recordIndex];
        if (!ownsEntry(entries[recordIndex] - PREFIX_BYTES, handle)) {
          throw new IllegalStateException("small allocation has no stable slot handle");
        }
      }
      long bits0 = 0L;
      long bits1 = 0L;
      long bits2 = 0L;
      long bits3 = 0L;
      long bits4 = 0L;
      long bits5 = 0L;
      long bits6 = 0L;
      long bits7 = 0L;
      for (int index = 0; index < count; index++) {
        int slot = slotOf(handles[indexes[offset + index]]);
        long bit = 1L << (slot & 63);
        switch (slot >>> 6) {
          case 0:
            bits0 |= bit;
            break;
          case 1:
            bits1 |= bit;
            break;
          case 2:
            bits2 |= bit;
            break;
          case 3:
            bits3 |= bit;
            break;
          case 4:
            bits4 |= bit;
            break;
          case 5:
            bits5 |= bit;
            break;
          case 6:
            bits6 |= bit;
            break;
          case 7:
            bits7 |= bit;
            break;
          default:
            throw new AssertionError(slot);
        }
      }
      publishFreeBits(bits0, bits1, bits2, bits3, bits4, bits5, bits6, bits7);
      int remaining = recordFreedSlots(count);
      checkRemaining(remaining, 0L, 0L);
      return remaining;
    }

    /**
     * Grouped release with a whole-page fast path: when this group frees the last live slots
     * of a detached (FULL) page, skip the bitmap publication, reset the page to fresh bump
     * state under the FULL→AVAILABLE claim, and return {@link #PAGE_RECYCLED_PENDING_PUBLICATION}
     * — the caller publishes after its per-record epilogue. Misses fall back to the exact
     * slow-path publication (the freed count is never taken twice).
     */
    int freeValidatedSlotsTurn(
        int[] slots, int[] indexes, int offset, int count, long[] mask) {
      if (slots == null
          || indexes == null
          || mask == null
          || mask.length < FREE_BITMAP_WORDS
          || offset < 0
          || count <= 0
          || offset > indexes.length - count) {
        throw new IllegalArgumentException("invalid native page free batch");
      }
      // Whole-page candidacy first: the fast path never publishes a bitmap, so its slot mask
      // is built only when a publication actually needs it.
      long freed = (long) FREED_SLOTS.getVolatile(this);
      long allocated = (long) ALLOCATED_SLOTS.getAcquire(this);
      if (freed + (long) count == allocated && state() == FULL) {
        int remaining = recordFreedSlots(count);
        if (remaining == 0 && STATE.compareAndSet(this, FULL, AVAILABLE)) {
          resetFreeStateForReuse();
          return PAGE_RECYCLED_PENDING_PUBLICATION;
        }
        // Candidate miss after the count was recorded (owner raced the counters or the
        // transition): publish only the bitmap; the freed count is already durable.
        buildFreeMaskInto(slots, indexes, offset, count, mask);
        publishFreeBits(
            mask[0], mask[1], mask[2], mask[3], mask[4], mask[5], mask[6], mask[7]);
        checkRemaining(remaining, 0L, 0L);
        return remaining;
      }
      buildFreeMaskInto(slots, indexes, offset, count, mask);
      publishFreeBits(
          mask[0], mask[1], mask[2], mask[3], mask[4], mask[5], mask[6], mask[7]);
      int remaining = recordFreedSlots(count);
      checkRemaining(remaining, 0L, 0L);
      return remaining;
    }

    /** Publishes a recycled page after the caller's epilogue; fires the test hook like the
     * availability callback path. */
    void publishRecycledPage() {
      publishAvailable();
      Runnable hook = ownerClass.arena.retirementHookForTest();
      if (hook != null) {
        hook.run();
      }
    }

    /** Rewinds the allocator to fresh (bump-mode) state; caller holds the FULL→AVAILABLE claim. */
    private void resetFreeStateForReuse() {
      FREE_BITS_0.set(this, 0L);
      FREE_BITS_1.set(this, 0L);
      FREE_BITS_2.set(this, 0L);
      FREE_BITS_3.set(this, 0L);
      FREE_BITS_4.set(this, 0L);
      FREE_BITS_5.set(this, 0L);
      FREE_BITS_6.set(this, 0L);
      FREE_BITS_7.set(this, 0L);
      FREE_SUMMARY.set(this, 0L);
      nextSlot = 0;
    }

    private void buildFreeMaskInto(int[] slots, int[] indexes, int offset, int count, long[] mask) {
      for (int word = 0; word < FREE_BITMAP_WORDS; word++) {
        mask[word] = 0L;
      }
      for (int index = 0; index < count; index++) {
        int slot = slots[indexes[offset + index]];
        mask[slot >>> 6] |= 1L << (slot & 63);
      }
    }

    void ownerExhausted() {
      if (STATE.compareAndSet(this, ACTIVE, FULL)) {
        publishAvailableIfFull();
      }
    }

    void detachOwner() {
      ownerExhausted();
    }

    void publishAvailableAfterRemoteFree() {
      if (state() != FULL) {
        return;
      }
      if (STATE.compareAndSet(this, FULL, AVAILABLE)) {
        publishAvailable();
      }
    }

    void completeRemoteFree(int remaining) {
      ownerClass.arena.completeRemoteFree(this, remaining);
    }

    void publishAvailableIfFull() {
      if (state() != FULL || !hasFreeSlot()) {
        return;
      }
      if (!STATE.compareAndSet(this, FULL, AVAILABLE)) {
        return;
      }
      publishAvailable();
    }

    void publishAvailable() {
      // Read the owner after the FULL->AVAILABLE CAS won; a pre-CAS snapshot could be stale.
      SizeClassState owner = ownerClass;
      // Retain only pages with live slots; completely free pages must stay trimmable.
      if (hasLiveSlots() && owner.tryRetainPage(this)) {
        if (!hasLiveSlots()) {
          // The page completed freeing while retaining; hand it to the shared stack.
          if (owner.tryReleaseRetainedPage(this)) {
            owner.arena.memory.publishAvailablePageGlobal(this);
          }
        }
        return;
      }
      owner.arena.memory.publishAvailablePage(this);
    }

    /** Moves an owner-retained copy of this page back to the shared ready stack. */
    private void releaseRetentionToSharedStack() {
      SizeClassState owner = ownerClass;
      if (owner != null && owner.tryReleaseRetainedPage(this)) {
        owner.arena.memory.publishAvailablePageGlobal(this);
      }
    }

    boolean tryActivate(SizeClassState expectedOwner) {
      return ownerClass == expectedOwner && STATE.compareAndSet(this, AVAILABLE, ACTIVE);
    }

    boolean tryRebalance(SizeClassState expectedOwner, SizeClassState newOwner) {
      if (ownerClass != expectedOwner
          || !STATE.compareAndSet(this, AVAILABLE, REBALANCING)) {
        return false;
      }
      ownerClass = newOwner;
      STATE.setRelease(this, ACTIVE);
      return true;
    }

    boolean beginTrimming() {
      long freed = (long) FREED_SLOTS.getAcquire(this);
      long allocated = (long) ALLOCATED_SLOTS.getAcquire(this);
      return allocated == freed && STATE.compareAndSet(this, AVAILABLE, TRIMMING);
    }

    boolean freePhysical(boolean force) {
      for (;;) {
        int current = state();
        if (current == FREED) {
          return false;
        }
        if (!force && current != TRIMMING) {
          return false;
        }
        if (STATE.compareAndSet(this, current, FREED)) {
          return true;
        }
      }
    }

    private boolean hasFreeSlot() {
      return freeSummary() != 0L || nextSlot < slotCount;
    }

    private boolean hasLiveSlots() {
      return (long) ALLOCATED_SLOTS.getAcquire(this) != (long) FREED_SLOTS.getAcquire(this);
    }

    private int popFreeSlot() {
      for (;;) {
        long summary = freeSummary();
        if (summary == 0L) {
          return -1;
        }
        int word = Long.numberOfTrailingZeros(summary);
        long bits = freeBits(word);
        if (bits == 0L) {
          clearFreeSummaryBit(word);
          continue;
        }
        int bit = Long.numberOfTrailingZeros(bits);
        long updated = bits & ~(1L << bit);
        if (compareAndSetFreeBits(word, bits, updated)) {
          if (updated == 0L) {
            clearFreeSummaryBit(word);
          }
          return (word << 6) + bit;
        }
      }
    }

    private void publishFreeSlot(int slot) {
      int word = slot >>> 6;
      long bit = 1L << (slot & 63);
      if (orFreeBits(word, bit) == 0L) {
        FREE_SUMMARY.getAndBitwiseOr(this, 1L << word);
      }
    }

    private void publishFreeBits(
        long bits0,
        long bits1,
        long bits2,
        long bits3,
        long bits4,
        long bits5,
        long bits6,
        long bits7) {
      long summary = 0L;
      summary = publishFreeWord(0, bits0, summary);
      summary = publishFreeWord(1, bits1, summary);
      summary = publishFreeWord(2, bits2, summary);
      summary = publishFreeWord(3, bits3, summary);
      summary = publishFreeWord(4, bits4, summary);
      summary = publishFreeWord(5, bits5, summary);
      summary = publishFreeWord(6, bits6, summary);
      summary = publishFreeWord(7, bits7, summary);
      if (summary != 0L) {
        FREE_SUMMARY.getAndBitwiseOr(this, summary);
      }
    }

    private long publishFreeWord(int word, long bits, long summary) {
      if (bits == 0L) {
        return summary;
      }
      return orFreeBits(word, bits) == 0L ? summary | (1L << word) : summary;
    }

    private void clearFreeSummaryBit(int word) {
      long bit = 1L << word;
      for (;;) {
        long current = freeSummary();
        if ((current & bit) == 0L
            || FREE_SUMMARY.compareAndSet(this, current, current & ~bit)) {
          break;
        }
      }
      if (freeBits(word) != 0L) {
        FREE_SUMMARY.getAndBitwiseOr(this, bit);
      }
    }

    private void checkRemaining(int remaining, long handle, long block) {
      if (remaining >= 0) {
        return;
      }
      throw new IllegalStateException(
          "allocator page live-slot underflow"
              + ": pageKey="
              + pageKey
              + ", pageId="
              + id
              + ", sizeClass="
              + sizeClass
              + ", handle="
              + handle
              + ", block="
              + block
              + ", liveSlots="
              + remaining
              + ", state="
              + state());
    }

    private int recordFreedSlots(int count) {
      long freed = (long) FREED_SLOTS.getAndAdd(this, (long) count) + count;
      long allocated = (long) ALLOCATED_SLOTS.getAcquire(this);
      // Both counters are cumulative. Java's two's-complement overflow preserves their bounded
      // modular difference because the number of simultaneously live slots never exceeds one page.
      long remaining = allocated - freed;
      if (remaining < Integer.MIN_VALUE || remaining > Integer.MAX_VALUE) {
        throw new IllegalStateException("allocator page live-slot count exceeds page bounds");
      }
      return (int) remaining;
    }

    private int state() {
      return (int) STATE.getVolatile(this);
    }

    long auditAllocatedSlots() {
      return (long) ALLOCATED_SLOTS.getAcquire(this);
    }

    private long freeSummary() {
      return (long) FREE_SUMMARY.getVolatile(this);
    }

    long handleAt(int slot) {
      if (slot < 0 || slot >= slotCount) {
        throw new IllegalArgumentException("slot out of range: " + slot);
      }
      return (pageKey << HANDLE_PAGE_SHIFT) | (slot & HANDLE_SLOT_MASK);
    }

    long blockFor(int slot) {
      return address + (long) slot * slotBytes;
    }

    boolean ownsEntry(long block, long handle) {
      return ownsEntry(handle >>> HANDLE_PAGE_SHIFT, block, slotOf(handle));
    }

    /** Checks decoded ownership without repeating handle shifts in the grouped release loop. */
    boolean ownsEntry(long expectedPageKey, long block, int slot) {
      return expectedPageKey == pageKey
          && slot >= 0
          && slot < slotCount
          && block == address + (long) slot * slotBytes;
    }

    /** Full first-record validation for a page descriptor resolved from the table or memo. */
    boolean matchesRelease(long expectedPageKey, int expectedSizeClass, long block, int slot) {
      int currentState = state();
      return currentState != TRIMMING
          && currentState != FREED
          && sizeClass == expectedSizeClass
          && ownsEntry(expectedPageKey, block, slot);
    }

    private int slotForBlock(long block) {
      long offset = block - address;
      if (offset < 0L || offset % slotBytes != 0L) {
        throw new IllegalStateException("entry does not belong to allocator page " + pageKey);
      }
      long slot = offset / slotBytes;
      if (slot >= slotCount) {
        throw new IllegalStateException("entry slot is outside allocator page " + pageKey);
      }
      return (int) slot;
    }

    static int slotOf(long handle) {
      return (int) (handle & HANDLE_SLOT_MASK);
    }

    private long freeBits(int word) {
      switch (word) {
        case 0:
          return (long) FREE_BITS_0.getVolatile(this);
        case 1:
          return (long) FREE_BITS_1.getVolatile(this);
        case 2:
          return (long) FREE_BITS_2.getVolatile(this);
        case 3:
          return (long) FREE_BITS_3.getVolatile(this);
        case 4:
          return (long) FREE_BITS_4.getVolatile(this);
        case 5:
          return (long) FREE_BITS_5.getVolatile(this);
        case 6:
          return (long) FREE_BITS_6.getVolatile(this);
        case 7:
          return (long) FREE_BITS_7.getVolatile(this);
        default:
          throw new AssertionError(word);
      }
    }

    private boolean compareAndSetFreeBits(int word, long expected, long updated) {
      switch (word) {
        case 0:
          return FREE_BITS_0.compareAndSet(this, expected, updated);
        case 1:
          return FREE_BITS_1.compareAndSet(this, expected, updated);
        case 2:
          return FREE_BITS_2.compareAndSet(this, expected, updated);
        case 3:
          return FREE_BITS_3.compareAndSet(this, expected, updated);
        case 4:
          return FREE_BITS_4.compareAndSet(this, expected, updated);
        case 5:
          return FREE_BITS_5.compareAndSet(this, expected, updated);
        case 6:
          return FREE_BITS_6.compareAndSet(this, expected, updated);
        case 7:
          return FREE_BITS_7.compareAndSet(this, expected, updated);
        default:
          throw new AssertionError(word);
      }
    }

    private long orFreeBits(int word, long bits) {
      switch (word) {
        case 0:
          return (long) FREE_BITS_0.getAndBitwiseOr(this, bits);
        case 1:
          return (long) FREE_BITS_1.getAndBitwiseOr(this, bits);
        case 2:
          return (long) FREE_BITS_2.getAndBitwiseOr(this, bits);
        case 3:
          return (long) FREE_BITS_3.getAndBitwiseOr(this, bits);
        case 4:
          return (long) FREE_BITS_4.getAndBitwiseOr(this, bits);
        case 5:
          return (long) FREE_BITS_5.getAndBitwiseOr(this, bits);
        case 6:
          return (long) FREE_BITS_6.getAndBitwiseOr(this, bits);
        case 7:
          return (long) FREE_BITS_7.getAndBitwiseOr(this, bits);
        default:
          throw new AssertionError(word);
      }
    }
  }
}
