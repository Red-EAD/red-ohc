package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * One cache-owned allocator stripe. Allocation is local to a size-class page; only the page's
 * slot stack is contended. A page that is full and has no live slots leaves the stripe entirely,
 * allowing {@link PageDepot} to reuse it from another writer stripe.
 */
public final class WriterArena {
    public static final int PREFIX_BYTES = 16;
    public static final int DIRECT_CLASS = 0xffff;
    static final int HANDLE_SLOT_BITS = 9;
    static final int PAGE_ID_BITS = 18;
    static final int PAGE_ID_MASK = (1 << PAGE_ID_BITS) - 1;
    private static final int HANDLE_SLOT_MASK = (1 << HANDLE_SLOT_BITS) - 1;
    private static final long HANDLE_MASK = (1L << 31) - 1L;
    private static final long STAMP_MASK = (1L << (Long.SIZE - 31)) - 1L;

    private final NativeMemory.Memory memory;
    private final int id;
    /** At most one partial page per size class remains owned by a stripe. */
    private final AtomicReferenceArray<Page> currentPages = new AtomicReferenceArray<>(SizeClasses.count());

    WriterArena(NativeMemory.Memory memory, int id) {
        this.memory = memory;
        this.id = id;
    }

    public long allocate(long entryBytes) {
        int sizeClass = SizeClasses.indexForEntry(entryBytes);
        if (sizeClass < 0) return allocateDirect(entryBytes);
        for (;;) {
            Page page = currentPages.get(sizeClass);
            if (page == null) {
                Page acquired = memory.acquireEntryPage(sizeClass);
                if (!currentPages.compareAndSet(sizeClass, null, acquired)) {
                    memory.returnUnusedPage(acquired);
                    continue;
                }
                page = acquired;
            }
            long block = page.allocateSlot();
            if (block == 0L) {
                currentPages.compareAndSet(sizeClass, page, null);
                continue;
            }
            NativeMemory.putLong(block, metadata(sizeClass));
            return block + PREFIX_BYTES;
        }
    }

    /** Worker-side QSBR reclaim returns the slot to the fixed stripe encoded in its prefix. */
    public void remoteFree(long block, int sizeClass) {
        int handle = (int) NativeMemory.getLong(block + 8L);
        if (handle == 0 || (handle & ~HANDLE_MASK) != 0L) {
            throw new IllegalStateException("small allocation has no stable slot handle");
        }
        Page page = memory.pageForHandle(handle);
        if (page == null || page.sizeClass != sizeClass) {
            throw new IllegalStateException("unknown allocator slot handle " + handle);
        }
        page.freeSlot(block, handle);
        if (page.isFullAndEmpty()) {
            currentPages.compareAndSet(sizeClass, page, null);
            if (page.beginRetirement()) memory.releaseEmptyPage(page);
        }
    }

    /** Close has already excluded all writers/readers; physical page freeing is owned by Memory. */
    public void releasePages() {
        for (int index = 0; index < SizeClasses.count(); index++) currentPages.set(index, null);
    }

    public static long directAllocationBytes(long entryBytes) {
        return SizeClasses.directBytes(entryBytes);
    }

    /** Native weight consumed by one allocation, including the allocator prefix and class rounding. */
    public static long allocationWeight(long entryBytes) {
        int sizeClass = SizeClasses.indexForEntry(entryBytes);
        return sizeClass < 0 ? directAllocationBytes(entryBytes) : SizeClasses.slotBytes(sizeClass);
    }

    private long allocateDirect(long entryBytes) {
        long block = memory.allocateDirectEntry(directAllocationBytes(entryBytes));
        NativeMemory.putLong(block, metadata(DIRECT_CLASS));
        NativeMemory.putLong(block + 8L, 0L);
        return block + PREFIX_BYTES;
    }

    private long metadata(int sizeClass) {
        return (id & 0xffffffffL) | (((long) sizeClass & 0xffffL) << 32);
    }

    /**
     * A registered physical page. The stamped free-head prevents ABA among concurrent frees and
     * allocations; the state/in-flight pair prevents an empty page being returned while a writer
     * has observed it but has not yet claimed a slot.
     */
    static final class Page {
        private static final int ACTIVE = 1;
        private static final int RETIRING = 2;
        private static final int DEPOT = 3;
        private static final int FREED = 4;

        /** Reusable sparse page-table index; the encoded key also carries its allocation version. */
        final int id;
        final int pageKey;
        final long address;
        final int sizeClass;
        final int slotBytes;
        final int slotCount;
        final AtomicInteger nextSlot = new AtomicInteger();
        final AtomicInteger liveSlots = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        /** Low 31 bits are a page/slot handle, high 33 bits form an ABA stamp. */
        final AtomicLong freeHead = new AtomicLong();
        final AtomicInteger state = new AtomicInteger(ACTIVE);

        Page(int id, int pageKey, long address, int sizeClass, int slotBytes) {
            this.id = id;
            this.pageKey = pageKey;
            this.address = address;
            this.sizeClass = sizeClass;
            this.slotBytes = slotBytes;
            this.slotCount = SizeClasses.PAGE_BYTES / slotBytes;
        }

        long allocateSlot() {
            if (!enterAllocation()) return 0L;
            try {
                int handle = popFreeHandle();
                if (handle != 0) {
                    long block = blockFor(handle & HANDLE_SLOT_MASK);
                    NativeMemory.putLong(block + 8L, handle & HANDLE_MASK);
                    liveSlots.incrementAndGet();
                    return block;
                }
                int slot = nextSlot.getAndIncrement();
                if (slot >= slotCount) return 0L;
                long block = blockFor(slot);
                NativeMemory.putLong(block + 8L, handleAt(slot) & HANDLE_MASK);
                liveSlots.incrementAndGet();
                return block;
            } finally {
                inFlight.decrementAndGet();
            }
        }

        void freeSlot(long block, int handle) {
            pushFreeHandle(block, handle);
            int remaining = liveSlots.decrementAndGet();
            if (remaining < 0) throw new IllegalStateException("allocator page live-slot underflow");
        }

        boolean isFullAndEmpty() {
            return nextSlot.get() >= slotCount && liveSlots.get() == 0;
        }

        /** Transitions only a stable, fully empty page out of a stripe. */
        boolean beginRetirement() {
            if (!state.compareAndSet(ACTIVE, RETIRING)) return false;
            if (inFlight.get() == 0 && liveSlots.get() == 0 && nextSlot.get() >= slotCount) return true;
            state.compareAndSet(RETIRING, ACTIVE);
            return false;
        }

        boolean moveToDepot() {
            return state.compareAndSet(RETIRING, DEPOT);
        }

        boolean acquireFromDepot() {
            return state.compareAndSet(DEPOT, ACTIVE);
        }

        boolean freePhysical() {
            for (;;) {
                int current = state.get();
                if (current == FREED) return false;
                if (state.compareAndSet(current, FREED)) return true;
            }
        }

        private boolean enterAllocation() {
            for (;;) {
                if (state.get() != ACTIVE) return false;
                inFlight.incrementAndGet();
                if (state.get() == ACTIVE) return true;
                inFlight.decrementAndGet();
            }
        }

        private int popFreeHandle() {
            for (;;) {
                long state = freeHead.get();
                int handle = (int) (state & HANDLE_MASK);
                if (handle == 0) return 0;
                long block = blockFor(handle & HANDLE_SLOT_MASK);
                int next = (int) NativeMemory.getLong(block + 8L);
                if (freeHead.compareAndSet(state, nextState(state, next))) return handle;
            }
        }

        private void pushFreeHandle(long block, int handle) {
            for (;;) {
                long state = freeHead.get();
                int head = (int) (state & HANDLE_MASK);
                NativeMemory.putLong(block + 8L, head & HANDLE_MASK);
                if (freeHead.compareAndSet(state, nextState(state, handle))) return;
            }
        }

        int handleAt(int slot) {
            if (slot < 0 || slot >= slotCount) throw new IllegalArgumentException("slot out of range: " + slot);
            return (pageKey << HANDLE_SLOT_BITS) | slot;
        }

        long blockFor(int slot) { return address + (long) slot * slotBytes; }

        private static long nextState(long state, int handle) {
            long stamp = ((state >>> 31) + 1L) & STAMP_MASK;
            return (stamp << 31) | (handle & HANDLE_MASK);
        }
    }
}
