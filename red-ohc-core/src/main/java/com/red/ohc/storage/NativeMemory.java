package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.red.ohc.AllocatorType;

import sun.misc.Unsafe;

/** Native allocation and primitive access. JNA remains the production default. */
public final class NativeMemory {
    static final Unsafe U;
    static final long BYTE_ARRAY_BASE;

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

    private NativeMemory() { }

    public static final class Memory {
        private final NativeAllocator allocator;
        private final long hardLimit;
        private final AtomicLong allocated = new AtomicLong();
        private final AtomicLong rawAllocations = new AtomicLong();
        private final AtomicLong entryAllocations = new AtomicLong();
        private final WriterArena[] arenas;
        private final int stripeMask;
        private static final int PAGE_CHUNK_BITS = 10;
        private static final int PAGE_CHUNK_SIZE = 1 << PAGE_CHUNK_BITS;
        private static final int PAGE_CHUNK_COUNT = 1 << (WriterArena.PAGE_ID_BITS - PAGE_CHUNK_BITS);
        private static final int MAX_PAGE_ID = WriterArena.PAGE_ID_MASK;
        private static final int PAGE_VERSION_BITS = 22 - WriterArena.PAGE_ID_BITS;
        private static final int PAGE_VERSION_MASK = (1 << PAGE_VERSION_BITS) - 1;
        private final AtomicInteger nextPageId = new AtomicInteger(1);
        /** Sparse page-id free stack; nodes are indices in fixed primitive arrays, never Page objects. */
        private final AtomicInteger freePageIdHead = new AtomicInteger();
        private final AtomicIntegerArray freePageIdNext = new AtomicIntegerArray(MAX_PAGE_ID + 1);
        private final AtomicIntegerArray pageVersions = new AtomicIntegerArray(MAX_PAGE_ID + 1);
        private final AtomicReferenceArray<AtomicReferenceArray<WriterArena.Page>> pageChunks =
                new AtomicReferenceArray<>(PAGE_CHUNK_COUNT);
        private final PageDepot pageDepot;

        public Memory(AllocatorType type) {
            this(type, Long.MAX_VALUE);
        }

        public Memory(AllocatorType type, long hardLimit) {
            if (hardLimit <= 0L) throw new IllegalArgumentException("hardLimit must be positive");
            this.allocator = new NativeAllocator(type);
            this.hardLimit = hardLimit;
            this.pageDepot = new PageDepot(this);
            int stripeCount = 1;
            int target = Math.max(1, Runtime.getRuntime().availableProcessors() * 4);
            while (stripeCount < target && stripeCount < (1 << 30)) stripeCount <<= 1;
            this.arenas = new WriterArena[stripeCount];
            for (int index = 0; index < stripeCount; index++) arenas[index] = new WriterArena(this, index + 1);
            this.stripeMask = stripeCount - 1;
        }

        public long allocate(long bytes) {
            long address = allocateRaw(bytes);
            U.setMemory(address, bytes, (byte) 0);
            return address;
        }

        public long allocateRaw(long bytes) {
            if (bytes <= 0L) throw new IllegalArgumentException("bytes must be positive");
            reservePhysical(bytes);
            try {
                long address = allocator.allocate(bytes);
                rawAllocations.incrementAndGet();
                return address;
            } catch (Throwable failure) {
                allocated.addAndGet(-bytes);
                throw failure;
            }
        }

        public void free(long address, long bytes) {
            if (address == 0L) return;
            allocator.free(address);
            allocated.addAndGet(-bytes);
        }

        public long allocated() { return allocated.get(); }
        public long hardLimit() { return hardLimit; }

        public long rawAllocationCount() { return rawAllocations.get(); }
        public long entryAllocationCount() { return entryAllocations.get(); }

        public long allocateEntryPage() {
            entryAllocations.incrementAndGet();
            return allocateRaw(SizeClasses.PAGE_BYTES);
        }

        public long allocateDirectEntry(long bytes) {
            entryAllocations.incrementAndGet();
            return allocateRaw(bytes);
        }

        public WriterArena newWriterArena() {
            return writerForCurrentThread();
        }

        public WriterArena writerForCurrentThread() {
            return arenas[((int) Thread.currentThread().getId()) & stripeMask];
        }

        public int writerStripeCount() {
            return arenas.length;
        }

        public void releaseEntry(long entryAddress, long entryBytes) {
            if (entryAddress == 0L) return;
            long block = entryAddress - WriterArena.PREFIX_BYTES;
            long metadata = U.getLong(block);
            int arenaId = (int) metadata;
            int sizeClass = (int) ((metadata >>> 32) & 0xffffL);
            if (sizeClass == WriterArena.DIRECT_CLASS) {
                free(block, WriterArena.directAllocationBytes(entryBytes));
                return;
            }
            int index = arenaId - 1;
            if (index < 0 || index >= arenas.length) {
                throw new IllegalStateException("unknown allocator stripe: " + arenaId);
            }
            arenas[index].remoteFree(block, sizeClass);
        }

        public void closeArenas() {
            for (WriterArena arena : arenas) arena.releasePages();
            pageDepot.clear();
            for (int chunkIndex = 0; chunkIndex < pageChunks.length(); chunkIndex++) {
                AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(chunkIndex);
                if (chunk == null) continue;
                for (int slot = 0; slot < chunk.length(); slot++) {
                    WriterArena.Page page = chunk.get(slot);
                    if (page != null) freeEntryPage(page);
                }
            }
        }

        WriterArena.Page acquireEntryPage(int sizeClass) {
            WriterArena.Page reused = pageDepot.acquire(sizeClass);
            if (reused != null) return reused;
            int pageKey = nextPageKey();
            int pageId = pageKey & MAX_PAGE_ID;
            long address = allocateEntryPage();
            WriterArena.Page page = new WriterArena.Page(pageId, pageKey, address,
                    sizeClass, SizeClasses.slotBytes(sizeClass));
            registerPage(page);
            return page;
        }

        void returnUnusedPage(WriterArena.Page page) {
            freeEntryPage(page);
        }

        void releaseEmptyPage(WriterArena.Page page) {
            pageDepot.release(page);
        }

        void freeEntryPage(WriterArena.Page page) {
            if (!page.freePhysical()) return;
            unregisterPage(page);
            free(page.address, SizeClasses.PAGE_BYTES);
            recyclePageId(page.id);
        }

        int nextPageKey() {
            int pageId = takeFreePageId();
            if (pageId == 0) pageId = nextPageId.getAndIncrement();
            if (pageId <= 0 || pageId > MAX_PAGE_ID) {
                throw new AllocationLimitException(hardLimit, allocated.get(), SizeClasses.PAGE_BYTES);
            }
            int version = pageVersions.incrementAndGet(pageId) & PAGE_VERSION_MASK;
            return (version << WriterArena.PAGE_ID_BITS) | pageId;
        }

        void registerPage(WriterArena.Page page) {
            int chunkIndex = page.id >>> PAGE_CHUNK_BITS;
            AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(chunkIndex);
            if (chunk == null) {
                AtomicReferenceArray<WriterArena.Page> created = new AtomicReferenceArray<>(PAGE_CHUNK_SIZE);
                if (!pageChunks.compareAndSet(chunkIndex, null, created)) created = pageChunks.get(chunkIndex);
                chunk = created;
            }
            if (!chunk.compareAndSet(page.id & (PAGE_CHUNK_SIZE - 1), null, page)) {
                throw new IllegalStateException("duplicate native page id " + page.id);
            }
        }

        void unregisterPage(WriterArena.Page page) {
            AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(page.id >>> PAGE_CHUNK_BITS);
            if (chunk != null) chunk.compareAndSet(page.id & (PAGE_CHUNK_SIZE - 1), page, null);
        }

        WriterArena.Page pageForHandle(int handle) {
            int pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
            int pageId = pageKey & MAX_PAGE_ID;
            if (pageId == 0) return null;
            AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(pageId >>> PAGE_CHUNK_BITS);
            WriterArena.Page page = chunk == null ? null : chunk.get(pageId & (PAGE_CHUNK_SIZE - 1));
            return page != null && page.pageKey == pageKey ? page : null;
        }

        private int takeFreePageId() {
            for (;;) {
                int head = freePageIdHead.get();
                if (head == 0) return 0;
                int next = freePageIdNext.get(head);
                if (freePageIdHead.compareAndSet(head, next)) return head;
            }
        }

        private void recyclePageId(int pageId) {
            for (;;) {
                int head = freePageIdHead.get();
                freePageIdNext.set(pageId, head);
                if (freePageIdHead.compareAndSet(head, pageId)) return;
            }
        }

        private void reservePhysical(long bytes) {
            for (;;) {
                long current = allocated.get();
                if (current > hardLimit - bytes) {
                    throw new AllocationLimitException(hardLimit, current, bytes);
                }
                if (allocated.compareAndSet(current, current + bytes)) return;
            }
        }
    }

    /** Admission failure is recoverable for cache writes; it is not a JVM-wide OutOfMemoryError. */
    public static final class AllocationLimitException extends RuntimeException {
        AllocationLimitException(long hardLimit, long allocated, long requested) {
            super("native hard limit " + hardLimit + " exceeded: allocated=" + allocated + ", requested=" + requested);
        }
    }

    public static long getLong(long address) { return U.getLong(address); }
    public static long getLong(byte[] bytes, int offset) { return U.getLong(bytes, BYTE_ARRAY_BASE + offset); }
    public static int getInt(byte[] bytes, int offset) { return U.getInt(bytes, BYTE_ARRAY_BASE + offset); }
    public static Unsafe unsafe() { return U; }
    public static long byteArrayBaseOffset() { return BYTE_ARRAY_BASE; }
    /** The actual heap reference width used by this HotSpot process (compressed or wide OOPs). */
    public static int objectReferenceSize() { return U.arrayIndexScale(Object[].class); }
    public static long getLongVolatile(long address) { return U.getLongVolatile(null, address); }
    public static void putLong(long address, long value) { U.putLong(address, value); }
    public static void putLongRelease(long address, long value) { U.putOrderedLong(null, address, value); }
    public static int getInt(long address) { return U.getInt(address); }
    public static void putInt(long address, int value) { U.putInt(address, value); }
    public static byte getByte(long address) { return U.getByte(address); }
    public static void putByte(long address, byte value) { U.putByte(address, value); }
    public static void copy(byte[] source, int sourceOffset, long destination, long bytes) {
        U.copyMemory(source, BYTE_ARRAY_BASE + sourceOffset, null, destination, bytes);
    }
    public static void copy(long source, byte[] destination, int destinationOffset, long bytes) {
        U.copyMemory(null, source, destination, BYTE_ARRAY_BASE + destinationOffset, bytes);
    }
    public static boolean equals(long address, byte[] bytes, int offset, int length) {
        int i = 0;
        for (; i + 8 <= length; i += 8) {
            if (U.getLong(address + i) != U.getLong(bytes, BYTE_ARRAY_BASE + offset + i)) return false;
        }
        for (; i < length; i++) if (U.getByte(address + i) != bytes[offset + i]) return false;
        return true;
    }
    public static boolean equals(long left, long right, int length) {
        int i = 0;
        for (; i + 8 <= length; i += 8) if (U.getLong(left + i) != U.getLong(right + i)) return false;
        for (; i < length; i++) if (U.getByte(left + i) != U.getByte(right + i)) return false;
        return true;
    }
}
