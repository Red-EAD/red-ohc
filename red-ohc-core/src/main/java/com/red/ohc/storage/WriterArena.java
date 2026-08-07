package com.red.ohc.storage;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Thread-confined small-object allocator. Remote reclamation is maintenance-side and only performs a
 * single compare-and-set per returned slot.
 */
public final class WriterArena {
    public static final int PREFIX_BYTES = 16;
    public static final int DIRECT_CLASS = 0xffff;
    private static final long LOCAL_CACHE_LIMIT = 256L << 10;

    private final NativeMemory.Memory memory;
    private final int id;
    private final PageDepot depot;
    private final long[] localHeads = new long[SizeClasses.count()];
    private final long[] pageAddresses = new long[SizeClasses.count()];
    private final int[] pageOffsets = new int[SizeClasses.count()];
    private final AtomicLongArray remoteHeads = new AtomicLongArray(SizeClasses.count());
    private final AtomicLong remoteBytes = new AtomicLong();
    private final LongArrayList pages = new LongArrayList();
    private long localBytes;

    public WriterArena(NativeMemory.Memory memory, int id, PageDepot depot) {
        this.memory = memory;
        this.id = id;
        this.depot = depot;
    }

    public long allocate(long entryBytes) {
        int sizeClass = SizeClasses.indexForEntry(entryBytes);
        if (sizeClass < 0) return allocateDirect(entryBytes);
        long block = popLocal(sizeClass);
        if (block == 0L) {
            remoteBytes.addAndGet(-drain(remoteHeads.getAndSet(sizeClass, 0L), sizeClass, false));
            block = popLocal(sizeClass);
        }
        if (block == 0L) {
            drain(depot.takeAll(sizeClass), sizeClass, false);
            block = popLocal(sizeClass);
        }
        if (block == 0L) block = allocateFromPage(sizeClass);
        NativeMemory.putLong(block, metadata(sizeClass));
        NativeMemory.putLong(block + 8L, 0L);
        return block + PREFIX_BYTES;
    }

    public void remoteFree(long block, int sizeClass) {
        long bytes = SizeClasses.slotBytes(sizeClass);
        if (remoteBytes.addAndGet(bytes) > LOCAL_CACHE_LIMIT) {
            remoteBytes.addAndGet(-bytes);
            depot.offer(block, sizeClass);
            return;
        }
        long head;
        do {
            head = remoteHeads.get(sizeClass);
            NativeMemory.putLong(block + 8L, head);
        } while (!remoteHeads.compareAndSet(sizeClass, head, block));
    }

    public void releasePages() {
        for (int i = 0; i < pages.size(); i++) memory.free(pages.getLong(i), SizeClasses.PAGE_BYTES);
        pages.clear();
    }

    public static long directAllocationBytes(long entryBytes) {
        return SizeClasses.directBytes(entryBytes);
    }

    private long allocateDirect(long entryBytes) {
        long block = memory.allocateDirectEntry(directAllocationBytes(entryBytes));
        NativeMemory.putLong(block, metadata(DIRECT_CLASS));
        NativeMemory.putLong(block + 8L, 0L);
        return block + PREFIX_BYTES;
    }

    private long allocateFromPage(int sizeClass) {
        int slotBytes = SizeClasses.slotBytes(sizeClass);
        long page = pageAddresses[sizeClass];
        int offset = pageOffsets[sizeClass];
        if (page == 0L || offset + slotBytes > SizeClasses.PAGE_BYTES) {
            page = memory.allocateEntryPage();
            pages.add(page);
            pageAddresses[sizeClass] = page;
            offset = 0;
        }
        pageOffsets[sizeClass] = offset + slotBytes;
        return page + offset;
    }

    private long popLocal(int sizeClass) {
        long block = localHeads[sizeClass];
        if (block != 0L) {
            localHeads[sizeClass] = NativeMemory.getLong(block + 8L);
            localBytes -= SizeClasses.slotBytes(sizeClass);
        }
        return block;
    }

    private long drain(long head, int sizeClass, boolean trim) {
        long slotBytes = SizeClasses.slotBytes(sizeClass);
        long drainedBytes = 0L;
        for (long block = head; block != 0L;) {
            long next = NativeMemory.getLong(block + 8L);
            NativeMemory.putLong(block + 8L, localHeads[sizeClass]);
            localHeads[sizeClass] = block;
            localBytes += slotBytes;
            drainedBytes += slotBytes;
            block = next;
        }
        if (trim || localBytes > LOCAL_CACHE_LIMIT) trimToDepot();
        return drainedBytes;
    }

    private void trimToDepot() {
        for (int sizeClass = 0; localBytes > LOCAL_CACHE_LIMIT && sizeClass < SizeClasses.count(); sizeClass++) {
            for (long block = popLocal(sizeClass); block != 0L && localBytes >= LOCAL_CACHE_LIMIT; block = popLocal(sizeClass)) {
                depot.offer(block, sizeClass);
            }
        }
    }

    private long metadata(int sizeClass) {
        return (id & 0xffffffffL) | (((long) sizeClass & 0xffffL) << 32);
    }
}
