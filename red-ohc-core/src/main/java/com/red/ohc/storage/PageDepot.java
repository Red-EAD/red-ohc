package com.red.ohc.storage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded cache-owned pool of fully empty 64KiB pages shared by all allocator stripes. */
final class PageDepot {
    private static final int MAX_IDLE_PAGES = 128;

    private final NativeMemory.Memory memory;
    private final ConcurrentLinkedQueue<WriterArena.Page>[] pages;
    private final AtomicInteger idlePages = new AtomicInteger();

    @SuppressWarnings("unchecked")
    PageDepot(NativeMemory.Memory memory) {
        this.memory = memory;
        this.pages = new ConcurrentLinkedQueue[SizeClasses.count()];
        for (int index = 0; index < pages.length; index++) pages[index] = new ConcurrentLinkedQueue<>();
    }

    WriterArena.Page acquire(int sizeClass) {
        ConcurrentLinkedQueue<WriterArena.Page> queue = pages[sizeClass];
        for (;;) {
            WriterArena.Page page = queue.poll();
            if (page == null) return null;
            idlePages.decrementAndGet();
            if (page.acquireFromDepot()) return page;
        }
    }

    void release(WriterArena.Page page) {
        if (!page.moveToDepot()) return;
        if (idlePages.incrementAndGet() <= MAX_IDLE_PAGES) {
            pages[page.sizeClass].offer(page);
            return;
        }
        idlePages.decrementAndGet();
        memory.freeEntryPage(page);
    }

    void clear() {
        for (ConcurrentLinkedQueue<WriterArena.Page> queue : pages) queue.clear();
        idlePages.set(0);
    }
}
