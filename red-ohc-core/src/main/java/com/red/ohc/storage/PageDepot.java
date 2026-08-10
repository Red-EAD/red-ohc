package com.red.ohc.storage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded cache-owned pool of fully empty 64KiB pages shared by all allocator stripes. */
final class PageDepot {
  private final NativeMemory.Memory memory;
  private final int maxIdlePages;
  private final ConcurrentLinkedQueue<WriterArena.Page>[] pages;
  private final AtomicInteger idlePages = new AtomicInteger();

  @SuppressWarnings("unchecked")
  PageDepot(NativeMemory.Memory memory, int maxIdlePages) {
    this.memory = memory;
    this.maxIdlePages = maxIdlePages;
    this.pages = new ConcurrentLinkedQueue[SizeClasses.count()];
    for (int index = 0; index < pages.length; index++) {
      pages[index] = new ConcurrentLinkedQueue<>();
    }
  }

  WriterArena.Page acquire(int sizeClass) {
    ConcurrentLinkedQueue<WriterArena.Page> queue = pages[sizeClass];
    while (true) {
      WriterArena.Page page = queue.poll();
      if (page == null) {
        return null;
      }
      idlePages.decrementAndGet();
      if (page.acquireFromDepot()) {
        return page;
      }
    }
  }

  void release(WriterArena.Page page) {
    if (!page.moveToDepot()) {
      return;
    }
    if (idlePages.incrementAndGet() <= maxIdlePages) {
      pages[page.sizeClass].offer(page);
      return;
    }
    idlePages.decrementAndGet();
    memory.freeEntryPage(page);
  }

  void clear() {
    trimIdlePages();
  }

  long trimIdlePages() {
    long freed = 0L;
    for (ConcurrentLinkedQueue<WriterArena.Page> queue : pages) {
      WriterArena.Page page;
      while ((page = queue.poll()) != null) {
        idlePages.decrementAndGet();
        // Go through Memory so the page-table entry and reusable page id are retired
        // together with the native bytes. A direct free would leave stale handles
        // addressable by a later remoteFree.
        memory.freeEntryPage(page);
        freed += SizeClasses.PAGE_BYTES;
      }
    }
    return freed;
  }
}
