package com.red.ohc.storage;

import java.util.concurrent.atomic.AtomicLongArray;

/** Shared overflow magazines. Writers only touch this while refilling a local list. */
public final class PageDepot {
    private final AtomicLongArray heads = new AtomicLongArray(SizeClasses.count());

    void offer(long block, int sizeClass) {
        long head;
        do {
            head = heads.get(sizeClass);
            NativeMemory.putLong(block + 8L, head);
        } while (!heads.compareAndSet(sizeClass, head, block));
    }

    long takeAll(int sizeClass) {
        return heads.getAndSet(sizeClass, 0L);
    }

    void clear() {
        for (int i = 0; i < SizeClasses.count(); i++) heads.set(i, 0L);
    }
}
