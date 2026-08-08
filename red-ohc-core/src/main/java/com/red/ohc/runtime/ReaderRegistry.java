package com.red.ohc.runtime;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A lock-free, append-only reader registry. Registration copies a tiny array once per
 * ThreadContext; the maintenance actor scans the already-published array without allocating an
 * Iterator on each QSBR pass.
 */
public final class ReaderRegistry {
    private static final ReaderSlot[] EMPTY = new ReaderSlot[0];
    private final AtomicReference<ReaderSlot[]> slots = new AtomicReference<>(EMPTY);

    public void register(ReaderSlot slot) {
        for (;;) {
            ReaderSlot[] current = slots.get();
            for (ReaderSlot existing : current) if (existing == slot) return;
            ReaderSlot[] updated = Arrays.copyOf(current, current.length + 1);
            updated[current.length] = slot;
            if (slots.compareAndSet(current, updated)) return;
        }
    }

    /** Acquire-published snapshot for actor-only scanning; callers must not mutate it. */
    public ReaderSlot[] snapshot() {
        return slots.get();
    }

    public boolean hasActiveReader() {
        for (ReaderSlot slot : slots.get()) if (slot.epoch != 0L) return true;
        return false;
    }

    /** Close-side scan of distributed writer admission flags; normal writers never share a counter. */
    public boolean hasActiveWriter() {
        for (ReaderSlot slot : slots.get()) if (slot.writerActive) return true;
        return false;
    }

    public int activeWriterCount() {
        int count = 0;
        for (ReaderSlot slot : slots.get()) if (slot.writerActive) count++;
        return count;
    }
}
