package com.red.ohc.runtime;

import com.red.ohc.index.Entry;

/** Fixed-size SPSC access sampler: the business thread publishes, the worker consumes. */
public final class AccessRing {
    private static final int CAPACITY = 256;
    private static final int MASK = CAPACITY - 1;
    private final Entry[] entries = new Entry[CAPACITY];
    private final long[] generations = new long[CAPACITY];
    private volatile int head;
    private volatile int tail;

    public boolean offer(Entry entry, long generation) {
        int current = head;
        if (current - tail == CAPACITY) return false;
        int slot = current & MASK;
        entries[slot] = entry;
        generations[slot] = generation;
        head = current + 1;
        return true;
    }

    public boolean poll(AccessConsumer consumer) {
        int current = tail;
        if (current == head) return false;
        int slot = current & MASK;
        Entry entry = entries[slot];
        entries[slot] = null;
        consumer.accept(entry, generations[slot]);
        tail = current + 1;
        return true;
    }

    public boolean isEmpty() {
        return tail == head;
    }
}
