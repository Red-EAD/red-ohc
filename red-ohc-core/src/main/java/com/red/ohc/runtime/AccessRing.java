package com.red.ohc.runtime;

import com.red.ohc.index.Entry;

/** Fixed-size SPSC access sampler: the business thread publishes, the worker consumes. */
public final class AccessRing {
    private final Entry[] entries = new Entry[16];
    private final long[] generations = new long[16];
    private volatile int head;
    private volatile int tail;

    public boolean offer(Entry entry, long generation) {
        int next = (head + 1) & 15;
        if (next == tail) return false;
        entries[head] = entry;
        generations[head] = generation;
        head = next;
        return true;
    }

    public boolean poll(AccessConsumer consumer) {
        int current = tail;
        if (current == head) return false;
        Entry entry = entries[current];
        entries[current] = null;
        consumer.accept(entry, generations[current]);
        tail = (current + 1) & 15;
        return true;
    }
}
