package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ReaderSlot {
    public volatile long epoch;
    /** Set only while this ThreadContext owns native allocation or retirement work. */
    public volatile boolean writerActive;
    public long localHits;
    public long localMisses;
    public long localAccessDropped;
    public volatile long publishedHits;
    public volatile long publishedMisses;
    public volatile long publishedAccessDropped;
    /** Producer-to-maintenance hint; the CAS closes the actor-clear/producer-offer wake window. */
    private final AtomicBoolean accessPending = new AtomicBoolean();
    public long consumedHits;
    public long consumedMisses;
    public long consumedAccessDropped;
    public final AccessRing access = new AccessRing();

    public boolean markAccessPending() {
        return accessPending.compareAndSet(false, true);
    }

    public boolean clearAccessPending() {
        return accessPending.compareAndSet(true, false);
    }

    public boolean hasAccessPending() {
        return accessPending.get();
    }
}
