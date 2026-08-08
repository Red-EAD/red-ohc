package com.red.ohc.runtime;

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
    /** Producer-to-maintenance hint; the actor clears it before scanning this slot. */
    public volatile boolean accessPending;
    public long consumedHits;
    public long consumedMisses;
    public long consumedAccessDropped;
    public final AccessRing access = new AccessRing();
}
