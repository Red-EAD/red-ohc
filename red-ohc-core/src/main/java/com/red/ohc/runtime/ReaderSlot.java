package com.red.ohc.runtime;

public final class ReaderSlot {
    public volatile long epoch;
    public long localHits;
    public long localMisses;
    public long localAccessDropped;
    public volatile long publishedHits;
    public volatile long publishedMisses;
    public volatile long publishedAccessDropped;
    public long consumedHits;
    public long consumedMisses;
    public long consumedAccessDropped;
    public final AccessRing access = new AccessRing();
}
