package com.red.ohc.runtime;

public final class ReaderSlot {
  public volatile long epoch;

  /** Set only while this ThreadContext owns native allocation or retirement work. */
  public volatile boolean writerActive;

  public long localHits;
  public long localMisses;
  public volatile long publishedHits;
  public volatile long publishedMisses;

  public long consumedHits;
  public long consumedMisses;
  /** Lazily allocated after the first sampled access. */
  public volatile AccessRing access;
}
