package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ReaderSlot {
  public volatile long epoch;

  /** Set only while this ThreadContext owns native allocation or retirement work. */
  public volatile boolean writerActive;

  public long localHits;
  public long localMisses;
  public volatile long publishedHits;
  public volatile long publishedMisses;

  /** Producer-to-maintenance hint; the CAS closes the actor-clear/producer-offer wake window. */
  private final AtomicBoolean accessPending = new AtomicBoolean();

  public long consumedHits;
  public long consumedMisses;
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
