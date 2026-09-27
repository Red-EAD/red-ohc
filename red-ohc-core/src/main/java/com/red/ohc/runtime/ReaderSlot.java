package com.red.ohc.runtime;

public final class ReaderSlot {
  volatile ReaderRegistry registry;
  int registryIndex = -1;
  /** Absolute address of word0 in this reader's stable 64-byte native lane. */
  long readerStateAddress;
  /** Address of the per-op sequence word (word1) in the same lane. */
  long seqAddress;
  volatile Thread owner;
  volatile WriterResource writerResource;
  /** Actor-owned slow-path marker; reader exit consumes it to request one maintenance rescan. */
  volatile int readerNotification;
  private volatile Runnable accessSignal;

  public long localHits;
  public long localMisses;
  public volatile long publishedHits;
  public volatile long publishedMisses;

  /** Lazily allocated after the first sampled access. */
  public volatile AccessRing access;

  public void setAccessSignal(Runnable accessSignal) {
    this.accessSignal = accessSignal;
  }

  public void signalAccess() {
    Runnable signal = accessSignal;
    if (signal != null) {
      signal.run();
    }
  }
}
