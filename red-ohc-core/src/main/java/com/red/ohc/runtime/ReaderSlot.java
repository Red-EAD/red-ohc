package com.red.ohc.runtime;

public final class ReaderSlot extends ReaderSlotReaderPadding {
  volatile ReaderRegistry registry;
  int registryIndex = -1;
  /** Absolute address of word0 in this reader's stable 64-byte native lane. */
  long readerStateAddress;
  /** Address of the per-op sequence word (word1) in the same lane. */
  long seqAddress;
  volatile Thread owner;
  volatile WriterResource writerResource;
  private volatile Runnable accessSignal;

  public long localHits;
  public long localMisses;
  public volatile long publishedHits;
  public volatile long publishedMisses;
  public long localSkipAccesses;
  public volatile long publishedSkipAccesses;

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

/** Actor-only consumption state. */
class ReaderSlotActorFields {
  /** Actor-owned consumption cursor, reset with the slot's lifetime. */
  public long consumedSkipAccesses;
}

/** A 64-byte gap separates actor consumption from the mixed-access notification marker. */
class ReaderSlotActorPadding extends ReaderSlotActorFields {
  // Consume a possible superclass tail hole before the next layout segment can backfill it.
  int actorAlignmentPadding;
  long actorLinePad0;
  long actorLinePad1;
  long actorLinePad2;
  long actorLinePad3;
  long actorLinePad4;
  long actorLinePad5;
  long actorLinePad6;
  long actorLinePad7;
}

/** Actor marking and reader exit both touch this slow-path notification marker. */
class ReaderSlotNotificationFields extends ReaderSlotActorPadding {
  volatile int readerNotification;
}

/** A 64-byte gap isolates notification writes from reader hot-path state. */
class ReaderSlotReaderPadding extends ReaderSlotNotificationFields {
  // Consume a possible superclass tail hole before ReaderSlot can backfill it.
  int readerAlignmentPadding;
  long readerLinePad0;
  long readerLinePad1;
  long readerLinePad2;
  long readerLinePad3;
  long readerLinePad4;
  long readerLinePad5;
  long readerLinePad6;
  long readerLinePad7;
}
