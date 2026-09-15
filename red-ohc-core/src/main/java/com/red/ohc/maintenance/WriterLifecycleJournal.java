package com.red.ohc.maintenance;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Dynamic set of writer-exclusive lifecycle lanes signalled directly to the actor. */
public final class WriterLifecycleJournal {
  public static final int SEGMENT_CAPACITY = 256;

  private final Object laneCreationLock = new Object();
  private final WriterLifecycleLane.SegmentPool segmentPool =
      new WriterLifecycleLane.SegmentPool(SEGMENT_CAPACITY);
  private final AtomicLong readinessGeneration = new AtomicLong();
  private volatile WriterLifecycleLane[] lanes = new WriterLifecycleLane[0];
  private volatile Runnable readySignal;
  private volatile Consumer<Throwable> readySignalFailure;

  public WriterLifecycleJournal() {}

  WriterLifecycleJournal(int initialLaneCount) {
    if (initialLaneCount < 0) {
      throw new IllegalArgumentException("initialLaneCount must be non-negative");
    }
    for (int index = 0; index < initialLaneCount; index++) {
      createLane();
    }
  }

  /** Cold-path lane creation; an active writer keeps the returned lane exclusively. */
  public WriterLifecycleLane createLane() {
    synchronized (laneCreationLock) {
      WriterLifecycleLane lane =
          new WriterLifecycleLane(SEGMENT_CAPACITY, segmentPool, this::onLaneReadinessChanged);
      Runnable signal = readySignal;
      if (signal != null) {
        lane.bindReadySignal(signal, readySignalFailure);
      }
      WriterLifecycleLane[] current = lanes;
      WriterLifecycleLane[] expanded = Arrays.copyOf(current, current.length + 1);
      expanded[current.length] = lane;
      lanes = expanded;
      return lane;
    }
  }

  public WriterLifecycleLane lane(int index) {
    WriterLifecycleLane[] snapshot = lanes;
    if (index < 0 || index >= snapshot.length) {
      throw new IndexOutOfBoundsException("lifecycle lane: " + index);
    }
    return snapshot[index];
  }

  public int laneCount() {
    return lanes.length;
  }

  public long queuedRecords() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.queuedRecords();
    }
    return total;
  }

  public long reservedRecords() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.reservedRecords();
    }
    return total;
  }

  public long[] captureWatermark() {
    WriterLifecycleLane[] snapshot = lanes;
    long[] watermark = new long[snapshot.length];
    for (int index = 0; index < snapshot.length; index++) {
      watermark[index] = snapshot[index].reservationWatermark();
    }
    return watermark;
  }

  public void captureWatermark(long[] watermark) {
    WriterLifecycleLane[] snapshot = lanes;
    checkWatermark(watermark, snapshot.length);
    for (int index = 0; index < watermark.length; index++) {
      watermark[index] = snapshot[index].reservationWatermark();
    }
  }

  public boolean watermarkComplete(long[] watermark) {
    WriterLifecycleLane[] snapshot = lanes;
    checkWatermark(watermark, snapshot.length);
    for (int index = 0; index < watermark.length; index++) {
      if (!snapshot[index].watermarkComplete(watermark[index])) {
        return false;
      }
    }
    return true;
  }

  public long publishedRecordsTotal() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.publishedRecordsTotal();
    }
    return total;
  }

  public long completedRecordsTotal() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.completedRecordsTotal();
    }
    return total;
  }

  public long lagRecords() {
    return queuedRecords();
  }

  public boolean hasCommittedRecords() {
    for (WriterLifecycleLane lane : lanes) {
      if (lane.hasCommittedRecords()) {
        return true;
      }
    }
    return false;
  }

  public boolean hasReadyRecords() {
    for (WriterLifecycleLane lane : lanes) {
      if (lane.hasHeadCommitted()) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether an explicit maintenance wake has an executable head record. */
  public boolean hasUnmanagedReadyRecords() {
    for (WriterLifecycleLane lane : lanes) {
      if (lane.hasUnmanagedHead()) {
        return true;
      }
    }
    return false;
  }

  /** Actor-only slow path that refreshes every lane's coalesced ready marker. */
  boolean probeUnmanagedReadyRecords() {
    boolean ready = false;
    for (WriterLifecycleLane lane : lanes) {
      if (lane.refreshReadySignal()) {
        ready = true;
      }
    }
    return ready;
  }

  /** Changes only when a lane's coalesced ready marker changes state. */
  long readinessGeneration() {
    return readinessGeneration.get();
  }

  public void bindReadySignal(Runnable signal) {
    bindReadySignal(signal, null);
  }

  public void bindReadySignal(Runnable signal, Consumer<Throwable> failureHandler) {
    if (signal == null) {
      throw new NullPointerException("signal");
    }
    synchronized (laneCreationLock) {
      readySignal = signal;
      readySignalFailure = failureHandler;
      for (WriterLifecycleLane lane : lanes) {
        lane.bindReadySignal(signal, failureHandler);
      }
    }
  }

  public long allocatedSegments() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.allocatedSegments();
    }
    return total;
  }

  public long headOfLineStopCount() {
    long total = 0L;
    for (WriterLifecycleLane lane : lanes) {
      total += lane.headOfLineStopCount();
    }
    return total;
  }

  private void onLaneReadinessChanged() {
    readinessGeneration.incrementAndGet();
  }

  private static void checkWatermark(long[] watermark, int availableLanes) {
    if (watermark == null || watermark.length > availableLanes) {
      throw new IllegalArgumentException("lifecycle watermark does not match lane count");
    }
  }
}
