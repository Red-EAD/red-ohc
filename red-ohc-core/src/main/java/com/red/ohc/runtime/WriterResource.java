package com.red.ohc.runtime;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.storage.WriterArena;

/** Cache-local writer bundle owned by one live thread context at a time. */
public final class WriterResource {
  static final int ACTIVE = 1;
  static final int RETIRING = 2;
  static final int POOLED = 3;
  private static final VarHandle STATE;

  static {
    try {
      STATE = MethodHandles.lookup().findVarHandle(WriterResource.class, "state", int.class);
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private final int id;
  private final WriterArena arena;
  private final WriterLifecycleLane lifecycleLane;
  private final RetirementJournal.Lane retirementLane;
  private volatile long generation = 1L;
  private volatile long registrationVersion;
  private volatile int state = ACTIVE;
  private long lifecycleWatermark;
  private long retirementWatermark;
  private boolean cut;

  WriterResource(
      int id,
      WriterArena arena,
      WriterLifecycleLane lifecycleLane,
      RetirementJournal.Lane retirementLane) {
    this.id = id;
    this.arena = arena;
    this.lifecycleLane = lifecycleLane;
    this.retirementLane = retirementLane;
  }

  public WriterArena arena() {
    return arena;
  }

  public WriterLifecycleLane lifecycleLane() {
    return lifecycleLane;
  }

  public RetirementJournal.Lane retirementLane() {
    return retirementLane;
  }

  public long generation() {
    return generation;
  }

  public long registrationVersion() {
    return registrationVersion;
  }

  int id() {
    return id;
  }

  int state() {
    return (int) STATE.getVolatile(this);
  }

  void activate(long version) {
    int current = state();
    if (current != POOLED && registrationVersion != 0L) {
      throw new IllegalStateException("writer resource is not pooled: " + id);
    }
    registrationVersion = version;
    cut = false;
    STATE.setRelease(this, ACTIVE);
  }

  boolean beginRetiring() {
    return STATE.compareAndSet(this, ACTIVE, RETIRING);
  }

  void captureRetirementBoundaries() {
    if (cut) {
      return;
    }
    arena.detach();
    lifecycleWatermark = lifecycleLane.reservationWatermark();
    retirementWatermark = retirementLane.cutAndCaptureWatermark();
    cut = true;
  }

  boolean retirementComplete() {
    return cut
        && lifecycleLane.watermarkComplete(lifecycleWatermark)
        && retirementLane.watermarkComplete(retirementWatermark);
  }

  void pool() {
    if (state() != RETIRING || !retirementComplete()) {
      throw new IllegalStateException("writer resource retirement is incomplete: " + id);
    }
    generation++;
    registrationVersion = 0L;
    cut = false;
    STATE.setRelease(this, POOLED);
  }
}
