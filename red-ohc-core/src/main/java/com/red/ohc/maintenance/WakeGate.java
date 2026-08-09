package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coalesces producer notifications with an event-loop's transition into a park. A {@code true}
 * result from {@link #signal()} is the sole case in which the caller must unpark the worker thread.
 */
public final class WakeGate {
  private static final int IDLE = 0;
  private static final int REQUIRED = 1;
  private static final int PROCESSING_TO_IDLE = 2;
  private static final int PROCESSING_TO_REQUIRED = 3;

  private final AtomicInteger state = new AtomicInteger(IDLE);

  /** Actor-owned observation of coalesced producer signals while an idle arm was in flight. */
  private volatile long mergedTransitions;

  /** Returns true only for the idle-to-required transition. */
  public boolean signal() {
    while (true) {
      int current = state.get();
      if (current == REQUIRED || current == PROCESSING_TO_REQUIRED) {
        return false;
      }
      if (current == IDLE) {
        if (state.compareAndSet(IDLE, REQUIRED)) {
          return true;
        }
      } else {
        if (state.compareAndSet(PROCESSING_TO_IDLE, PROCESSING_TO_REQUIRED)) {
          return false;
        }
      }
    }
  }

  /** Begins the actor's final no-work check before a park. */
  public boolean armIdle() {
    return state.compareAndSet(REQUIRED, PROCESSING_TO_IDLE);
  }

  /**
   * Completes a no-work check. False means a producer raced with the check and the actor must
   * continue processing rather than park.
   */
  public boolean finishIdle() {
    while (true) {
      int current = state.get();
      if (current == PROCESSING_TO_IDLE) {
        return state.compareAndSet(PROCESSING_TO_IDLE, IDLE);
      }
      if (current == PROCESSING_TO_REQUIRED) {
        if (state.compareAndSet(PROCESSING_TO_REQUIRED, REQUIRED)) {
          mergedTransitions++;
          return false;
        }
      } else {
        return false;
      }
    }
  }

  /** Cancels a park attempt after the actor's final source check found work. */
  public void requireProcessing() {
    while (true) {
      int current = state.get();
      if (current == REQUIRED) {
        return;
      }
      if (current == PROCESSING_TO_REQUIRED) {
        if (state.compareAndSet(PROCESSING_TO_REQUIRED, REQUIRED)) {
          return;
        }
      } else {
        if (current == PROCESSING_TO_IDLE) {
          if (state.compareAndSet(PROCESSING_TO_IDLE, REQUIRED)) {
            return;
          }
        } else {
          if (state.compareAndSet(IDLE, REQUIRED)) {
            return;
          }
        }
      }
    }
  }

  public boolean isRequired() {
    return state.get() != IDLE;
  }

  public long mergedTransitions() {
    return mergedTransitions;
  }
}
