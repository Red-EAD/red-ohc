package com.red.ohc.maintenance;

/** Actor-local idle backoff for the transition from confirmed no-work to parking. */
final class IdleBackoff {
  private static final int MAX_SPINS = 100;
  private static final int MAX_YIELDS = 5;
  private static final long INITIAL_PARK_NANOS = 1_000_000L;
  private static final long MAX_PARK_NANOS = 10_000_000L;

  private int spins;
  private int yields;
  private long parkNanos;

  IdleBackoff() {
    parkNanos = INITIAL_PARK_NANOS;
  }

  boolean takeSpinTurn() {
    if (spins >= MAX_SPINS) {
      return false;
    }
    spins++;
    return true;
  }

  boolean takeYieldTurn() {
    if (yields >= MAX_YIELDS) {
      return false;
    }
    yields++;
    return true;
  }

  long nextParkNanos() {
    long current = parkNanos;
    if (current < MAX_PARK_NANOS) {
      parkNanos = Math.min(MAX_PARK_NANOS, current << 1);
    }
    return current;
  }

  void reset() {
    spins = 0;
    yields = 0;
    parkNanos = INITIAL_PARK_NANOS;
  }
}
