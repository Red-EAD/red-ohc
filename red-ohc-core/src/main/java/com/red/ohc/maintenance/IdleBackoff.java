package com.red.ohc.maintenance;

/** Actor-local idle backoff for the transition from confirmed no-work to parking. */
final class IdleBackoff {
  private final int maxSpins;
  private final int maxYields;
  private final long initialParkNanos;
  private final long maxParkNanos;

  private int spins;
  private int yields;
  private long parkNanos;

  IdleBackoff() {
    this(MaintenanceTuning.DEFAULT);
  }

  IdleBackoff(MaintenanceTuning tuning) {
    if (tuning == null) {
      throw new NullPointerException("tuning");
    }
    maxSpins = tuning.idleMaxSpins;
    maxYields = tuning.idleMaxYields;
    initialParkNanos = tuning.idleInitialParkNanos;
    maxParkNanos = tuning.idleMaxParkNanos;
    parkNanos = initialParkNanos;
  }

  boolean takeSpinTurn() {
    if (spins >= maxSpins) {
      return false;
    }
    spins++;
    return true;
  }

  boolean takeYieldTurn() {
    if (yields >= maxYields) {
      return false;
    }
    yields++;
    return true;
  }

  long nextParkNanos() {
    long current = parkNanos;
    if (current < maxParkNanos) {
      parkNanos = Math.min(maxParkNanos, current << 1);
    }
    return current;
  }

  void reset() {
    spins = 0;
    yields = 0;
    parkNanos = initialParkNanos;
  }
}
