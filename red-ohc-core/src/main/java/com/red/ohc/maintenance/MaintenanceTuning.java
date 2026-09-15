package com.red.ohc.maintenance;

/** Immutable actor-local timing and budget parameters. */
final class MaintenanceTuning {
  static final MaintenanceTuning DEFAULT =
      new MaintenanceTuning(
          100,
          5,
          1_000_000L,
          10_000_000L,
          1_000_000L,
          1_000_000L,
          10_000_000L,
          1_000_000_000L,
          250_000L,
          1_000_000L,
          5_000_000L,
          100_000_000L);

  final int idleMaxSpins;
  final int idleMaxYields;
  final long idleInitialParkNanos;
  final long idleMaxParkNanos;
  final long capacityRetryNanos;
  final long reclaimRetryInitialNanos;
  final long reclaimRetryMaxNanos;
  final long readerLifecyclePeriodNanos;
  final long minimumBudgetNanos;
  final long initialBudgetNanos;
  final long maximumBudgetNanos;
  final long controllerSampleNanos;

  MaintenanceTuning(
      int idleMaxSpins,
      int idleMaxYields,
      long idleInitialParkNanos,
      long idleMaxParkNanos,
      long capacityRetryNanos,
      long reclaimRetryInitialNanos,
      long reclaimRetryMaxNanos,
      long readerLifecyclePeriodNanos,
      long minimumBudgetNanos,
      long initialBudgetNanos,
      long maximumBudgetNanos,
      long controllerSampleNanos) {
    if (idleMaxSpins < 0 || idleMaxYields < 0) {
      throw new IllegalArgumentException("idle spin/yield limits must be non-negative");
    }
    requirePositive("idleInitialParkNanos", idleInitialParkNanos);
    requirePositive("idleMaxParkNanos", idleMaxParkNanos);
    if (idleInitialParkNanos > idleMaxParkNanos
        || idleMaxParkNanos > Long.MAX_VALUE / 2L) {
      throw new IllegalArgumentException("idle park bounds are invalid");
    }
    requirePositive("capacityRetryNanos", capacityRetryNanos);
    requirePositive("reclaimRetryInitialNanos", reclaimRetryInitialNanos);
    requirePositive("reclaimRetryMaxNanos", reclaimRetryMaxNanos);
    if (reclaimRetryInitialNanos > reclaimRetryMaxNanos
        || reclaimRetryMaxNanos > Long.MAX_VALUE / 2L) {
      throw new IllegalArgumentException("reclaim retry bounds are invalid");
    }
    requirePositive("readerLifecyclePeriodNanos", readerLifecyclePeriodNanos);
    requirePositive("minimumBudgetNanos", minimumBudgetNanos);
    requirePositive("initialBudgetNanos", initialBudgetNanos);
    requirePositive("maximumBudgetNanos", maximumBudgetNanos);
    if (minimumBudgetNanos > initialBudgetNanos || initialBudgetNanos > maximumBudgetNanos) {
      throw new IllegalArgumentException("maintenance budget bounds are invalid");
    }
    requirePositive("controllerSampleNanos", controllerSampleNanos);

    this.idleMaxSpins = idleMaxSpins;
    this.idleMaxYields = idleMaxYields;
    this.idleInitialParkNanos = idleInitialParkNanos;
    this.idleMaxParkNanos = idleMaxParkNanos;
    this.capacityRetryNanos = capacityRetryNanos;
    this.reclaimRetryInitialNanos = reclaimRetryInitialNanos;
    this.reclaimRetryMaxNanos = reclaimRetryMaxNanos;
    this.readerLifecyclePeriodNanos = readerLifecyclePeriodNanos;
    this.minimumBudgetNanos = minimumBudgetNanos;
    this.initialBudgetNanos = initialBudgetNanos;
    this.maximumBudgetNanos = maximumBudgetNanos;
    this.controllerSampleNanos = controllerSampleNanos;
  }

  private static void requirePositive(String name, long value) {
    if (value <= 0L) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }
}
