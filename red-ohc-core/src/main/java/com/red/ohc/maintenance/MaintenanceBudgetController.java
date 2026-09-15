package com.red.ohc.maintenance;

/**
 * Actor-local controller for the soft maintenance work budget.
 *
 * <p>The controller deliberately uses weak, periodically refreshed observations. It never reads a
 * clock or touches an atomic from an entry-level loop. The budget is a target used to calculate
 * bounded work quotas; it is not a wall-clock deadline.
 */
final class MaintenanceBudgetController {
  static final int TOTAL_MAINTENANCE_HARD_CAP = 4_096;

  private static final int LIFECYCLE_REFERENCE_RECORDS = 2_048;
  private static final int MAILBOX_REFERENCE_RECORDS = 2_048;
  private static final double DECAY = 0.7d;
  private static final double SAMPLE_WEIGHT = 1.0d - DECAY;
  private static final double CONFIDENCE_SIGMA = 0.5d;
  private final MaintenanceTuning tuning;
  private long budgetNanos;
  private long lastPressureSampleNanos = Long.MIN_VALUE;
  private long lastCostSampleNanos = Long.MIN_VALUE;
  private long accumulatedElapsedNanos;
  private long accumulatedWorkUnits;
  private double averageCostNanos;
  private double varianceCostNanosSquared;
  private int costSamples;

  MaintenanceBudgetController() {
    this(MaintenanceTuning.DEFAULT);
  }

  MaintenanceBudgetController(MaintenanceTuning tuning) {
    if (tuning == null) {
      throw new NullPointerException("tuning");
    }
    this.tuning = tuning;
    budgetNanos = tuning.initialBudgetNanos;
    averageCostNanos = tuning.initialBudgetNanos / 1_024.0d;
  }

  void observeTurn(long endNanos, long elapsedNanos, int workUnits) {
    if (elapsedNanos < 0L || workUnits < 0) {
      return;
    }
    accumulatedElapsedNanos = saturatingAdd(accumulatedElapsedNanos, elapsedNanos);
    accumulatedWorkUnits = saturatingAdd(accumulatedWorkUnits, workUnits);
    if (lastCostSampleNanos == Long.MIN_VALUE) {
      lastCostSampleNanos = endNanos;
      return;
    }
    long sampleElapsedNanos = endNanos - lastCostSampleNanos;
    if (sampleElapsedNanos < tuning.controllerSampleNanos) {
      return;
    }
    double sampleCost =
        (double) accumulatedElapsedNanos / (double) Math.max(1L, accumulatedWorkUnits);
    updateCostPrediction(sampleCost);
    accumulatedElapsedNanos = 0L;
    accumulatedWorkUnits = 0L;
    lastCostSampleNanos = endNanos;
  }

  void updatePressure(
      long nowNanos,
      long retirementDebtBytes,
      long nativeDebtBudgetBytes,
      double generatedBytesPerSecond,
      double completedBytesPerSecond,
      long lifecycleLagRecords,
      long oldestSafeWaitNanos,
      long mailboxDepth) {
    if (lastPressureSampleNanos == Long.MIN_VALUE) {
      lastPressureSampleNanos = nowNanos;
      return;
    }
    long elapsedNanos = nowNanos - lastPressureSampleNanos;
    if (elapsedNanos < tuning.controllerSampleNanos) {
      return;
    }

    double sampleWindowSeconds = elapsedNanos / 1_000_000_000.0d;
    double debtPressure = ratio(retirementDebtBytes, nativeDebtBudgetBytes);
    double growthBytes =
        Math.max(0.0d, finiteOrZero(generatedBytesPerSecond) - finiteOrZero(completedBytesPerSecond))
            * sampleWindowSeconds;
    double growthPressure = ratio(growthBytes, nativeDebtBudgetBytes);
    double lifecyclePressure = ratio(lifecycleLagRecords, LIFECYCLE_REFERENCE_RECORDS);
    double safeAgePressure = ratio(oldestSafeWaitNanos, tuning.controllerSampleNanos);
    double servicePressure =
        1.0d
            - (1.0d - debtPressure)
                * (1.0d - growthPressure)
                * (1.0d - lifecyclePressure)
                * (1.0d - safeAgePressure);
    double mailboxPressure = ratio(mailboxDepth, MAILBOX_REFERENCE_RECORDS);
    double rawBudget =
        tuning.initialBudgetNanos
            + servicePressure * (tuning.maximumBudgetNanos - tuning.initialBudgetNanos)
            - (1.0d - servicePressure)
                * mailboxPressure
                * (tuning.initialBudgetNanos - tuning.minimumBudgetNanos);
    long boundedRawBudget = clampBudget(Math.round(rawBudget));
    budgetNanos =
        clampBudget(
            Math.round(DECAY * budgetNanos + SAMPLE_WEIGHT * boundedRawBudget));
    lastPressureSampleNanos = nowNanos;
  }

  long budgetNanos() {
    return budgetNanos;
  }

  int workQuota() {
    double predictedCost = Math.max(1.0d, predictedCostNanos());
    long quota = (long) Math.floor(budgetNanos / predictedCost);
    if (quota < 1L) {
      return 1;
    }
    return quota >= TOTAL_MAINTENANCE_HARD_CAP
        ? TOTAL_MAINTENANCE_HARD_CAP
        : (int) quota;
  }

  int phaseQuota(
      int baseQuota, int demandUnits, int totalDemandUnits, int hardCap, int minimum) {
    if (demandUnits <= 0 || hardCap <= 0) {
      return 0;
    }
    int boundedDemand = Math.min(demandUnits, hardCap);
    int boundedMinimum = Math.max(0, Math.min(minimum, boundedDemand));
    long proportional =
        totalDemandUnits <= 0
            ? baseQuota
            : ((long) baseQuota * (long) demandUnits) / (long) totalDemandUnits;
    long quota = Math.max((long) boundedMinimum, proportional);
    return quota >= boundedDemand ? boundedDemand : (int) quota;
  }

  private void updateCostPrediction(double sampleCostNanos) {
    if (!Double.isFinite(sampleCostNanos) || sampleCostNanos < 1.0d) {
      return;
    }
    if (costSamples == 0) {
      averageCostNanos = sampleCostNanos;
      varianceCostNanosSquared = 0.0d;
    } else {
      averageCostNanos = DECAY * averageCostNanos + SAMPLE_WEIGHT * sampleCostNanos;
      double difference = sampleCostNanos - averageCostNanos;
      varianceCostNanosSquared =
          DECAY * varianceCostNanosSquared + SAMPLE_WEIGHT * difference * difference;
    }
    if (costSamples < Integer.MAX_VALUE) {
      costSamples++;
    }
  }

  private double predictedCostNanos() {
    if (costSamples == 0) {
      return tuning.initialBudgetNanos / 1_024.0d;
    }
    double standardDeviation = Math.sqrt(Math.max(0.0d, varianceCostNanosSquared));
    if (costSamples < 5) {
      standardDeviation =
          Math.max(standardDeviation, averageCostNanos * (5 - costSamples) / 2.0d);
    }
    return Math.max(1.0d, averageCostNanos + CONFIDENCE_SIGMA * standardDeviation);
  }

  private static double ratio(long value, long reference) {
    if (value <= 0L || reference <= 0L || reference == Long.MAX_VALUE) {
      return 0.0d;
    }
    if (value >= reference) {
      return 1.0d;
    }
    return (double) value / (double) reference;
  }

  private static double ratio(double value, long reference) {
    if (!Double.isFinite(value) || value <= 0.0d || reference <= 0L || reference == Long.MAX_VALUE) {
      return 0.0d;
    }
    if (value >= reference) {
      return 1.0d;
    }
    return value / (double) reference;
  }

  private static double finiteOrZero(double value) {
    return Double.isFinite(value) && value > 0.0d ? value : 0.0d;
  }

  private long clampBudget(long value) {
    return Math.max(
        tuning.minimumBudgetNanos, Math.min(tuning.maximumBudgetNanos, value));
  }

  private static long saturatingAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }
}
