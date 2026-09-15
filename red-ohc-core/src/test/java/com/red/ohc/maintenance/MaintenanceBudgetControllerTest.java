package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Random;

import org.testng.annotations.Test;

public final class MaintenanceBudgetControllerTest {
  @Test
  public void budgetStartsAtOneMillisecondAndWorkQuotaIsBounded() {
    MaintenanceBudgetController controller = new MaintenanceBudgetController();

    assertEquals(controller.budgetNanos(), 1_000_000L);
    int quota = controller.workQuota();
    assertTrue(quota >= 1L, "work quota must always make progress");
    assertTrue(quota <= 4_096L, "work quota must respect the hard cap");
  }

  @Test
  public void pressureChangesAreSmoothedAndRemainContinuous() {
    MaintenanceBudgetController low = new MaintenanceBudgetController();
    MaintenanceBudgetController medium = new MaintenanceBudgetController();
    MaintenanceBudgetController high = new MaintenanceBudgetController();

    initializePressure(low);
    initializePressure(medium);
    initializePressure(high);

    updatePressure(low, 100_000_000L, 0L, Long.MAX_VALUE, 0.0d, 0.0d, 0L, 0L, 0L);
    updatePressure(medium, 100_000_000L, 500_000L, 1_000_000L, 0.0d, 0.0d, 0L, 0L, 0L);
    updatePressure(high, 100_000_000L, 1_000_000L, 1_000_000L, 0.0d, 0.0d, 0L, 0L, 0L);

    long lowBudget = low.budgetNanos();
    long mediumBudget = medium.budgetNanos();
    long highBudget = high.budgetNanos();

    assertTrue(lowBudget >= 250_000L && lowBudget <= 5_000_000L);
    assertTrue(mediumBudget > lowBudget, "medium pressure must move the budget upward");
    assertTrue(highBudget > mediumBudget, "high pressure must move the budget upward");
    assertTrue(highBudget < 5_000_000L, "one sample must be smoothed, not jump to the ceiling");
  }

  @Test
  public void pressureSignalsCombineAndMailboxPressureReducesAnIdleBudget() {
    MaintenanceBudgetController neutral = new MaintenanceBudgetController();
    MaintenanceBudgetController mailbox = new MaintenanceBudgetController();
    MaintenanceBudgetController combined = new MaintenanceBudgetController();
    initializePressure(neutral);
    initializePressure(mailbox);
    initializePressure(combined);

    updatePressure(neutral, 100_000_000L, 0L, 1_000_000L, 0.0d, 0.0d, 0L, 0L, 0L);
    updatePressure(mailbox, 100_000_000L, 0L, 1_000_000L, 0.0d, 0.0d, 0L, 0L, 1_024L);
    updatePressure(
        combined,
        100_000_000L,
        250_000L,
        1_000_000L,
        1_000_000.0d,
        0.0d,
        512L,
        25_000_000L,
        1_024L);

    assertTrue(mailbox.budgetNanos() < neutral.budgetNanos());
    assertTrue(combined.budgetNanos() > neutral.budgetNanos());
    assertTrue(combined.budgetNanos() < 5_000_000L);
  }

  @Test
  public void smallSamplesUseConservativePredictionBeforeConverging() {
    MaintenanceBudgetController controller = new MaintenanceBudgetController();

    observeTurn(controller, 0L, 1_000_000L, 1_000);
    observeTurn(controller, 100_000_000L, 1_000_000L, 1_000);
    int conservativeQuota = controller.workQuota();

    for (int sample = 2; sample <= 8; sample++) {
      observeTurn(controller, sample * 100_000_000L, 1_000_000L, 1_000);
    }
    int convergedQuota = controller.workQuota();

    assertTrue(conservativeQuota < 1_024L, "small samples must include a safety margin");
    assertTrue(convergedQuota > conservativeQuota, "prediction should converge as samples grow");
  }

  @Test
  public void phaseQuotaRespectsDemandCapsAndReliableMinimum() {
    MaintenanceBudgetController controller = new MaintenanceBudgetController();
    int baseQuota = controller.workQuota();

    int lifecycleQuota = controller.phaseQuota(baseQuota, 100, 110, 1_024, 1);
    int retirementQuota = controller.phaseQuota(baseQuota, 10, 110, 32, 1);
    int advisoryQuota = controller.phaseQuota(baseQuota, 0, 110, 1_024, 0);

    assertTrue(lifecycleQuota >= 1 && lifecycleQuota <= 100);
    assertTrue(retirementQuota >= 1 && retirementQuota <= 10);
    assertEquals(advisoryQuota, 0);
  }

  @Test
  public void explicitBaseQuotaMatchesThePreviousFormulaAtBoundariesAndRandomDemands() {
    MaintenanceBudgetController controller = new MaintenanceBudgetController();
    Random random = new Random(0x5EEDL);
    for (int iteration = 0; iteration < 10_000; iteration++) {
      int baseQuota = 1 + random.nextInt(MaintenanceBudgetController.TOTAL_MAINTENANCE_HARD_CAP);
      int demand = random.nextInt(8_193);
      int totalDemand = random.nextBoolean() ? random.nextInt(16_385) : 0;
      int hardCap = random.nextInt(4_098);
      int minimum = random.nextInt(4_098);

      assertEquals(
          controller.phaseQuota(baseQuota, demand, totalDemand, hardCap, minimum),
          referencePhaseQuota(baseQuota, demand, totalDemand, hardCap, minimum));
    }
  }

  @Test
  public void customTuningChangesBudgetAndSamplingBoundaries() {
    MaintenanceTuning tuning =
        new MaintenanceTuning(
            10,
            5,
            1_000_000L,
            10_000_000L,
            2_000_000L,
            2_000_000L,
            8_000_000L,
            1_000_000_000L,
            500_000L,
            2_000_000L,
            4_000_000L,
            10L);
    MaintenanceBudgetController controller = new MaintenanceBudgetController(tuning);

    assertEquals(controller.budgetNanos(), 2_000_000L);
    controller.updatePressure(0L, 0L, 0L, 0.0d, 0.0d, 0L, 0L, 0L);
    controller.updatePressure(10L, 4_000_000L, 4_000_000L, 0.0d, 0.0d, 0L, 0L, 0L);

    assertTrue(controller.budgetNanos() > 2_000_000L);
    assertTrue(controller.budgetNanos() <= 4_000_000L);
  }

  private static void initializePressure(MaintenanceBudgetController controller) {
    updatePressure(controller, 0L, 0L, Long.MAX_VALUE, 0.0d, 0.0d, 0L, 0L, 0L);
  }

  private static void updatePressure(
      MaintenanceBudgetController controller,
      long nowNanos,
      long debtBytes,
      long debtBudgetBytes,
      double generatedRate,
      double completedRate,
      long lifecycleLag,
      long oldestSafeWait,
      long mailboxDepth) {
    controller.updatePressure(
        nowNanos,
        debtBytes,
        debtBudgetBytes,
        generatedRate,
        completedRate,
        lifecycleLag,
        oldestSafeWait,
        mailboxDepth);
  }

  private static void observeTurn(
      MaintenanceBudgetController controller, long endNanos, long elapsedNanos, int workUnits) {
    controller.observeTurn(endNanos, elapsedNanos, workUnits);
  }

  private static int referencePhaseQuota(
      int baseQuota, int demand, int totalDemand, int hardCap, int minimum) {
    if (demand <= 0 || hardCap <= 0) {
      return 0;
    }
    int boundedDemand = Math.min(demand, hardCap);
    int boundedMinimum = Math.max(0, Math.min(minimum, boundedDemand));
    long proportional =
        totalDemand <= 0
            ? baseQuota
            : ((long) baseQuota * (long) demand) / (long) totalDemand;
    return (int) Math.min(boundedDemand, Math.max((long) boundedMinimum, proportional));
  }
}
