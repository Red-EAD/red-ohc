package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public final class IdleBackoffTest {
  @Test
  public void usesOneHundredSpinsFiveYieldsAndKeepsTheTenMillisecondParkCap()
      throws Exception {
    IdleBackoff backoff = new IdleBackoff();

    for (int index = 0; index < 100; index++) {
      assertTrue(backoff.takeSpinTurn(), "spin turn " + index);
    }
    assertFalse(backoff.takeSpinTurn(), "spin phase must end after one hundred turns");

    for (int index = 0; index < 5; index++) {
      assertTrue(backoff.takeYieldTurn(), "yield turn " + index);
    }
    assertFalse(backoff.takeYieldTurn(), "yield phase must end after five turns");

    assertEquals(backoff.nextParkNanos(), 1_000_000L);
    assertEquals(backoff.nextParkNanos(), 2_000_000L);
    assertEquals(backoff.nextParkNanos(), 4_000_000L);
    assertEquals(backoff.nextParkNanos(), 8_000_000L);
    assertEquals(backoff.nextParkNanos(), 10_000_000L);
    assertEquals(backoff.nextParkNanos(), 10_000_000L);
    assertEquals(
        backoff.nextParkNanos(), 10_000_000L, "the capped phase must keep checking at the maximum");
  }

  @Test
  public void resetStartsANewIdleEpisode() throws Exception {
    IdleBackoff backoff = new IdleBackoff();

    for (int index = 0; index < 10; index++) {
      assertTrue(backoff.takeSpinTurn());
    }
    assertEquals(backoff.nextParkNanos(), 1_000_000L);
    assertEquals(backoff.nextParkNanos(), 2_000_000L);

    backoff.reset();

    assertTrue(backoff.takeSpinTurn());
    assertEquals(backoff.nextParkNanos(), 1_000_000L);
  }

  @Test
  public void customTuningChangesTheIdleEpisodeBoundaries() {
    MaintenanceTuning tuning = tuning(0, 0, 2_000_000L, 4_000_000L);
    IdleBackoff backoff = new IdleBackoff(tuning);

    assertFalse(backoff.takeSpinTurn());
    assertFalse(backoff.takeYieldTurn());
    assertEquals(backoff.nextParkNanos(), 2_000_000L);
    assertEquals(backoff.nextParkNanos(), 4_000_000L);
    assertEquals(backoff.nextParkNanos(), 4_000_000L);
  }

  private static MaintenanceTuning tuning(
      int maxSpins, int maxYields, long initialParkNanos, long maxParkNanos) {
    return new MaintenanceTuning(
        maxSpins,
        maxYields,
        initialParkNanos,
        maxParkNanos,
        1_000_000L,
        1_000_000L,
        10_000_000L,
        1_000_000_000L,
        250_000L,
        1_000_000L,
        5_000_000L,
        100_000_000L);
  }
}
