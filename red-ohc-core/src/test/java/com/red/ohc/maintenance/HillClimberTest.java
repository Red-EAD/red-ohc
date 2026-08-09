package com.red.ohc.maintenance;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;

public class HillClimberTest {
  @Test
  public void aStableHighHitSampleShrinksTheRecencyWindowAndAPoorSampleRestartsIt() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 16_384L);
    long initialWindow = policy.windowMaximum();

    for (long count = 0; count < policy.sampleSize(); count++) {
      policy.recordAccessHit();
    }
    long frequencyFavouringWindow = policy.windowMaximum();
    assertTrue(
        frequencyFavouringWindow < initialWindow,
        "the initial Caffeine-style negative step must prefer the main frequency region");

    for (long count = 0; count < policy.sampleSize(); count++) {
      policy.recordWriteMiss();
    }
    assertTrue(
        policy.windowMaximum() > frequencyFavouringWindow,
        "a >=5% hit-rate drop must restart the step and grow the recency window");
  }
}
