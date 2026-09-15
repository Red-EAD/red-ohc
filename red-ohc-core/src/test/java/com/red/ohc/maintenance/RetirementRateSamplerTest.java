package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public final class RetirementRateSamplerTest {
  @Test
  public void ratesUseWallTime() {
    RetirementRateSampler sampler = new RetirementRateSampler();

    sampler.observe(0L, 0L, 0L);
    sampler.observe(100_000_000L, 1_000L, 1_000L);

    assertEquals(sampler.generatedBytesPerSecond(), 10_000.0d, 0.001d);
    assertEquals(sampler.completedBytesPerSecond(), 10_000.0d, 0.001d);
  }

  @Test
  public void subMinimumSamplesDoNotAdvanceTheWindow() {
    RetirementRateSampler sampler = new RetirementRateSampler();

    sampler.observe(0L, 0L, 0L);
    sampler.observe(99_999_999L, 1_000L, 1_000L);

    assertEquals(sampler.generatedBytesPerSecond(), 0.0d, 0.001d);
    assertEquals(sampler.completedBytesPerSecond(), 0.0d, 0.001d);
  }

  @Test
  public void cadenceCheckIncludesTheBoundaryAndSurvivesNanoTimeWrap() {
    RetirementRateSampler sampler = new RetirementRateSampler();
    long start = Long.MAX_VALUE - 50_000_000L;

    assertTrue(sampler.shouldSample(start));
    sampler.observe(start, 10L, 20L);
    assertFalse(sampler.shouldSample(start + 99_999_999L));
    long wrappedBoundary = start + 100_000_000L;
    assertTrue(wrappedBoundary < 0L, "the fixture must cross signed nanoTime wrap");
    assertTrue(sampler.shouldSample(wrappedBoundary));

    sampler.observe(wrappedBoundary, 110L, 220L);
    assertEquals(sampler.generatedBytesPerSecond(), 1_000.0d, 0.001d);
    assertEquals(sampler.completedBytesPerSecond(), 2_000.0d, 0.001d);
  }
}
