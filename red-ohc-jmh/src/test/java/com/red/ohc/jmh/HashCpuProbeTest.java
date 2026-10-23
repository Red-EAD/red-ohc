package com.red.ohc.jmh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.testng.annotations.Test;

public final class HashCpuProbeTest {
  @Test
  public void controlModeChecksumMatchesTheFixedWorkCount() {
    byte[][] keys = HashCpuProbe.createKeys(17);

    assertEquals(HashCpuProbe.run(keys, 3_000L, false), 51_000L);
  }

  @Test
  public void seededProbeFixtureIsRepeatableAndUsesTheRequestedKeySize() {
    byte[][] first = HashCpuProbe.createKeys(36);
    byte[][] second = HashCpuProbe.createKeys(36);

    assertEquals(first.length, HashCpuProbe.KEY_COUNT);
    assertTrue(Arrays.deepEquals(first, second));
    for (byte[] key : first) {
      assertEquals(key.length, 36);
    }
  }
}
