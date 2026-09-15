package com.red.ohc.jmh;

import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.testng.annotations.Test;

public final class OHCMaxSizeAdmissionBenchmarkTest {
  @Test
  public void actorOwnedReclaimIsReportedAsAnIterationMetric() throws Exception {
    assertTrue(isPublicField("retirementActorReclaimedRecords"));
    assertTrue(isPublicField("retirementSafeSegmentCount"));
    assertTrue(isPublicField("retirementReclaimBatchCount"));
  }

  private static boolean isPublicField(String name) throws Exception {
    Field field = OHCMaxSizeAdmissionBenchmark.AdmissionResults.class.getDeclaredField(name);
    return Modifier.isPublic(field.getModifiers());
  }
}
