package com.red.ohc.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.testng.annotations.Test;

public final class OHCacheStatsTest {
  @Test
  public void exposesStandardCacheMetricsAndDerivedRates() {
    OHCacheStats stats =
        new OHCacheStats(
            10L,
            5L,
            3L,
            1L,
            40L,
            7L,
            9L,
            2L,
            4L,
            100L,
            11L,
            12L,
            13L,
            false,
            14L,
            15L,
            16L,
            17L);

    assertEquals(stats.hitCount(), 10L);
    assertEquals(stats.missCount(), 5L);
    assertEquals(stats.requestCount(), 15L);
    assertEquals(stats.hitRate(), 10.0 / 15.0, 0.000001);
    assertEquals(stats.missRate(), 5.0 / 15.0, 0.000001);
    assertEquals(stats.loadSuccessCount(), 3L);
    assertEquals(stats.loadFailureCount(), 1L);
    assertEquals(stats.loadCount(), 4L);
    assertEquals(stats.loadFailureRate(), 0.25, 0.000001);
    assertEquals(stats.totalLoadTime(), 40L);
    assertEquals(stats.averageLoadPenalty(), 10.0, 0.000001);
    assertEquals(stats.evictionCount(), 7L);
    assertEquals(stats.evictionWeight(), 9L);
    assertEquals(stats.expirationCount(), 2L);
    assertEquals(stats.entryResidenceCount(), 4L);
    assertEquals(stats.totalEntryResidenceTimeMillis(), 100L);
    assertEquals(stats.averageEntryResidenceTimeMillis(), 25.0, 0.000001);
    assertEquals(stats.size(), 11L);
    assertEquals(stats.liveWeight(), 12L);
    assertEquals(stats.nativeAllocatedBytes(), 13L);
    assertTrue(!stats.maintenanceUnhealthy());
    assertEquals(stats.maintenanceQueueDepth(), 14L);
    assertEquals(stats.ttlLagMillis(), 15L);
    assertEquals(stats.ttlBacklog(), 16L);
    assertEquals(stats.nativeAllocationFailureCount(), 17L);
  }

  @Test
  public void derivedRatesUseEmptyDefaults() {
    OHCacheStats stats = new OHCacheStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, false, 0L, 0L, 0L, 0L);

    assertEquals(stats.requestCount(), 0L);
    assertEquals(stats.hitRate(), 1.0, 0.0);
    assertEquals(stats.missRate(), 0.0, 0.0);
    assertEquals(stats.loadCount(), 0L);
    assertEquals(stats.loadFailureRate(), 0.0, 0.0);
    assertEquals(stats.averageLoadPenalty(), 0.0, 0.0);
    assertEquals(stats.averageEntryResidenceTimeMillis(), 0.0, 0.0);
  }

  @Test
  public void statsFieldsArePrivate() {
    String[] expectedFields = {
      "hitCount",
      "missCount",
      "loadSuccessCount",
      "loadFailureCount",
      "totalLoadTime",
      "evictionCount",
      "evictionWeight",
      "expirationCount",
      "entryResidenceCount",
      "totalEntryResidenceTimeMillis",
      "size",
      "liveWeight",
      "nativeAllocatedBytes",
      "maintenanceUnhealthy",
      "maintenanceQueueDepth",
      "ttlLagMillis",
      "ttlBacklog",
      "nativeAllocationFailureCount"
    };
    assertEquals(OHCacheStats.class.getDeclaredFields().length, expectedFields.length);
    for (String expectedField : expectedFields) {
      assertTrue(
          Modifier.isPrivate(getField(expectedField).getModifiers()),
          "stats field must be private: " + expectedField);
    }
    for (Field field : OHCacheStats.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        assertTrue(
            Modifier.isPrivate(field.getModifiers()),
            "stats field must be private: " + field.getName());
      }
    }
  }

  private static Field getField(String name) {
    try {
      return OHCacheStats.class.getDeclaredField(name);
    } catch (NoSuchFieldException missing) {
      throw new AssertionError("missing stats field: " + name, missing);
    }
  }
}
