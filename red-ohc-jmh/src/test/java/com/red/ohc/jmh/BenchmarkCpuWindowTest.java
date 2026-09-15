package com.red.ohc.jmh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.openjdk.jmh.annotations.Benchmark;
import org.testng.annotations.Test;

public final class BenchmarkCpuWindowTest {
  @Test
  public void serializedBenchmarksExposeMeasurementWindowState() {
    boolean exposed =
        Arrays.stream(OHCSerializedBenchmark.class.getDeclaredMethods())
            .flatMap(method -> Arrays.stream(method.getParameterTypes()))
            .anyMatch(type -> type.getName().endsWith("BenchmarkWindowResults"));

    assertTrue(exposed, "the benchmark must expose same-window CPU sampling state");
  }

  @Test
  public void everyOHCMeasurementBenchmarkWithoutInvocationLifecycleCarriesTheWindowState() {
    for (Class<?> benchmarkClass :
        new Class<?>[] {
          OHCSerializedBenchmark.class,
          OHCWriteAdmissionBenchmark.class,
          OHCWriteSustainedBenchmark.class,
          OHCMaxSizeAdmissionBenchmark.class,
          OHCEvictionChurnBenchmark.class,
          OHCGetAllPutAllBenchmark.class,
          OHCBenchmark.class,
        }) {
      for (java.lang.reflect.Method method : benchmarkClass.getDeclaredMethods()) {
        if (!method.isAnnotationPresent(Benchmark.class)) {
          continue;
        }
        boolean exposed =
            Arrays.stream(method.getParameterTypes())
                .anyMatch(type -> type == BenchmarkWindowResults.class);
        assertTrue(
            exposed,
            benchmarkClass.getSimpleName() + "." + method.getName()
                + " must carry same-window CPU sampling state");
      }
    }
  }

  @Test
  public void timeoutDrainBenchmarkDoesNotIncludeInvocationLifecycleInTheWindow() {
    boolean exposed =
        Arrays.stream(OHCTimeoutDrainBenchmark.class.getDeclaredMethods())
            .filter(method -> method.isAnnotationPresent(Benchmark.class))
            .flatMap(method -> Arrays.stream(method.getParameterTypes()))
            .anyMatch(type -> type == BenchmarkWindowResults.class);

    assertFalse(
        exposed,
        "the TTL drain window must exclude its invocation-level setup and teardown");
  }

  @Test
  public void deltaReportsProcessAndActorCpuForTheSameWindow() {
    BenchmarkCpuWindow.Snapshot before =
        BenchmarkCpuWindow.Snapshot.forTest(
            100L, 200L, Collections.singletonMap(7L, 30L));
    BenchmarkCpuWindow.Snapshot after =
        BenchmarkCpuWindow.Snapshot.forTest(
            160L, 290L, Collections.singletonMap(7L, 70L));

    BenchmarkCpuWindow.Delta delta = BenchmarkCpuWindow.delta(before, after);

    assertEquals(delta.wallNanos, 60L);
    assertTrue(delta.processCpuKnown);
    assertEquals(delta.processCpuNanos, 90L);
    assertTrue(delta.actorCpuKnown);
    assertEquals(delta.actorCpuNanos, 40L);
  }

  @Test
  public void actorCpuIsUnknownWhenTheActorThreadIsReplaced() {
    BenchmarkCpuWindow.Snapshot before =
        BenchmarkCpuWindow.Snapshot.forTest(
            100L, 200L, Collections.singletonMap(7L, 30L));
    BenchmarkCpuWindow.Snapshot after =
        BenchmarkCpuWindow.Snapshot.forTest(
            160L, 290L, Collections.singletonMap(8L, 70L));

    BenchmarkCpuWindow.Delta delta = BenchmarkCpuWindow.delta(before, after);

    assertFalse(delta.actorCpuKnown);
    assertEquals(delta.actorCpuNanos, -1L);
  }

  @Test
  public void unavailableProcessCpuIsNotRepresentedAsZero() {
    BenchmarkCpuWindow.Snapshot before =
        BenchmarkCpuWindow.Snapshot.forTest(
            100L, -1L, Collections.singletonMap(7L, 30L));
    BenchmarkCpuWindow.Snapshot after =
        BenchmarkCpuWindow.Snapshot.forTest(
            160L, -1L, Collections.singletonMap(7L, 70L));

    BenchmarkCpuWindow.Delta delta = BenchmarkCpuWindow.delta(before, after);

    assertFalse(delta.processCpuKnown);
    assertEquals(delta.processCpuNanos, -1L);
  }
}
