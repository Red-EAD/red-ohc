package com.red.ohc.jmh;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OHCSerializedBenchmarkTest {
  @Test
  public void exposesMaterializedAndDirectReadBenchmarks() {
    Set<String> benchmarkNames = new HashSet<>();
    Arrays.stream(OHCSerializedBenchmark.class.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(Benchmark.class))
        .map(Method::getName)
        .forEach(benchmarkNames::add);

    Assert.assertTrue(benchmarkNames.contains("oneThread"));
    Assert.assertTrue(benchmarkNames.contains("cpuThreads"));
    Assert.assertTrue(benchmarkNames.contains("directOneThread"));
    Assert.assertTrue(benchmarkNames.contains("directCpuThreads"));
  }

  @Test
  public void exposesTrueNewKeyWriteShape() throws NoSuchFieldException {
    Param shape = OHCSerializedBenchmark.class.getDeclaredField("writeShape").getAnnotation(Param.class);

    Assert.assertNotNull(shape);
    Assert.assertTrue(Arrays.asList(shape.value()).contains("NEW_KEY"));
  }

  @Test
  public void exposesIndependentDirectReadShapes() throws NoSuchFieldException {
    Class<?> directState =
        Arrays.stream(OHCSerializedBenchmark.class.getDeclaredClasses())
            .filter(type -> type.getSimpleName().equals("DirectReadState"))
            .findFirst()
            .orElse(null);
    Assert.assertNotNull(directState, "direct read parameters must belong to a direct-only state");
    Param shape = directState.getDeclaredField("directReadShape").getAnnotation(Param.class);

    Assert.assertNotNull(shape);
    Assert.assertEquals(
        new HashSet<>(Arrays.asList(shape.value())),
        new HashSet<>(
            Arrays.asList("DIRECT_PRIMITIVE", "DIRECT_FULL_SCAN", "DIRECT_COPY")));
    Assert.assertFalse(
        Arrays.stream(OHCSerializedBenchmark.class.getDeclaredFields())
            .anyMatch(field -> field.getName().equals("directReadShape")),
        "directReadShape must not multiply materialized benchmark methods");
  }

  @Test
  public void diagnosticCpuThreadsPublishesRetirementCounters() {
    Method method =
        Arrays.stream(OHCSerializedBenchmark.class.getDeclaredMethods())
            .filter(candidate -> candidate.getName().equals("diagnosticCpuThreads"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("diagnostic benchmark method is missing"));

    Assert.assertTrue(
        Arrays.stream(method.getParameterTypes())
            .anyMatch(type -> type.getSimpleName().equals("DebtResults")),
        "diagnosticCpuThreads must bind the retirement debt auxiliary counters");
  }

}
