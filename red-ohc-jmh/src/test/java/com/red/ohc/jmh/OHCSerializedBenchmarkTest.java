package com.red.ohc.jmh;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.openjdk.jmh.annotations.Benchmark;
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
}
