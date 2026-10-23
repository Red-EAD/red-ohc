package com.red.ohc.jmh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.testng.annotations.Test;

public final class KeyHashBenchmarkTest {
  @Test
  public void hashAndTraversalBenchmarksNormalizeEveryInvocationToOneThousandTwentyFourKeys()
      throws Exception {
    assertEquals(KeyHashBenchmark.class.getAnnotation(State.class).value(), Scope.Thread);
    for (String methodName : new String[] {"hashKey", "traversalControl"}) {
      Method method = KeyHashBenchmark.class.getDeclaredMethod(methodName, Blackhole.class);
      assertTrue(method.isAnnotationPresent(Benchmark.class));
      assertEquals(
          method.getAnnotation(OperationsPerInvocation.class).value(),
          1_024L,
          methodName + " must report average time per key");
    }
  }

  @Test
  public void setupBuildsFixedAndMixedHotFixtures() throws Exception {
    KeyHashBenchmark benchmark = new KeyHashBenchmark();
    benchmark.keyBytes = 36;
    benchmark.workingSetMiB = 0;
    benchmark.lengths = "FIXED";
    benchmark.setup();
    assertKeyLengths(readKeys(benchmark), 36, 36);

    benchmark.lengths = "MIXED";
    benchmark.setup();
    byte[][] mixed = readKeys(benchmark);
    assertEquals(mixed.length, 1_024);
    assertTrue(Arrays.stream(mixed).anyMatch(key -> key.length < 36));
    assertKeyLengths(mixed, 0, 36);
  }

  private static byte[][] readKeys(KeyHashBenchmark benchmark) throws Exception {
    Field keys = KeyHashBenchmark.class.getDeclaredField("keys");
    keys.setAccessible(true);
    return (byte[][]) keys.get(benchmark);
  }

  private static void assertKeyLengths(byte[][] keys, int minimum, int maximum) {
    for (byte[] key : keys) {
      assertTrue(
          key.length >= minimum && key.length <= maximum,
          "key length outside [" + minimum + "," + maximum + "]: " + key.length);
    }
  }
}
