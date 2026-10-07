package com.red.ohc.jmh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Cache;
import org.openjdk.jmh.infra.Blackhole;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.OHCache;

public final class OHCGetAllPutAllBenchmarkTest {
  @DataProvider(name = "occupancyChanges")
  public Object[][] occupancyChanges() {
    return new Object[][] {
      {"OHC", 0L}, {"OHC", -5L}, {"OHC", 200L},
      {"CAFFEINE", 0L}, {"CAFFEINE", -5L}, {"CAFFEINE", 200L}
    };
  }

  @Test(dataProvider = "occupancyChanges")
  public void completedBatchDoesNotUseConcurrentOccupancyAsAdmission(
      String implementation, long sizeChange) throws Exception {
    AtomicInteger writes = new AtomicInteger();
    OHCGetAllPutAllBenchmark benchmark = fixture(implementation, sizeChange, writes, false);
    WriteResults results = new WriteResults();

    benchmark.putAllCpuThreads(blackhole(), results, null);

    assertEquals(writes.get(), 64);
    assertEquals(results.attempted, 64L);
    assertEquals(results.accepted, 64L);
    assertEquals(results.rejected, 0L);
    assertEquals(results.exceptions, 0L);
    assertEquals(results.incomplete, 0L);
  }

  @DataProvider(name = "backends")
  public Object[][] backends() {
    return new Object[][] {{"OHC"}, {"CAFFEINE"}};
  }

  @Test(dataProvider = "backends")
  public void failedBatchRecordsIncompleteWrites(String implementation) throws Exception {
    OHCGetAllPutAllBenchmark benchmark =
        fixture(implementation, 0L, new AtomicInteger(), true);
    WriteResults results = new WriteResults();

    expectThrows(
        IllegalStateException.class, () -> benchmark.putAllCpuThreads(blackhole(), results, null));

    assertEquals(results.attempted, 64L);
    assertEquals(results.accepted, 0L);
    assertEquals(results.rejected, 0L);
    assertEquals(results.exceptions, 1L);
    assertEquals(results.incomplete, 64L);
  }

  private static OHCGetAllPutAllBenchmark fixture(
      String implementation, long sizeChange, AtomicInteger writes, boolean fail)
      throws Exception {
    OHCGetAllPutAllBenchmark benchmark = new OHCGetAllPutAllBenchmark();
    benchmark.implementation = implementation;
    setField(benchmark, "values", new byte[1 << 14][1]);
    AtomicInteger sizeReads = new AtomicInteger();
    Class<?> backend = "OHC".equals(implementation) ? OHCache.class : Cache.class;
    Object cache =
        Proxy.newProxyInstance(
            backend.getClassLoader(),
            new Class<?>[] {backend},
            (proxy, method, args) -> {
              if ("putAll".equals(method.getName())) {
                if (fail) {
                  throw new IllegalStateException("injected partial batch failure");
                }
                writes.addAndGet(((Map<?, ?>) args[0]).size());
                return null;
              }
              if ("size".equals(method.getName()) || "estimatedSize".equals(method.getName())) {
                long size = sizeReads.getAndIncrement() == 0 ? 1_000L : 1_000L + sizeChange;
                if (method.getReturnType() == int.class) {
                  return (int) size;
                }
                return size;
              }
              throw new UnsupportedOperationException(method.getName());
            });
    setField(benchmark, "OHC".equals(implementation) ? "ohc" : "caffeine", cache);
    return benchmark;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Blackhole blackhole() {
    return new Blackhole(
        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
  }
}
