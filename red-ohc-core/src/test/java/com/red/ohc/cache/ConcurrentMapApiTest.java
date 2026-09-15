package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

import org.testng.annotations.Test;

import com.red.ohc.api.OHCache;

public final class ConcurrentMapApiTest {
  @Test
  public void ohCacheIsADedicatedAutoCloseableApi() {
    assertTrue(
        AutoCloseable.class.isAssignableFrom(OHCache.class),
        "OHCache must remain closeable");
    assertFalse(
        ConcurrentMap.class.isAssignableFrom(OHCache.class),
        "OHCache must not expose the ConcurrentMap contract");
  }

  @Test
  public void unconditionalMutationsReturnVoid() {
    assertReturnType("put", void.class, Object.class, Object.class);
    assertReturnType("put", void.class, Object.class, Object.class, long.class);
    assertReturnType("remove", void.class, Object.class);
  }

  @Test
  public void conditionalMutationsRetainTheirResults() {
    assertReturnType("putIfAbsent", Object.class, Object.class, Object.class);
    assertReturnType("putIfAbsent", Object.class, Object.class, Object.class, long.class);
    assertReturnType("remove", boolean.class, Object.class, Object.class);
    assertReturnType("replace", boolean.class, Object.class, Object.class, Object.class);
    assertReturnType("replace", Object.class, Object.class, Object.class);
    assertReturnType("replace", boolean.class, Object.class, Object.class, Object.class, long.class);
    assertReturnType("compute", Object.class, Object.class, java.util.function.BiFunction.class);
    assertReturnType("merge", Object.class, Object.class, Object.class, java.util.function.BiFunction.class);
  }

  @Test
  public void queryAndBulkMethodsRetainTheirResults() {
    assertReturnType("get", Object.class, Object.class);
    assertReturnType("containsKey", boolean.class, Object.class);
    assertReturnType("size", int.class);
    assertReturnType("mappingCount", long.class);
    assertReturnType("removeAll", int.class, java.util.Collection.class);
  }

  @Test
  public void fastMutationMethodsAreNotPartOfThePublicApi() {
    assertMissing(OHCache.class, "putFast", Object.class, Object.class);
    assertMissing(OHCache.class, "putFast", Object.class, Object.class, long.class);
    assertMissing(OHCache.class, "putIfAbsentFast", Object.class, Object.class);
    assertMissing(OHCache.class, "putIfAbsentFast", Object.class, Object.class, long.class);
    assertMissing(OHCache.class, "removeFast", Object.class);

    assertMissing(OffHeapCache.class, "putFast", Object.class, Object.class);
    assertMissing(OffHeapCache.class, "putFast", Object.class, Object.class, long.class);
    assertMissing(OffHeapCache.class, "putIfAbsentFast", Object.class, Object.class);
    assertMissing(OffHeapCache.class, "putIfAbsentFast", Object.class, Object.class, long.class);
    assertMissing(OffHeapCache.class, "removeFast", Object.class);
  }

  private static void assertReturnType(String methodName, Class<?> expected, Class<?>... parameters) {
    try {
      Method method = OHCache.class.getMethod(methodName, parameters);
      assertEquals(
          method.getReturnType(), expected, "unexpected return type for " + methodName);
    } catch (NoSuchMethodException missing) {
      fail("missing ConcurrentMap method " + methodName, missing);
    }
  }

  private static void assertMissing(Class<?> type, String methodName, Class<?>... parameters) {
    try {
      type.getMethod(methodName, parameters);
      fail("unexpected public fast method " + type.getName() + '#' + methodName);
    } catch (NoSuchMethodException expected) {
      // Expected after the public API is reduced to the CHM-style surface.
    }
  }
}
