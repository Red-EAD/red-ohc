package com.red.ohc.runtime;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.testng.annotations.Test;

public class ThreadContextBulkKeySetTest {
  @Test
  public void reusesBoundedSetsAndIsolatesNestedAndHugeCalls() {
    ThreadContext context = new ThreadContext(null, null);
    try {
      Method acquire = ThreadContext.class.getDeclaredMethod("acquireBulkKeys", int.class);
      Method release = ThreadContext.class.getDeclaredMethod("releaseBulkKeys", Object.class);
      acquire.setAccessible(true);
      release.setAccessible(true);

      Object outer = invoke(acquire, context, 256);
      invoke(release, context, outer);
      Object reused = invoke(acquire, context, 256);
      assertSame(reused, outer, "bounded bulk key set should be reused");

      Object nested = invoke(acquire, context, 64);
      assertNotSame(nested, outer, "nested bulk calls need independent dedup state");
      invoke(release, context, nested);
      invoke(release, context, reused);

      Object huge = invoke(acquire, context, 8_192);
      invoke(release, context, huge);
      Object afterHuge = invoke(acquire, context, 256);
      assertNotSame(afterHuge, huge, "huge calls must not pin their dedup table");
      invoke(release, context, afterHuge);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("bulk key set reuse contract is missing", e);
    }
  }

  private static Object invoke(Method method, ThreadContext context, Object argument) {
    try {
      return method.invoke(context, argument);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (InvocationTargetException e) {
      throw new AssertionError(e.getCause());
    }
  }
}
