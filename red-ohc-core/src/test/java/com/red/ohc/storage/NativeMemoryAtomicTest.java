package com.red.ohc.storage;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;

public final class NativeMemoryAtomicTest {
  @Test
  public void compareAndSwapLongPublishesNativeMetadataWithoutAJavaField() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long address = memory.allocate(Long.BYTES);
    try {
      Method cas =
          NativeMemory.class.getDeclaredMethod(
              "compareAndSwapLong", long.class, long.class, long.class);
      assertTrue((Boolean) cas.invoke(null, address, 0L, 7L));
      assertFalse((Boolean) cas.invoke(null, address, 0L, 9L));
      assertTrue((Boolean) cas.invoke(null, address, 7L, 11L));
      assertTrue(NativeMemory.getLongVolatile(address) == 11L);
    } finally {
      memory.free(address, Long.BYTES);
      memory.closeArenas();
    }
  }
}
