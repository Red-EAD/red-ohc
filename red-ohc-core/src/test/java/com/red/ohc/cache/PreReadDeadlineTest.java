package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

public final class PreReadDeadlineTest {
  @Test
  public void absentAndNonTtlValuesDoNotNeedTheirDeadlineHeader() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long value = memory.allocate(32L);
    Method preRead = OffHeapCache.class.getDeclaredMethod("preReadValueDeadline", long.class);
    preRead.setAccessible(true);
    try {
      NativeMemory.putLong(value, 123456789L);
      assertEquals(preRead.invoke(null, Entry.tagValueAddress(value, false)), 0L);
      assertEquals(
          preRead.invoke(
              null,
              Entry.tagValueAddress(value, true) | Entry.VALUE_LOGICALLY_ABSENT),
          0L);
      assertEquals(preRead.invoke(null, Entry.tagValueAddress(value, true)), 123456789L);
    } finally {
      memory.free(value, 32L);
      memory.closeArenas();
    }
  }
}
