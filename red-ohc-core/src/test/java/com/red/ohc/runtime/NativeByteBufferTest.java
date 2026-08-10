package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.testng.annotations.Test;

import com.red.ohc.storage.NativeMemory;

public final class NativeByteBufferTest {
  @Test
  public void rebindsAWritableViewWithoutAllocatingAnotherViewObject() {
    NativeByteBuffer.verifySupported();
    long address = NativeMemory.unsafe().allocateMemory(16L);
    try {
      ByteBuffer first = NativeByteBuffer.writable(address, 16);
      NativeByteBuffer.invalidate(first);
      try {
        Method rebind =
            NativeByteBuffer.class.getDeclaredMethod(
                "writable", ByteBuffer.class, long.class, int.class);
        ByteBuffer second =
            (ByteBuffer) rebind.invoke(null, first, address, 8);
        assertTrue(second == first);
        assertEquals(second.limit(), 8);
        second.putLong(0x0102030405060708L);
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new AssertionError("writable view rebinding is missing", e);
      } finally {
        NativeByteBuffer.invalidate(first);
      }
    } finally {
      NativeMemory.unsafe().freeMemory(address);
    }
  }

  @Test
  public void exposesExistingNativeMemoryWithoutHeapArray() {
    NativeByteBuffer.verifySupported();
    long address = NativeMemory.unsafe().allocateMemory(16L);
    try {
      ByteBuffer writable = NativeByteBuffer.writable(address, 16);
      assertTrue(writable.isDirect());
      assertFalse(writable.isReadOnly());
      assertFalse(writable.hasArray());
      assertEquals(writable.position(), 0);
      assertEquals(writable.limit(), 16);
      assertEquals(writable.capacity(), 16);
      assertEquals(writable.order(), ByteOrder.BIG_ENDIAN);
      writable.putLong(0x0102030405060708L);

      ByteBuffer readOnly = NativeByteBuffer.readOnly(address, 16);
      assertTrue(readOnly.isDirect());
      assertTrue(readOnly.isReadOnly());
      assertFalse(readOnly.hasArray());
      assertEquals(readOnly.getLong(), 0x0102030405060708L);
      expectThrows(java.nio.ReadOnlyBufferException.class, () -> readOnly.put((byte) 1));

      NativeByteBuffer.invalidate(readOnly);
      assertEquals(readOnly.position(), 0);
      assertEquals(readOnly.limit(), 0);
      assertEquals(readOnly.capacity(), 0);
      expectThrows(java.nio.BufferUnderflowException.class, readOnly::get);
    } finally {
      NativeMemory.unsafe().freeMemory(address);
    }
  }

  @Test
  public void rejectsForeignBuffersWhenInvalidatingOrRebinding() {
    NativeByteBuffer.verifySupported();
    long address = NativeMemory.unsafe().allocateMemory(16L);
    ByteBuffer foreign = ByteBuffer.allocateDirect(16);
    try {
      expectThrows(IllegalArgumentException.class, () -> NativeByteBuffer.invalidate(foreign));
      ByteBuffer owned = NativeByteBuffer.writable(address, 16);
      NativeByteBuffer.invalidate(owned);
      expectThrows(
          IllegalArgumentException.class,
          () -> NativeByteBuffer.writable(foreign, address, 8));
    } finally {
      NativeMemory.unsafe().freeMemory(address);
    }
  }
}
