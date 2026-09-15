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
  public void rebindsAReadOnlyViewThroughTheTrustedThreadContextPath() {
    NativeByteBuffer.verifySupported();
    long firstAddress = NativeMemory.unsafe().allocateMemory(16L);
    long secondAddress = NativeMemory.unsafe().allocateMemory(8L);
    try {
      ByteBuffer firstSource = NativeByteBuffer.writable(firstAddress, 16);
      try {
        firstSource.putLong(0x0102030405060708L);
      } finally {
        NativeByteBuffer.invalidate(firstSource);
      }
      ByteBuffer secondSource = NativeByteBuffer.writable(secondAddress, 8);
      try {
        secondSource.putLong(0x1112131415161718L);
      } finally {
        NativeByteBuffer.invalidate(secondSource);
      }

      ThreadContext context = new ThreadContext(null);
      ByteBuffer outer = context.readOnlyValueBuffer(firstAddress, 16);
      ByteBuffer inner = context.readOnlyValueBuffer(secondAddress, 8);
      assertTrue(outer != inner);
      assertEquals(outer.limit(), 16);
      assertEquals(inner.limit(), 8);
      context.releaseReadOnlyValueBuffer();
      assertEquals(inner.limit(), 0);
      assertEquals(outer.limit(), 16, "nested release must preserve the outer view");

      context.releaseReadOnlyValueBuffer();
      assertEquals(outer.limit(), 0, "callback return must invalidate the outer view");
      ByteBuffer rebound = context.readOnlyValueBuffer(secondAddress, 8);
      assertTrue(rebound == outer, "the first read-only shell must be reused across operations");
      assertTrue(rebound.isReadOnly());
      assertEquals(rebound.getLong(), 0x1112131415161718L);
      expectThrows(java.nio.ReadOnlyBufferException.class, () -> rebound.put((byte) 1));
      context.releaseReadOnlyValueBuffer();
      assertEquals(rebound.limit(), 0);
    } finally {
      NativeMemory.unsafe().freeMemory(firstAddress);
      NativeMemory.unsafe().freeMemory(secondAddress);
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

  @Test
  public void checkedReadOnlyRebindValidatesOwnerAndAddressBounds() {
    NativeByteBuffer.verifySupported();
    long address = NativeMemory.unsafe().allocateMemory(16L);
    ByteBuffer foreign = ByteBuffer.allocateDirect(16);
    ByteBuffer owned = NativeByteBuffer.readOnly(address, 16);
    try {
      expectThrows(
          IllegalArgumentException.class,
          () -> NativeByteBuffer.readOnly(foreign, address, 8));
      expectThrows(
          IllegalArgumentException.class,
          () -> NativeByteBuffer.readOnly(owned, address, -1));
      expectThrows(
          IllegalArgumentException.class,
          () -> NativeByteBuffer.readOnly(owned, 0L, 8));
      expectThrows(
          NullPointerException.class,
          () -> NativeByteBuffer.readOnly((ByteBuffer) null, address, 8));

      NativeByteBuffer.readOnly(owned, 0L, 0);
      assertEquals(owned.limit(), 0);
    } finally {
      NativeByteBuffer.invalidate(owned);
      NativeMemory.unsafe().freeMemory(address);
    }
  }
}
