package com.red.ohc.runtime;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import sun.misc.Unsafe;

import com.red.ohc.storage.NativeMemory;

/** Internal non-owning {@link ByteBuffer} views over cache-owned native memory. */
final class NativeByteBuffer {
  private static final Unsafe UNSAFE = NativeMemory.unsafe();
  private static final Object VIEW_MARKER = new Object();
  private static final boolean SUPPORTED;
  private static final Throwable FAILURE;
  private static final Class<?> DIRECT_CLASS;
  private static final Class<?> READ_ONLY_CLASS;
  private static final long MARK_OFFSET;
  private static final long POSITION_OFFSET;
  private static final long LIMIT_OFFSET;
  private static final long CAPACITY_OFFSET;
  private static final long ADDRESS_OFFSET;
  private static final long HEAP_BUFFER_OFFSET;
  private static final long OFFSET_OFFSET;
  private static final long READ_ONLY_OFFSET;
  private static final long BIG_ENDIAN_OFFSET;
  private static final long NATIVE_BYTE_ORDER_OFFSET;
  private static final long ATTACHMENT_OFFSET;

  static {
    Layout layout = Layout.detect();
    SUPPORTED = layout.supported;
    FAILURE = layout.failure;
    DIRECT_CLASS = layout.directClass;
    READ_ONLY_CLASS = layout.readOnlyClass;
    MARK_OFFSET = layout.markOffset;
    POSITION_OFFSET = layout.positionOffset;
    LIMIT_OFFSET = layout.limitOffset;
    CAPACITY_OFFSET = layout.capacityOffset;
    ADDRESS_OFFSET = layout.addressOffset;
    HEAP_BUFFER_OFFSET = layout.heapBufferOffset;
    OFFSET_OFFSET = layout.offsetOffset;
    READ_ONLY_OFFSET = layout.readOnlyOffset;
    BIG_ENDIAN_OFFSET = layout.bigEndianOffset;
    NATIVE_BYTE_ORDER_OFFSET = layout.nativeByteOrderOffset;
    ATTACHMENT_OFFSET = layout.attachmentOffset;
  }

  private NativeByteBuffer() {}

  static void verifySupported() {
    if (!SUPPORTED) {
      throw new UnsupportedOperationException(
          "native-backed ByteBuffer is unsupported by this JDK", FAILURE);
    }
  }

  static ByteBuffer readOnly(long address, int length) {
    return newView(address, length, true);
  }

  static ByteBuffer writable(long address, int length) {
    return newView(address, length, false);
  }

  /** Rebinds an invalidated writable view to another existing native block. */
  static ByteBuffer writable(ByteBuffer buffer, long address, int length) {
    verifySupported();
    Objects.requireNonNull(buffer, "buffer");
    requireOwned(buffer, DIRECT_CLASS, "buffer is not a writable native view");
    validateAddress(address, length);
    initialize(buffer, address, length, false);
    return buffer;
  }

  /** Rebinds the ThreadContext-owned writable shell after its first validated binding. */
  static ByteBuffer writableTrusted(ByteBuffer buffer, long address, int length) {
    // The shell was created by writable() and is reachable only from its owning ThreadContext.
    // Keep validation and allocation out of this path, but restore mutable Buffer state that a
    // caller may have changed before the previous lease was invalidated.
    UNSAFE.putInt(buffer, POSITION_OFFSET, 0);
    UNSAFE.putInt(buffer, LIMIT_OFFSET, length);
    UNSAFE.putInt(buffer, CAPACITY_OFFSET, length);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, address);
    UNSAFE.putBoolean(buffer, BIG_ENDIAN_OFFSET, true);
    UNSAFE.putBoolean(
        buffer,
        NATIVE_BYTE_ORDER_OFFSET,
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  /** Rebinds an invalidated read-only view to another existing native block. */
  static ByteBuffer readOnly(ByteBuffer buffer, long address, int length) {
    verifySupported();
    Objects.requireNonNull(buffer, "buffer");
    requireOwned(buffer, READ_ONLY_CLASS, "buffer is not a read-only native view");
    validateAddress(address, length);
    initialize(buffer, address, length, true);
    return buffer;
  }

  /** Rebinds a ThreadContext-owned read-only shell after its first validated binding. */
  static ByteBuffer readOnlyTrusted(ByteBuffer buffer, long address, int length) {
    // The shell was created by readOnly() and is reachable only from its owning ThreadContext.
    // Keep validation and allocation out of this path, but restore mutable Buffer state that a
    // caller may have changed before the previous lease was invalidated.
    UNSAFE.putInt(buffer, POSITION_OFFSET, 0);
    UNSAFE.putInt(buffer, LIMIT_OFFSET, length);
    UNSAFE.putInt(buffer, CAPACITY_OFFSET, length);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, address);
    UNSAFE.putBoolean(buffer, BIG_ENDIAN_OFFSET, true);
    UNSAFE.putBoolean(
        buffer,
        NATIVE_BYTE_ORDER_OFFSET,
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  /** Invalidates a view so accidental use after the reader/writer lease fails immediately. */
  static void invalidate(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer");
    verifySupported();
    requireOwned(buffer, null, "buffer is not an OHC native view");
    UNSAFE.putInt(buffer, MARK_OFFSET, -1);
    UNSAFE.putInt(buffer, POSITION_OFFSET, 0);
    UNSAFE.putInt(buffer, LIMIT_OFFSET, 0);
    UNSAFE.putInt(buffer, CAPACITY_OFFSET, 0);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, 0L);
  }

  /** Invalidates a shell whose ownership was established by ThreadContext. */
  static void invalidateTrusted(ByteBuffer buffer) {
    UNSAFE.putInt(buffer, MARK_OFFSET, -1);
    UNSAFE.putInt(buffer, POSITION_OFFSET, 0);
    UNSAFE.putInt(buffer, LIMIT_OFFSET, 0);
    UNSAFE.putInt(buffer, CAPACITY_OFFSET, 0);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, 0L);
  }

  private static void requireOwned(ByteBuffer buffer, Class<?> expectedClass, String message) {
    if ((expectedClass != null && buffer.getClass() != expectedClass)
        || (expectedClass == null
            && buffer.getClass() != DIRECT_CLASS
            && buffer.getClass() != READ_ONLY_CLASS)
        || UNSAFE.getObject(buffer, ATTACHMENT_OFFSET) != VIEW_MARKER) {
      throw new IllegalArgumentException(message);
    }
  }

  private static ByteBuffer newView(long address, int length, boolean readOnly) {
    verifySupported();
    validateAddress(address, length);
    try {
      Object buffer =
          UNSAFE.allocateInstance(readOnly ? READ_ONLY_CLASS : DIRECT_CLASS);
      initialize(buffer, address, length, readOnly);
      return (ByteBuffer) buffer;
    } catch (InstantiationException failure) {
      throw new UnsupportedOperationException("cannot allocate a direct ByteBuffer view", failure);
    }
  }

  private static void validateAddress(long address, int length) {
    if (length < 0) {
      throw new IllegalArgumentException("negative native buffer length: " + length);
    }
    if (length != 0 && address == 0L) {
      throw new IllegalArgumentException("non-empty native buffer has a null address");
    }
  }

  private static void initialize(Object buffer, long address, int length, boolean readOnly) {
    UNSAFE.putInt(buffer, MARK_OFFSET, -1);
    UNSAFE.putInt(buffer, POSITION_OFFSET, 0);
    UNSAFE.putInt(buffer, LIMIT_OFFSET, length);
    UNSAFE.putInt(buffer, CAPACITY_OFFSET, length);
    UNSAFE.putLong(buffer, ADDRESS_OFFSET, address);
    UNSAFE.putObject(buffer, HEAP_BUFFER_OFFSET, null);
    UNSAFE.putInt(buffer, OFFSET_OFFSET, 0);
    UNSAFE.putObject(buffer, ATTACHMENT_OFFSET, VIEW_MARKER);
    UNSAFE.putBoolean(buffer, READ_ONLY_OFFSET, readOnly);
    UNSAFE.putBoolean(buffer, BIG_ENDIAN_OFFSET, true);
    UNSAFE.putBoolean(
        buffer,
        NATIVE_BYTE_ORDER_OFFSET,
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);
  }

  private static final class Layout {
    final boolean supported;
    final Throwable failure;
    final Class<?> directClass;
    final Class<?> readOnlyClass;
    final long markOffset;
    final long positionOffset;
    final long limitOffset;
    final long capacityOffset;
    final long addressOffset;
    final long heapBufferOffset;
    final long offsetOffset;
    final long readOnlyOffset;
    final long bigEndianOffset;
    final long nativeByteOrderOffset;
    final long attachmentOffset;

    private Layout(
        Class<?> directClass,
        Class<?> readOnlyClass,
        long markOffset,
        long positionOffset,
        long limitOffset,
        long capacityOffset,
        long addressOffset,
        long heapBufferOffset,
        long offsetOffset,
        long readOnlyOffset,
        long bigEndianOffset,
        long nativeByteOrderOffset,
        long attachmentOffset) {
      this.supported = true;
      this.failure = null;
      this.directClass = directClass;
      this.readOnlyClass = readOnlyClass;
      this.markOffset = markOffset;
      this.positionOffset = positionOffset;
      this.limitOffset = limitOffset;
      this.capacityOffset = capacityOffset;
      this.addressOffset = addressOffset;
      this.heapBufferOffset = heapBufferOffset;
      this.offsetOffset = offsetOffset;
      this.readOnlyOffset = readOnlyOffset;
      this.bigEndianOffset = bigEndianOffset;
      this.nativeByteOrderOffset = nativeByteOrderOffset;
      this.attachmentOffset = attachmentOffset;
    }

    private Layout(Throwable failure) {
      this.supported = false;
      this.failure = failure;
      this.directClass = null;
      this.readOnlyClass = null;
      this.markOffset = 0L;
      this.positionOffset = 0L;
      this.limitOffset = 0L;
      this.capacityOffset = 0L;
      this.addressOffset = 0L;
      this.heapBufferOffset = 0L;
      this.offsetOffset = 0L;
      this.readOnlyOffset = 0L;
      this.bigEndianOffset = 0L;
      this.nativeByteOrderOffset = 0L;
      this.attachmentOffset = 0L;
    }

    static Layout detect() {
      try {
        Class<?> directClass = Class.forName("java.nio.DirectByteBuffer");
        Class<?> readOnlyClass = Class.forName("java.nio.DirectByteBufferR");
        if (!directClass.isAssignableFrom(readOnlyClass)) {
          throw new IllegalStateException("DirectByteBufferR is not a DirectByteBuffer");
        }
        long mark = offset(java.nio.Buffer.class, "mark");
        long position = offset(java.nio.Buffer.class, "position");
        long limit = offset(java.nio.Buffer.class, "limit");
        long capacity = offset(java.nio.Buffer.class, "capacity");
        long address = offset(java.nio.Buffer.class, "address");
        long heapBuffer = offset(ByteBuffer.class, "hb");
        long offset = offset(ByteBuffer.class, "offset");
        long readOnly = offset(ByteBuffer.class, "isReadOnly");
        long bigEndian = offset(ByteBuffer.class, "bigEndian");
        long nativeByteOrder = offset(ByteBuffer.class, "nativeByteOrder");
        long attachment = offset(directClass, "att");
        Object instance = UNSAFE.allocateInstance(directClass);
        if (!(instance instanceof ByteBuffer)) {
          throw new IllegalStateException("DirectByteBuffer is not a ByteBuffer");
        }
        return new Layout(
            directClass,
            readOnlyClass,
            mark,
            position,
            limit,
            capacity,
            address,
            heapBuffer,
            offset,
            readOnly,
            bigEndian,
            nativeByteOrder,
            attachment);
      } catch (Throwable failure) {
        return new Layout(failure);
      }
    }

    private static long offset(Class<?> type, String name) throws NoSuchFieldException {
      Field field = type.getDeclaredField(name);
      return UNSAFE.objectFieldOffset(field);
    }
  }
}
