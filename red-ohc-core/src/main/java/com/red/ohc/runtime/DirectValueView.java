package com.red.ohc.runtime;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.red.ohc.api.ValueView;
import com.red.ohc.storage.NativeMemory;

public final class DirectValueView implements ValueView {
  private static final boolean LITTLE_ENDIAN =
      ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private long address;
  private int length;
  private ThreadContext context;
  private ByteBuffer byteBuffer;

  void reset(long address, int length, ThreadContext context) {
    this.address = address;
    this.length = length;
    this.context = context;
    this.byteBuffer = null;
  }

  @Override
  public int length() {
    ensureActive();
    return length;
  }

  @Override
  public byte getByte(int offset) {
    ensureActive();
    check(offset, 1);
    return NativeMemory.getByte(address + offset);
  }

  @Override
  public long getLong(int offset) {
    ensureActive();
    check(offset, 8);
    long value = NativeMemory.getLong(address + offset);
    return LITTLE_ENDIAN ? Long.reverseBytes(value) : value;
  }

  @Override
  public void copyTo(byte[] target, int targetOffset) {
    ensureActive();
    if (targetOffset < 0 || target.length - targetOffset < length) {
      throw new IndexOutOfBoundsException();
    }
    NativeMemory.copy(address, target, targetOffset, length);
  }

  @Override
  public ByteBuffer asReadOnlyByteBuffer() {
    ByteBuffer current = byteBuffer;
    if (current == null) {
      ThreadContext owner = context;
      if (owner == null) {
        throw new IllegalStateException("direct value view is not active");
      }
      current = owner.readOnlyValueBuffer(address, length);
      byteBuffer = current;
    }
    return current;
  }

  boolean hasMaterializedByteBuffer() {
    return byteBuffer != null;
  }

  private void check(int offset, int bytes) {
    if (offset < 0 || offset > length - bytes) {
      throw new IndexOutOfBoundsException();
    }
  }

  private void ensureActive() {
    if (context == null) {
      throw new IllegalStateException("direct value view is not active");
    }
  }
}
