package com.red.ohc.runtime;

import com.red.ohc.api.ValueView;
import com.red.ohc.storage.NativeMemory;

public final class DirectValueView implements ValueView {
  private long address;
  private int length;

  public void reset(long address, int length) {
    this.address = address;
    this.length = length;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public byte getByte(int offset) {
    check(offset, 1);
    return NativeMemory.getByte(address + offset);
  }

  @Override
  public long getLong(int offset) {
    check(offset, 8);
    return NativeMemory.getLong(address + offset);
  }

  @Override
  public void copyTo(byte[] target, int targetOffset) {
    if (targetOffset < 0 || target.length - targetOffset < length) {
      throw new IndexOutOfBoundsException();
    }
    NativeMemory.copy(address, target, targetOffset, length);
  }

  private void check(int offset, int bytes) {
    if (offset < 0 || offset > length - bytes) {
      throw new IndexOutOfBoundsException();
    }
  }
}
