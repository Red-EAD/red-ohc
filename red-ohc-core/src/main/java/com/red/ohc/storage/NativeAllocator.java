package com.red.ohc.storage;

import java.util.Objects;

import com.sun.jna.Native;

import com.red.ohc.api.AllocatorType;

/** Owns the selected native allocation backend without cache-specific accounting. */
public final class NativeAllocator {
  private final AllocatorType type;

  public NativeAllocator(AllocatorType type) {
    this.type = Objects.requireNonNull(type, "type");
  }

  public long allocate(long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException("bytes must be positive");
    }
    long address =
        type == AllocatorType.JNA ? Native.malloc(bytes) : NativeMemory.U.allocateMemory(bytes);
    if (address == 0L) {
      throw new OutOfMemoryError("native allocation failed: " + bytes);
    }
    return address;
  }

  public void free(long address) {
    if (address == 0L) {
      return;
    }
    if (type == AllocatorType.JNA) {
      Native.free(address);
    } else {
      NativeMemory.U.freeMemory(address);
    }
  }
}
