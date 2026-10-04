package com.red.ohc.storage;

import com.sun.jna.Function;
import com.sun.jna.Native;
import com.sun.jna.Platform;

/** Owns the native allocation backend without cache-specific accounting. */
public class NativeAllocator {
  private static final int PROT_READ_WRITE = 0x3;
  private static final int MAP_PRIVATE_ANONYMOUS = Platform.isMac() ? 0x1002 : 0x22;
  private static volatile Function mmap;
  private static volatile Function munmap;

  public NativeAllocator() {}

  /** Pages bypass libc when mappings are available so frees return to the OS deterministically. */
  public boolean pageMappingsAvailable() {
    if (mmap != null && munmap != null) {
      return true;
    }
    resolvePageMappingFunctions();
    return mmap != null && munmap != null;
  }

  public long mapPage(long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException("bytes must be positive: " + bytes);
    }
    Function mapper = mmap;
    if (mapper == null) {
      throw new IllegalStateException("page mappings are not available");
    }
    long address =
        mapper.invokeLong(
            new Object[] {null, bytes, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS, -1, 0L});
    if (address == -1L) {
      throw new OutOfMemoryError("page mapping failed: " + bytes);
    }
    return address;
  }

  public void unmapPage(long address, long bytes) {
    Function unmapper = munmap;
    if (unmapper == null) {
      throw new IllegalStateException("page mappings are not available");
    }
    if (unmapper.invokeInt(new Object[] {address, bytes}) != 0) {
      throw new IllegalStateException("page unmapping failed: " + bytes);
    }
  }

  private static synchronized void resolvePageMappingFunctions() {
    if (mmap != null && munmap != null) {
      return;
    }
    try {
      mmap = Function.getFunction("c", "mmap");
      munmap = Function.getFunction("c", "munmap");
    } catch (Throwable failure) {
      // Platforms without mapping symbols stay on the malloc path.
      mmap = null;
      munmap = null;
    }
  }

  public long allocate(long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException("bytes must be positive");
    }
    long address = Native.malloc(bytes);
    if (address == 0L) {
      throw new OutOfMemoryError("native allocation failed: " + bytes);
    }
    return address;
  }

  public void free(long address) {
    if (address == 0L) {
      return;
    }
    Native.free(address);
  }
}
