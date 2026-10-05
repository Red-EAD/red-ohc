package com.red.ohc.storage;

import com.sun.jna.Function;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * Owns native allocation without cache-specific accounting. Pages use anonymous mmap on Linux/macOS
 * and VirtualAlloc/VirtualFree on Windows; releasing a Windows page frees its entire reservation.
 */
public class NativeAllocator {
  private static volatile PageMapping sharedPageMapping;
  private final PageMapping pageMapping;

  public NativeAllocator() {
    this(null);
  }

  NativeAllocator(PageMapping pageMapping) {
    this.pageMapping = pageMapping;
  }

  /** Pages bypass libc when mappings are available so frees return to the OS deterministically. */
  public boolean pageMappingsAvailable() {
    if (currentPageMapping() != null) {
      return true;
    }
    resolvePageMappingFunctions();
    return currentPageMapping() != null;
  }

  public long mapPage(long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException("bytes must be positive: " + bytes);
    }
    PageMapping mapping = currentPageMapping();
    if (mapping == null) {
      throw new IllegalStateException("page mappings are not available");
    }
    return mapping.map(bytes);
  }

  public void unmapPage(long address, long bytes) {
    PageMapping mapping = currentPageMapping();
    if (mapping == null) {
      throw new IllegalStateException("page mappings are not available");
    }
    mapping.unmap(address, bytes);
  }

  private PageMapping currentPageMapping() {
    return pageMapping != null ? pageMapping : sharedPageMapping;
  }

  private static synchronized void resolvePageMappingFunctions() {
    if (sharedPageMapping != null) {
      return;
    }
    try {
      int platform = Platform.getOSType();
      if (platform == Platform.WINDOWS) {
        sharedPageMapping =
            new PageMapping(
                platform,
                Function.getFunction("kernel32", "VirtualAlloc", Function.ALT_CONVENTION),
                Function.getFunction("kernel32", "VirtualFree", Function.ALT_CONVENTION));
      } else if (platform == Platform.MAC
          || platform == Platform.LINUX
          || platform == Platform.ANDROID) {
        sharedPageMapping =
            new PageMapping(
                platform, Function.getFunction("c", "mmap"), Function.getFunction("c", "munmap"));
      }
    } catch (UnsatisfiedLinkError | SecurityException failure) {
      // Platforms without mapping symbols stay on the malloc path.
      sharedPageMapping = null;
    }
  }

  static final class PageMapping {
    private static final int PROT_READ_WRITE = 0x3;
    private static final int MAP_PRIVATE = 0x2;
    private static final int MAC_MAP_ANONYMOUS = 0x1000;
    private static final int LINUX_MAP_ANONYMOUS = 0x20;
    private static final int MEM_RESERVE_COMMIT = 0x3000;
    private static final int PAGE_READ_WRITE = 0x04;
    private static final int MEM_RELEASE = 0x8000;

    private final int platform;
    private final Function mapper;
    private final Function unmapper;

    PageMapping(int platform, Function mapper, Function unmapper) {
      this.platform = platform;
      this.mapper = mapper;
      this.unmapper = unmapper;
    }

    long map(long bytes) {
      boolean windows = platform == Platform.WINDOWS;
      int flags =
          MAP_PRIVATE | (platform == Platform.MAC ? MAC_MAP_ANONYMOUS : LINUX_MAP_ANONYMOUS);
      Pointer result =
          mapper.invokePointer(
              windows
                  ? new Object[] {null, sizeArgument(bytes), MEM_RESERVE_COMMIT, PAGE_READ_WRITE}
                  : new Object[] {null, sizeArgument(bytes), PROT_READ_WRITE, flags, -1, 0L});
      long address = Pointer.nativeValue(result);
      boolean failed =
          windows
              ? address == 0L
              : address == -1L || (Native.POINTER_SIZE == Integer.BYTES && address == 0xffff_ffffL);
      if (failed) {
        throw new OutOfMemoryError(
            "page mapping failed: " + bytes + ", native error: " + Native.getLastError());
      }
      return address;
    }

    void unmap(long address, long bytes) {
      boolean windows = platform == Platform.WINDOWS;
      // MEM_RELEASE requires the allocation base and zero size to release the entire reservation.
      int result =
          unmapper.invokeInt(
              windows
                  ? new Object[] {new Pointer(address), sizeArgument(0L), MEM_RELEASE}
                  : new Object[] {new Pointer(address), sizeArgument(bytes)});
      if (windows ? result == 0 : result != 0) {
        throw new IllegalStateException(
            "page unmapping failed: " + bytes + ", native error: " + Native.getLastError());
      }
    }

    private static Number sizeArgument(long bytes) {
      if (Native.SIZE_T_SIZE == Long.BYTES) {
        return Long.valueOf(bytes);
      }
      if (bytes < 0L || bytes > 0xffff_ffffL) {
        throw new IllegalArgumentException("page size exceeds native size_t: " + bytes);
      }
      return Integer.valueOf((int) bytes);
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
