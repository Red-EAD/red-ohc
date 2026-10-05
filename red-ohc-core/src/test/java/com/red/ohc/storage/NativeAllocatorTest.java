package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.ref.Reference;

import com.sun.jna.CallbackProxy;
import com.sun.jna.CallbackReference;
import com.sun.jna.Function;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NativeAllocatorTest {
  @DataProvider(name = "mappingPlatforms")
  public Object[][] mappingPlatforms() {
    return new Object[][] {{Platform.WINDOWS}, {Platform.MAC}, {Platform.LINUX}};
  }

  @Test(dataProvider = "mappingPlatforms")
  public void mappingsAreWritableAndReleaseTheEntireRegion(int platform) {
    try (MappingFixture fixture = new MappingFixture(platform)) {
      NativeAllocator allocator = fixture.allocator();
      assertTrue(allocator.pageMappingsAvailable());
      long address = allocator.mapPage(fixture.page.size());
      assertEquals(address, Pointer.nativeValue(fixture.page));
      NativeMemory.putLong(address, 41L);
      NativeMemory.putLong(address + fixture.page.size() - Long.BYTES, 43L);
      assertEquals(NativeMemory.getLong(address), 41L);
      assertEquals(NativeMemory.getLong(address + fixture.page.size() - Long.BYTES), 43L);
      allocator.unmapPage(address, fixture.page.size());
      assertTrue(fixture.released, "the entire mapped region must be released");
    }
  }

  @Test(dataProvider = "mappingPlatforms", expectedExceptions = OutOfMemoryError.class)
  public void failedAllocationDoesNotExposeAnAddress(int platform) {
    try (MappingFixture fixture = new MappingFixture(platform)) {
      fixture.failAllocation = true;
      fixture.allocator().mapPage(fixture.page.size());
    }
  }

  @Test(dataProvider = "mappingPlatforms", expectedExceptions = IllegalStateException.class)
  public void failedReleaseIsReported(int platform) {
    try (MappingFixture fixture = new MappingFixture(platform)) {
      fixture.failRelease = true;
      fixture.allocator().unmapPage(Pointer.nativeValue(fixture.page), fixture.page.size());
    }
  }

  @Test
  public void windowsPreservesPointerAndSizeWidthsAboveFourGiB() {
    if (Native.SIZE_T_SIZE != Long.BYTES) {
      throw new SkipException("requires a 64-bit native size_t");
    }
    try (MappingFixture fixture = new MappingFixture(Platform.WINDOWS)) {
      // The native fixture checks the ABI without reserving a multi-GiB region on the host.
      fixture.expectedBytes = (1L << 32) + 65_536L;
      fixture.mappingAddress = 0x1234_0000_0000L;
      NativeAllocator allocator = fixture.allocator();
      long address = allocator.mapPage(fixture.expectedBytes);
      assertEquals(address, fixture.mappingAddress);
      allocator.unmapPage(address, fixture.expectedBytes);
      assertTrue(fixture.released);
    }
  }

  @Test
  public void mapsWritableAnonymousPagesAndReturnsThemToTheOperatingSystem() {
    NativeAllocator allocator = new NativeAllocator();
    assertTrue(allocator.pageMappingsAvailable());
    long bytes = 65_536L;
    long address = allocator.mapPage(bytes);
    try {
      NativeMemory.putLong(address, 41L);
      NativeMemory.putLong(address + bytes - Long.BYTES, 43L);
      assertEquals(NativeMemory.getLong(address), 41L);
      assertEquals(NativeMemory.getLong(address + bytes - Long.BYTES), 43L);
    } finally {
      allocator.unmapPage(address, bytes);
    }
  }

  @Test
  public void memoryTracksActualNativeAllocationWithoutCacheAdmission() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long address = memory.allocate(64L);
    try {
      long second = memory.allocate(8L);
      try {
        assertTrue(memory.allocated() >= 72L);
      } finally {
        memory.free(second, 8L);
      }
    } finally {
      memory.free(address, 64L);
    }
  }

  @Test
  public void allocatesAndFreesThroughTheNativeBackend() {
    NativeAllocator allocator = new NativeAllocator();
    long address = allocator.allocate(32L);
    try {
      assertTrue(address != 0L);
    } finally {
      allocator.free(address);
    }
  }

  private static final class MappingFixture implements AutoCloseable {
    private final int platform;
    private final Memory page = new Memory(65_536L);
    private long expectedBytes = page.size();
    private long mappingAddress = Pointer.nativeValue(page);
    private boolean failAllocation;
    private boolean failRelease;
    private boolean released;
    private final CallbackProxy allocation =
        new CallbackProxy() {
          @Override
          public Object callback(Object[] arguments) {
            long bytes = ((Number) arguments[1]).longValue();
            boolean valid =
                arguments[0] == null
                    && bytes == expectedBytes
                    && (windows()
                        ? ((Integer) arguments[2]) == 0x3000 && ((Integer) arguments[3]) == 0x04
                        : ((Integer) arguments[2]) == 0x3
                            && ((Integer) arguments[3])
                                == (platform == Platform.MAC ? 0x1002 : 0x22)
                            && ((Integer) arguments[4]) == -1
                            && ((Long) arguments[5]) == 0L);
            if (!valid || failAllocation) {
              return windows() ? null : new Pointer(-1L);
            }
            return new Pointer(mappingAddress);
          }

          @Override
          public Class<?>[] getParameterTypes() {
            return windows()
                ? new Class<?>[] {Pointer.class, sizeType(), int.class, int.class}
                : new Class<?>[] {
                  Pointer.class, sizeType(), int.class, int.class, int.class, long.class
                };
          }

          @Override
          public Class<?> getReturnType() {
            return Pointer.class;
          }
        };
    private final CallbackProxy release =
        new CallbackProxy() {
          @Override
          public Object callback(Object[] arguments) {
            boolean valid =
                Pointer.nativeValue((Pointer) arguments[0]) == mappingAddress
                    && (windows()
                        ? ((Number) arguments[1]).longValue() == 0L
                            && ((Integer) arguments[2]) == 0x8000
                        : ((Number) arguments[1]).longValue() == expectedBytes);
            released = valid && !failRelease;
            return windows() ? (released ? 1 : 0) : (released ? 0 : -1);
          }

          @Override
          public Class<?>[] getParameterTypes() {
            return windows()
                ? new Class<?>[] {Pointer.class, sizeType(), int.class}
                : new Class<?>[] {Pointer.class, sizeType()};
          }

          @Override
          public Class<?> getReturnType() {
            return int.class;
          }
        };

    private MappingFixture(int platform) {
      this.platform = platform;
    }

    private boolean windows() {
      return platform == Platform.WINDOWS;
    }

    private static Class<?> sizeType() {
      return Native.SIZE_T_SIZE == Long.BYTES ? long.class : int.class;
    }

    private NativeAllocator allocator() {
      return new NativeAllocator(
          new NativeAllocator.PageMapping(
              platform,
              Function.getFunction(CallbackReference.getFunctionPointer(allocation)),
              Function.getFunction(CallbackReference.getFunctionPointer(release))));
    }

    @Override
    public void close() {
      page.close();
      Reference.reachabilityFence(allocation);
      Reference.reachabilityFence(release);
    }
  }
}
