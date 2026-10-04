package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class NativeAllocatorTest {
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
}
