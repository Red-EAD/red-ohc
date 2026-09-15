package com.red.ohc.storage;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;

public class NativeAllocatorTest {
  @Test
  public void memoryTracksActualNativeAllocationWithoutCacheAdmission() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
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
  public void allocatesAndFreesThroughTheConfiguredNativeBackend() {
    NativeAllocator allocator = new NativeAllocator(AllocatorType.JNA);
    long address = allocator.allocate(32L);
    try {
      assertTrue(address != 0L);
    } finally {
      allocator.free(address);
    }
  }
}
