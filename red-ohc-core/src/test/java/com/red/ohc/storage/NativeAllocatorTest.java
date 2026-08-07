package com.red.ohc.storage;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.AllocatorType;

public class NativeAllocatorTest {
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
