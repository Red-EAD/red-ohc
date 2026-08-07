package com.red.ohc.storage;

import com.red.ohc.AllocatorType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class NativeMemoryEqualsTest {
    @Test
    public void comparesHeapSlicesAndNativeBlocksAcrossWordAndTailBoundaries() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        byte[] bytes = new byte[67];
        for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (index * 37 + 11);
        long address = memory.allocate(65L);
        long other = memory.allocate(65L);
        try {
            NativeMemory.copy(bytes, 1, address, 65L);
            NativeMemory.copy(bytes, 1, other, 65L);
            for (int length = 0; length <= 65; length++) {
                assertTrue(NativeMemory.equals(address, bytes, 1, length), "length=" + length);
                assertTrue(NativeMemory.equals(address, other, length), "native length=" + length);
            }
            for (int mismatch : new int[] {0, 31, 64}) {
                byte original = NativeMemory.getByte(address + mismatch);
                NativeMemory.putByte(address + mismatch, (byte) (original ^ 1));
                assertFalse(NativeMemory.equals(address, bytes, 1, 65), "mismatch=" + mismatch);
                NativeMemory.putByte(address + mismatch, original);
            }
        } finally {
            memory.free(address, 65L);
            memory.free(other, 65L);
            memory.closeArenas();
        }
    }
}
