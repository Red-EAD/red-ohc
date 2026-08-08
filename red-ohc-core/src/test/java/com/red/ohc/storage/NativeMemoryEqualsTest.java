package com.red.ohc.storage;

import com.red.ohc.AllocatorType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class NativeMemoryEqualsTest {
    @Test
    public void usesTheConfiguredBulkComparisonShape() {
        assertEquals(NativeMemory.BULK_EQUALS_THRESHOLD, 128);
        assertEquals(NativeMemory.BULK_EQUALS_LONGS, 8);
    }

    @Test
    public void comparesHeapSlicesAndNativeBlocksAcrossWordAndTailBoundaries() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        byte[] bytes = new byte[259];
        for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (index * 37 + 11);
        long address = memory.allocate(257L);
        long other = memory.allocate(257L);
        try {
            NativeMemory.copy(bytes, 1, address, 257L);
            NativeMemory.copy(bytes, 1, other, 257L);
            for (int length = 0; length <= 257; length++) {
                assertTrue(NativeMemory.equals(address, bytes, 1, length), "length=" + length);
                assertTrue(NativeMemory.equals(address, other, length), "native length=" + length);
            }
            for (int mismatch : new int[] {0, 7, 63, 64, 71, 127, 128, 135, 255, 256}) {
                byte original = NativeMemory.getByte(address + mismatch);
                NativeMemory.putByte(address + mismatch, (byte) (original ^ 1));
                try {
                    assertFalse(NativeMemory.equals(address, bytes, 1, 257), "heap mismatch=" + mismatch);
                    assertFalse(NativeMemory.equals(address, other, 257), "native mismatch=" + mismatch);
                } finally {
                    NativeMemory.putByte(address + mismatch, original);
                }
            }
        } finally {
            memory.free(address, 257L);
            memory.free(other, 257L);
            memory.closeArenas();
        }
    }

    @Test
    public void comparesTheNonZeroArrayOffsetAndTailAfterBulkBlocks() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        byte[] bytes = new byte[300];
        for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (index * 13 + 5);
        long address = memory.allocate(129L);
        try {
            NativeMemory.copy(bytes, 37, address, 129L);
            assertTrue(NativeMemory.equals(address, bytes, 37, 129));

            byte original = NativeMemory.getByte(address + 128);
            try {
                NativeMemory.putByte(address + 128, (byte) (original ^ 1));
                assertFalse(NativeMemory.equals(address, bytes, 37, 129));
            } finally {
                NativeMemory.putByte(address + 128, original);
            }
        } finally {
            memory.free(address, 129L);
            memory.closeArenas();
        }
    }
}
