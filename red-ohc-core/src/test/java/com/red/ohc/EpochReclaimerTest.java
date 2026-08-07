package com.red.ohc;

import com.red.ohc.*;
import com.red.ohc.maintenance.EpochReclaimer;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;

import org.testng.annotations.Test;

public class EpochReclaimerTest {
    @Test
    public void activeReaderPreventsRetiredNativeMemoryFromBeingFreed() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        long address = memory.allocate(64L);
        EpochReclaimer reclaimer = new EpochReclaimer(memory);
        ReaderSlot reader = new ReaderSlot();
        reader.epoch = 7L;
        reclaimer.retireRaw(address, 64L, 7L);

        assertEquals(reclaimer.reclaim(memory, Collections.singletonList(reader), 1), 0);
        assertTrue(memory.allocated() >= 64L);

        reader.epoch = 0L;
        assertEquals(reclaimer.reclaim(memory, Collections.singletonList(reader), 1), 1);
        assertEquals(memory.allocated(), 0L);
        assertEquals(reclaimer.bytes(), 0L);
    }

    @Test
    public void retiredEntryReturnsToItsWriterArenaAfterQuiescence() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        WriterArena arena = memory.newWriterArena();
        long bytes = ValueBlock.allocationLength(8);
        long address = arena.allocate(bytes);
        EpochReclaimer reclaimer = new EpochReclaimer(memory);
        ReaderSlot reader = new ReaderSlot();
        reader.epoch = 3L;
        reclaimer.retireEntry(address, bytes, 3L);

        assertEquals(reclaimer.reclaim(memory, Collections.singletonList(reader), 1), 0);
        reader.epoch = 0L;
        assertEquals(reclaimer.reclaim(memory, Collections.singletonList(reader), 1), 1);
        assertEquals(arena.allocate(bytes), address);
        memory.closeArenas();
    }
}
