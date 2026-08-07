package com.red.ohc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.SizeClasses;
import com.red.ohc.storage.WriterArena;

public class WriterArenaTest {
    @Test
    public void smallEntryReuseDoesNotAllocateANativeBlockPerEntry() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        try {
            WriterArena arena = memory.newWriterArena();
            long bytes = 112L;

            long first = arena.allocate(bytes);
            Assert.assertEquals(memory.rawAllocationCount(), 1L);
            memory.releaseEntry(first, bytes);

            long second = arena.allocate(bytes);
            Assert.assertEquals(second, first);
            Assert.assertEquals(memory.rawAllocationCount(), 1L);
            memory.releaseEntry(second, bytes);
        } finally {
            memory.closeArenas();
        }
    }

    @Test
    public void largeEntryUsesOneDirectNativeBlock() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        try {
            WriterArena arena = memory.newWriterArena();
            long bytes = 32_769L;

            long entry = arena.allocate(bytes);
            Assert.assertEquals(memory.rawAllocationCount(), 1L);
            memory.releaseEntry(entry, bytes);
            Assert.assertEquals(memory.allocated(), 0L);
        } finally {
            memory.closeArenas();
        }
    }

    @Test
    public void smallEntriesAllocateOnlyThePagesRequiredByTheirSizeClass() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        try {
            WriterArena arena = memory.newWriterArena();
            long bytes = 112L;
            int slotBytes = 128;
            int count = 1_025;
            List<Long> entries = new ArrayList<>(count);

            for (int i = 0; i < count; i++) entries.add(arena.allocate(bytes));

            long requiredPages = (count * (long) slotBytes + SizeClasses.PAGE_BYTES - 1L) / SizeClasses.PAGE_BYTES;
            Assert.assertTrue(memory.rawAllocationCount() <= requiredPages,
                    "small writes must consume pooled pages rather than one native allocation per entry");
            for (long entry : entries) memory.releaseEntry(entry, bytes);
        } finally {
            memory.closeArenas();
        }
    }

    @Test
    public void sizeClassLookupCoversEachSmallAllocationBoundary() {
        Assert.assertEquals(SizeClasses.indexForEntry(112L), 0);
        Assert.assertEquals(SizeClasses.indexForEntry(113L), 1);
        Assert.assertEquals(SizeClasses.indexForEntry(1_008L), 28);
        Assert.assertEquals(SizeClasses.indexForEntry(32_752L), 85);
        Assert.assertEquals(SizeClasses.indexForEntry(32_753L), -1);
    }

    @Test
    public void drainingRemoteFreesReturnsTheirBytesToTheRemoteBudget() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        try {
            WriterArena arena = memory.newWriterArena();
            long bytes = 112L;
            int sizeClass = SizeClasses.indexForEntry(bytes);
            int count = (256 << 10) / SizeClasses.slotBytes(sizeClass);
            List<Long> entries = new ArrayList<>(count);

            for (int i = 0; i < count; i++) entries.add(arena.allocate(bytes));
            for (long entry : entries) memory.releaseEntry(entry, bytes);
            Assert.assertEquals(remoteBytes(arena), count * (long) SizeClasses.slotBytes(sizeClass));

            long reclaimed = arena.allocate(bytes);
            Assert.assertEquals(remoteBytes(arena), 0L,
                    "a drained remote stack must no longer consume the remote-free budget");
            memory.releaseEntry(reclaimed, bytes);
        } finally {
            memory.closeArenas();
        }
    }

    private static long remoteBytes(WriterArena arena) throws Exception {
        Field field = WriterArena.class.getDeclaredField("remoteBytes");
        field.setAccessible(true);
        return ((AtomicLong) field.get(arena)).get();
    }
}
