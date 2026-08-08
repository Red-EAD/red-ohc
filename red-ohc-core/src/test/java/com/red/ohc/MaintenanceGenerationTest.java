package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

public class MaintenanceGenerationTest {
    @Test
    public void staleRemovalCannotDeleteAValuePublishedAfterTheMaintenanceSnapshot() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        long oldValue = memory.allocate(ValueBlock.allocationLength(1));
        ValueBlock.initialize(oldValue, 1L, 1);
        long newValue = memory.allocate(ValueBlock.allocationLength(1));
        ValueBlock.initialize(newValue, 2L, 1);
        Entry entry = new Entry(0L, 0, 7, oldValue);
        ConcurrentHashMap<Entry, Entry> data = index();
        data.putIfAbsent(entry, entry);
        MaintenanceEventLoop worker = new MaintenanceEventLoop(data, memory, new Budget(1 << 20),
                Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());

        long generation = entry.generation();
        assertTrue(entry.claimWriter());
        entry.valueAddress = newValue;
        entry.finishWriter();

        assertFalse(worker.removeFromMap(entry, false, generation, oldValue));
        assertSame(data.get(entry), entry);
        assertEquals(entry.valueAddress, newValue);

        memory.free(oldValue, ValueBlock.allocationLength(1));
        memory.free(newValue, ValueBlock.allocationLength(1));
    }

    @Test
    public void detachedEntryCannotRemoveAChmMappingWithNoValueAddress() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        Entry entry = new Entry(0L, 0, 7, 0L);
        ConcurrentHashMap<Entry, Entry> data = index();
        data.putIfAbsent(entry, entry);
        MaintenanceEventLoop worker = new MaintenanceEventLoop(data, memory, new Budget(1 << 20),
                Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());

        assertFalse(worker.removeFromMap(entry, false, entry.generation(), 0L));
        assertSame(data.get(entry), entry);
        data.clear();
        memory.closeArenas();
    }

    @Test(timeOut = 500L)
    public void evictionSkipsAnEntryWhoseWriterMutexIsHeld() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        long value = memory.allocate(ValueBlock.allocationLength(1));
        ValueBlock.initialize(value, Long.MAX_VALUE, 1);
        Entry entry = new Entry(0L, 0, 9, value);
        ConcurrentHashMap<Entry, Entry> data = index();
        data.put(entry, entry);
        MaintenanceEventLoop worker = new MaintenanceEventLoop(data, memory, new Budget(1 << 20),
                Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());
        try {
            assertTrue(entry.claimWriter());
            assertFalse(worker.removeFromMap(entry, true, entry.generation(), entry.valueAddress));
            assertSame(data.get(entry), entry);
        } finally {
            entry.finishWriter();
            data.clear();
            memory.free(value, ValueBlock.allocationLength(1));
            memory.closeArenas();
        }
    }

    private static void assertTrue(boolean value) {
        if (!value) throw new AssertionError("expected true");
    }

    private static ConcurrentHashMap<Entry, Entry> index() {
        return new ConcurrentHashMap<>();
    }
}
