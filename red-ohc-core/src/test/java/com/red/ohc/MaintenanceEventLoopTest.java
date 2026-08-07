package com.red.ohc;

import static org.testng.Assert.assertTrue;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.FlatConcurrentMap;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class MaintenanceEventLoopTest {
    @Test
    public void boundedHintQueueRejectsWithoutGrowing() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, 256,
                new AtomicLong(), new CopyOnWriteArrayList<ReaderSlot>());
        for (int i = 0; i < 256; i++) {
            assertTrue(loop.enqueue(Entry.bootstrap(), Entry.PENDING_UPDATE));
        }
        assertTrue(!loop.enqueue(Entry.bootstrap(), Entry.PENDING_UPDATE));
        assertTrue(loop.queueDepth() <= 256L);
        loop.stop();
        memory.closeArenas();
    }

    @Test(timeOut = 2_000L)
    public void idleLoopParksInsteadOfBusySpinning() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, 256,
                new AtomicLong(), new CopyOnWriteArrayList<ReaderSlot>());
        loop.start();
        try {
            long deadline = System.nanoTime() + 1_000_000_000L;
            while (!loop.isParked() && System.nanoTime() < deadline) Thread.yield();
            assertTrue(loop.isParked(), "maintenance loop did not park while idle");
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test(timeOut = 10_000L)
    public void fullHintQueueIsRepairedFromTheAuthoritativeMap() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        WriterArena arena = memory.newWriterArena();
        long value = arena.allocate(ValueBlock.allocationLength(1));
        ValueBlock.initialize(value, System.currentTimeMillis() + 60_000L, 1);
        Entry real = new Entry(0L, 0, 7L, value);
        FlatConcurrentMap data = index();
        data.putIfAbsent(real, real);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(data, memory, new Budget(1 << 20),
                Ticker.DEFAULT, 1 << 20, Eviction.LRU, 256, new AtomicLong(),
                new CopyOnWriteArrayList<ReaderSlot>());
        for (int i = 0; i < 256; i++) assertTrue(loop.enqueue(new Entry(0L, 0, i + 100L, 0L), Entry.PENDING_UPDATE));
        assertTrue(!loop.enqueue(real, Entry.PENDING_UPDATE));
        loop.start();
        try {
            loop.flush().join();
            MaintenanceEventLoop.Snapshot snapshot = loop.snapshot();
            assertTrue(snapshot.rejectedQueue > 0L, "rejectedQueue=" + snapshot.rejectedQueue);
            assertTrue(snapshot.applied > 0L, "applied=" + snapshot.applied
                    + ", queueDepth=" + snapshot.queueDepth);
        } finally {
            loop.stop();
            loop.join(1_000L);
        }
    }

    private static FlatConcurrentMap index() {
        return new FlatConcurrentMap(64L, 0L);
    }
}
