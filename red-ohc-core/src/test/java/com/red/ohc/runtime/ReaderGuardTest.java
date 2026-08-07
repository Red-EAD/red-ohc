package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.AllocatorType;
import com.red.ohc.Eviction;
import com.red.ohc.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;

public class ReaderGuardTest {
    @Test
    public void enterPublishesAnEpochAndExitQuiescesTheReader() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(new ConcurrentHashMap<>(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, 256,
                new AtomicLong(), new CopyOnWriteArrayList<ReaderSlot>());
        ReaderGuard guard = new ReaderGuard(loop, () -> false);
        ThreadContext context = new ThreadContext();
        try {
            assertTrue(guard.enter(context));
            assertTrue(context.slot.epoch != 0L);
            guard.exit(context);
            assertEquals(context.slot.epoch, 0L);
        } finally {
            memory.closeArenas();
        }
    }
}
