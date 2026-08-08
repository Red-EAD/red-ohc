package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        ReaderGuard guard = new ReaderGuard(loop, () -> false);
        ThreadContext context = new ThreadContext(null, null);
        try {
            assertTrue(guard.enter(context));
            assertTrue(context.slot.epoch != 0L);
            guard.exit(context);
            assertEquals(context.slot.epoch, 0L);
        } finally {
            memory.closeArenas();
        }
    }

    @Test
    public void closeRacingAfterEpochPublicationRejectsTheReaderBeforeItCanDereferenceNativeMemory() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(new ConcurrentHashMap<>(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        AtomicInteger closeChecks = new AtomicInteger();
        ReaderGuard guard = new ReaderGuard(loop, () -> closeChecks.getAndIncrement() != 0);
        ThreadContext context = new ThreadContext(null, null);
        try {
            assertFalse(guard.enter(context),
                    "close after epoch publication must reject the reader instead of letting it race shutdown free");
            assertEquals(context.slot.epoch, 0L);
        } finally {
            memory.closeArenas();
        }
    }
}
