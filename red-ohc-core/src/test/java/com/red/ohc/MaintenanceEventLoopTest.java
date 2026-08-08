package com.red.ohc;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.TimerWheel;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.runtime.ThreadContext;

public class MaintenanceEventLoopTest {
    @Test
    public void eventLoopOwnsTheExpiryConsumerInsteadOfAllocatingAMethodReferencePerPass() {
        assertTrue(TimerWheel.TimerConsumer.class.isAssignableFrom(MaintenanceEventLoop.class),
                "the actor itself must be the stable timer consumer");
    }

    @Test
    public void eventLoopOwnsTheAccessConsumerInsteadOfCreatingOneForEveryReaderScan() {
        assertTrue(AccessConsumer.class.isAssignableFrom(MaintenanceEventLoop.class),
                "the actor itself must be the stable access-ring consumer");
    }

    @Test
    public void readerRegistrationDoesNotWakeAnIdleWorker() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            loop.registerReader(new ReaderSlot());
            assertEquals(loop.snapshot().wakeSignals, 0L,
                    "registering a reader is not maintenance work");
        } finally {
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void idleWorkerDoesNotReadAClockWithoutTimerOrRetirementWork() throws Exception {
        CountingTicker ticker = new CountingTicker();
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), ticker, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        int constructorCalls = ticker.calls.get();
        loop.start();
        try {
            waitUntilParked(loop);
            Thread.sleep(20L);
            assertEquals(ticker.calls.get(), constructorCalls,
                    "an idle worker must not sample a clock just to decide to park");
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void boundedQueueMovesPublishedHintsToRepairWhenTheArrayIsFull() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry(), 2);
        Entry[] entries = new Entry[4];
        try {
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Entry(0L, 0, i + 1, 0L);
                assertTrue(loop.reserveMutation(entries[i], Entry.PENDING_ADD));
                loop.publishMutation(entries[i]);
                loop.afterWrite();
            }
            assertTrue(loop.queueDepth() <= 4L);
            assertEquals(loop.snapshot().rejectedQueue, 0L,
                    "a full transport must repair asynchronously, not reject a visible mutation");

            loop.start();
            loop.flush().join();
            for (Entry entry : entries) assertEquals(entry.pendingFlags, 0);
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void publishedAccessHintDrainsCountersWithoutScanningEveryIdlePass() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        ReaderRegistry readers = new ReaderRegistry();
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
        ThreadContext context = new ThreadContext();
        readers.register(context.slot);
        context.bindMaintenance(loop);
        loop.start();
        try {
            for (int i = 0; i < 1_024; i++) context.hit();
            context.finishRead(1_024L);
            loop.flush().join();
            assertEquals(loop.snapshot().hits, 1_024L);
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test
    public void dirtyCreditsBoundPendingMaintenanceAndReturnAfterDrain() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            int limit = (int) loop.queueCapacity() * 2;
            for (int i = 0; i < limit; i++) {
                Entry entry = new Entry(0L, 0, i + 1, 0L);
                assertTrue(loop.reserveMutation(entry, Entry.PENDING_ADD));
                loop.publishMutation(entry);
                loop.afterWrite();
            }
            Entry rejected = new Entry(0L, 0, limit + 1, 0L);
            assertFalse(loop.reserveMutation(rejected, Entry.PENDING_ADD), "dirty credits must reject before publication");

            loop.start();
            loop.flush().join();
            assertTrue(loop.reserveMutation(rejected, Entry.PENDING_ADD), "draining returns the unique event credit");
            loop.cancelMutation(rejected);
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test
    public void dirtyCreditLimitTracksCpuCountRatherThanAllocatorStripeCount() {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            assertEquals(loop.queueCapacity(),
                    (long) ChmSizing.maintenanceQueueCapacity(0L, 1 << 20, 1L));
        } finally {
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void idleLoopParksInsteadOfBusySpinning() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
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

    @Test(timeOut = 2_000L)
    public void retirementBurstsDoNotAdvanceTheReaderEpochForEverySingleRecord() throws Exception {
        FrozenTicker ticker = new FrozenTicker();
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        Budget budget = new Budget(1 << 20);
        ReaderRegistry readers = new ReaderRegistry();
        ReaderSlot activeReader = new ReaderSlot();
        activeReader.epoch = 1L;
        readers.register(activeReader);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                budget, ticker, 1 << 20, Eviction.LRU, readers);
        ThreadContext context = new ThreadContext();
        loop.start();
        try {
            retireOne(loop, memory, budget, context);
            waitForEpoch(loop, 2L);
            long firstEpoch = loop.epoch();
            waitForRetiredEntries(loop, 1);

            retireOne(loop, memory, budget, context);
            waitForRetiredEntries(loop, 2);

            assertEquals(loop.epoch(), firstEpoch,
                    "a second record in the same actor time window must not force every reader to retry its epoch enter");
        } finally {
            activeReader.epoch = 0L;
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void ordinaryReaderQuiescenceUsesTheEpochDeadlineInsteadOfAnImmediateWake() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        Budget budget = new Budget(1 << 20);
        ReaderRegistry readers = new ReaderRegistry();
        ReaderSlot reader = new ReaderSlot();
        reader.epoch = 1L;
        readers.register(reader);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
        ThreadContext context = new ThreadContext();
        loop.start();
        try {
            retireOne(loop, memory, budget, context);
            waitForRetiredEntries(loop, 1);
            long wakeSignals = loop.snapshot().wakeSignals;

            reader.epoch = 0L;
            loop.readerQuiescent();

            assertEquals(loop.snapshot().wakeSignals, wakeSignals,
                    "normal QSBR reclaim waits for the bounded epoch deadline instead of signalling every reader exit");
            waitForNoRetiredEntries(loop);
        } finally {
            loop.stop();
            loop.join(1_000L);
            memory.closeArenas();
        }
    }

    private static void retireOne(MaintenanceEventLoop loop, NativeMemory.Memory memory, Budget budget,
                                  ThreadContext context) {
        long allocation = ValueBlock.allocationLength(1);
        long value = memory.newWriterArena().allocate(allocation);
        ValueBlock.initialize(value, 0L, 1);
        assertTrue(budget.reserve(com.red.ohc.storage.WriterArena.allocationWeight(allocation)));
        assertTrue(loop.prepareRetirement(context, 1));
        loop.retireValue(context, value, allocation);
        loop.afterWrite();
    }

    private static void waitForEpoch(MaintenanceEventLoop loop, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + 1_000_000_000L;
        while (loop.epoch() < expected && System.nanoTime() < deadline) Thread.yield();
        assertTrue(loop.epoch() >= expected, "maintenance actor did not seal retirement");
    }

    private static void waitForRetiredEntries(MaintenanceEventLoop loop, int expected) {
        long deadline = System.nanoTime() + 1_000_000_000L;
        while (loop.retiredEntries() < expected && System.nanoTime() < deadline) Thread.yield();
        assertTrue(loop.retiredEntries() >= expected, "maintenance actor did not retain expected records");
    }

    private static void waitForNoRetiredEntries(MaintenanceEventLoop loop) {
        long deadline = System.nanoTime() + 1_000_000_000L;
        while (loop.retiredEntries() != 0 && System.nanoTime() < deadline) Thread.yield();
        assertEquals(loop.retiredEntries(), 0, "the bounded epoch deadline did not reclaim after reader quiescence");
    }

    private static void waitUntilParked(MaintenanceEventLoop loop) throws InterruptedException {
        long deadline = System.nanoTime() + 1_000_000_000L;
        while (!loop.isParked() && System.nanoTime() < deadline) Thread.sleep(1L);
        assertTrue(loop.isParked(), "maintenance actor did not park");
    }

    private static final class FrozenTicker implements Ticker {
        @Override public long nanos() { return 0L; }
        @Override public long currentTimeMillis() { return 0L; }
    }

    private static final class CountingTicker implements Ticker {
        final AtomicInteger calls = new AtomicInteger();

        @Override public long nanos() {
            calls.incrementAndGet();
            return System.nanoTime();
        }

        @Override public long currentTimeMillis() {
            calls.incrementAndGet();
            return System.currentTimeMillis();
        }
    }

    private static ConcurrentHashMap<Entry, Entry> index() {
        return new ConcurrentHashMap<>();
    }
}
