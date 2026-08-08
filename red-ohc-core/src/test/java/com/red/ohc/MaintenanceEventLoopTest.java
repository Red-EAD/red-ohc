package com.red.ohc;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.TimerWheel;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.runtime.ThreadContext;

import org.jctools.queues.MpscUnboundedXaddArrayQueue;

public class MaintenanceEventLoopTest {
    @Test
    public void acceptedWriteStatsUseAStripedCounter() throws Exception {
        Field accepted = MaintenanceEventLoop.class.getDeclaredField("accepted");
        assertEquals(accepted.getType(), LongAdder.class,
                "every successful put must not contend on one cache-global stats cache line");
    }

    @Test
    public void mutationTransportUsesTheUnboundedChunkedQueueAndNoRepairSideChannel() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            Field queue = MaintenanceEventLoop.class.getDeclaredField("queue");
            queue.setAccessible(true);
            assertTrue(queue.get(loop) instanceof MpscUnboundedXaddArrayQueue,
                    "dirty credits, rather than a bounded queue plus repair chain, must be the sole mutation backlog limit");
            for (Field field : MaintenanceEventLoop.class.getDeclaredFields()) {
                assertFalse(field.getName().contains("repair"),
                        "the unbounded mutation transport must not retain a producer-side repair side channel");
            }
        } finally {
            memory.closeArenas();
        }
    }

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
    public void unboundedTransportAcceptsVisibleHintsWithoutRepair() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        Entry[] entries = new Entry[4];
        try {
            for (int i = 0; i < entries.length; i++) {
                entries[i] = new Entry(0L, 0, i + 1, 0L);
                assertTrue(loop.reserveMutation(entries[i], Entry.PENDING_ADD));
                loop.publishMutation(entries[i]);
                loop.afterWrite();
            }
            assertEquals(loop.queueDepth(), 4L);
            assertEquals(loop.snapshot().rejectedQueue, 0L,
                    "a visible mutation must not need a producer-side repair transport");

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
        ThreadContext context = new ThreadContext(null, null);
        readers.register(context.slot);
        context.bindMaintenance(loop);
        loop.start();
        try {
            Entry entry = new Entry(0L, 0, 91, 0L);
            for (int i = 0; i < 1_024; i++) {
                long sequence = context.hit();
                context.access(entry);
                context.finishRead(sequence);
            }
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
            int limit = (int) loop.queueCapacity();
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
            int cpuPowerOfTwo = 1;
            int processors = Runtime.getRuntime().availableProcessors();
            while (cpuPowerOfTwo < processors) cpuPowerOfTwo <<= 1;
            assertEquals(loop.queueCapacity(), 128L * cpuPowerOfTwo,
                    "the fixed chunk transport must be bounded only by the CPU-scaled dirty credit budget");
        } finally {
            memory.closeArenas();
        }
    }

    @Test
    public void actorDefersQueuedMutationUntilTheEntryWriterPublishes() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        Entry entry = new Entry(0L, 0, 77, 0L);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(index(), memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
            loop.publishMutation(entry);
            // This is the deterministic version of the interleaving: the actor has taken the
            // old transport item while a later writer has claimed the Entry but not yet made
            // its new pointer visible.
            assertTrue(entry.claimWriter());
            assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
            invokeProcessEntry(loop, entry);

            assertTrue((entry.pendingFlags & Entry.PENDING_UPDATE) != 0,
                    "the actor must retain the pending event while a writer owns the Entry");
        } finally {
            if ((entry.lifecycle & Entry.WRITER_LOCK) != 0L) entry.finishWriter();
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void deferredMutationAppliesThePointerPublishedByTheLaterWriter() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        ConcurrentHashMap<Entry, Entry> data = index();
        long allocation = ValueBlock.allocationLength(1);
        long value = memory.newWriterArena().allocate(allocation);
        ValueBlock.initialize(value, System.currentTimeMillis() + 60_000L, 1);
        Entry entry = new Entry(0L, 0, 78, 0L);
        data.put(entry, entry);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(data, memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        try {
            assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
            loop.publishMutation(entry);
            assertTrue(entry.claimWriter());
            assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
            invokeProcessEntry(loop, entry);

            entry.valueAddress = Entry.tagValueAddress(value, true);
            entry.finishWriter();
            loop.publishMutation(entry);
            loop.start();
            loop.flush().join();

            assertEquals(loop.snapshot().applied, 1L);
            assertEquals(loop.snapshot().ttlBacklog, 1L,
                    "the actor must schedule the later writer's deadline, not clear its merged update");
        } finally {
            loop.stop();
            loop.join(1_000L);
            if (loop.isAlive()) {
                data.clear();
                memory.releaseEntry(value, allocation);
            }
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void actorCannotClearACoalescedMutationBeforeTheConcurrentWriterPublishes() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        ConcurrentHashMap<Entry, Entry> data = index();
        long allocation = ValueBlock.allocationLength(1);
        long value = memory.newWriterArena().allocate(allocation);
        ValueBlock.initialize(value, System.currentTimeMillis() + 60_000L, 1);
        Entry entry = new Entry(0L, 0, 79, 0L);
        data.put(entry, entry);
        MaintenanceEventLoop loop = new MaintenanceEventLoop(data, memory,
                new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
                new ReaderRegistry());
        CountDownLatch writerClaimed = new CountDownLatch(1);
        CountDownLatch allowPublication = new CountDownLatch(1);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                assertTrue(entry.claimWriter());
                assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
                writerClaimed.countDown();
                assertTrue(allowPublication.await(1L, TimeUnit.SECONDS));
                entry.valueAddress = Entry.tagValueAddress(value, true);
                entry.finishWriter();
                loop.publishMutation(entry);
            } catch (Throwable failure) {
                writerFailure.set(failure);
            }
        }, "pending-publication-writer");
        try {
            assertTrue(loop.reserveMutation(entry, Entry.PENDING_UPDATE));
            loop.publishMutation(entry);
            writer.start();
            assertTrue(writerClaimed.await(1L, TimeUnit.SECONDS));

            // This invocation represents the actor consuming the old queued transport item while
            // the second writer owns the pending claim but has not published its pointer yet.
            invokeProcessEntry(loop, entry);
            assertTrue((entry.pendingFlags & Entry.PENDING_UPDATE) != 0,
                    "the old queue item must defer instead of clearing the later writer's update");

            allowPublication.countDown();
            writer.join(1_000L);
            assertFalse(writer.isAlive(), "the writer did not publish its protected update");
            assertEquals(writerFailure.get(), null);
            loop.start();
            loop.flush().join();

            assertEquals(loop.snapshot().ttlBacklog, 1L,
                    "the actor must apply the TTL from the later writer, not the stale pointer");
        } finally {
            allowPublication.countDown();
            writer.join(1_000L);
            loop.stop();
            loop.join(1_000L);
            if (loop.isAlive()) {
                data.clear();
                memory.releaseEntry(value, allocation);
            }
            memory.closeArenas();
        }
    }

    @Test(timeOut = 2_000L)
    public void evictionRetriesAfterAWriterLockIsReleasedWithoutAnotherExternalWrite() throws Exception {
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        long allocation = ValueBlock.allocationLength(1);
        long keyAllocation = 8L;
        com.red.ohc.storage.WriterArena arena = memory.newWriterArena();
        long keyOne = arena.allocate(keyAllocation);
        long keyTwo = arena.allocate(keyAllocation);
        long valueOne = arena.allocate(allocation);
        long valueTwo = arena.allocate(allocation);
        ValueBlock.initialize(valueOne, 0L, 1);
        ValueBlock.initialize(valueTwo, 0L, 1);
        NativeMemory.putLong(keyOne, 101L);
        NativeMemory.putLong(keyTwo, 102L);
        Entry one = new Entry(keyOne, 0, 101, valueOne);
        Entry two = new Entry(keyTwo, 0, 102, valueTwo);
        ConcurrentHashMap<Entry, Entry> data = index();
        data.put(one, one);
        data.put(two, two);
        long weight = com.red.ohc.storage.WriterArena.allocationWeight(allocation)
                + com.red.ohc.storage.WriterArena.allocationWeight(one.keyAllocationLength());
        Budget budget = new Budget(1 << 20);
        assertTrue(budget.reserve(budget.stripeForCurrentThread(), weight * 2L));
        MaintenanceEventLoop loop = new MaintenanceEventLoop(data, memory, budget,
                Ticker.DEFAULT, weight + 1L, Eviction.S3_FIFO, new ReaderRegistry());
        try {
            invokeApplyEntry(loop, one);
            invokeApplyEntry(loop, two);
            assertTrue(one.claimWriter());
            assertTrue(two.claimWriter());
            loop.start();
            loop.flush().join();

            one.finishWriter();
            two.finishWriter();
            long deadline = System.nanoTime() + 1_000_000_000L;
            while (data.size() > 1 && System.nanoTime() < deadline) Thread.yield();
            assertTrue(data.size() <= 1,
                    "a deferred eviction must re-arm itself after the writer mutex becomes available");
        } finally {
            loop.stop();
            loop.join(1_000L);
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
        ThreadContext context = new ThreadContext(null, null);
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
        ThreadContext context = new ThreadContext(null, null);
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
        assertTrue(budget.reserve(budget.stripeForCurrentThread(),
                com.red.ohc.storage.WriterArena.allocationWeight(allocation)));
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

    private static void invokeProcessEntry(MaintenanceEventLoop loop, Entry entry) throws Exception {
        Method method = MaintenanceEventLoop.class.getDeclaredMethod("processEntry", Entry.class);
        method.setAccessible(true);
        method.invoke(loop, entry);
    }

    private static void invokeApplyEntry(MaintenanceEventLoop loop, Entry entry) throws Exception {
        Method method = MaintenanceEventLoop.class.getDeclaredMethod("applyEntry", Entry.class);
        method.setAccessible(true);
        method.invoke(loop, entry);
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
