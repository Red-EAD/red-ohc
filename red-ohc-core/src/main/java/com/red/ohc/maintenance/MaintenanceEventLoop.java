package com.red.ohc.maintenance;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.MpscArrayQueue;

import com.red.ohc.Eviction;
import com.red.ohc.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.FlatConcurrentMap;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

/**
 * One parked maintenance event-loop for a cache. It owns policy/timer state and never decides
 * ordinary index visibility. Business threads publish those changes directly to the flat index.
 */
public final class MaintenanceEventLoop implements Runnable {
    private final FlatConcurrentMap data;
    private final NativeMemory.Memory memory;
    private final Budget budget;
    private final Ticker ticker;
    private final long capacity;
    private final AtomicLong liveBytes;
    private final int queueCapacity;
    private final MpscArrayQueue<Entry> queue;
    private final CopyOnWriteArrayList<ReaderSlot> readers;
    private final CopyOnWriteArrayList<RetirementLedger> ledgers = new CopyOnWriteArrayList<>();
    private final ThreadLocal<RetirementLedger> localLedger;
    private final EpochReclaimer reclaimer;
    private final TimerWheel wheel;
    private final MaintenancePolicy policy;
    private final Thread thread;
    private final AtomicReference<CompletableFuture<Void>> flushRequest = new AtomicReference<>();
    private final AtomicBoolean repairNeeded = new AtomicBoolean();
    private volatile boolean stopping;
    private volatile boolean parked;
    private volatile long epoch = 1L;
    private volatile long nowMillis;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong accessDropped = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejectedQueue = new AtomicLong();
    private final AtomicLong rejectedBudget = new AtomicLong();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong evicted = new AtomicLong();
    private final AtomicLong logicalExpired = new AtomicLong();
    private final AtomicLong physicalExpired = new AtomicLong();
    private final AtomicLong timeoutLagMillis = new AtomicLong();
    private final AtomicLong maintenanceLoopNanos = new AtomicLong();
    private final AtomicBoolean unhealthy = new AtomicBoolean();
    private Iterator<Entry> repairIterator;
    private boolean policyRepairDone;
    private boolean timerRepairDone;
    private boolean evictionBlocked;

    public MaintenanceEventLoop(FlatConcurrentMap data,
                                NativeMemory.Memory memory, Budget budget, Ticker ticker,
                                long capacity, Eviction eviction, int queueCapacity,
                                AtomicLong liveBytes, CopyOnWriteArrayList<ReaderSlot> readers) {
        this.data = data;
        this.memory = memory;
        this.budget = budget;
        this.ticker = ticker;
        this.capacity = capacity;
        this.liveBytes = liveBytes;
        this.localLedger = ThreadLocal.withInitial(() -> {
            RetirementLedger ledger = new RetirementLedger(this.memory, this.budget);
            this.ledgers.add(ledger);
            return ledger;
        });
        this.queueCapacity = normalizeQueueCapacity(queueCapacity);
        this.queue = new MpscArrayQueue<>(this.queueCapacity);
        this.policy = new MaintenancePolicy(eviction, liveBytes);
        this.readers = readers;
        this.reclaimer = new EpochReclaimer(memory);
        this.wheel = new TimerWheel(ticker.currentTimeMillis());
        this.nowMillis = ticker.currentTimeMillis();
        this.thread = new Thread(this, "red-ohc-maintenance-event-loop");
        this.thread.setDaemon(true);
    }

    public void start() { thread.start(); }

    public void stop() {
        stopping = true;
        LockSupport.unpark(thread);
    }

    public void join(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
    }

    public boolean isAlive() { return thread.isAlive(); }
    public boolean isParked() { return parked; }
    public long epoch() { return epoch; }
    public long nowMillis() { return nowMillis; }
    public Thread thread() { return thread; }
    public long queueDepth() { return queue.size(); }
    public long queueCapacity() { return queueCapacity; }
    public long retiredBytes() { return reclaimer.bytes(); }
    public int retiredEntries() { return reclaimer.entries(); }
    public long oldestRetireEpoch() { return reclaimer.oldestEpoch(); }
    public long timerBytes() { return wheel.bytes(); }
    public long sketchBytes() { return policy.sketchBytes(); }
    public long policyEvictions() { return policy.evictions(); }
    public long ledgerBytes() {
        long bytes = 0L;
        for (RetirementLedger ledger : ledgers) bytes += ledger.allocatedBytes();
        return bytes;
    }

    public void registerReader(ReaderSlot slot) {
        readers.addIfAbsent(slot);
    }

    public void recordHit() { hits.incrementAndGet(); }
    public void recordMiss() { misses.incrementAndGet(); }
    public void recordDropped() { accessDropped.incrementAndGet(); }
    public void recordAccepted() { accepted.incrementAndGet(); }
    public void recordBudgetRejected() { rejectedBudget.incrementAndGet(); }
    public void markUnhealthy() { unhealthy.set(true); }

    /** Enqueues only a coalesced Entry hint. A full queue leaves the index authoritative. */
    public boolean enqueue(Entry entry, int flags) {
        if (!entry.markPending(flags)) return true;
        if (!queue.offer(entry)) {
            rejectedQueue.incrementAndGet();
            repairNeeded.set(true);
            LockSupport.unpark(thread);
            return false;
        }
        LockSupport.unpark(thread);
        return true;
    }

    /** Appends to the calling thread's native ledger; it never allocates a Java event object. */
    public void retireValue(long address, long allocation) {
        try {
            localLedger.get().append(address, allocation);
        } catch (Throwable failure) {
            markUnhealthy();
            throw failure;
        }
        LockSupport.unpark(thread);
    }

    /** Reserves native ledger space before an index mutation publishes a replacement or removal. */
    public boolean prepareRetirement(int records) {
        try {
            localLedger.get().prepare(records);
            return true;
        } catch (Throwable failure) {
            markUnhealthy();
            return false;
        }
    }

    /** Control-plane barrier; callers share one pending barrier and never enter the hint queue. */
    public CompletableFuture<Void> flush() {
        for (;;) {
            CompletableFuture<Void> existing = flushRequest.get();
            if (existing != null) return existing;
            CompletableFuture<Void> created = new CompletableFuture<>();
            if (flushRequest.compareAndSet(null, created)) {
                LockSupport.unpark(thread);
                return created;
            }
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(hits.get(), misses.get(), accessDropped.get(), accepted.get(),
                rejectedQueue.get(), rejectedBudget.get(), applied.get(), evicted.get(),
                logicalExpired.get(), physicalExpired.get(), maintenanceLoopNanos.get(),
                timeoutLagMillis.get(),
                unhealthy.get(), queueDepth(), retiredEntries(), retiredBytes(), oldestRetireEpoch(),
                policyEvictions(), timerBytes(), sketchBytes(), ledgerBytes(), queueCapacity);
    }

    @Override
    public void run() {
        while (!stopping || hasWork()) {
            long loopStart = ticker.nanos();
            nowMillis = ticker.currentTimeMillis();
            int work = wheel.advance(nowMillis, this::expire);
            work += drainAccesses(4096);
            work += drainMutations(4096);
            work += drainLedgers(1024);
            if (repairNeeded.get()) work += repair(4096);
            work += reclaim(1024);
            evictionBlocked = false;
            if (!stopping) work += evictIfNeeded(64);
            completeFlushIfIdle();
            maintenanceLoopNanos.set(ticker.nanos() - loopStart);
            if ((work == 0 && !hasWork()) || (evictionBlocked && !hasWork())) {
                parkUntilWorkOrTimer(evictionBlocked);
            }
        }
        shutdownAndFree();
    }

    private boolean hasWork() {
        return !queue.isEmpty() || hasPendingLedger() || repairNeeded.get() || flushRequest.get() != null;
    }

    private boolean hasPendingLedger() {
        for (RetirementLedger ledger : ledgers) if (ledger.hasPending()) return true;
        return false;
    }

    private void parkUntilWorkOrTimer(boolean retrySoon) {
        parked = true;
        try {
            long delay = wheel.nextDelayNanos(nowMillis);
            if (retrySoon) delay = Math.min(delay, 1_000_000L);
            if (delay == Long.MAX_VALUE) LockSupport.park(this);
            else LockSupport.parkNanos(this, delay);
        } finally {
            parked = false;
        }
    }

    private void completeFlushIfIdle() {
        if (!queue.isEmpty() || hasPendingLedger() || repairNeeded.get()) return;
        CompletableFuture<Void> future = flushRequest.getAndSet(null);
        if (future != null) future.complete(null);
    }

    private int drainMutations(int limit) {
        int work = 0;
        while (work < limit) {
            Entry entry = queue.poll();
            if (entry == null) break;
            processEntry(entry);
            work++;
        }
        return work;
    }

    private void processEntry(Entry entry) {
        processEntry(entry, entry.takePending());
    }

    private void processEntry(Entry entry, int flags) {
        if ((flags & Entry.PENDING_REMOVE) != 0) {
            wheel.remove(entry);
            policy.remove(entry, false);
            return;
        }
        if ((flags & Entry.PENDING_UPDATE) != 0) applyEntry(entry);
    }

    private void applyEntry(Entry entry) {
        long taggedAddress = entry.valueAddress;
        long address = Entry.rawValueAddress(taggedAddress);
        if (address == 0L || !data.isCurrent(entry)) {
            wheel.remove(entry);
            policy.remove(entry, false);
            return;
        }
        policy.add(entry);
        if (Entry.hasTtl(taggedAddress)) {
            wheel.reschedule(entry, ValueBlock.expireAtMillis(address));
        } else {
            wheel.remove(entry);
        }
        applied.incrementAndGet();
    }

    private int drainLedgers(int limit) {
        int work = 0;
        for (RetirementLedger ledger : ledgers) {
            if (work >= limit) break;
            work += ledger.drainTo(reclaimer, ++epoch, limit - work);
        }
        return work;
    }

    private int drainAccesses(int limit) {
        int work = 0;
        for (ReaderSlot slot : readers) {
            hits.addAndGet(slot.publishedHits - slot.consumedHits);
            misses.addAndGet(slot.publishedMisses - slot.consumedMisses);
            accessDropped.addAndGet(slot.publishedAccessDropped - slot.consumedAccessDropped);
            slot.consumedHits = slot.publishedHits;
            slot.consumedMisses = slot.publishedMisses;
            slot.consumedAccessDropped = slot.publishedAccessDropped;
            while (work < limit && slot.access.poll((entry, generation) -> {
                if (entry.generation() == generation && data.isCurrent(entry)) policy.access(entry);
            })) work++;
            if (work >= limit) return work;
        }
        return work;
    }

    private int repair(int limit) {
        int work = 0;
        if (repairIterator == null) repairIterator = data.values().iterator();
        while (work < limit && repairIterator.hasNext()) {
            Entry entry = repairIterator.next();
            int flags = entry.takePending();
            if (flags != 0) processEntry(entry, flags);
            else if (entry.valueAddress != 0L && data.isCurrent(entry)) applyEntry(entry);
            work++;
        }
        if (repairIterator.hasNext()) return work;
        repairIterator = null;
        policyRepairDone = policy.repair(data, limit);
        timerRepairDone = wheel.repair(data, limit);
        if (policyRepairDone && timerRepairDone) {
            repairNeeded.set(false);
            policyRepairDone = false;
            timerRepairDone = false;
        }
        return work + 1;
    }

    private int reclaim(int limit) {
        long before = reclaimer.bytes();
        int reclaimed = reclaimer.reclaim(memory, readers, limit);
        budget.release(before - reclaimer.bytes());
        return reclaimed;
    }

    private void expire(Entry entry, long expectedGeneration, long expectedValueAddress) {
        long taggedAddress = entry.valueAddress;
        long address = Entry.rawValueAddress(taggedAddress);
        if (taggedAddress != expectedValueAddress || address == 0L || !Entry.hasTtl(taggedAddress)
                || !ValueBlock.expired(address, nowMillis)) return;
        logicalExpired.incrementAndGet();
        long expiry = ValueBlock.expireAtMillis(address);
        if (expiry > 0L) updateMax(timeoutLagMillis, Math.max(0L, nowMillis - expiry));
        if (removeFromMap(entry, false, expectedGeneration, expectedValueAddress)) {
            physicalExpired.incrementAndGet();
        } else if (entry.valueAddress == expectedValueAddress && data.isCurrent(entry)) {
            wheel.add(entry, ValueBlock.expireAtMillis(address));
        }
    }

    private int evictIfNeeded(int limit) {
        int work = 0;
        long target = capacity - capacity / 4L;
        while (work < limit && policy.usedBytes() > target) {
            Entry victim = policy.victim();
            if (victim == null) break;
            long expectedGeneration = victim.generation();
            long expectedValueAddress = victim.valueAddress;
            if (!removeFromMap(victim, true, expectedGeneration, expectedValueAddress)
                    && (victim.valueAddress == 0L || !data.isCurrent(victim))) {
                policy.remove(victim, false);
            } else if (victim.valueAddress != 0L && data.isCurrent(victim)) {
                evictionBlocked = true;
                break;
            }
            work++;
        }
        return work;
    }

    public boolean removeFromMap(Entry entry, boolean eviction,
                                 long expectedGeneration, long expectedValueAddress) {
        if (expectedValueAddress == 0L) return false;
        while (!entry.claimWriter()) LockSupport.parkNanos(this, 1_000L);
        boolean removed = false;
        long value = 0L;
        try {
            if (entry.generation() != expectedGeneration || entry.valueAddress != expectedValueAddress) return false;
            if (!prepareRetirement(2)) {
                rejectedBudget.incrementAndGet();
                return false;
            }
            removed = data.removeIfSame(entry);
            if (removed) {
                value = Entry.rawValueAddress(entry.valueAddress);
                entry.valueAddress = 0L;
            }
        } finally {
            entry.finishWriter();
        }
        if (!removed) return false;
        wheel.remove(entry);
        policy.remove(entry, eviction);
        retireEntryBlocks(entry, value);
        if (eviction) evicted.incrementAndGet();
        return true;
    }

    private void retireEntryBlocks(Entry entry, long value) {
        if (value != 0L) retireValue(value, ValueBlock.allocationLength(ValueBlock.length(value)));
        retireValue(entry.nativeKeyAddress, entry.keyAllocationLength());
    }

    private void shutdownAndFree() {
        for (;;) {
            boolean active = false;
            for (ReaderSlot slot : readers) {
                if (slot.epoch != 0L) { active = true; break; }
            }
            if (!active) break;
            LockSupport.parkNanos(this, 1_000_000L);
        }
        drainLedgers(Integer.MAX_VALUE);
        for (Entry entry : data.values()) {
            long value = Entry.rawValueAddress(entry.valueAddress);
            entry.valueAddress = 0L;
            if (value != 0L) memory.releaseEntry(value, ValueBlock.allocationLength(ValueBlock.length(value)));
            memory.releaseEntry(entry.nativeKeyAddress, entry.keyAllocationLength());
        }
        data.clear();
        reclaimer.freeAll(memory);
        for (RetirementLedger ledger : ledgers) ledger.freeAll();
        memory.closeArenas();
        budget.returnUnusedCredits();
        budget.clear();
    }

    private static int normalizeQueueCapacity(int requested) {
        int capacity = Math.max(2, requested);
        int normalized = 1;
        while (normalized < capacity && normalized < (1 << 30)) normalized <<= 1;
        return normalized;
    }

    private static void updateMax(AtomicLong target, long value) {
        for (;;) {
            long current = target.get();
            if (value <= current || target.compareAndSet(current, value)) return;
        }
    }

    public static final class Snapshot {
        public final long hits;
        public final long misses;
        public final long accessDropped;
        public final long accepted;
        public final long rejectedQueue;
        public final long rejectedBudget;
        public final long applied;
        public final long evicted;
        public final long logicalExpired;
        public final long physicalExpired;
        public final long maintenanceLoopNanos;
        public final long timeoutLagMillis;
        public final boolean unhealthy;
        public final long queueDepth;
        public final long retiredEntries;
        public final long retiredBytes;
        public final long oldestRetireEpoch;
        public final long policyEvictions;
        public final long timerBytes;
        public final long sketchBytes;
        public final long ledgerBytes;
        public final long queueCapacity;

        Snapshot(long hits, long misses, long accessDropped, long accepted, long rejectedQueue,
                 long rejectedBudget, long applied, long evicted, long logicalExpired,
                 long physicalExpired, long maintenanceLoopNanos, long timeoutLagMillis, boolean unhealthy, long queueDepth,
                 long retiredEntries, long retiredBytes, long oldestRetireEpoch, long policyEvictions,
                 long timerBytes, long sketchBytes, long ledgerBytes, long queueCapacity) {
            this.hits = hits;
            this.misses = misses;
            this.accessDropped = accessDropped;
            this.accepted = accepted;
            this.rejectedQueue = rejectedQueue;
            this.rejectedBudget = rejectedBudget;
            this.applied = applied;
            this.evicted = evicted;
            this.logicalExpired = logicalExpired;
            this.physicalExpired = physicalExpired;
            this.maintenanceLoopNanos = maintenanceLoopNanos;
            this.timeoutLagMillis = timeoutLagMillis;
            this.unhealthy = unhealthy;
            this.queueDepth = queueDepth;
            this.retiredEntries = retiredEntries;
            this.retiredBytes = retiredBytes;
            this.oldestRetireEpoch = oldestRetireEpoch;
            this.policyEvictions = policyEvictions;
            this.timerBytes = timerBytes;
            this.sketchBytes = sketchBytes;
            this.ledgerBytes = ledgerBytes;
            this.queueCapacity = queueCapacity;
        }
    }
}
