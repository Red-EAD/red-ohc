package com.red.ohc.maintenance;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.MpscArrayQueue;

import com.red.ohc.Eviction;
import com.red.ohc.Ticker;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/**
 * One parked maintenance event-loop for a cache. It owns policy/timer state and never decides
 * ordinary index visibility. Business threads publish those changes directly to the CHM.
 */
public final class MaintenanceEventLoop implements Runnable, TimerWheel.TimerConsumer, AccessConsumer {
    private static final long EPOCH_ADVANCE_INTERVAL_NANOS = 1_000_000L;
    /**
     * After draining a producer burst, stay in REQUIRED and sleep briefly before returning to an
     * infinite idle park. This batches isolated retirement-only replacements without a polling
     * loop: an actually idle actor still parks forever, while a busy producer pays at most one
     * idle-to-required unpark per burst.
     */
    private static final long WRITE_BATCH_GRACE_NANOS = 1_000_000L;
    /**
     * The timer owns physical cleanup only; reads perform the strict TTL check. Sampling the
     * actor clock once per bounded group of passes therefore preserves safety while avoiding a
     * native wall/monotonic-clock trip for every tiny retirement batch.
     */
    private static final int CLOCK_SAMPLE_INTERVAL_PASSES = 16;
    private final ConcurrentHashMap<Entry, Entry> data;
    private final NativeMemory.Memory memory;
    private final Budget budget;
    private final Ticker ticker;
    private final long capacity;
    private final int queueLimit;
    private final int dirtyLimit;
    private final AtomicInteger dirtyEventCount = new AtomicInteger();
    private final MpscArrayQueue<Entry> queue;
    private final AtomicReference<Entry> repairHead = new AtomicReference<>();
    private final AtomicInteger repairCount = new AtomicInteger();
    private final AtomicBoolean accessHint = new AtomicBoolean();
    private final ReaderRegistry readers;
    private final RetirementQueue retirements;
    private final RetirementQueue.Reservation actorRetirement = new RetirementQueue.Reservation();
    private final TimerWheel wheel;
    private final MaintenancePolicy policy;
    private final Thread thread;
    private final WakeGate wakeGate = new WakeGate();
    private final AtomicReference<CompletableFuture<Void>> flushRequest = new AtomicReference<>();
    private volatile boolean stopping;
    private volatile boolean parked;
    /** Incremented before the actor's final empty-source check for an idle park. */
    private volatile long idleGeneration;
    private volatile long epoch = 1L;
    /** Actor-owned epoch of the newest sealed retirement record. */
    private long latestRetireEpoch;
    /** Actor-owned timestamp of the most recent epoch advance. */
    private long lastEpochAdvanceNanos;
    private boolean hasAdvancedEpoch;
    private volatile long nowMillis;
    /** A flush asks the actor to observe the current timer clock before completing the pass. */
    private volatile boolean clockRefreshRequested;
    /**
     * Cross-thread snapshot of the actor-owned policy weight. Writers never update this value:
     * it is deliberately published only after a bounded maintenance pass has applied all of its
     * policy mutations.  Reading {@link MaintenancePolicy#usedBytes()} from a cache caller
     * would otherwise be a data race with the actor.
     */
    private volatile long publishedLiveWeight;
    private long nowNanos;
    private int clockSampleCountdown;
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
    private final AtomicLong evictionScans = new AtomicLong();
    private final AtomicLong evictionLockedSkips = new AtomicLong();
    /** Updated only on the rare idle-to-required wake transition, never on a merged put. */
    private final AtomicLong wakeUnparks = new AtomicLong();
    private final AtomicBoolean unhealthy = new AtomicBoolean();
    /** An over-capacity policy whose remaining candidates are writer-locked must park, not spin. */
    private boolean evictionDeferred;
    /** A sealed retirement could not pass QSBR; recheck it on the bounded epoch deadline. */
    private boolean reclaimBlocked;
    /** Actor-owned marker: publish the policy weight only after a policy mutation. */
    private boolean policyDirty;

    public MaintenanceEventLoop(ConcurrentHashMap<Entry, Entry> data,
                                NativeMemory.Memory memory, Budget budget, Ticker ticker,
                                long capacity, Eviction eviction, ReaderRegistry readers) {
        this(data, memory, budget, ticker, capacity, eviction, readers,
                ChmSizing.maintenanceQueueCapacity(0L, capacity, 1L));
    }

    public MaintenanceEventLoop(ConcurrentHashMap<Entry, Entry> data,
                                NativeMemory.Memory memory, Budget budget, Ticker ticker,
                                long capacity, Eviction eviction, ReaderRegistry readers,
                                int queueCapacity) {
        if (Integer.bitCount(queueCapacity) != 1 || queueCapacity < 2) {
            throw new IllegalArgumentException("queueCapacity must be a power of two >= 2");
        }
        this.data = data;
        this.memory = memory;
        this.budget = budget;
        this.ticker = ticker;
        this.capacity = capacity;
        this.queueLimit = queueCapacity;
        this.dirtyLimit = dirtyLimit(queueCapacity);
        this.queue = new MpscArrayQueue<>(queueCapacity);
        this.policy = new MaintenancePolicy(eviction, capacity);
        this.readers = readers;
        this.retirements = new RetirementQueue(memory, stripeCount(), retirementRecordsPerStripe());
        this.wheel = new TimerWheel(ticker.currentTimeMillis());
        this.nowMillis = ticker.currentTimeMillis();
        this.thread = new Thread(this, "red-ohc-maintenance-event-loop");
        this.thread.setDaemon(true);
    }

    public void start() { thread.start(); }

    public void stop() {
        stopping = true;
        signal();
        // A close must not wait for the bounded producer-batch grace period.
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
    public long queueDepth() { return (long) queue.size() + repairCount.get(); }
    public long queueCapacity() { return queueLimit; }
    public long retiredBytes() { return retirements.retiredBytes(); }
    public int retiredEntries() { return retirements.retiredEntries(); }
    public long oldestRetireEpoch() { return retirements.oldestEpoch(); }
    public long timerBytes() { return wheel.bytes(); }
    public long sketchBytes() { return policy.sketchBytes(); }
    public long ghostHeapBytes() { return policy.ghostHeapBytes(); }
    public long ttlBacklog() { return wheel.scheduled(); }
    public long policyEvictions() { return policy.evictions(); }
    public long ledgerBytes() {
        return retirements.allocatedBytes();
    }

    public void registerReader(ReaderSlot slot) {
        readers.register(slot);
    }

    /** Reader-side access publication is the only reader activity that needs to wake the actor. */
    public void signalAccess(ReaderSlot slot) {
        accessHint.set(true);
        signal();
    }

    /** ReaderGuard calls this only after an active read becomes quiescent during close. */
    public void readerQuiescent() {
        if (stopping) {
            // shutdownAndFree parks outside WakeGate, so this must not rely on a REQUIRED gate
            // transition to wake the actor after its final reader leaves.
            LockSupport.unpark(thread);
        }
    }

    public void recordHit() { hits.incrementAndGet(); }
    public void recordMiss() { misses.incrementAndGet(); }
    public void recordDropped() { accessDropped.incrementAndGet(); }
    public void recordAccepted() { accepted.incrementAndGet(); }
    public void recordBudgetRejected() { rejectedBudget.incrementAndGet(); }
    public void markUnhealthy() { unhealthy.set(true); }

    /**
     * Reserves the one dirty-event credit before callers publish a CHM or value-pointer change.
     * Subsequent mutations of an already pending Entry coalesce without consuming another credit.
     */
    public boolean reserveMutation(Entry entry, int flags) {
        if (unhealthy.get()) return false;
        if (!entry.beginPending(flags)) return true;
        if (!tryAcquireDirtyCredit()) {
            entry.cancelPendingClaim();
            rejectedQueue.incrementAndGet();
            return false;
        }
        entry.commitPendingClaim(flags);
        return true;
    }

    /** Publishes a reservation after the associated CHM/value mutation is visible. */
    public void publishMutation(Entry entry) {
        if (!entry.publishPending()) return;
        if (queue.offer(entry)) return;
        if (entry.moveToRepair()) {
            pushRepair(entry);
        }
    }

    /** Cancels a pre-publication reservation and returns its unique dirty credit. */
    public void cancelMutation(Entry entry) {
        if (entry.cancelPending()) releaseDirtyCredit();
    }

    /** Publishes a pre-reserved block to the cache-owned native retirement transport. */
    public void retireValue(com.red.ohc.runtime.ThreadContext context, long address, long allocation) {
        retirements.append(context.retirement, address, allocation);
    }

    /**
     * The producer calls this exactly once after publishing every CHM/pointer mutation and its
     * retirement records. Keeping the wake at this tail avoids a second WakeGate CAS for a
     * replacement that changes both actor state and the retirement FIFO.
     */
    public void afterWrite(com.red.ohc.runtime.ThreadContext context) {
        if (context.needsMaintenanceWake(idleGeneration)) signal();
    }

    /** Low-frequency callers without a cache ThreadContext always request a wake. */
    public void afterWrite() {
        signal();
    }

    /** Reserves native retirement records before an index mutation publishes a replacement or removal. */
    public boolean prepareRetirement(com.red.ohc.runtime.ThreadContext context, int records) {
        return retirements.reserve(context.retirement, records);
    }

    public void cancelRetirement(com.red.ohc.runtime.ThreadContext context) {
        retirements.cancel(context.retirement);
    }

    /** Control-plane barrier; callers share one pending barrier and never enter the hint queue. */
    public CompletableFuture<Void> flush() {
        for (;;) {
            CompletableFuture<Void> existing = flushRequest.get();
            if (existing != null) return existing;
            CompletableFuture<Void> created = new CompletableFuture<>();
            if (flushRequest.compareAndSet(null, created)) {
                clockRefreshRequested = true;
                signal();
                return created;
            }
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(hits.get(), misses.get(), accessDropped.get(), accepted.get(),
                rejectedQueue.get(), rejectedBudget.get(), applied.get(), evicted.get(),
                logicalExpired.get(), physicalExpired.get(), maintenanceLoopNanos.get(), publishedLiveWeight,
                timeoutLagMillis.get(),
                unhealthy.get(), queueDepth(), retiredEntries(), retiredBytes(), oldestRetireEpoch(),
                policyEvictions(), evictionScans.get(), evictionLockedSkips.get(),
                timerBytes(), ttlBacklog(), sketchBytes(), ghostHeapBytes(), ledgerBytes(), queueLimit,
                wakeUnparks.get(), wakeGate.mergedTransitions());
    }

    @Override
    public void run() {
        wakeGate.requireProcessing();
        while (!stopping || hasWork()) {
            if (!stopping && !hasSourceWork() && !retirements.hasPendingReclaim()) {
                parkUntilWorkOrTimer(false);
                continue;
            }

            boolean needsClock = clockRefreshRequested || wheel.hasPending()
                    || retirements.hasPendingReclaim();
            boolean sampledClock = needsClock && sampleClockIfDue();
            int work = wheel.hasPending() ? wheel.advance(nowMillis, 1_000, this) : 0;
            if (!queue.isEmpty()) work += drainMutations(4096);
            if (repairHead.get() != null) work += drainRepairs(4096);
            if (accessHint.get()) work += drainAccesses(4096);
            if (retirements.hasReadyHint()) work += sealRetirements(1024);
            if (advanceEpochIfDue(nowNanos)) work++;
            if (retirements.hasPendingReclaim()) work += reclaim(1024);
            evictionDeferred = false;
            if (!stopping) work += evictIfNeeded(64);
            publishLiveWeight();
            completeFlushIfIdle();
            if (sampledClock) maintenanceLoopNanos.set(ticker.nanos() - nowNanos);
            if (!hasSourceWork()) {
                parkUntilWorkOrTimer(work != 0 && !stopping);
            }
        }
        shutdownAndFree();
    }

    private void publishLiveWeight() {
        if (!policyDirty) return;
        long weight = policy.usedBytes();
        if (publishedLiveWeight != weight) publishedLiveWeight = weight;
        policyDirty = false;
    }

    private boolean hasWork() {
        return hasSourceWork() || retirements.hasPendingReclaim();
    }

    private boolean hasSourceWork() {
        return !queue.isEmpty() || repairHead.get() != null || accessHint.get()
                || flushRequest.get() != null || retirements.hasReadyHint()
                || clockRefreshRequested;
    }

    private void parkUntilWorkOrTimer(boolean batchGrace) {
        if (batchGrace) {
            parked = true;
            try {
                long timerDelay = wheel.nextDelayNanos(nowMillis);
                LockSupport.parkNanos(this, Math.min(WRITE_BATCH_GRACE_NANOS, timerDelay));
                if (timerDelay != Long.MAX_VALUE) clockRefreshRequested = true;
            } finally {
                parked = false;
            }
            return;
        }
        if (!wakeGate.armIdle()) return;
        // Publish the new generation before the final source check. A producer that races after
        // this point observes it and changes PROCESSING_TO_IDLE to PROCESSING_TO_REQUIRED;
        // a producer before it is still covered by hasWork().
        idleGeneration++;
        if (stopping || hasSourceWork()) {
            wakeGate.requireProcessing();
            return;
        }
        // A producer that published before idleGeneration changed is caught by this final
        // actor-owned scan. A later producer observes the new generation and changes the gate
        // to PROCESSING_TO_REQUIRED, so neither side can strand a completed retirement record.
        if (sealRetirements(1024) != 0) {
            wakeGate.requireProcessing();
            return;
        }
        if (!wakeGate.finishIdle()) return;
        parked = true;
        try {
            long delay = wheel.nextDelayNanos(nowMillis);
            if (retirements.hasPendingReclaim()) {
                delay = Math.min(delay, epochAdvanceDelayNanos(ticker.nanos()));
            }
            if (delay == Long.MAX_VALUE) LockSupport.park(this);
            else {
                LockSupport.parkNanos(this, delay);
                // The cached actor clock is only a throughput optimisation. Once a timed
                // deadline wakes the actor, it must be refreshed before calculating the next
                // wheel advance; otherwise the same deadline can be slept repeatedly.
                clockRefreshRequested = true;
            }
        } finally {
            parked = false;
            wakeGate.requireProcessing();
        }
    }

    private void completeFlushIfIdle() {
        CompletableFuture<Void> future = flushRequest.get();
        if (future == null) return;
        if (clockRefreshRequested) return;
        if (!queue.isEmpty() || repairHead.get() != null || accessHint.get()
                || retirements.hasReadyHint()) return;
        if (flushRequest.compareAndSet(future, null)) future.complete(null);
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

    private int drainRepairs(int limit) {
        Entry current = repairHead.getAndSet(null);
        int work = 0;
        while (current != null && work < limit) {
            Entry next = current.repairNext;
            current.repairNext = null;
            repairCount.decrementAndGet();
            processEntry(current);
            current = next;
            work++;
        }
        if (current != null) pushRepairChain(current);
        return work;
    }

    private void processEntry(Entry entry) {
        int flags = entry.takePending();
        if (flags == 0) return;
        releaseDirtyCredit();
        processEntry(entry, flags);
    }

    private void processEntry(Entry entry, int flags) {
        if ((flags & Entry.PENDING_REMOVE) != 0) {
            wheel.remove(entry);
            policy.remove(entry, false);
            policyDirty = true;
            return;
        }
        if ((flags & (Entry.PENDING_ADD | Entry.PENDING_UPDATE)) != 0) applyEntry(entry);
    }

    private void applyEntry(Entry entry) {
        long taggedAddress = entry.valueAddress;
        long address = Entry.rawValueAddress(taggedAddress);
        if (address == 0L || !isCurrent(entry)) {
            wheel.remove(entry);
            policy.remove(entry, false);
            policyDirty = true;
            return;
        }
        policy.add(entry);
        policyDirty = true;
        if (Entry.hasTtl(taggedAddress)) {
            wheel.reschedule(entry, ValueBlock.expireAtMillis(address));
        } else {
            wheel.remove(entry);
        }
        applied.incrementAndGet();
    }

    private int sealRetirements(int limit) {
        if (!retirements.consumeReadyHint()) return 0;
        int sealed = retirements.seal(limit, epoch);
        // The signal is coalesced. If the bounded pass consumed its complete budget, schedule
        // another pass rather than re-scanning every native retirement stripe unconditionally.
        if (sealed == limit) retirements.requestSeal();
        if (sealed != 0) latestRetireEpoch = epoch;
        return sealed;
    }

    /**
     * Retirements are first marked with the current epoch and only then move readers forward.
     * The cadence bounds the retry window in {@code ReaderGuard.enter()} without letting a new
     * reader bypass an address that was made unreachable in its own epoch.
     */
    private boolean advanceEpochIfDue(long nowNanos) {
        if (!retirements.hasPendingReclaim() || latestRetireEpoch < epoch) return false;
        if (hasAdvancedEpoch && nowNanos - lastEpochAdvanceNanos < EPOCH_ADVANCE_INTERVAL_NANOS) return false;
        epoch++;
        lastEpochAdvanceNanos = nowNanos;
        hasAdvancedEpoch = true;
        return true;
    }

    private boolean sampleClockIfDue() {
        if (!clockRefreshRequested && clockSampleCountdown-- > 0) return false;
        clockRefreshRequested = false;
        nowNanos = ticker.nanos();
        nowMillis = ticker.currentTimeMillis();
        clockSampleCountdown = CLOCK_SAMPLE_INTERVAL_PASSES - 1;
        return true;
    }

    /** A QSBR epoch deadline is real work; an idle cache with no retirements still parks forever. */
    private long epochAdvanceDelayNanos(long nowNanos) {
        if (!retirements.hasPendingReclaim()) return Long.MAX_VALUE;
        if (latestRetireEpoch < epoch) {
            return reclaimBlocked ? EPOCH_ADVANCE_INTERVAL_NANOS : Long.MAX_VALUE;
        }
        if (!hasAdvancedEpoch) return 0L;
        long elapsed = nowNanos - lastEpochAdvanceNanos;
        return elapsed >= EPOCH_ADVANCE_INTERVAL_NANOS ? 0L : EPOCH_ADVANCE_INTERVAL_NANOS - elapsed;
    }

    private int drainAccesses(int limit) {
        if (!accessHint.getAndSet(false)) return 0;
        int work = 0;
        for (ReaderSlot slot : readers.snapshot()) {
            if (!slot.accessPending && slot.access.isEmpty()) continue;
            // Clear before reading the counters/ring. A producer racing after this point either
            // publishes data into this scan or leaves the hint set for the next pass.
            slot.accessPending = false;
            long hitDelta = slot.publishedHits - slot.consumedHits;
            long missDelta = slot.publishedMisses - slot.consumedMisses;
            long droppedDelta = slot.publishedAccessDropped - slot.consumedAccessDropped;
            if (hitDelta != 0L) {
                hits.addAndGet(hitDelta);
                policy.recordHits(hitDelta);
                slot.consumedHits += hitDelta;
            }
            if (missDelta != 0L) {
                misses.addAndGet(missDelta);
                policy.recordMisses(missDelta);
                slot.consumedMisses += missDelta;
            }
            if (droppedDelta != 0L) {
                accessDropped.addAndGet(droppedDelta);
                slot.consumedAccessDropped += droppedDelta;
            }
            while (work < limit && slot.access.poll(this)) work++;
            if (work >= limit) {
                // The original hint may cover more than one bounded batch. Keep the hint live
                // until the ring is actually empty instead of relying on a future read to wake us.
                if (!slot.access.isEmpty()) {
                    slot.accessPending = true;
                    accessHint.set(true);
                }
                return work;
            }
        }
        return work;
    }

    @Override
    public void accept(Entry entry, long generation) {
        if (entry.generation() == generation && isCurrent(entry)) policy.access(entry);
    }

    private int reclaim(int limit) {
        long before = retirements.retiredBytes();
        int reclaimed = retirements.reclaim(readers, limit);
        budget.release(before - retirements.retiredBytes());
        // An active reader cannot be observed through a write-side queue event. Keep a bounded
        // QSBR deadline while it remains active so its ordinary exit need not perform a WakeGate
        // CAS on every cache operation. This is deadline-driven reclaim work, not idle polling.
        reclaimBlocked = reclaimed == 0 && retirements.hasPendingReclaim();
        return reclaimed;
    }

    @Override
    public void expire(Entry entry, long expectedGeneration, long expectedValueAddress) {
        long taggedAddress = entry.valueAddress;
        long address = Entry.rawValueAddress(taggedAddress);
        if (taggedAddress != expectedValueAddress || address == 0L || !Entry.hasTtl(taggedAddress)
                || !ValueBlock.expired(address, nowMillis)) return;
        logicalExpired.incrementAndGet();
        long expiry = ValueBlock.expireAtMillis(address);
        if (expiry > 0L) updateMax(timeoutLagMillis, Math.max(0L, nowMillis - expiry));
        if (removeFromMap(entry, false, expectedGeneration, expectedValueAddress)) {
            physicalExpired.incrementAndGet();
        } else if (entry.valueAddress == expectedValueAddress && isCurrent(entry)) {
            wheel.add(entry, ValueBlock.expireAtMillis(address));
        }
    }

    private int evictIfNeeded(int limit) {
        int work = 0;
        int scans = 0;
        long target = capacity;
        while (work < limit && scans < limit && policy.usedBytes() > target) {
            Entry victim = policy.victim(limit - scans);
            int selectionScans = policy.lastVictimScanCount();
            scans += selectionScans;
            if (victim == null) {
                evictionDeferred = policy.usedBytes() > target;
                break;
            }
            long expectedGeneration = victim.generation();
            long expectedValueAddress = victim.valueAddress;
            if (!removeFromMap(victim, true, expectedGeneration, expectedValueAddress)
                    && (victim.valueAddress == 0L || !isCurrent(victim))) {
                policy.remove(victim, false);
                policyDirty = true;
                work++;
            } else if (victim.valueAddress != 0L && isCurrent(victim)) {
                policy.skipLocked(victim);
                evictionLockedSkips.incrementAndGet();
            } else {
                work++;
            }
        }
        if (scans != 0) evictionScans.addAndGet(scans);
        if (policy.usedBytes() > target && work == 0) evictionDeferred = true;
        return work;
    }

    public boolean removeFromMap(Entry entry, boolean eviction,
                                 long expectedGeneration, long expectedValueAddress) {
        if (expectedValueAddress == 0L) return false;
        if (!entry.claimWriter()) return false;
        boolean removed = false;
        long value = 0L;
        try {
            if (entry.generation() != expectedGeneration || entry.valueAddress != expectedValueAddress) return false;
            if (!prepareActorRetirement(2)) {
                rejectedBudget.incrementAndGet();
                return false;
            }
            removed = removeCurrent(entry);
            if (removed) {
                value = Entry.rawValueAddress(entry.valueAddress);
                entry.valueAddress = 0L;
            }
        } finally {
            entry.finishWriter();
        }
        if (!removed) {
            retirements.cancel(actorRetirement);
            return false;
        }
        wheel.remove(entry);
        policy.remove(entry, eviction);
        policyDirty = true;
        retireEntryBlocks(entry, value);
        if (eviction) evicted.incrementAndGet();
        return true;
    }

    private void retireEntryBlocks(Entry entry, long value) {
        long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
        retireActorValue(value, valueAllocation);
        long keyAllocation = entry.keyAllocationLength();
        retireActorValue(entry.nativeKeyAddress, keyAllocation);
    }

    private boolean isCurrent(Entry entry) {
        return entry.isAlive();
    }

    private boolean removeCurrent(Entry entry) {
        if (!entry.isAlive()) return false;
        entry.markRetired();
        if (data.remove(entry, entry)) return true;
        entry.restoreAlive();
        return false;
    }

    private void shutdownAndFree() {
        while (hasActiveReaders()) LockSupport.park(this);
        while (retirements.consumeReadyHint()) {
            int sealed = retirements.seal(Integer.MAX_VALUE, epoch);
            if (sealed != 0) latestRetireEpoch = epoch;
        }
        for (Entry entry : data.values()) {
            long value = Entry.rawValueAddress(entry.valueAddress);
            entry.valueAddress = 0L;
            if (value != 0L) memory.releaseEntry(value, ValueBlock.allocationLength(ValueBlock.length(value)));
            memory.releaseEntry(entry.nativeKeyAddress, entry.keyAllocationLength());
        }
        data.clear();
        retirements.freeAll();
        retirements.close();
        memory.closeArenas();
        budget.returnUnusedCredits();
        budget.clear();
    }

    private boolean hasActiveReaders() {
        return readers.hasActiveReader();
    }

    private boolean tryAcquireDirtyCredit() {
        for (;;) {
            int current = dirtyEventCount.get();
            if (current >= dirtyLimit) return false;
            if (dirtyEventCount.compareAndSet(current, current + 1)) return true;
        }
    }

    private void releaseDirtyCredit() {
        int remaining = dirtyEventCount.decrementAndGet();
        if (remaining < 0) throw new IllegalStateException("dirty-event credit underflow");
    }

    private boolean prepareActorRetirement(int records) {
        return retirements.reserve(actorRetirement, records);
    }

    private void retireActorValue(long address, long allocation) {
        retirements.append(actorRetirement, address, allocation);
    }

    private void signal() {
        if (wakeGate.signal()) {
            wakeUnparks.incrementAndGet();
            LockSupport.unpark(thread);
        }
    }

    private void pushRepair(Entry entry) {
        repairCount.incrementAndGet();
        for (;;) {
            Entry head = repairHead.get();
            entry.repairNext = head;
            if (repairHead.compareAndSet(head, entry)) return;
        }
    }

    private void pushRepairChain(Entry chain) {
        Entry tail = chain;
        while (tail.repairNext != null) tail = tail.repairNext;
        for (;;) {
            Entry head = repairHead.get();
            tail.repairNext = head;
            if (repairHead.compareAndSet(head, chain)) return;
        }
    }

    private static int dirtyLimit(int queueCapacity) {
        return Math.multiplyExact(queueCapacity, 2);
    }

    private static int stripeCount() {
        int target = Math.max(1, Runtime.getRuntime().availableProcessors() * 4);
        int stripes = 1;
        while (stripes < target && stripes < (1 << 30)) stripes <<= 1;
        return stripes;
    }

    /** Keeps the fixed native retire transport within the cache's 512KiB ledger reserve. */
    private static int retirementRecordsPerStripe() {
        long perStripe = (512L << 10) / ((long) stripeCount() * 32L);
        int records = 2;
        while ((records << 1) <= perStripe) records <<= 1;
        return records;
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
        public final long liveWeight;
        public final long timeoutLagMillis;
        public final boolean unhealthy;
        public final long queueDepth;
        public final long retiredEntries;
        public final long retiredBytes;
        public final long oldestRetireEpoch;
        public final long policyEvictions;
        public final long evictionScans;
        public final long evictionLockedSkips;
        public final long timerBytes;
        public final long ttlBacklog;
        public final long sketchBytes;
        public final long ghostHeapBytes;
        public final long ledgerBytes;
        public final long queueCapacity;
        public final long wakeSignals;
        public final long mergedWakeSignals;

        Snapshot(long hits, long misses, long accessDropped, long accepted, long rejectedQueue,
                 long rejectedBudget, long applied, long evicted, long logicalExpired,
                 long physicalExpired, long maintenanceLoopNanos, long liveWeight, long timeoutLagMillis, boolean unhealthy, long queueDepth,
                 long retiredEntries, long retiredBytes, long oldestRetireEpoch, long policyEvictions,
                 long evictionScans, long evictionLockedSkips, long timerBytes, long ttlBacklog,
                 long sketchBytes, long ghostHeapBytes, long ledgerBytes, long queueCapacity,
                 long wakeSignals, long mergedWakeSignals) {
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
            this.liveWeight = liveWeight;
            this.timeoutLagMillis = timeoutLagMillis;
            this.unhealthy = unhealthy;
            this.queueDepth = queueDepth;
            this.retiredEntries = retiredEntries;
            this.retiredBytes = retiredBytes;
            this.oldestRetireEpoch = oldestRetireEpoch;
            this.policyEvictions = policyEvictions;
            this.evictionScans = evictionScans;
            this.evictionLockedSkips = evictionLockedSkips;
            this.timerBytes = timerBytes;
            this.ttlBacklog = ttlBacklog;
            this.sketchBytes = sketchBytes;
            this.ghostHeapBytes = ghostHeapBytes;
            this.ledgerBytes = ledgerBytes;
            this.queueCapacity = queueCapacity;
            this.wakeSignals = wakeSignals;
            this.mergedWakeSignals = mergedWakeSignals;
        }
    }
}
