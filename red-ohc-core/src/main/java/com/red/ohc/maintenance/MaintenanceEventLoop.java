package com.red.ohc.maintenance;

import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.jctools.queues.MpscArrayQueue;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

/**
 * One parked maintenance event-loop for a cache. It owns policy/timer state and never decides
 * ordinary index visibility. Business threads publish those changes directly to the CHM.
 */
public final class MaintenanceEventLoop
    implements Runnable, TimerWheel.TimerConsumer, AccessConsumer {
  private static final long EPOCH_ADVANCE_INTERVAL_NANOS = 1_000_000L;

  /**
   * After draining a producer burst, stay in REQUIRED and sleep briefly before returning to an
   * infinite idle park. This batches isolated retirement-only replacements without a polling loop:
   * an actually idle actor still parks forever, while a busy producer pays at most one
   * idle-to-required unpark per burst.
   */
  private static final long WRITE_BATCH_GRACE_NANOS = 1_000_000L;

  /**
   * The timer owns physical cleanup only; reads perform the strict TTL check. Sampling the actor
   * clock once per bounded group of passes therefore preserves safety while avoiding a native
   * wall/monotonic-clock trip for every tiny retirement batch.
   */
  private static final int CLOCK_SAMPLE_INTERVAL_PASSES = 16;

  private static final int RETIREMENT_BATCH_RECORDS = 128;

  /**
   * A deferred mutation has a writer in progress; retry it without turning idle into a poll loop.
   */
  private static final long DEFERRED_MUTATION_RETRY_NANOS = 1_000L;

  private final ConcurrentHashMap<Entry, Entry> data;
  private final NativeMemory.Memory memory;
  private final Budget budget;
  private final Ticker ticker;
  private final long capacity;
  private final int queueCapacity;
  private final MpscArrayQueue<Entry> queue;

  /** Reliable removal transport: it is reserved before CHM removal and may contain tombstones. */
  private final ReliableRemovalQueue reliableRemovals;

  private final AtomicBoolean repairNeeded = new AtomicBoolean();
  private final AtomicLong repairVersion = new AtomicLong();
  private final AtomicBoolean allocationPressureRequested = new AtomicBoolean();
  private final MpscArrayQueue<Entry>[] repairQueues;
  private final int repairShardMask;
  private int repairShardCursor;
  private final AtomicLong[] repairDebts;

  /** Actor-owned Entries whose transport item arrived while a writer owns the Entry mutex. */
  private final ArrayDeque<Entry> deferredMutations = new ArrayDeque<>();

  private long deferredMutationRetryNanos = Long.MAX_VALUE;
  private final AtomicBoolean accessHint = new AtomicBoolean();
  private final ReaderRegistry readers;
  private final RetirementQueue retirements;
  private final RetirementQueue.Reservation actorRetirement = new RetirementQueue.Reservation();
  private final TimerWheel wheel;
  private final MaintenancePolicy policy;
  private final Thread thread;
  private final WakeGate wakeGate = new WakeGate();

  /** Cold-path owner for actor state and writer assistance. */
  private final ReentrantLock ownerLock = new ReentrantLock();

  /** Separate wait channel for writers blocked on resource progress; never held by the actor. */
  private final ReentrantLock progressLock = new ReentrantLock();

  private final Condition progressChanged = progressLock.newCondition();
  private final AtomicLong progressVersion = new AtomicLong();
  private final AtomicInteger waitingWriters = new AtomicInteger();
  private final AtomicLong assistCount = new AtomicLong();
  private final AtomicLong assistWork = new AtomicLong();
  private final AtomicLong waitCount = new AtomicLong();
  private final AtomicLong waitNanos = new AtomicLong();
  private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
  private final AtomicReference<CompletableFuture<Void>> flushRequest = new AtomicReference<>();
  private volatile boolean closing;
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
   * Cross-thread snapshot of the actor-owned policy weight. Writers never update this value: it is
   * deliberately published only after a bounded maintenance pass has applied all of its policy
   * mutations. Reading {@link MaintenancePolicy#usedBytes()} from a cache caller would otherwise be
   * a data race with the actor.
   */
  private volatile long publishedLiveWeight;

  private long nowNanos;
  private int clockSampleCountdown;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong accessDropped = new AtomicLong();

  /** A write-path statistic: striped so successful puts do not serialize on one cache line. */
  private final LongAdder accepted = new LongAdder();

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

  /** A retry blocked by a business writer may complete a control-plane flush without deadlock. */
  private boolean evictionBlockedOnWriter;

  private long evictionRetryNanos = Long.MAX_VALUE;
  private long evictionRetryBackoffNanos = 1_000L;

  /** A sealed retirement could not pass QSBR; recheck it on the bounded epoch deadline. */
  private boolean reclaimBlocked;

  /** A bounded reclaim pass hit its limit and must be continued before the worker can park. */
  private boolean reclaimContinuation;

  /** Actor-owned marker: publish the policy weight only after a policy mutation. */
  private boolean policyDirty;

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Budget budget,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      ReaderRegistry readers) {
    this(
        data,
        memory,
        budget,
        ticker,
        capacity,
        eviction,
        readers,
        ChmSizing.maintenanceQueueCapacity(0L, capacity, capacity));
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Budget budget,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      ReaderRegistry readers,
      int queueCapacity) {
    this.data = data;
    this.memory = memory;
    this.budget = budget;
    this.ticker = ticker;
    this.capacity = capacity;
    if (Integer.bitCount(queueCapacity) != 1 || queueCapacity < 1_024) {
      throw new IllegalArgumentException("queueCapacity must be a power of two >= 1024");
    }
    this.queueCapacity = queueCapacity;
    this.queue = new MpscArrayQueue<>(queueCapacity);
    int shardCount = 1;
    int requestedShards = Math.min(16, Math.max(1, queueCapacity / 1_024));
    while (shardCount < requestedShards) {
      shardCount <<= 1;
    }
    @SuppressWarnings("unchecked")
    MpscArrayQueue<Entry>[] repairQueues = new MpscArrayQueue[shardCount];
    int repairQueueCapacity = Math.max(1_024, queueCapacity / shardCount);
    for (int i = 0; i < shardCount; i++) {
      repairQueues[i] = new MpscArrayQueue<>(repairQueueCapacity);
    }
    this.repairQueues = repairQueues;
    this.repairShardMask = shardCount - 1;
    this.repairDebts = new AtomicLong[shardCount];
    for (int i = 0; i < shardCount; i++) {
      this.repairDebts[i] = new AtomicLong();
    }
    this.reliableRemovals = new ReliableRemovalQueue(queueCapacity);
    this.policy = new MaintenancePolicy(eviction, capacity);
    this.readers = readers;
    this.retirements = new RetirementQueue(memory, stripeCount(), retirementRecordsPerStripe());
    this.wheel = new TimerWheel(ticker.currentTimeMillis());
    this.nowMillis = ticker.currentTimeMillis();
    this.thread = new Thread(this, "red-ohc-maintenance-event-loop");
    this.thread.setDaemon(true);
  }

  public void start() {
    thread.start();
  }

  public void stop() {
    stopping = true;
    signal();
    signalProgressAll();
    // A close must not wait for the bounded producer-batch grace period.
    LockSupport.unpark(thread);
  }

  public void beginClosing() {
    closing = true;
    signalProgressAll();
  }

  public void join(long timeoutMillis) throws InterruptedException {
    thread.join(timeoutMillis);
  }

  public boolean isAlive() {
    return thread.isAlive();
  }

  public boolean isParked() {
    return parked;
  }

  public long epoch() {
    return epoch;
  }

  public long nowMillis() {
    return nowMillis;
  }

  /** Writer-side TTL operations use the actor's published clock, refreshed only on demand. */
  public void refreshClock() {
    nowMillis = ticker.currentTimeMillis();
  }

  public Thread thread() {
    return thread;
  }

  /** Logical unique-entry backlog; the physical advisory queue is bounded and repairable. */
  public long queueDepth() {
    return queue.size() + repairDebt();
  }

  public long queueCapacity() {
    return queueCapacity;
  }

  public boolean mutationBacklogExceeds() {
    // A full advisory queue is expected under burst load and must not add writer backpressure.
    // Only unrecoverable repair debt is a write-admission high watermark.
    return repairDebt() >= queueCapacity * 2L;
  }

  /**
   * Re-enables bounded writer backpressure only after the advisory repair backlog reaches its high
   * watermark. Ordinary hints never call this method.
   */
  public void awaitMutationAdmission() {
    while (mutationAdmissionBlocked()) {
      throwIfUnavailable();
      requestMaintenance();
      if (!assistMaintenance()) {
        awaitMaintenanceProgress();
      }
    }
  }

  private boolean mutationAdmissionBlocked() {
    return repairDebt() >= queueCapacity * 2L;
  }

  public long retiredBytes() {
    return retirements.retiredBytes();
  }

  public int retiredEntries() {
    return retirements.retiredEntries();
  }

  public long oldestRetireEpoch() {
    return retirements.oldestEpoch();
  }

  public long timerBytes() {
    return wheel.bytes();
  }

  public long sketchBytes() {
    return policy.sketchBytes();
  }

  public long ghostHeapBytes() {
    return policy.ghostHeapBytes();
  }

  public long ttlBacklog() {
    return wheel.scheduled();
  }

  public long policyEvictions() {
    return policy.evictions();
  }

  public long ledgerBytes() {
    return retirements.allocatedBytes();
  }

  public long retirementQueueDepth() {
    return retirements.queuedRecords();
  }

  public long retirementQueueCapacity() {
    return retirements.capacityRecords();
  }

  public boolean retirementBacklogExceeds() {
    return retirements.exceedsHighWatermark();
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
    if (waitingWriters.get() != 0) {
      signalProgress();
    }
  }

  public void recordHit() {
    hits.incrementAndGet();
  }

  public void recordMiss() {
    misses.incrementAndGet();
  }

  public void recordDropped() {
    accessDropped.incrementAndGet();
  }

  public void recordAccepted() {
    accepted.increment();
  }

  public void recordAccepted(int count) {
    if (count > 0) {
      accepted.add(count);
    }
  }

  public void throwIfUnavailable() {
    Throwable failure = terminalFailure.get();
    if (failure != null) {
      throw new com.red.ohc.api.CacheMaintenanceException(failure);
    }
    if (closing) {
      throw new IllegalStateException("cache is closing");
    }
    if (stopping) {
      throw new IllegalStateException("cache maintenance is stopping");
    }
  }

  /** Run one bounded pass on a writer when the asynchronous actor is behind. */
  public boolean assistMaintenance() {
    if (!ownerLock.tryLock()) {
      return false;
    }
    try {
      throwIfUnavailable();
      assistCount.incrementAndGet();
      int work = maintenancePass(true);
      if (work != 0) {
        assistWork.addAndGet(work);
        signalProgressLocked();
        return true;
      }
      return false;
    } catch (RuntimeException failure) {
      if (!(failure instanceof IllegalStateException && closing && terminalFailure.get() == null)) {
        recordTerminalFailure(failure);
      }
      throw failure;
    } catch (Error failure) {
      recordTerminalFailure(failure);
      throw failure;
    } finally {
      finishActorRetirementBatch();
      ownerLock.unlock();
    }
  }

  /** Waits for a maintenance generation after the caller's single bounded assist. */
  public void awaitMaintenanceProgress() {
    long started = System.nanoTime();
    throwIfUnavailable();
    if (!thread.isAlive()) {
      return;
    }
    boolean interrupted = false;
    long observed = progressVersion.get();
    progressLock.lock();
    try {
      waitingWriters.incrementAndGet();
      waitCount.incrementAndGet();
      try {
        while (progressVersion.get() == observed
            && terminalFailure.get() == null
            && !closing
            && !stopping) {
          try {
            progressChanged.await();
          } catch (InterruptedException error) {
            interrupted = true;
            break;
          }
        }
      } finally {
        waitingWriters.decrementAndGet();
      }
    } finally {
      progressLock.unlock();
    }
    if (interrupted || Thread.interrupted()) {
      Thread.currentThread().interrupt();
      throw new com.red.ohc.api.CacheWriteInterruptedException(
          new InterruptedException("maintenance progress wait interrupted"));
    }
    waitNanos.addAndGet(System.nanoTime() - started);
  }

  public long assistCount() {
    return assistCount.get();
  }

  public long assistWork() {
    return assistWork.get();
  }

  public long waitCount() {
    return waitCount.get();
  }

  public long waitNanos() {
    return waitNanos.get();
  }

  public void recordTerminalFailure(Throwable failure) {
    if (terminalFailure.compareAndSet(null, failure)) {
      unhealthy.set(true);
      CompletableFuture<Void> future = flushRequest.getAndSet(null);
      if (future != null) {
        future.completeExceptionally(
            new com.red.ohc.api.CacheMaintenanceException(failure));
      }
      if (ownerLock.isHeldByCurrentThread()) {
        signalProgressAllLocked();
      } else {
        signalProgressAll();
      }
      signal();
    }
  }

  /** Reliable removal only: ADD/UPDATE are published after their data-plane mutation. */
  public boolean reserveMutation(Entry entry, int flags) {
    if (flags != Entry.PENDING_REMOVE) {
      throw new IllegalArgumentException("only removal mutations require a reservation");
    }
    if (unhealthy.get()) {
      return false;
    }
    entry.beginPending(flags);
    return true;
  }

  /** Publishes an ADD/UPDATE hint after the associated CHM/value mutation is visible. */
  public void publishMutation(Entry entry, int flags) {
    publishMutation(entry, flags, true);
  }

  /** Publishes a hint and optionally defers its wake to a surrounding write batch. */
  public void publishMutation(Entry entry, int flags, boolean wake) {
    if ((flags & (Entry.PENDING_ADD | Entry.PENDING_UPDATE)) == 0
        || (flags & ~(Entry.PENDING_ADD | Entry.PENDING_UPDATE)) != 0) {
      throw new IllegalArgumentException("invalid advisory mutation flags: " + flags);
    }
    if (!entry.publishMutation(flags)) {
      return;
    }
    try {
      if (!queue.offer(entry)) {
        markMutationForRepair(entry, wake);
      }
    } catch (OutOfMemoryError error) {
      // The mapping/value pointer is already visible. A maintenance transport failure is
      // advisory for this operation: keep the put result synchronous and fail closed for
      // subsequent writes while close() frees the authoritative CHM/native state.
      markMutationForRepair(entry, wake);
      recordTerminalFailure(error);
    }
  }

  private void markMutationForRepair(Entry entry) {
    markMutationForRepair(entry, true);
  }

  private void markMutationForRepair(Entry entry, boolean wake) {
    int shard = entry.keyHash() & repairShardMask;
    boolean firstRepair = entry.queueOfferFailed();
    if (firstRepair) {
      repairDebts[shard].incrementAndGet();
    }
    repairVersion.incrementAndGet();
    repairNeeded.set(true);
    if (wake) {
      signal();
    }
    // A marker owns exactly one repair-queue item. Subsequent coalesced mutations only
    // update the Entry flags; enqueueing duplicates would consume the bounded shard queue
    // without adding repair debt and could make a writer wait for work that is already queued.
    if (firstRepair) {
      enqueueRepair(entry, shard);
    }
  }

  private void enqueueRepair(Entry entry, int shard) {
    MpscArrayQueue<Entry> repairQueue = repairQueues[shard];
    while (!repairQueue.offer(entry)) {
      requestMaintenance();
      if (Thread.currentThread() == thread) {
        Entry ready = repairQueue.poll();
        if (ready != null) {
          processEntry(ready);
        }
      } else {
        if (!assistMaintenance()) {
          awaitMaintenanceProgress();
        }
      }
    }
  }

  /** Cancels a reliable removal reservation before CHM visibility changes. */
  public void cancelMutation(Entry entry) {
    entry.cancelPendingClaim();
  }

  /** Publishes a pre-reserved block to the cache-owned native retirement transport. */
  public void retireValue(
      com.red.ohc.runtime.ThreadContext context, long address, long allocation) {
    retirements.append(context.retirement, address, allocation);
    context.markRetirementPublished();
  }

  /**
   * The producer calls this exactly once after publishing every CHM/pointer mutation and its
   * retirement records. Keeping the wake at this tail avoids a second WakeGate CAS for a
   * replacement that changes both actor state and the retirement FIFO.
   */
  public void afterWrite(com.red.ohc.runtime.ThreadContext context) {
    boolean generationWake = context.needsMaintenanceWake(idleGeneration);
    boolean retirementWake = context.consumeRetirementPublished();
    if (generationWake || retirementWake) {
      signal();
    }
  }

  /** Low-frequency callers without a cache ThreadContext always request a wake. */
  public void afterWrite() {
    signal();
  }

  /** Requests an actor pass after a writer-side resource pressure event. */
  public void requestMaintenance() {
    signal();
  }

  /** Page trimming is requested only after a native allocation-limit failure. */
  public void requestAllocationPressure() {
    allocationPressureRequested.set(true);
    signal();
  }

  /**
   * Reserves native retirement records before an index mutation publishes a replacement or removal.
   */
  public boolean prepareRetirement(com.red.ohc.runtime.ThreadContext context, int records) {
    boolean reserved = retirements.reserve(context.retirement, records);
    return reserved;
  }

  /**
   * Reserves removal cleanup and a reliable queue slot before the caller changes CHM visibility.
   */
  public void prepareReliableRemoval(
      com.red.ohc.runtime.ThreadContext context, Entry entry) {
    if (context == null || entry == null) {
      throw new NullPointerException("context/entry");
    }
    while (true) {
      throwIfUnavailable();
      if (reliableRemovals.reserve(context.reliableRemoval)) {
        try {
          if (retirements.reserve(context.retirement, 2)
              && reserveMutation(entry, Entry.PENDING_REMOVE)) {
            return;
          }
        } catch (Throwable failure) {
          if (context.retirement.active()) {
            retirements.cancel(context.retirement);
          }
          reliableRemovals.cancel(context.reliableRemoval);
          throw failure;
        }
        if (context.retirement.active()) {
          retirements.cancel(context.retirement);
        }
        reliableRemovals.cancel(context.reliableRemoval);
      }
      requestMaintenance();
      if (!assistMaintenance()) {
        awaitMaintenanceProgress();
      }
    }
  }

  /** Completes the pre-reserved removal claim after CHM and retirement records are published. */
  public void publishRemoval(com.red.ohc.runtime.ThreadContext context, Entry entry) {
    try {
      entry.completePendingClaim();
      reliableRemovals.commit(context.reliableRemoval, entry);
    } catch (Throwable failure) {
      if (context.reliableRemoval.active()) {
        reliableRemovals.cancel(context.reliableRemoval);
      }
      throw failure;
    }
  }

  public void cancelReliableRemoval(
      com.red.ohc.runtime.ThreadContext context, Entry entry) {
    entry.cancelPendingClaimIfPresent();
    if (context.retirement.active()) {
      retirements.cancel(context.retirement);
    }
    if (context.reliableRemoval.active()) {
      reliableRemovals.cancel(context.reliableRemoval);
    }
  }

  public void cancelRetirement(com.red.ohc.runtime.ThreadContext context) {
    retirements.cancel(context.retirement);
  }

  /** Control-plane barrier; callers share one pending barrier and never enter the hint queue. */
  public CompletableFuture<Void> flush() {
    Throwable failure = terminalFailure.get();
    if (failure != null) {
      CompletableFuture<Void> failed = new CompletableFuture<>();
      failed.completeExceptionally(new com.red.ohc.api.CacheMaintenanceException(failure));
      return failed;
    }
    while (true) {
      CompletableFuture<Void> existing = flushRequest.get();
      if (existing != null) {
        return existing;
      }
      CompletableFuture<Void> created = new CompletableFuture<>();
      if (flushRequest.compareAndSet(null, created)) {
        clockRefreshRequested = true;
        signal();
        return created;
      }
    }
  }

  public Snapshot snapshot() {
    return new Snapshot(
        hits.get(),
        misses.get(),
        accessDropped.get(),
        accepted.sum(),
        applied.get(),
        evicted.get(),
        logicalExpired.get(),
        physicalExpired.get(),
        maintenanceLoopNanos.get(),
        publishedLiveWeight,
        timeoutLagMillis.get(),
        unhealthy.get(),
        queueDepth(),
        retiredEntries(),
        retiredBytes(),
        oldestRetireEpoch(),
        policyEvictions(),
        evictionScans.get(),
        evictionLockedSkips.get(),
        timerBytes(),
        ttlBacklog(),
        sketchBytes(),
        ghostHeapBytes(),
        ledgerBytes(),
        queueCapacity,
        retirementQueueDepth(),
        retirementQueueCapacity(),
        wakeUnparks.get(),
        wakeGate.mergedTransitions(),
        assistCount.get(),
        assistWork.get(),
        waitCount.get(),
        waitNanos.get(),
        progressVersion.get());
  }

  @Override
  public void run() {
    wakeGate.requireProcessing();
    while (!stopping || hasWork()) {
      // A terminal maintenance failure makes the cache unavailable, but the actor remains
      // alive until close() owns the final native teardown. Park without retrying the
      // corrupted maintenance state while preserving in-flight reader safety.
      if (terminalFailure.get() != null && !stopping) {
        parked = true;
        try {
          LockSupport.park(this);
        } finally {
          parked = false;
        }
        continue;
      }
      if (!stopping
          && !hasImmediateSourceWork()
          && deferredMutations.isEmpty()
          && !retirements.hasPendingReclaim()
          && !evictionWorkDue()) {
        parkUntilWorkOrTimer(false);
        continue;
      }

      int work;
      ownerLock.lock();
      try {
        long started = ticker.nanos();
        work = maintenancePass(false);
        maintenanceLoopNanos.set(ticker.nanos() - started);
        if (work != 0) {
          signalProgressLocked();
        }
      } catch (Throwable failure) {
        recordTerminalFailure(failure);
        work = 0;
      } finally {
        finishActorRetirementBatch();
        ownerLock.unlock();
      }
      if (!hasImmediateSourceWork()) {
        parkUntilWorkOrTimer(work != 0 && !stopping);
      }
    }
    shutdownAndFree();
  }

  /** Must be called while ownerLock is held. */
  private int maintenancePass(boolean forceEpoch) {
    if (sampleClockIfDue()) {
      budget.reclaimDeadLeases();
    }
    int work = 0;
    if (forceEpoch) {
      // Pressure assist follows the fixed progress order: seal, advance epoch, reclaim.
      if (retirements.hasReadyHint()) {
        work += sealRetirements(1024);
      }
      if (forceAdvanceEpoch()) {
        work++;
      }
      if (retirements.hasPendingReclaim()) {
        work += reclaim(1024);
      }
    }
    boolean mutationsPending =
        !queue.isEmpty() || repairNeeded.get() || !deferredMutations.isEmpty();
    if (mutationsPending) {
      policy.beginWriteBatch();
    }
    if (reliableRemovals.size() != 0L) {
      work += drainReliableRemovals(4096);
    }
    if (!queue.isEmpty()) {
      work += drainMutations(4096);
    }
    if (repairNeeded.get()) {
      work += repairMutations(4096);
    }
    if (!deferredMutations.isEmpty()) {
      work += drainDeferredMutations(4096);
    }
    if (accessHint.get()) {
      work += drainAccesses(4096);
    }
    // Expiry/eviction runs after mutation repair so actor policy never observes a stale
    // pointer or pending flag. Pressure assist still seals/reclaims again below.
    if (wheel.hasPending()) {
      work += wheel.advance(nowMillis, 1_024, this);
    }
    if (retirements.hasReadyHint()) {
      work += sealRetirements(1024);
    }
    if (forceEpoch) {
      work += forceAdvanceEpoch() ? 1 : 0;
    } else {
      if (advanceEpochIfDue(nowNanos)) {
        work++;
      }
    }
    if (retirements.hasPendingReclaim()) {
      work += reclaim(1024);
    }
    if (!stopping && (evictionRetryNanos == Long.MAX_VALUE || evictionWorkDue())) {
      try {
        work += evictIfNeeded(64);
      } finally {
        finishActorRetirementBatch();
      }
    }
    if (forceEpoch) {
      // A mutation/eviction pass can publish more retirement records. Close the pressure
      // pass by sealing and reclaiming once more before trimming native pages.
      if (retirements.hasReadyHint()) {
        work += sealRetirements(1024);
      }
      if (retirements.hasPendingReclaim()) {
        work += reclaim(1024);
      }
    }
    if (allocationPressureRequested.getAndSet(false)) {
      work += memory.trimIdlePages() > 0L ? 1 : 0;
    }
    publishLiveWeight();
    completeFlushIfIdle();
    return work;
  }

  private void publishLiveWeight() {
    if (!policyDirty) {
      return;
    }
    long weight = policy.usedBytes();
    if (publishedLiveWeight != weight) {
      publishedLiveWeight = weight;
    }
    policyDirty = false;
  }

  private boolean hasWork() {
    return hasSourceWork() || retirements.hasPendingReclaim();
  }

  private boolean hasSourceWork() {
    return hasImmediateSourceWork() || !deferredMutations.isEmpty();
  }

  private boolean hasImmediateSourceWork() {
    return reliableRemovals.size() != 0L
        || !queue.isEmpty()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || accessHint.get()
        || flushNeedsImmediatePass()
        || retirements.hasReadyHint()
        || clockRefreshRequested
        || reclaimContinuation;
  }

  /** A flush remains pending across a deferred eviction retry, but that retry is timer work. */
  private boolean flushNeedsImmediatePass() {
    return flushRequest.get() != null
        && (clockRefreshRequested || evictionRetryNanos == Long.MAX_VALUE || evictionWorkDue());
  }

  private void parkUntilWorkOrTimer(boolean batchGrace) {
    if (batchGrace) {
      parked = true;
      try {
        long timerDelay = nextParkDelayNanos();
        LockSupport.parkNanos(this, Math.min(WRITE_BATCH_GRACE_NANOS, timerDelay));
        if (timerDelay != Long.MAX_VALUE) {
          clockRefreshRequested = true;
        }
      } finally {
        parked = false;
      }
      return;
    }
    if (!wakeGate.armIdle()) {
      return;
    }
    // Publish the new generation before the final source check. A producer that races after
    // this point observes it and changes PROCESSING_TO_IDLE to PROCESSING_TO_REQUIRED;
    // a producer before it is still covered by hasWork().
    idleGeneration++;
    if (stopping || hasImmediateSourceWork()) {
      wakeGate.requireProcessing();
      return;
    }
    // A producer that published before idleGeneration changed is caught by this final
    // actor-owned scan. A later producer observes the new generation and changes the gate
    // to PROCESSING_TO_REQUIRED, so neither side can strand a completed retirement record.
    ownerLock.lock();
    int sealed;
    try {
      sealed = sealRetirements(1024);
    } finally {
      ownerLock.unlock();
    }
    if (sealed != 0) {
      wakeGate.requireProcessing();
      return;
    }
    if (!wakeGate.finishIdle()) {
      return;
    }
    parked = true;
    try {
      long delay = nextParkDelayNanos();
      if (retirements.hasPendingReclaim()) {
        delay = Math.min(delay, epochAdvanceDelayNanos(ticker.nanos()));
      }
      if (delay == Long.MAX_VALUE) {
        LockSupport.park(this);
      } else {
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
    if (future == null) {
      return;
    }
    // A bounded eviction scan may have deferred its next retry to protect the worker from
    // repeated scans. Flush must retain its barrier until that retry either brings the
    // policy back under capacity or schedules another genuinely pending maintenance pass.
    if (policy.usedBytes() > capacity && !evictionWorkDue() && !evictionBlockedOnWriter) {
      return;
    }
    if (clockRefreshRequested) {
      return;
    }
    if (reliableRemovals.size() != 0L
        || !queue.isEmpty()
        || !deferredMutations.isEmpty()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || accessHint.get()
        || retirements.hasReadyHint()
        || reclaimContinuation) {
      return;
    }
    if (flushRequest.compareAndSet(future, null)) {
      future.complete(null);
    }
  }

  private int drainMutations(int limit) {
    int work = 0;
    while (work < limit) {
      Entry entry = queue.poll();
      if (entry == null) {
        break;
      }
      processEntry(entry);
      work++;
    }
    return work;
  }

  private int drainReliableRemovals(int limit) {
    int work = 0;
    while (work < limit && reliableRemovals.size() != 0L) {
      Entry entry = reliableRemovals.poll();
      if (entry != null) {
        processEntry(entry);
        work++;
      } else {
        if (reliableRemovals.pollTombstone()) {
          work++;
        } else {
          break;
        }
      }
    }
    return work;
  }

  /** Repairs coalesced mutations whose advisory queue offer lost a race with a full queue. */
  private int repairMutations(int limit) {
    int work = 0;
    long observedVersion = repairVersion.get();
    while (work < limit) {
      Entry entry = null;
      for (int attempts = 0; attempts <= repairShardMask; attempts++) {
        int shard = repairShardCursor++ & repairShardMask;
        entry = repairQueues[shard].poll();
        if (entry != null) {
          break;
        }
      }
      if (entry == null) {
        break;
      }
      if (!entry.isRepairMarked()) {
        continue;
      }
      processEntry(entry);
      work++;
    }
    if (observedVersion == repairVersion.get() && repairDebt() == 0L && repairQueuesEmpty()) {
      repairNeeded.set(false);
    }
    return work;
  }

  private boolean repairQueuesEmpty() {
    for (MpscArrayQueue<Entry> queue : repairQueues) {
      if (!queue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private void processEntry(Entry entry) {
    int flags = entry.takePending();
    // beginPending() holds this claim from reservation through pointer publication. It is
    // stronger than observing the writer mutex: a racing actor can never clear the merged
    // flags after a writer started but before the writer publishes its new value pointer.
    if (flags == Entry.PENDING_BUSY) {
      deferMutation(entry);
      return;
    }
    if (entry.clearRepairMarker()) {
      decrementRepairDebt(entry);
    }
    if (flags == 0) {
      entry.tryRolloverMaintenanceVersion();
      return;
    }
    if (entry.isWriterLocked()) {
      // ADD/UPDATE no longer hold a pending claim. A worker may therefore dequeue the
      // advisory hint while the per-entry writer is between allocation and publication.
      // Put the hint back into the bounded deferred path and let the retry deadline observe
      // the fully published value.
      if (entry.requeueMutation(flags)) {
        deferredMutations.addLast(entry);
      }
      deferredMutationRetryNanos = ticker.nanos() + DEFERRED_MUTATION_RETRY_NANOS;
      return;
    }
    processEntry(entry, flags);
  }

  private int drainDeferredMutations(int limit) {
    int attempts = deferredMutations.size();
    int work = 0;
    while (attempts-- > 0 && work < limit) {
      Entry entry = deferredMutations.removeFirst();
      int flags = entry.takePending();
      if (flags == Entry.PENDING_BUSY) {
        deferredMutations.addLast(entry);
        continue;
      }
      if (entry.clearRepairMarker()) {
        decrementRepairDebt(entry);
      }
      if (flags != 0) {
        processEntry(entry, flags);
        work++;
      }
    }
    deferredMutationRetryNanos =
        deferredMutations.isEmpty()
            ? Long.MAX_VALUE
            : ticker.nanos() + DEFERRED_MUTATION_RETRY_NANOS;
    return work;
  }

  private void deferMutation(Entry entry) {
    deferredMutations.addLast(entry);
    deferredMutationRetryNanos = ticker.nanos() + DEFERRED_MUTATION_RETRY_NANOS;
  }

  private void decrementRepairDebt(Entry entry) {
    AtomicLong debt = repairDebts[entry.keyHash() & repairShardMask];
    while (true) {
      long current = debt.get();
      if (current == 0L || debt.compareAndSet(current, current - 1L)) {
        return;
      }
    }
  }

  private long repairDebt() {
    long total = 0L;
    for (AtomicLong debt : repairDebts) {
      total += debt.get();
    }
    return total;
  }

  private void processEntry(Entry entry, int flags) {
    long version = entry.mutationVersion();
    if ((flags & Entry.PENDING_REMOVE) != 0) {
      wheel.remove(entry);
      policy.remove(entry, false);
      policyDirty = true;
      entry.markAppliedVersion(version);
      entry.tryRolloverMaintenanceVersion();
      return;
    }
    if ((flags & (Entry.PENDING_ADD | Entry.PENDING_UPDATE)) != 0) {
      applyEntry(entry, version, flags);
      entry.tryRolloverMaintenanceVersion();
    }
  }

  private void applyEntry(Entry entry, long version, int flags) {
    long taggedAddress = entry.valueAddress;
    long address = Entry.rawValueAddress(taggedAddress);
    if (address == 0L || !isCurrent(entry)) {
      wheel.remove(entry);
      policy.remove(entry, false);
      policyDirty = true;
      if (!entry.markAppliedVersion(version)) {
        republishMutation(entry, flags);
      }
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
    if (!entry.markAppliedVersion(version)) {
      republishMutation(entry, flags);
    }
  }

  /** Compatibility for the actor-focused unit tests and local maintenance probes. */
  private void applyEntry(Entry entry) {
    applyEntry(entry, entry.mutationVersion(), Entry.PENDING_UPDATE);
  }

  private void republishMutation(Entry entry, int flags) {
    if (!entry.requeueMutation(flags)) {
      return;
    }
    if (!queue.offer(entry)) {
      markMutationForRepair(entry);
    }
  }

  private int sealRetirements(int limit) {
    if (!retirements.consumeReadyHint()) {
      return 0;
    }
    int sealed = retirements.seal(limit, epoch);
    // The signal is coalesced. If the bounded pass consumed its complete budget, schedule
    // another pass rather than re-scanning every native retirement stripe unconditionally.
    if (sealed == limit) {
      retirements.requestSeal();
    }
    if (sealed != 0) {
      latestRetireEpoch = epoch;
    }
    return sealed;
  }

  /**
   * Retirements are first marked with the current epoch and only then move readers forward. The
   * cadence bounds the retry window in {@code ReaderGuard.enter()} without letting a new reader
   * bypass an address that was made unreachable in its own epoch.
   */
  private boolean advanceEpochIfDue(long nowNanos) {
    if (!retirements.hasPendingReclaim() || latestRetireEpoch < epoch) {
      return false;
    }
    if (hasAdvancedEpoch && nowNanos - lastEpochAdvanceNanos < EPOCH_ADVANCE_INTERVAL_NANOS) {
      return false;
    }
    epoch++;
    lastEpochAdvanceNanos = nowNanos;
    hasAdvancedEpoch = true;
    return true;
  }

  private boolean forceAdvanceEpoch() {
    if (!retirements.hasPendingReclaim() || latestRetireEpoch < epoch) {
      return false;
    }
    epoch++;
    lastEpochAdvanceNanos = nowNanos;
    hasAdvancedEpoch = true;
    return true;
  }

  private boolean sampleClockIfDue() {
    if (!clockRefreshRequested && clockSampleCountdown-- > 0) {
      return false;
    }
    clockRefreshRequested = false;
    nowNanos = ticker.nanos();
    nowMillis = ticker.currentTimeMillis();
    clockSampleCountdown = CLOCK_SAMPLE_INTERVAL_PASSES - 1;
    return true;
  }

  /** A QSBR epoch deadline is real work; an idle cache with no retirements still parks forever. */
  private long epochAdvanceDelayNanos(long nowNanos) {
    if (!retirements.hasPendingReclaim()) {
      return Long.MAX_VALUE;
    }
    if (latestRetireEpoch < epoch) {
      return reclaimBlocked ? EPOCH_ADVANCE_INTERVAL_NANOS : Long.MAX_VALUE;
    }
    if (!hasAdvancedEpoch) {
      return 0L;
    }
    long elapsed = nowNanos - lastEpochAdvanceNanos;
    return elapsed >= EPOCH_ADVANCE_INTERVAL_NANOS ? 0L : EPOCH_ADVANCE_INTERVAL_NANOS - elapsed;
  }

  private long nextParkDelayNanos() {
    ownerLock.lock();
    try {
      long delay = wheel.nextDelayNanos(nowMillis);
      if (deferredMutationRetryNanos != Long.MAX_VALUE) {
        long retryDelay = deferredMutationRetryNanos - ticker.nanos();
        if (retryDelay <= 0L) {
          delay = 1L;
        } else {
          delay = Math.min(delay, retryDelay);
        }
      }
      if (policy.usedBytes() <= capacity || evictionRetryNanos == Long.MAX_VALUE) {
        return delay;
      }
      long evictionDelay = evictionRetryNanos - ticker.nanos();
      return evictionDelay <= 0L ? 1L : Math.min(delay, evictionDelay);
    } finally {
      ownerLock.unlock();
    }
  }

  private boolean evictionWorkDue() {
    ownerLock.lock();
    try {
      if (policy.usedBytes() <= capacity) {
        return false;
      }
      return evictionRetryNanos <= ticker.nanos();
    } finally {
      ownerLock.unlock();
    }
  }

  private void scheduleEvictionRetry() {
    long now = ticker.nanos();
    evictionRetryNanos = now + evictionRetryBackoffNanos;
    evictionRetryBackoffNanos = Math.min(1_000_000L, evictionRetryBackoffNanos << 1);
  }

  private int drainAccesses(int limit) {
    if (!accessHint.getAndSet(false)) {
      return 0;
    }
    int work = 0;
    for (WeakReference<ReaderSlot> reference : readers.snapshot()) {
      ReaderSlot slot = reference.get();
      if (slot == null) {
        continue;
      }
      if (!slot.hasAccessPending() && slot.access.isEmpty()) {
        continue;
      }
      // Clear before reading the counters/ring. A producer racing after this point either
      // publishes data into this scan or leaves the hint set for the next pass.
      slot.clearAccessPending();
      long hitDelta = slot.publishedHits - slot.consumedHits;
      long missDelta = slot.publishedMisses - slot.consumedMisses;
      long droppedDelta = slot.publishedAccessDropped - slot.consumedAccessDropped;
      if (hitDelta != 0L) {
        hits.addAndGet(hitDelta);
        slot.consumedHits += hitDelta;
      }
      if (missDelta != 0L) {
        misses.addAndGet(missDelta);
        slot.consumedMisses += missDelta;
      }
      if (droppedDelta != 0L) {
        accessDropped.addAndGet(droppedDelta);
        slot.consumedAccessDropped += droppedDelta;
      }
      while (work < limit && slot.access.poll(this)) {
        work++;
      }
      if (work >= limit) {
        // The original hint may cover more than one bounded batch. Keep the hint live
        // until the ring is actually empty instead of relying on a future read to wake us.
        if (!slot.access.isEmpty()) {
          slot.markAccessPending();
          accessHint.set(true);
        }
        return work;
      }
    }
    return work;
  }

  @Override
  public void accept(Entry entry, long generation) {
    if (entry.generation() == generation && isCurrent(entry)) {
      policy.access(entry);
      policy.recordAccessHit();
    }
  }

  private int reclaim(int limit) {
    long before = retirements.retiredBytes();
    int reclaimed = retirements.reclaim(readers, limit);
    budget.release(before - retirements.retiredBytes());
    // An active reader cannot be observed through a write-side queue event. Keep a bounded
    // QSBR deadline while it remains active so its ordinary exit need not perform a WakeGate
    // CAS on every cache operation. This is deadline-driven reclaim work, not idle polling.
    // A reclaim pass can make progress in one stripe and then stop at the first record in a
    // later stripe whose reader is still active. Treat that partial-progress case as blocked
    // too; otherwise latestRetireEpoch < epoch makes epochAdvanceDelayNanos() return
    // Long.MAX_VALUE and the worker can park forever with a full retirement ring.
    reclaimBlocked = reclaimed < limit && retirements.hasPendingReclaim();
    reclaimContinuation = reclaimed == limit && retirements.hasPendingReclaim();
    return reclaimed;
  }

  @Override
  public void expire(Entry entry, long expectedGeneration, long expectedValueAddress) {
    long taggedAddress = entry.valueAddress;
    long address = Entry.rawValueAddress(taggedAddress);
    if (taggedAddress != expectedValueAddress
        || address == 0L
        || !Entry.hasTtl(taggedAddress)
        || !ValueBlock.expired(address, nowMillis)) {
      return;
    }
    logicalExpired.incrementAndGet();
    long expiry = ValueBlock.expireAtMillis(address);
    if (expiry > 0L) {
      updateMax(timeoutLagMillis, Math.max(0L, nowMillis - expiry));
    }
    if (removeFromMap(entry, false, expectedGeneration, expectedValueAddress)) {
      physicalExpired.incrementAndGet();
    } else {
      if (entry.valueAddress == expectedValueAddress && isCurrent(entry)) {
        wheel.add(entry, ValueBlock.expireAtMillis(address));
      }
    }
  }

  private int evictIfNeeded(int limit) {
    int work = 0;
    int scans = 0;
    long target = capacity;
    boolean scanExhausted = false;
    evictionBlockedOnWriter = false;
    while (work < limit && scans < limit && policy.usedBytes() > target) {
      MaintenancePolicy.Selection selection = policy.selectVictim(limit - scans);
      int selectionScans = policy.lastVictimScanCount();
      scans += selectionScans;
      if (selection.kind == MaintenancePolicy.Selection.Kind.SCAN_EXHAUSTED) {
        scanExhausted = true;
        break;
      }
      if (selection.kind == MaintenancePolicy.Selection.Kind.NONE) {
        break;
      }
      Entry victim = selection.entry;
      long victimHash = victim.keyHash64();
      long expectedGeneration = victim.generation();
      long expectedValueAddress = victim.valueAddress;
      if (!removeFromMap(victim, true, expectedGeneration, expectedValueAddress, victimHash)
          && (victim.valueAddress == 0L || !isCurrent(victim))) {
        policy.remove(victim, false);
        policyDirty = true;
        work++;
      } else {
        if (victim.valueAddress != 0L && isCurrent(victim) && victim.isWriterLocked()) {
          policy.skipLocked(victim);
          evictionLockedSkips.incrementAndGet();
          evictionBlockedOnWriter = true;
          scheduleEvictionRetry();
        } else {
          scheduleEvictionRetry();
          break;
        }
      }
    }
    if (scans != 0) {
      evictionScans.addAndGet(scans);
    }
    if (policy.usedBytes() <= target) {
      evictionBlockedOnWriter = false;
      evictionRetryNanos = Long.MAX_VALUE;
      evictionRetryBackoffNanos = 1_000L;
    } else {
      if (scanExhausted || evictionRetryNanos == Long.MAX_VALUE) {
        scheduleEvictionRetry();
      }
    }
    return work;
  }

  public boolean removeFromMap(
      Entry entry, boolean eviction, long expectedGeneration, long expectedValueAddress) {
    return removeFromMap(entry, eviction, expectedGeneration, expectedValueAddress, 0L, false);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      long evictionHash) {
    return removeFromMap(
        entry, eviction, expectedGeneration, expectedValueAddress, evictionHash, true);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      long evictionHash,
      boolean hashProvided) {
    if (expectedValueAddress == 0L) {
      return false;
    }
    if (!entry.claimWriter()) {
      return false;
    }
    boolean removed = false;
    boolean retirementPrepared = false;
    long value = 0L;
    try {
      if (entry.generation() != expectedGeneration || entry.valueAddress != expectedValueAddress) {
        return false;
      }
      if (!prepareActorRetirement(2)) {
        return false;
      }
      retirementPrepared = true;
      removed = removeCurrent(entry);
      if (removed) {
        value = Entry.rawValueAddress(entry.valueAddress);
        entry.valueAddress = 0L;
        clearRepairWork(entry);
      }
    } finally {
      entry.finishWriter();
    }
    if (!removed) {
      if (retirementPrepared) {
        retirements.cancelPrefix(actorRetirement, 2);
      }
      return false;
    }
    wheel.remove(entry);
    if (hashProvided) {
      policy.remove(entry, eviction, evictionHash);
    } else {
      policy.remove(entry, eviction);
    }
    policyDirty = true;
    retireEntryBlocks(entry, value);
    if (eviction) {
      evicted.incrementAndGet();
    }
    return true;
  }

  private void retireEntryBlocks(Entry entry, long value) {
    long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
    retireActorValue(value, valueAllocation);
    long keyAllocation = entry.keyAllocationLength();
    retireActorValue(entry.nativeKeyAddress, keyAllocation);
  }

  private void clearRepairWork(Entry entry) {
    if (entry.clearRepairMarker()) {
      decrementRepairDebt(entry);
    }
  }

  private boolean isCurrent(Entry entry) {
    return entry.isAlive();
  }

  private boolean removeCurrent(Entry entry) {
    if (!entry.isAlive()) {
      return false;
    }
    entry.markRetired();
    if (data.remove(entry, entry)) {
      return true;
    }
    entry.restoreAlive();
    return false;
  }

  private void shutdownAndFree() {
    finishActorRetirementBatch();
    while (hasActiveReaders()) {
      LockSupport.park(this);
    }
    reliableRemovals.drain();
    while (retirements.consumeReadyHint()) {
      int sealed = retirements.seal(Integer.MAX_VALUE, epoch);
      if (sealed != 0) {
        latestRetireEpoch = epoch;
      }
    }
    for (Entry entry : data.values()) {
      long value = Entry.rawValueAddress(entry.valueAddress);
      entry.valueAddress = 0L;
      if (value != 0L) {
        memory.releaseEntry(value, ValueBlock.allocationLength(ValueBlock.length(value)));
      }
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

  private boolean prepareActorRetirement(int records) {
    int batchRecords =
        Math.max(records, Math.min(RETIREMENT_BATCH_RECORDS, retirementRecordsPerStripe()));
    if (actorRetirement.active()) {
      if (retirements.remaining(actorRetirement) >= records) {
        return true;
      }
      retirements.cancel(actorRetirement);
    }
    if (retirements.reserve(actorRetirement, Math.max(records, batchRecords))) {
      return true;
    }
    // Actor eviction/timeout is allowed to make bounded progress before deferring. The
    // normal writer path performs the same sequence through assistMaintenance(); keeping it
    // here prevents a full retirement ring from becoming an actor-only dead zone.
    if (retirements.hasReadyHint()) {
      sealRetirements(1024);
    }
    forceAdvanceEpoch();
    if (retirements.hasPendingReclaim()) {
      reclaim(1024);
    }
    return retirements.reserve(actorRetirement, Math.max(records, batchRecords));
  }

  private void finishActorRetirementBatch() {
    if (actorRetirement.active()) {
      retirements.cancel(actorRetirement);
    }
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

  private void signalProgress() {
    progressLock.lock();
    try {
      progressVersion.incrementAndGet();
      progressChanged.signalAll();
    } finally {
      progressLock.unlock();
    }
  }

  private void signalProgressAll() {
    signalProgress();
  }

  private void signalProgressLocked() {
    signalProgress();
  }

  private void signalProgressAllLocked() {
    signalProgress();
  }

  private static int stripeCount() {
    int target = Math.max(1, Runtime.getRuntime().availableProcessors() * 4);
    int stripes = 1;
    while (stripes < target && stripes < (1 << 30)) {
      stripes <<= 1;
    }
    return stripes;
  }

  /** Keeps the fixed native retire transport within the cache's 512KiB ledger reserve. */
  private static int retirementRecordsPerStripe() {
    long perStripe = (512L << 10) / ((long) stripeCount() * 32L);
    int records = 2;
    while ((records << 1) <= perStripe) {
      records <<= 1;
    }
    return records;
  }

  private static void updateMax(AtomicLong target, long value) {
    while (true) {
      long current = target.get();
      if (value <= current || target.compareAndSet(current, value)) {
        return;
      }
    }
  }

  public static final class Snapshot {
    public final long hits;
    public final long misses;
    public final long accessDropped;
    public final long accepted;
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
    public final long retirementQueueDepth;
    public final long retirementQueueCapacity;
    public final long wakeSignals;
    public final long mergedWakeSignals;
    public final long assistCount;
    public final long assistWork;
    public final long waitCount;
    public final long waitNanos;
    public final long progressVersion;

    Snapshot(
        long hits,
        long misses,
        long accessDropped,
        long accepted,
        long applied,
        long evicted,
        long logicalExpired,
        long physicalExpired,
        long maintenanceLoopNanos,
        long liveWeight,
        long timeoutLagMillis,
        boolean unhealthy,
        long queueDepth,
        long retiredEntries,
        long retiredBytes,
        long oldestRetireEpoch,
        long policyEvictions,
        long evictionScans,
        long evictionLockedSkips,
        long timerBytes,
        long ttlBacklog,
        long sketchBytes,
        long ghostHeapBytes,
        long ledgerBytes,
        long queueCapacity,
        long retirementQueueDepth,
        long retirementQueueCapacity,
        long wakeSignals,
        long mergedWakeSignals,
        long assistCount,
        long assistWork,
        long waitCount,
        long waitNanos,
        long progressVersion) {
      this.hits = hits;
      this.misses = misses;
      this.accessDropped = accessDropped;
      this.accepted = accepted;
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
      this.retirementQueueDepth = retirementQueueDepth;
      this.retirementQueueCapacity = retirementQueueCapacity;
      this.wakeSignals = wakeSignals;
      this.mergedWakeSignals = mergedWakeSignals;
      this.assistCount = assistCount;
      this.assistWork = assistWork;
      this.waitCount = waitCount;
      this.waitNanos = waitNanos;
      this.progressVersion = progressVersion;
    }
  }
}
