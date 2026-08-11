package com.red.ohc.maintenance;

import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jctools.queues.MpscArrayQueue;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.RemovalCause;
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
  @FunctionalInterface
  public interface EvictionNotifier {
    void notify(Entry entry, long valueAddress, RemovalCause cause);
  }

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
  private static final int ASYNC_MUTATION_BATCH = 64;

  /**
   * A deferred mutation has a writer in progress; retry it without turning idle into a poll loop.
   */
  private static final long DEFERRED_MUTATION_RETRY_NANOS = 1_000L;

  private final ConcurrentHashMap<Entry, Entry> data;
  private final NativeMemory.Memory memory;
  private final Budget budget;
  private final Ticker ticker;
  private final long capacity;
  private final EvictionNotifier evictionNotifier;
  private final int queueCapacity;
  private final MpscArrayQueue<Entry> queue;

  /** Reliable removal transport: it is reserved before CHM removal and may contain tombstones. */
  private final ReliableRemovalQueue reliableRemovals;

  private final AtomicBoolean repairNeeded = new AtomicBoolean();
  private final AtomicLong repairVersion = new AtomicLong();
  private final AtomicBoolean allocationPressureRequested = new AtomicBoolean();
  private final AtomicBoolean budgetPressureRequested = new AtomicBoolean();
  private final ConcurrentLinkedQueue<Entry>[] repairQueues;
  private final int repairShardMask;
  private int repairShardCursor;
  private final AtomicLong[] repairDebts;
  private final ConcurrentLinkedQueue<AsyncMutationTask> asyncMutations =
      new ConcurrentLinkedQueue<>();
  /** Linearizes async sequence assignment with queue publication for the flush barrier. */
  private final Object asyncSubmissionLock = new Object();
  private final AtomicLong asyncSubmitted = new AtomicLong();
  private final AtomicLong asyncFailed = new AtomicLong();
  private final AtomicLong asyncRejected = new AtomicLong();
  private volatile long asyncCompletedSequence;

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

  private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
  private final AtomicReference<FlushRequest> flushRequest = new AtomicReference<>();
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
   * mutations. Reading actor-owned policy state from a cache caller would otherwise be a data race
   * with the actor.
   */
  private volatile long publishedLiveWeight;

  private long nowNanos;
  private int clockSampleCountdown;
  private volatile long hits;
  private volatile long misses;
  private volatile long physicalExpired;
  private volatile long timeoutLagMillis;
  private final AtomicLong nonBlockingPutFailures = new AtomicLong();
  private final AtomicLong nonBlockingReplaceFailures = new AtomicLong();
  private final AtomicLong nonBlockingRemoveFailures = new AtomicLong();
  private final AtomicLong writerContentionFailures = new AtomicLong();
  private final AtomicLong retirementAdmissionFailures = new AtomicLong();
  private final AtomicLong reliableRemovalAdmissionFailures = new AtomicLong();
  private final AtomicLong nativeAllocationFailures = new AtomicLong();

  private final AtomicBoolean unhealthy = new AtomicBoolean();

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
        null,
        readers,
        ChmSizing.maintenanceQueueCapacity(0L, capacity));
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Budget budget,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers) {
    this(
        data,
        memory,
        budget,
        ticker,
        capacity,
        eviction,
        evictionNotifier,
        readers,
        ChmSizing.maintenanceQueueCapacity(0L, capacity));
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
    this(data, memory, budget, ticker, capacity, eviction, null, readers, queueCapacity, false);
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Budget budget,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers,
      int queueCapacity) {
    this(
        data,
        memory,
        budget,
        ticker,
        capacity,
        eviction,
        evictionNotifier,
        readers,
        queueCapacity,
        false);
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Budget budget,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers,
      int queueCapacity,
      boolean countBounded) {
    this.data = data;
    this.memory = memory;
    this.budget = budget;
    this.ticker = ticker;
    this.capacity = capacity;
    this.evictionNotifier = evictionNotifier;
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
    ConcurrentLinkedQueue<Entry>[] repairQueues = new ConcurrentLinkedQueue[shardCount];
    for (int i = 0; i < shardCount; i++) {
      repairQueues[i] = new ConcurrentLinkedQueue<>();
    }
    this.repairQueues = repairQueues;
    this.repairShardMask = shardCount - 1;
    this.repairDebts = new AtomicLong[shardCount];
    for (int i = 0; i < shardCount; i++) {
      this.repairDebts[i] = new AtomicLong();
    }
    this.reliableRemovals = new ReliableRemovalQueue(queueCapacity);
    this.policy = new MaintenancePolicy(eviction, capacity, countBounded);
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
    // A close must not wait for the bounded producer-batch grace period.
    LockSupport.unpark(thread);
  }

  public void beginClosing() {
    closing = true;
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

  public long retiredBytes() {
    return retirements.retiredBytes();
  }

  public int retiredEntries() {
    return retirements.retiredEntries();
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

  public long ledgerBytes() {
    return retirements.allocatedBytes();
  }

  public long retirementQueueDepth() {
    return retirements.queuedRecords();
  }

  public long retirementQueueCapacity() {
    return retirements.capacityRecords();
  }

  public long repairQueueDepth() {
    return repairDebt();
  }

  public long asyncMutationQueueDepth() {
    return asyncMutations.size();
  }

  public long asyncMutationFailedCount() {
    return asyncFailed.get();
  }

  public long asyncMutationRejectedCount() {
    return asyncRejected.get();
  }

  /** Adds a mutation task without executing it on the calling thread. */
  public boolean submitAsyncMutation(Runnable action, Consumer<Throwable> reject) {
    if (action == null || reject == null) {
      throw new NullPointerException("action/reject");
    }
    Throwable rejection = null;
    try {
      synchronized (asyncSubmissionLock) {
        if (closing || stopping) {
          rejection = new IllegalStateException("cache is closing");
        } else {
          Throwable unavailable = terminalFailure.get();
          if (unavailable != null) {
            rejection =
                new com.red.ohc.api.CacheMaintenanceException(unavailable);
          } else {
            long sequence = asyncSubmitted.incrementAndGet();
            asyncMutations.offer(new AsyncMutationTask(sequence, action, reject));
          }
        }
      }
    } catch (OutOfMemoryError error) {
      asyncRejected.incrementAndGet();
      recordTerminalFailure(error);
      reject.accept(error);
      return false;
    }
    if (rejection != null) {
      asyncRejected.incrementAndGet();
      reject.accept(rejection);
      return false;
    }
    signal();
    return true;
  }

  public void registerReader(ReaderSlot slot) {
    readers.register(slot);
  }

  /** Reader-side access publication is the only reader activity that needs to wake the actor. */
  public void signalAccess(ReaderSlot slot) {
    if (accessHint.compareAndSet(false, true)) {
      signal();
    }
  }

  /** ReaderGuard calls this only after an active read becomes quiescent during close. */
  public void readerQuiescent() {
    if (stopping) {
      // shutdownAndFree parks outside WakeGate, so this must not rely on a REQUIRED gate
      // transition to wake the actor after its final reader leaves.
      LockSupport.unpark(thread);
    }
  }

  public void recordNonBlockingPutFailure() {
    nonBlockingPutFailures.incrementAndGet();
  }

  public void recordNonBlockingReplaceFailure() {
    nonBlockingReplaceFailures.incrementAndGet();
  }

  public void recordNonBlockingRemoveFailure() {
    nonBlockingRemoveFailures.incrementAndGet();
  }

  public void recordWriterContentionFailure() {
    writerContentionFailures.incrementAndGet();
  }

  public void recordRetirementAdmissionFailure() {
    retirementAdmissionFailures.incrementAndGet();
  }

  public void recordReliableRemovalAdmissionFailure() {
    reliableRemovalAdmissionFailures.incrementAndGet();
  }

  public void recordNativeAllocationFailure() {
    nativeAllocationFailures.incrementAndGet();
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

  public void recordTerminalFailure(Throwable failure) {
    if (failure == null) {
      throw new NullPointerException("failure");
    }
    FlushRequest request;
    List<AsyncMutationTask> pending;
    synchronized (asyncSubmissionLock) {
      if (!terminalFailure.compareAndSet(null, failure)) {
        return;
      }
      unhealthy.set(true);
      request = flushRequest.getAndSet(null);
      pending = drainPendingAsyncTasks();
    }
    com.red.ohc.api.CacheMaintenanceException unavailable =
        new com.red.ohc.api.CacheMaintenanceException(failure);
    if (request != null) {
      request.future.completeExceptionally(unavailable);
    }
    rejectAsyncTasks(pending, unavailable);
    signal();
  }

  /** Reliable removal only: ADD/UPDATE are published after their data-plane mutation. */
  public boolean reserveMutation(Entry entry, int flags) {
    if (flags != Entry.PENDING_REMOVE) {
      throw new IllegalArgumentException("only removal mutations require a reservation");
    }
    if (unhealthy.get()) {
      return false;
    }
    return entry.tryBeginPending(flags);
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
    // without adding repair debt or useful maintenance work.
    if (firstRepair) {
      enqueueRepair(entry, shard);
    }
  }

  private void enqueueRepair(Entry entry, int shard) {
    repairQueues[shard].offer(entry);
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
    if (allocationPressureRequested.compareAndSet(false, true)) {
      signal();
    }
  }

  /** Idle writer credit is reclaimed only after a budget reservation failure. */
  public void requestBudgetPressure() {
    if (budgetPressureRequested.compareAndSet(false, true)) {
      signal();
    }
  }

  /**
   * Reserves native retirement records before an index mutation publishes a replacement or removal.
   */
  public boolean prepareRetirement(com.red.ohc.runtime.ThreadContext context, int records) {
    return retirements.tryReserve(context.retirement, records);
  }

  /**
   * Reserves removal cleanup and a reliable queue slot before the caller changes CHM visibility.
   */
  public boolean prepareReliableRemoval(
      com.red.ohc.runtime.ThreadContext context, Entry entry) {
    if (context == null || entry == null) {
      throw new NullPointerException("context/entry");
    }
    throwIfUnavailable();
    if (!reliableRemovals.tryReserve(context.reliableRemoval)) {
      return false;
    }
    try {
      if (!retirements.tryReserve(context.retirement, 2)) {
        reliableRemovals.cancel(context.reliableRemoval);
        signal();
        return false;
      }
      if (!reserveMutation(entry, Entry.PENDING_REMOVE)) {
        retirements.cancel(context.retirement);
        reliableRemovals.cancel(context.reliableRemoval);
        signal();
        return false;
      }
      return true;
    } catch (Throwable failure) {
      if (context.retirement.active()) {
        retirements.cancel(context.retirement);
      }
      if (context.reliableRemoval.active()) {
        reliableRemovals.cancel(context.reliableRemoval);
      }
      signal();
      throw failure;
    }
  }

  /**
   * Commits a pre-reserved removal notification before publishing its retirement records.
   *
   * <p>The notification may deserialize the supplied value and the entry key. The single
   * maintenance owner therefore observes the notification before either native address becomes
   * reclaimable.
   */
  public void publishRemovalAndRetire(
      com.red.ohc.runtime.ThreadContext context,
      Entry entry,
      boolean wake,
      long valueAddress,
      long valueAllocation,
      RemovalCause cause) {
    entry.completePendingClaim();
    reliableRemovals.commit(context.reliableRemoval, entry, valueAddress, cause);
    retireValue(context, valueAddress, valueAllocation);
    retireValue(context, entry.nativeKeyAddress, entry.keyAllocationLength());
    if (wake) {
      signal();
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
      signal();
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
    synchronized (asyncSubmissionLock) {
      failure = terminalFailure.get();
      if (failure != null) {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(
            new com.red.ohc.api.CacheMaintenanceException(failure));
        return failed;
      }
      FlushRequest existing = flushRequest.get();
      if (existing != null) {
        existing.sequence = asyncSubmitted.get();
        clockRefreshRequested = true;
        signal();
        return existing.future;
      }
      FlushRequest created =
          new FlushRequest(asyncSubmitted.get(), new CompletableFuture<>());
      if (!flushRequest.compareAndSet(null, created)) {
        return flushRequest.get().future;
      }
      clockRefreshRequested = true;
      signal();
      return created.future;
    }
  }

  public Snapshot snapshot() {
    return new Snapshot(
        hits,
        misses,
        policy.evictions(),
        physicalExpired,
        publishedLiveWeight,
        timeoutLagMillis,
        unhealthy.get(),
        queueDepth(),
        retiredEntries(),
        retiredBytes(),
        timerBytes(),
        ttlBacklog(),
        sketchBytes(),
        ghostHeapBytes(),
        ledgerBytes(),
        queueCapacity,
        retirementQueueDepth(),
        retirementQueueCapacity(),
        nonBlockingPutFailures.get(),
        nonBlockingReplaceFailures.get(),
        nonBlockingRemoveFailures.get(),
        writerContentionFailures.get(),
        retirementAdmissionFailures.get(),
        reliableRemovalAdmissionFailures.get(),
        nativeAllocationFailures.get(),
        repairQueueDepth(),
        asyncMutationQueueDepth(),
        asyncMutationFailedCount(),
        asyncMutationRejectedCount());
  }

  @Override
  public void run() {
    wakeGate.requireProcessing();
    while (!stopping || hasWork()) {
      if (stopping && terminalFailure.get() != null) {
        // A terminal failure has invalidated maintenance state. Once close has stopped the
        // actor, retrying a pending reservation can spin forever; teardown owns the final
        // native cleanup and does not require the maintenance transport to make progress.
        break;
      }
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
      try {
        work = maintenancePass();
      } catch (Throwable failure) {
        recordTerminalFailure(failure);
        work = 0;
      } finally {
        finishActorRetirementBatch();
      }
      if (!hasImmediateSourceWork()) {
        parkUntilWorkOrTimer(work != 0 && !stopping);
      }
    }
    shutdownAndFree();
  }

  private int maintenancePass() {
    sampleClockIfDue();
    int work = 0;
    if (budgetPressureRequested.get() && budgetPressureRequested.getAndSet(false)) {
      budget.reclaimIdleLeases();
      work++;
    }
    boolean mutationsPending =
        !queue.isEmpty() || repairNeeded.get() || !deferredMutations.isEmpty();
    if (mutationsPending) {
      policy.beginWriteBatch();
    }
    if (reliableRemovals.hasCommittedHead()) {
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
    // pointer or pending flag.
    if (wheel.hasPending()) {
      work += wheel.advance(nowMillis, 1_024, this);
    }
    if (retirements.hasReadyHint()) {
      work += sealRetirements(1024);
    }
    if (advanceEpochIfDue(nowNanos)) {
      work++;
    }
    if (retirements.hasPendingReclaim()) {
      work += reclaim(1024);
    }
    if (!stopping && evictionWorkDue()) {
      try {
        work += evictIfNeeded(64);
      } finally {
        finishActorRetirementBatch();
      }
    }
    if (allocationPressureRequested.get() && allocationPressureRequested.getAndSet(false)) {
      work += memory.trimIdlePages() > 0L ? 1 : 0;
    }
    work += drainAsyncMutations(ASYNC_MUTATION_BATCH);
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
    return hasSourceWork()
        || reliableRemovals.size() != 0L
        || !asyncMutations.isEmpty()
        || retirements.hasPendingReclaim();
  }

  private boolean hasSourceWork() {
    return hasImmediateSourceWork() || !deferredMutations.isEmpty();
  }

  private boolean hasImmediateSourceWork() {
    return reliableRemovals.hasCommittedHead()
        || !queue.isEmpty()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || budgetPressureRequested.get()
        || accessHint.get()
        || !asyncMutations.isEmpty()
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
        long wheelDelay = wheel.nextDelayNanos(nowMillis);
        long timerDelay = nextParkDelayNanos(wheelDelay);
        long parkDelay = Math.min(WRITE_BATCH_GRACE_NANOS, timerDelay);
        LockSupport.parkNanos(this, parkDelay);
        if (timerDelay <= parkDelay) {
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
    int sealed = sealRetirements(1024);
    if (sealed != 0) {
      wakeGate.requireProcessing();
      return;
    }
    if (!wakeGate.finishIdle()) {
      return;
    }
    parked = true;
    try {
      long wheelDelay = wheel.nextDelayNanos(nowMillis);
      long delay = nextParkDelayNanos(wheelDelay);
      if (delay == Long.MAX_VALUE) {
        LockSupport.park(this);
      } else {
        LockSupport.parkNanos(this, delay);
        // A finite park is always bounded by real timer, retry, or QSBR work.
        clockRefreshRequested = true;
      }
    } finally {
      parked = false;
      wakeGate.requireProcessing();
    }
  }

  private void completeFlushIfIdle() {
    if (flushRequest.get() == null) {
      return;
    }
    FlushRequest completed;
    synchronized (asyncSubmissionLock) {
      FlushRequest request = flushRequest.get();
      if (request == null) {
        return;
      }
      // A bounded eviction scan may have deferred its next retry to protect the worker from
      // repeated scans. Flush must retain its barrier until that retry brings the policy back under
      // capacity, including when the previous scan was blocked by a writer.
      if (policy.usedWeight() > capacity) {
        return;
      }
      if (clockRefreshRequested) {
        return;
      }
      if (reliableRemovals.size() != 0L
          || !queue.isEmpty()
          || !deferredMutations.isEmpty()
          || repairNeeded.get()
          || asyncCompletedSequence < request.sequence
          || allocationPressureRequested.get()
          || budgetPressureRequested.get()
          || accessHint.get()
          || retirements.hasReadyHint()
          || retirements.hasPendingReclaim()
          || reclaimContinuation) {
        return;
      }
      if (!flushRequest.compareAndSet(request, null)) {
        return;
      }
      completed = request;
    }
    completed.future.complete(null);
  }

  private int drainAsyncMutations(int limit) {
    if (asyncMutations.isEmpty()) {
      return 0;
    }
    int work = 0;
    while (work < limit) {
      FlushRequest barrier = flushRequest.get();
      AsyncMutationTask next = asyncMutations.peek();
      if (next == null || (barrier != null && next.sequence > barrier.sequence)) {
        break;
      }
      AsyncMutationTask task = asyncMutations.poll();
      if (closing || stopping) {
        asyncRejected.incrementAndGet();
        task.reject.accept(new IllegalStateException("cache is closing"));
      } else {
        try {
          task.action.run();
        } catch (Throwable failure) {
          asyncFailed.incrementAndGet();
          task.reject.accept(failure);
        }
      }
      asyncCompletedSequence = task.sequence;
      work++;
    }
    return work;
  }

  private void rejectPendingAsync(Throwable failure) {
    rejectAsyncTasks(drainPendingAsyncTasks(), failure);
  }

  private List<AsyncMutationTask> drainPendingAsyncTasks() {
    List<AsyncMutationTask> pending = new ArrayList<>();
    AsyncMutationTask task;
    while ((task = asyncMutations.poll()) != null) {
      asyncCompletedSequence = task.sequence;
      pending.add(task);
    }
    return pending;
  }

  private void rejectAsyncTasks(List<AsyncMutationTask> tasks, Throwable failure) {
    for (AsyncMutationTask task : tasks) {
      asyncRejected.incrementAndGet();
      task.reject.accept(failure);
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
    while (work < limit && reliableRemovals.hasCommittedHead()) {
      ReliableRemovalQueue.Notification notification =
          evictionNotifier == null ? null : reliableRemovals.pollNotification();
      Entry entry = notification == null ? reliableRemovals.poll() : notification.entry;
      if (entry != null) {
        if (notification != null && notification.cause != null) {
          evictionNotifier.notify(
              entry, notification.valueAddress, notification.cause);
        }
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
    for (ConcurrentLinkedQueue<Entry> queue : repairQueues) {
      if (!queue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private void processEntry(Entry entry) {
    int flags = entry.takePending();
    // tryBeginPending() holds this claim from reservation through pointer publication. It is
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
    if (!entry.markAppliedVersion(version)) {
      republishMutation(entry, flags);
    }
  }

  /** Compatibility entry point used by actor-focused tests to apply one current mutation. */
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

  private void sampleClockIfDue() {
    if (!clockRefreshRequested && clockSampleCountdown-- > 0) {
      return;
    }
    clockRefreshRequested = false;
    nowNanos = ticker.nanos();
    nowMillis = ticker.currentTimeMillis();
    clockSampleCountdown = CLOCK_SAMPLE_INTERVAL_PASSES - 1;
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

  private long nextParkDelayNanos(long wheelDelay) {
    boolean evictionDeadline =
        policy.usedWeight() > capacity && evictionRetryNanos != Long.MAX_VALUE;
    boolean epochDeadline = retirements.hasPendingReclaim();
    if (deferredMutationRetryNanos == Long.MAX_VALUE && !evictionDeadline && !epochDeadline) {
      return wheelDelay;
    }
    long monotonicNow = ticker.nanos();
    long delay = wheelDelay;
    if (deferredMutationRetryNanos != Long.MAX_VALUE) {
      delay = Math.min(delay, deadlineDelayNanos(deferredMutationRetryNanos, monotonicNow));
    }
    if (evictionDeadline) {
      delay = Math.min(delay, deadlineDelayNanos(evictionRetryNanos, monotonicNow));
    }
    if (epochDeadline) {
      delay = Math.min(delay, epochAdvanceDelayNanos(monotonicNow));
    }
    return delay;
  }

  private boolean evictionWorkDue() {
    if (policy.usedWeight() <= capacity) {
      return false;
    }
    return evictionRetryNanos == Long.MAX_VALUE || evictionRetryNanos <= ticker.nanos();
  }

  private static long deadlineDelayNanos(long deadline, long now) {
    long delay = deadline - now;
    return delay <= 0L ? 1L : delay;
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
      if (hitDelta != 0L) {
        hits += hitDelta;
        slot.consumedHits += hitDelta;
      }
      if (missDelta != 0L) {
        misses += missDelta;
        slot.consumedMisses += missDelta;
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
    long expiry = ValueBlock.expireAtMillis(address);
    if (expiry > 0L) {
      timeoutLagMillis = Math.max(timeoutLagMillis, Math.max(0L, nowMillis - expiry));
    }
    if (removeFromMap(
        entry, false, expectedGeneration, expectedValueAddress, RemovalCause.EXPIRED)) {
      physicalExpired++;
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
    while (work < limit && scans < limit && policy.usedWeight() > target) {
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
      if (!removeFromMap(
              victim,
              true,
              expectedGeneration,
              expectedValueAddress,
              victimHash,
              RemovalCause.SIZE)
          && (victim.valueAddress == 0L || !isCurrent(victim))) {
        policy.remove(victim, false);
        policyDirty = true;
        work++;
      } else {
        if (victim.valueAddress != 0L && isCurrent(victim) && victim.isWriterLocked()) {
          policy.skipLocked(victim);
          scheduleEvictionRetry();
        } else {
          scheduleEvictionRetry();
          break;
        }
      }
    }
    if (policy.usedWeight() <= target) {
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
    return removeFromMap(
        entry, eviction, expectedGeneration, expectedValueAddress, 0L, false, null);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      RemovalCause cause) {
    return removeFromMap(
        entry, eviction, expectedGeneration, expectedValueAddress, 0L, false, cause);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      long evictionHash,
      RemovalCause cause) {
    return removeFromMap(
        entry, eviction, expectedGeneration, expectedValueAddress, evictionHash, true, cause);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      long evictionHash,
      boolean hashProvided,
      RemovalCause cause) {
    if (expectedValueAddress == 0L) {
      return false;
    }
    if (!entry.claimWriter()) {
      return false;
    }
    boolean removed = false;
      long value = 0L;
    try {
      if (entry.generation() != expectedGeneration || entry.valueAddress != expectedValueAddress) {
        return false;
      }
      if (!prepareActorRetirement(2)) {
        return false;
      }
        removed = removeCurrent(entry);
      if (removed) {
        value = Entry.rawValueAddress(entry.valueAddress);
        entry.clearValue();
        clearRepairWork(entry);
      }
    } finally {
      entry.finishWriter();
    }
    if (!removed) {
      finishActorRetirementBatch();
      return false;
    }
    wheel.remove(entry);
    if (hashProvided) {
      policy.remove(entry, eviction, evictionHash);
    } else {
      policy.remove(entry, eviction);
    }
    policyDirty = true;
    if (cause != null && evictionNotifier != null) {
      evictionNotifier.notify(entry, value, cause);
    }
    retireEntryBlocks(entry, value);
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
    rejectPendingAsync(new IllegalStateException("cache is closed"));
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
    clearPendingMutationQueues();
    for (Entry entry : data.values()) {
      long value = Entry.rawValueAddress(entry.valueAddress);
      entry.clearValue();
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

  private void clearPendingMutationQueues() {
    while (queue.poll() != null) {
      // Drop the transport reference without touching entry ownership.
    }
    for (ConcurrentLinkedQueue<Entry> repairQueue : repairQueues) {
      repairQueue.clear();
    }
    deferredMutations.clear();
    repairNeeded.set(false);
    for (AtomicLong debt : repairDebts) {
      debt.set(0L);
    }
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
    // Actor eviction/timeout is allowed to make bounded progress before deferring so a full
    // retirement ring does not become an actor-only dead zone.
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
      LockSupport.unpark(thread);
    }
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

  private static final class AsyncMutationTask {
    private final long sequence;
    private final Runnable action;
    private final Consumer<Throwable> reject;

    private AsyncMutationTask(long sequence, Runnable action, Consumer<Throwable> reject) {
      this.sequence = sequence;
      this.action = action;
      this.reject = reject;
    }
  }

  private static final class FlushRequest {
    private volatile long sequence;
    private final CompletableFuture<Void> future;

    private FlushRequest(long sequence, CompletableFuture<Void> future) {
      this.sequence = sequence;
      this.future = future;
    }
  }

  public static final class Snapshot {
    public final long hits;
    public final long misses;
    public final long evicted;
    public final long physicalExpired;
    public final long liveWeight;
    public final long timeoutLagMillis;
    public final boolean unhealthy;
    public final long queueDepth;
    public final long retiredEntries;
    public final long retiredBytes;
    public final long timerBytes;
    public final long ttlBacklog;
    public final long sketchBytes;
    public final long ghostHeapBytes;
    public final long ledgerBytes;
    public final long queueCapacity;
    public final long retirementQueueDepth;
    public final long retirementQueueCapacity;
    public final long nonBlockingPutFailures;
    public final long nonBlockingReplaceFailures;
    public final long nonBlockingRemoveFailures;
    public final long writerContentionFailures;
    public final long retirementAdmissionFailures;
    public final long reliableRemovalAdmissionFailures;
    public final long nativeAllocationFailures;
    public final long repairQueueDepth;
    public final long asyncMutationQueueDepth;
    public final long asyncMutationFailedCount;
    public final long asyncMutationRejectedCount;

    Snapshot(
        long hits,
        long misses,
        long evicted,
        long physicalExpired,
        long liveWeight,
        long timeoutLagMillis,
        boolean unhealthy,
        long queueDepth,
        long retiredEntries,
        long retiredBytes,
        long timerBytes,
        long ttlBacklog,
        long sketchBytes,
        long ghostHeapBytes,
        long ledgerBytes,
        long queueCapacity,
        long retirementQueueDepth,
        long retirementQueueCapacity,
        long nonBlockingPutFailures,
        long nonBlockingReplaceFailures,
        long nonBlockingRemoveFailures,
        long writerContentionFailures,
        long retirementAdmissionFailures,
        long reliableRemovalAdmissionFailures,
        long nativeAllocationFailures,
        long repairQueueDepth,
        long asyncMutationQueueDepth,
        long asyncMutationFailedCount,
        long asyncMutationRejectedCount) {
      this.hits = hits;
      this.misses = misses;
      this.evicted = evicted;
      this.physicalExpired = physicalExpired;
      this.liveWeight = liveWeight;
      this.timeoutLagMillis = timeoutLagMillis;
      this.unhealthy = unhealthy;
      this.queueDepth = queueDepth;
      this.retiredEntries = retiredEntries;
      this.retiredBytes = retiredBytes;
      this.timerBytes = timerBytes;
      this.ttlBacklog = ttlBacklog;
      this.sketchBytes = sketchBytes;
      this.ghostHeapBytes = ghostHeapBytes;
      this.ledgerBytes = ledgerBytes;
      this.queueCapacity = queueCapacity;
      this.retirementQueueDepth = retirementQueueDepth;
      this.retirementQueueCapacity = retirementQueueCapacity;
      this.nonBlockingPutFailures = nonBlockingPutFailures;
      this.nonBlockingReplaceFailures = nonBlockingReplaceFailures;
      this.nonBlockingRemoveFailures = nonBlockingRemoveFailures;
      this.writerContentionFailures = writerContentionFailures;
      this.retirementAdmissionFailures = retirementAdmissionFailures;
      this.reliableRemovalAdmissionFailures = reliableRemovalAdmissionFailures;
      this.nativeAllocationFailures = nativeAllocationFailures;
      this.repairQueueDepth = repairQueueDepth;
      this.asyncMutationQueueDepth = asyncMutationQueueDepth;
      this.asyncMutationFailedCount = asyncMutationFailedCount;
      this.asyncMutationRejectedCount = asyncMutationRejectedCount;
    }
  }
}
