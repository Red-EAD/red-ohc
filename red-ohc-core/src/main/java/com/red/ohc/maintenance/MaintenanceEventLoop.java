package com.red.ohc.maintenance;

import java.lang.ref.ReferenceQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
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
import com.red.ohc.runtime.AccessRing;
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
  private static final long WINDOW_NANOS = 1_000_000L;
  private static final long EVICTION_RETRY_INITIAL_NANOS = WINDOW_NANOS;
  private static final long EVICTION_RETRY_MAX_NANOS = 10_000_000L;
  private static final long RECLAIM_RETRY_INITIAL_NANOS = WINDOW_NANOS;
  private static final long RECLAIM_RETRY_MAX_NANOS = 10_000_000L;
  /**
   * A deferred mutation has a writer in progress. Retry it with bounded backoff rather than
   * converting one long writer publication into a microsecond poll loop.
   */
  private static final long DEFERRED_MUTATION_RETRY_INITIAL_NANOS = WINDOW_NANOS;
  private static final long DEFERRED_MUTATION_RETRY_MAX_NANOS = 10_000_000L;

  private static final int WORK_MUTATION = 1;
  private static final int WORK_REMOVAL = 1 << 1;
  private static final int WORK_ACCESS = 1 << 2;
  private static final int WORK_PRESSURE = 1 << 3;
  private static final int WORK_RETIREMENT = 1 << 4;
  private static final int WORK_ASYNC = 1 << 5;
  private static final int WORK_FLUSH = 1 << 6;
  private static final int WORK_CLOCK = 1 << 7;
  private static final int WORK_WEAK_VALUES = 1 << 8;
  private static final long WEAK_VALUE_CLEANUP_INTERVAL_NANOS = 1_000_000_000L;

  private final ConcurrentHashMap<Entry, Entry> data;
  private final NativeMemory.Memory memory;
  private final Budget budget;
  private final Ticker ticker;
  private final long capacity;
  private final boolean countBounded;
  private volatile EvictionNotifier evictionNotifier;
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
  private final AtomicIntegerArray repairShardDirty;
  private final RepairShardToken[] repairShardTokens;
  private final ConcurrentLinkedQueue<RepairShardToken> dirtyRepairShardQueue =
      new ConcurrentLinkedQueue<>();
  private final AtomicLong repairDebtTotal = new AtomicLong();
  private final ConcurrentLinkedQueue<AsyncMutationTask> asyncMutations =
      new ConcurrentLinkedQueue<>();
  /** Linearizes async sequence assignment with queue publication for the flush barrier. */
  private final Object asyncSubmissionLock = new Object();
  private final AtomicLong asyncSubmitted = new AtomicLong();
  private final AtomicLong asyncFailed = new AtomicLong();
  private final AtomicLong asyncRejected = new AtomicLong();
  /** Last FIFO sequence removed from the physical queue; executing work is already dequeued. */
  private final AtomicLong asyncDequeuedSequence = new AtomicLong();
  private volatile long asyncCompletedSequence;

  /** Actor-owned Entries whose transport item arrived while a writer owns the Entry mutex. */
  private final ArrayDeque<Entry> deferredMutations = new ArrayDeque<>();

  private long deferredMutationRetryNanos = Long.MAX_VALUE;
  private long deferredMutationRetryBackoffNanos = DEFERRED_MUTATION_RETRY_INITIAL_NANOS;
  private final AtomicBoolean accessHint = new AtomicBoolean();
  /** Reusable normal-path storage; a steady access stream must not allocate queue nodes. */
  private final MpscArrayQueue<ReaderSlot> dirtyReaderQueue;
  /** Bounded-queue overflow is rare and must retain tokens rather than dropping them. */
  private final ConcurrentLinkedQueue<ReaderSlot> dirtyReaderOverflowQueue =
      new ConcurrentLinkedQueue<>();
  private final ReaderRegistry readers;
  private final RetirementQueue retirements;
  private final int retirementRecordsPerStripe;
  private final int actorRetirementBatchRecords;
  private final RetirementQueue.Reservation actorRetirement = new RetirementQueue.Reservation();
  private final ReliableRemovalQueue.Record removalRecord = new ReliableRemovalQueue.Record();
  private final TimerWheel wheel;
  private final MaintenancePolicy policy;
  private final Thread thread;
  private final WakeGate wakeGate = new WakeGate();
  private final AtomicInteger requestedWork = new AtomicInteger();
  private final BatchLimits batchLimits = BatchLimits.forCpu(NativeMemory.LOGICAL_CPU_COUNT);
  private final WindowScheduler windowScheduler = new WindowScheduler();
  private final WorkPlan workPlan = new WorkPlan();

  private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
  private final AtomicReference<FlushRequest> flushRequest = new AtomicReference<>();
  private volatile boolean closing;
  private volatile boolean stopping;
  private volatile boolean parked;
  private volatile ReferenceQueue<Object> weakValueQueue;
  private boolean weakValueCleanupContinuation;

  /** Incremented before the actor's final empty-source check for an idle park. */

  private volatile long epoch = 1L;

  /** Actor-owned epoch of the newest sealed retirement record. */
  private long latestRetireEpoch;

  /** Actor-owned timestamp of the most recent epoch advance. */
  private long lastEpochAdvanceNanos;

  private boolean hasAdvancedEpoch;
  private volatile long nowMillis;

  /**
   * Cross-thread snapshot of the actor-owned policy weight. Writers never update this value: it is
   * deliberately published only after a bounded maintenance pass has applied all of its policy
   * mutations. Reading actor-owned policy state from a cache caller would otherwise be a data race
   * with the actor.
   */
  private volatile long publishedLiveWeight;

  private long nowNanos;
  private boolean monotonicSampledThisPass;
  private volatile long hits;
  private volatile long misses;
  private volatile long physicalExpired;
  private final AtomicLong entryResidenceCount = new AtomicLong();
  private final AtomicLong totalEntryResidenceTimeMillis = new AtomicLong();
  private volatile long timeoutLagMillis;
  private final AtomicLong nativeAllocationFailures = new AtomicLong();

  private final AtomicBoolean unhealthy = new AtomicBoolean();

  private long evictionRetryNanos = Long.MAX_VALUE;
  private long evictionRetryBackoffNanos = EVICTION_RETRY_INITIAL_NANOS;

  /** A sealed retirement could not pass QSBR; recheck it on the bounded epoch deadline. */
  private boolean reclaimBlocked;
  private long reclaimRetryNanos = Long.MAX_VALUE;
  private long reclaimRetryBackoffNanos = RECLAIM_RETRY_INITIAL_NANOS;

  /** A bounded reclaim pass hit its limit and must be continued before the worker can park. */
  private boolean reclaimContinuation;

  /** The actor has processed a removal but is waiting for two retirement slots. */
  private boolean pendingRemovalRetirement;
  private Throwable pendingRemovalFailure;
  private long pendingRemovalRetryNanos = Long.MAX_VALUE;
  private long pendingRemovalRetryBackoffNanos = WINDOW_NANOS;

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
    this.countBounded = countBounded;
    this.evictionNotifier = evictionNotifier;
    if (Integer.bitCount(queueCapacity) != 1 || queueCapacity < 1_024) {
      throw new IllegalArgumentException("queueCapacity must be a power of two >= 1024");
    }
    this.queueCapacity = queueCapacity;
    this.queue = new MpscArrayQueue<>(queueCapacity);
    this.dirtyReaderQueue = new MpscArrayQueue<>(queueCapacity);
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
    this.repairShardDirty = new AtomicIntegerArray(shardCount);
    this.repairShardTokens = new RepairShardToken[shardCount];
    for (int i = 0; i < shardCount; i++) {
      this.repairShardTokens[i] = new RepairShardToken(i);
    }
    this.reliableRemovals = new ReliableRemovalQueue(queueCapacity);
    this.policy = new MaintenancePolicy(eviction, capacity, countBounded);
    this.readers = readers;
    this.retirementRecordsPerStripe = NativeMemory.defaultRetirementRecordsPerStripe();
    this.actorRetirementBatchRecords =
        Math.min(batchLimits.general, retirementRecordsPerStripe);
    this.retirements =
        new RetirementQueue(
            memory,
            memory.writerStripeCount(),
            retirementRecordsPerStripe);
    long initialNowMillis = ticker.currentTimeMillis();
    this.wheel = new TimerWheel(initialNowMillis);
    this.nowMillis = initialNowMillis;
    this.thread = new Thread(this, "red-ohc-maintenance-event-loop");
    this.thread.setDaemon(true);
  }

  public void start() {
    thread.start();
  }

  /** Binds cache-local weak-value cleanup before the actor thread starts. */
  public void bindWeakValueQueue(ReferenceQueue<Object> queue) {
    if (thread.getState() != Thread.State.NEW) {
      throw new IllegalStateException("weak-value queue must be bound before maintenance starts");
    }
    weakValueQueue = queue;
  }

  public void stop() {
    synchronized (asyncSubmissionLock) {
      stopping = true;
    }
    signal();
    LockSupport.unpark(thread);
  }

  public void beginClosing() {
    synchronized (asyncSubmissionLock) {
      closing = true;
    }
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
    long depth = asyncSubmitted.get() - asyncDequeuedSequence.get();
    return Math.max(0L, depth);
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
            long sequence = asyncSubmitted.get() + 1L;
            AsyncMutationTask task = new AsyncMutationTask(sequence, action, reject);
            asyncMutations.offer(task);
            // Publish the sequence only after the queue owns the task. Flush submission takes
            // this same monitor, so it cannot capture a sequence with no corresponding task.
            asyncSubmitted.set(sequence);
          }
        }
      }
    } catch (OutOfMemoryError error) {
      asyncRejected.incrementAndGet();
      recordTerminalFailure(error);
      notifyAsyncRejection(reject, error);
      return false;
    }
    if (rejection != null) {
      asyncRejected.incrementAndGet();
      notifyAsyncRejection(reject, rejection);
      return false;
    }
    requestWork(WORK_ASYNC);
    return true;
  }

  public void registerReader(ReaderSlot slot) {
    readers.register(slot);
  }

  /** Reader-side access publication is the only reader activity that needs to wake the actor. */
  public void signalAccess(ReaderSlot slot) {
    if (slot == null) {
      throw new NullPointerException("slot");
    }
    if (!slot.markAccessPending()) {
      return;
    }
    publishDirtyReader(slot);
    if (accessHint.compareAndSet(false, true)) {
      requestWork(WORK_ACCESS);
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
    boolean offered;
    try {
      offered = queue.offer(entry);
      if (!offered) {
        handleMutationTransportFailure(entry, wake, null);
      }
    } catch (OutOfMemoryError error) {
      handleMutationTransportFailure(entry, wake, error);
    }
  }

  private void handleMutationTransportFailure(Entry entry, boolean wake, OutOfMemoryError error) {
    // The mapping/value pointer is already visible. A maintenance transport failure is
    // advisory for this operation: keep the put result synchronous and fail closed for
    // subsequent writes while close() frees the authoritative CHM/native state.
    markMutationForRepair(entry, wake);
    if (error != null) {
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
      repairDebtTotal.incrementAndGet();
      if (repairShardDirty.compareAndSet(shard, 0, 1)) {
        dirtyRepairShardQueue.offer(repairShardTokens[shard]);
      }
    }
    repairVersion.incrementAndGet();
    repairNeeded.set(true);
    if (wake) {
      requestWork(WORK_MUTATION);
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
    retirements.appendValue(context.retirement(), address, allocation);
    context.markRetirementPublished();
  }

  /**
   * The producer calls this exactly once after publishing every CHM/pointer mutation and its
   * retirement records. It only publishes a coalesced work class; the actor waits for its next
   * one-millisecond window before inspecting the queues.
   */
  public void afterWrite(com.red.ohc.runtime.ThreadContext context) {
    boolean retirementWake = context.consumeRetirementPublished();
    requestWork(retirementWake ? WORK_MUTATION | WORK_RETIREMENT : WORK_MUTATION);
  }

  /** Low-frequency callers without a cache ThreadContext always request a wake. */
  public void afterWrite() {
    requestWork(WORK_MUTATION | WORK_RETIREMENT);
  }

  /** Requests an actor pass after a writer-side resource pressure event. */
  public void requestMaintenance() {
    requestWork(WORK_MUTATION | WORK_RETIREMENT | WORK_CLOCK);
  }

  /** Page trimming is requested only after a native allocation-limit failure. */
  public void requestAllocationPressure() {
    if (allocationPressureRequested.compareAndSet(false, true)) {
      requestWork(WORK_PRESSURE | WORK_CLOCK);
    }
  }

  /** Idle writer credit is reclaimed only after a budget reservation failure. */
  public void requestBudgetPressure() {
    if (budgetPressureRequested.compareAndSet(false, true)) {
      requestWork(WORK_PRESSURE | WORK_CLOCK);
    }
  }

  /**
   * Reserves native retirement records before an index mutation publishes a replacement or removal.
   */
  public boolean prepareRetirement(com.red.ohc.runtime.ThreadContext context, int records) {
    return retirements.tryReserve(context.retirement(), records);
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
    if (!reliableRemovals.tryReserve(context.reliableRemoval())) {
      return false;
    }
    try {
      if (!reserveMutation(entry, Entry.PENDING_REMOVE)) {
        reliableRemovals.cancel(context.reliableRemoval());
        requestWork(WORK_REMOVAL);
        return false;
      }
      return true;
    } catch (Throwable failure) {
      if (context.reliableRemoval().active()) {
        reliableRemovals.cancel(context.reliableRemoval());
      }
      requestWork(WORK_REMOVAL);
      throw failure;
    }
  }

  /**
   * Commits a pre-reserved removal record; the actor publishes its retirement records only after
   * listener and policy work has completed.
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
    reliableRemovals.commit(
        context.reliableRemoval(), entry, valueAddress, valueAllocation, cause);
    if (wake) {
      requestWork(WORK_REMOVAL);
    }
  }

  public void cancelReliableRemoval(
      com.red.ohc.runtime.ThreadContext context, Entry entry) {
    entry.cancelPendingClaimIfPresent();
    if (context.reliableRemoval().active()) {
      reliableRemovals.cancel(context.reliableRemoval());
      requestWork(WORK_REMOVAL);
    }
  }

  public void cancelRetirement(com.red.ohc.runtime.ThreadContext context) {
    retirements.cancel(context.retirement());
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
        requestWork(WORK_FLUSH | WORK_CLOCK);
        return existing.future;
      }
      FlushRequest created =
          new FlushRequest(asyncSubmitted.get(), new CompletableFuture<>());
      if (!flushRequest.compareAndSet(null, created)) {
        return flushRequest.get().future;
      }
      requestWork(WORK_FLUSH | WORK_CLOCK);
      return created.future;
    }
  }

  public Snapshot snapshot() {
    long repairDebt = repairDebt();
    return new Snapshot(
        hits,
        misses,
        policy.evictions(),
        policy.evictionWeight(),
        physicalExpired,
        publishedLiveWeight,
        timeoutLagMillis,
        unhealthy.get(),
        queue.size() + repairDebt,
        ttlBacklog(),
        nativeAllocationFailures.get(),
        entryResidenceCount.get(),
        totalEntryResidenceTimeMillis.get());
  }

  @Override
  public void run() {
    wakeGate.requireProcessing();
    while (!stopping || hasShutdownWork()) {
      if (stopping && terminalFailure.get() != null) {
        break;
      }
      // A terminal maintenance failure makes the cache unavailable, but the actor remains
      // alive until close() owns the final native teardown. Park without retrying the
      // corrupted maintenance state while preserving in-flight reader safety.
      if (terminalFailure.get() != null) {
        parked = true;
        try {
          LockSupport.park(this);
        } finally {
          parked = false;
        }
        continue;
      }
      if (!hasPendingWork()) {
        parkUntilWork();
        continue;
      }

      // Cadence is real elapsed time. The cache ticker remains the semantic time source for TTL
      // and QSBR tests, and may legitimately be frozen by a caller.
      long windowNow = System.nanoTime();
      windowScheduler.request(windowNow);
      if (!windowScheduler.isDue(windowNow)) {
        parkUntilWindow(windowNow);
        continue;
      }

      int work;
      try {
        monotonicSampledThisPass = true;
        nowNanos = ticker.nanos();
        WorkPlan plan = selectWorkPlan();
        if (!plan.hasActions()) {
          windowScheduler.complete(windowNow, hasContinuationWork(), nextRetryDeadlineNanos());
          continue;
        }
        work = maintenancePass(plan);
      } catch (Throwable failure) {
        recordTerminalFailure(failure);
        work = 0;
      } finally {
        finishActorRetirementBatch();
      }
      long passEnd = System.nanoTime();
      windowScheduler.complete(passEnd, hasContinuationWork(), nextRetryDeadlineNanos());
    }
    shutdownAndFree();
  }

  /** Compatibility entry point used by actor-focused tests. */
  private int maintenancePass() {
    monotonicSampledThisPass = false;
    sampleMonotonicNow();
    return maintenancePass(selectWorkPlan());
  }

  /** Executes only the actions selected at the preceding one-millisecond window boundary. */
  private int maintenancePass(WorkPlan plan) {
    refreshClock(plan);
    int work = 0;
    if (weakValueQueue != null) {
      work += drainWeakValueReferences(batchLimits.general);
    }
    if (plan.budget) {
      boolean budgetRequested = budgetPressureRequested.getAndSet(false);
      if (!countBounded && budgetRequested && budget.reclaimIdleCredits() != 0L) {
        work++;
      }
    }
    if (plan.hasPolicyMutations()) {
      policy.beginWriteBatch();
    }
    if (plan.removals) {
      work += drainReliableRemovals(batchLimits.general);
    }
    if (plan.mutations) {
      work += drainMutations(batchLimits.general);
    }
    if (plan.repair) {
      work += repairMutations(batchLimits.general);
    }
    if (plan.deferred) {
      work += drainDeferredMutations(batchLimits.general);
    }
    if (plan.access) {
      work += drainAccesses(batchLimits.access);
    }
    // Expiry/eviction runs after mutation repair so actor policy never observes a stale
    // pointer or pending flag.
    if (plan.ttl) {
      work += wheel.advance(nowMillis, batchLimits.general, this);
    }
    if (plan.seal) {
      work += sealRetirements(batchLimits.general);
    }
    if (plan.reclaim) {
      if (advanceEpochIfDue(nowNanos)) {
        work++;
      }
      if (reclaimWorkDue(nowNanos)) {
        work += reclaim(batchLimits.general);
      }
    }
    if (plan.eviction) {
      try {
        work += evictIfNeeded(batchLimits.eviction);
      } finally {
        finishActorRetirementBatch();
      }
    }
    if (plan.allocation && allocationPressureRequested.getAndSet(false)) {
      work += memory.trimIdlePages() > 0L ? 1 : 0;
    }
    if (plan.async) {
      work += drainAsyncMutations(batchLimits.async);
    }
    publishLiveWeight();
    if (plan.flush) {
      completeFlushIfIdle();
    }
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

  private boolean hasPendingWork() {
    return requestedWork.get() != 0
        || reclaimContinuation
        || pendingRemovalRetirement
        || weakValueCleanupContinuation
        || !queue.isEmpty()
        || accessHint.get()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || budgetPressureRequested.get()
        || retirements.hasReadyHint()
        || retirements.hasPendingReclaim()
        || reliableRemovals.hasCommittedHint()
        || !asyncMutations.isEmpty()
        || !deferredMutations.isEmpty()
        || ((!stopping || flushRequest.get() != null) && ttlWorkDue())
        || ((!stopping || flushRequest.get() != null) && policy.usedWeight() > capacity)
        || flushRequest.get() != null;
  }

  /** Close drains already-published work, but never waits for a future TTL deadline. */
  private boolean hasShutdownWork() {
    return requestedWork.get() != 0
        || reclaimContinuation
        || pendingRemovalRetirement
        || !queue.isEmpty()
        || accessHint.get()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || budgetPressureRequested.get()
        || retirements.hasReadyHint()
        || retirements.hasPendingReclaim()
        || reliableRemovals.hasCommittedHint()
        || !asyncMutations.isEmpty()
        || !deferredMutations.isEmpty()
        || flushRequest.get() != null;
  }

  /** Producer masks are consumed by the current plan; only actor state schedules a continuation. */
  private boolean hasContinuationWork() {
    return reclaimContinuation
        || pendingRemovalRetirement
        || weakValueCleanupContinuation
        || !queue.isEmpty()
        || accessHint.get()
        || repairNeeded.get()
        || allocationPressureRequested.get()
        || budgetPressureRequested.get()
        || retirements.hasReadyHint()
        || retirements.hasPendingReclaim()
        || reliableRemovals.hasCommittedHint()
        || !asyncMutations.isEmpty()
        || !deferredMutations.isEmpty()
        || ((!stopping || flushRequest.get() != null) && ttlWorkDue())
        || ((!stopping || flushRequest.get() != null) && policy.usedWeight() > capacity)
        || flushRequest.get() != null;
  }

  private boolean pendingRemovalRetirementDue() {
    return pendingRemovalRetirement
        && (pendingRemovalRetryNanos == Long.MAX_VALUE || pendingRemovalRetryNanos <= nowNanos);
  }

  private WorkPlan selectWorkPlan() {
    WorkPlan plan = workPlan.reset(requestedWork.getAndSet(0));
    plan.flush = (plan.requested & WORK_FLUSH) != 0 || flushRequest.get() != null;
    plan.budget = (plan.requested & WORK_PRESSURE) != 0 || budgetPressureRequested.get();
    plan.allocation = (plan.requested & WORK_PRESSURE) != 0 || allocationPressureRequested.get();
    plan.removals =
        (plan.requested & WORK_REMOVAL) != 0
            || reliableRemovals.hasCommittedHint()
            || pendingRemovalRetirementDue();
    plan.mutations = (plan.requested & WORK_MUTATION) != 0 || !queue.isEmpty();
    plan.repair = (plan.requested & WORK_MUTATION) != 0 || repairNeeded.get();
    plan.deferred = !deferredMutations.isEmpty() && deferredMutationWorkDue();
    plan.access = (plan.requested & WORK_ACCESS) != 0 || accessHint.get();
    plan.ttl = ttlWorkDue() && (!stopping || plan.flush);
    plan.seal = (plan.requested & WORK_RETIREMENT) != 0 || retirements.hasReadyHint();
    plan.reclaim =
        (retirements.hasPendingReclaim() || reclaimContinuation) && reclaimWorkDue(nowNanos);
    plan.eviction = (!stopping || plan.flush) && evictionWorkDue(nowNanos);
    plan.async = (plan.requested & WORK_ASYNC) != 0 || !asyncMutations.isEmpty();
    plan.weakValues =
        (plan.requested & WORK_WEAK_VALUES) != 0 || weakValueCleanupContinuation;
    plan.refreshClock =
        (plan.requested & WORK_CLOCK) != 0
            || plan.flush
            || plan.ttl
            || plan.seal
            || (plan.requested & WORK_PRESSURE) != 0;
    return plan;
  }

  private void parkUntilWork() {
    if (!wakeGate.armIdle()) {
      return;
    }
    if (stopping || hasPendingWorkAfterIdleArm()) {
      wakeGate.requireProcessing();
      return;
    }
    if (!wakeGate.finishIdle()) {
      return;
    }
    parked = true;
    try {
      long ttlWakeNanos = nextTtlWakeNanos();
      if (ttlWakeNanos == Long.MAX_VALUE && weakValueQueue == null) {
        LockSupport.park(this);
      } else {
        long delay =
            ttlWakeNanos == Long.MAX_VALUE
                ? WEAK_VALUE_CLEANUP_INTERVAL_NANOS
                : ttlWakeNanos - System.nanoTime();
        if (weakValueQueue != null) {
          delay = Math.min(delay, WEAK_VALUE_CLEANUP_INTERVAL_NANOS);
        }
        LockSupport.parkNanos(this, Math.max(1L, delay));
      }
      if (weakValueQueue != null && !stopping) {
        requestWork(WORK_WEAK_VALUES);
      }
    } finally {
      parked = false;
      wakeGate.requireProcessing();
    }
  }

  /** Avoid repeating expensive source probes while the actor arms an otherwise idle park. */
  private boolean hasPendingWorkAfterIdleArm() {
    return requestedWork.get() != 0
        || allocationPressureRequested.get()
        || budgetPressureRequested.get()
        || accessHint.get()
        || repairNeeded.get()
        || reliableRemovals.hasCommittedHint()
        || retirements.hasReadyHint()
        || !queue.isEmpty()
        || !deferredMutations.isEmpty()
        || ttlWorkDue()
        || flushRequest.get() != null
        || pendingRemovalRetirement
        || reclaimContinuation
        || policy.usedWeight() > capacity;
  }

  private boolean ttlWorkDue() {
    if (!wheel.hasPending()) {
      return false;
    }
    if (wheel.hasPendingExpiry()) {
      return true;
    }
    long nextTick = wheel.nextWakeTick();
    return nextTick != Long.MAX_VALUE && nextTick <= ticker.currentTimeMillis() / 64L;
  }

  private long nextTtlWakeNanos() {
    if (!wheel.hasPending() || wheel.hasPendingExpiry()) {
      return Long.MAX_VALUE;
    }
    long nextTick = wheel.nextWakeTick();
    if (nextTick == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long nextMillis = nextTick > Long.MAX_VALUE / 64L ? Long.MAX_VALUE : nextTick * 64L;
    long nowMillis = ticker.currentTimeMillis();
    if (nextMillis <= nowMillis) {
      return System.nanoTime();
    }
    long delayMillis = nextMillis - nowMillis;
    long delayNanos =
        delayMillis > Long.MAX_VALUE / 1_000_000L
            ? Long.MAX_VALUE
            : delayMillis * 1_000_000L;
    long nowNanos = System.nanoTime();
    return Long.MAX_VALUE - nowNanos < delayNanos ? Long.MAX_VALUE : nowNanos + delayNanos;
  }

  private void parkUntilWindow(long nowNanos) {
    long deadline = windowScheduler.nextPassNanos();
    if (deadline == Long.MIN_VALUE) {
      return;
    }
    long delay = deadline - nowNanos;
    if (delay <= 0L) {
      return;
    }
    parked = true;
    try {
      LockSupport.parkNanos(this, delay);
    } finally {
      parked = false;
    }
  }

  private void completeFlushIfIdle() {
    if (flushRequest.get() == null) {
      return;
    }
    readers.cleanupCollected(batchLimits.general);
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
      if (requestedWork.get() != 0) {
        return;
      }
      if (reliableRemovals.size() != 0L
          || !queue.isEmpty()
          || !deferredMutations.isEmpty()
          || repairNeeded.get()
          || sequenceAfter(request.sequence, asyncCompletedSequence)
          || allocationPressureRequested.get()
          || budgetPressureRequested.get()
          || accessHint.get()
          || retirements.hasReadyHint()
          || retirements.hasPendingReclaim()
          || reclaimContinuation
          || pendingRemovalRetirement) {
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
      if (next == null || (barrier != null && sequenceAfter(next.sequence, barrier.sequence))) {
        break;
      }
      AsyncMutationTask task = asyncMutations.poll();
      if (task == null) {
        continue;
      }
      advanceAsyncDequeuedSequence(task.sequence);
      Throwable unavailable = terminalFailure.get();
      if (unavailable != null) {
        asyncRejected.incrementAndGet();
        notifyAsyncRejection(
            task.reject,
            new com.red.ohc.api.CacheMaintenanceException(unavailable));
      } else if (closing || stopping) {
        asyncRejected.incrementAndGet();
        notifyAsyncRejection(task.reject, new IllegalStateException("cache is closing"));
      } else {
        try {
          task.action.run();
        } catch (Throwable failure) {
          asyncFailed.incrementAndGet();
          notifyAsyncRejection(task.reject, failure);
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
      advanceAsyncDequeuedSequence(task.sequence);
      asyncCompletedSequence = task.sequence;
      pending.add(task);
    }
    return pending;
  }

  private void rejectAsyncTasks(List<AsyncMutationTask> tasks, Throwable failure) {
    for (AsyncMutationTask task : tasks) {
      asyncRejected.incrementAndGet();
      notifyAsyncRejection(task.reject, failure);
    }
  }

  private static void notifyAsyncRejection(Consumer<Throwable> reject, Throwable failure) {
    try {
      reject.accept(failure);
    } catch (Throwable callbackFailure) {
      if (callbackFailure != failure) {
        failure.addSuppressed(callbackFailure);
      }
    }
  }

  /** Sequence ordering modulo 2^64; live async backlog is always far below half the sequence space. */
  private static boolean sequenceAfter(long sequence, long reference) {
    return sequence != reference && sequence - reference > 0L;
  }

  private void advanceAsyncDequeuedSequence(long sequence) {
    long current = asyncDequeuedSequence.get();
    while (sequenceAfter(sequence, current)) {
      if (asyncDequeuedSequence.compareAndSet(current, sequence)) {
        return;
      }
      current = asyncDequeuedSequence.get();
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
    while (work < limit) {
      if (pendingRemovalRetirement) {
        if (!commitPendingRemovalRetirement()) {
          break;
        }
        pendingRemovalRetirement = false;
        resetPendingRemovalRetry();
        Throwable failure = pendingRemovalFailure;
        pendingRemovalFailure = null;
        removalRecord.clear();
        work++;
        if (failure != null) {
          throwUnchecked(failure);
        }
        continue;
      }
      if (!reliableRemovals.pollNext(removalRecord)) {
        break;
      }
      Entry entry = removalRecord.entry;
      if (entry == null) {
        removalRecord.clear();
        work++;
        continue;
      }
      Throwable failure = null;
      if (evictionNotifier != null && removalRecord.cause != null) {
        try {
          evictionNotifier.notify(entry, removalRecord.valueAddress, removalRecord.cause);
        } catch (Throwable listenerFailure) {
          failure = listenerFailure;
        }
      }
      try {
        processEntry(entry);
      } catch (Throwable processingFailure) {
        if (failure != null) {
          processingFailure.addSuppressed(failure);
        }
        failure = processingFailure;
      }
      pendingRemovalFailure = failure;
      if (!commitPendingRemovalRetirement()) {
        pendingRemovalRetirement = true;
        schedulePendingRemovalRetry();
        break;
      }
      Throwable completedFailure = pendingRemovalFailure;
      pendingRemovalFailure = null;
      removalRecord.clear();
      work++;
      if (completedFailure != null) {
        throwUnchecked(completedFailure);
      }
    }
    return work;
  }

  private boolean commitPendingRemovalRetirement() {
    if (!prepareActorRetirement(2)) {
      return false;
    }
    Entry entry = removalRecord.entry;
    retireActorValueBlock(removalRecord.valueAddress, removalRecord.valueAllocation);
    retireActorValue(entry.nativeKeyAddress, entry.keyAllocationLength());
    return true;
  }

  /** Repairs coalesced mutations whose advisory queue offer lost a race with a full queue. */
  private int repairMutations(int limit) {
    int work = 0;
    int attempts = 0;
    int shards = 0;
    long observedVersion = repairVersion.get();
    RepairShardToken token;
    while (shards < limit
        && attempts < limit
        && (token = dirtyRepairShardQueue.poll()) != null) {
      shards++;
      ConcurrentLinkedQueue<Entry> repairQueue = repairQueues[token.index];
      Entry entry;
      while (attempts < limit && (entry = repairQueue.poll()) != null) {
        attempts++;
        if (entry.isRepairMarked()) {
          processEntry(entry);
          work++;
        }
      }
      repairShardDirty.set(token.index, 0);
      if (!repairQueue.isEmpty()
          && repairShardDirty.compareAndSet(token.index, 0, 1)) {
        dirtyRepairShardQueue.offer(token);
      }
    }
    if (observedVersion == repairVersion.get()
        && repairDebtTotal.get() == 0L
        && dirtyRepairShardQueue.isEmpty()) {
      repairNeeded.set(false);
    }
    return work;
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
      scheduleDeferredMutationRetry();
      return;
    }
    processEntry(entry, flags);
  }

  private int drainDeferredMutations(int limit) {
    int attempts = Math.min(limit, deferredMutations.size());
    int work = 0;
    boolean progressed = false;
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
      if (flags != 0 && entry.isWriterLocked()) {
        if (entry.requeueMutation(flags)) {
          deferredMutations.addLast(entry);
        }
        continue;
      }
      if (flags != 0) {
        processEntry(entry, flags);
        work++;
      }
      progressed = true;
    }
    if (deferredMutations.isEmpty()) {
      resetDeferredMutationRetry();
    } else {
      if (progressed) {
        deferredMutationRetryBackoffNanos = DEFERRED_MUTATION_RETRY_INITIAL_NANOS;
      }
      scheduleDeferredMutationRetry();
    }
    return work;
  }

  private void deferMutation(Entry entry) {
    deferredMutations.addLast(entry);
    scheduleDeferredMutationRetry();
  }

  private void scheduleDeferredMutationRetry() {
    deferredMutationRetryNanos =
        saturatingAdd(sampleMonotonicNow(), deferredMutationRetryBackoffNanos);
    deferredMutationRetryBackoffNanos =
        Math.min(DEFERRED_MUTATION_RETRY_MAX_NANOS, deferredMutationRetryBackoffNanos << 1);
  }

  private int drainWeakValueReferences(int limit) {
    ReferenceQueue<Object> queue = weakValueQueue;
    if (queue == null || limit <= 0) {
      weakValueCleanupContinuation = false;
      return 0;
    }
    int processed = 0;
    int cleared = 0;
    while (processed < limit) {
      Object reference = queue.poll();
      if (reference == null) {
        break;
      }
      processed++;
      if (reference instanceof Entry.WeakValueSlot) {
        Entry.WeakValueSlot slot = (Entry.WeakValueSlot) reference;
        Entry owner = slot.owner();
        if (owner != null && owner.clearWeakValueIfCurrent(slot)) {
          cleared++;
        }
      }
    }
    weakValueCleanupContinuation = processed == limit;
    return cleared;
  }

  private void resetDeferredMutationRetry() {
    deferredMutationRetryNanos = Long.MAX_VALUE;
    deferredMutationRetryBackoffNanos = DEFERRED_MUTATION_RETRY_INITIAL_NANOS;
  }

  private void schedulePendingRemovalRetry() {
    pendingRemovalRetryNanos =
        saturatingAdd(sampleMonotonicNow(), pendingRemovalRetryBackoffNanos);
    pendingRemovalRetryBackoffNanos =
        Math.min(RECLAIM_RETRY_MAX_NANOS, pendingRemovalRetryBackoffNanos << 1);
  }

  private void resetPendingRemovalRetry() {
    pendingRemovalRetryNanos = Long.MAX_VALUE;
    pendingRemovalRetryBackoffNanos = WINDOW_NANOS;
  }

  private void decrementRepairDebt(Entry entry) {
    while (true) {
      long current = repairDebtTotal.get();
      if (current == 0L || repairDebtTotal.compareAndSet(current, current - 1L)) {
        return;
      }
    }
  }

  private long repairDebt() {
    return repairDebtTotal.get();
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
    int sealed = retirements.seal(limit, epoch, this::recordValueResidence);
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

  private boolean refreshClock(WorkPlan plan) {
    if (!plan.refreshClock) {
      return false;
    }
    nowMillis = ticker.currentTimeMillis();
    return true;
  }

  private long sampleMonotonicNow() {
    if (!monotonicSampledThisPass) {
      nowNanos = ticker.nanos();
      monotonicSampledThisPass = true;
    }
    return nowNanos;
  }

  private boolean reclaimWorkDue(long nowNanos) {
    return !reclaimBlocked
        || reclaimRetryNanos == Long.MAX_VALUE
        || reclaimRetryNanos <= nowNanos;
  }

  private boolean deferredMutationWorkDue() {
    return deferredMutationRetryNanos == Long.MAX_VALUE
        || deferredMutationRetryNanos <= sampleMonotonicNow();
  }

  private boolean evictionWorkDue(long monotonicNow) {
    if (policy.usedWeight() <= capacity) {
      return false;
    }
    return evictionRetryNanos == Long.MAX_VALUE || evictionRetryNanos <= monotonicNow;
  }

  /** A retry deadline can delay a later window, but no producer signal can make it earlier. */
  private long nextRetryDeadlineNanos() {
    long tickerDeadline = Long.MIN_VALUE;
    if (!deferredMutations.isEmpty() && deferredMutationRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.max(tickerDeadline, deferredMutationRetryNanos);
    }
    if (policy.usedWeight() > capacity && evictionRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.max(tickerDeadline, evictionRetryNanos);
    }
    if (retirements.hasPendingReclaim() && reclaimRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.max(tickerDeadline, reclaimRetryNanos);
    }
    if (pendingRemovalRetirement && pendingRemovalRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.max(tickerDeadline, pendingRemovalRetryNanos);
    }
    return tickerDeadline == Long.MIN_VALUE
        ? Long.MIN_VALUE
        : retryDeadlineToWallClock(tickerDeadline);
  }

  private long retryDeadlineToWallClock(long tickerDeadlineNanos) {
    long remaining = tickerDeadlineNanos - nowNanos;
    return remaining <= 0L ? System.nanoTime() : saturatingAdd(System.nanoTime(), remaining);
  }

  private void scheduleEvictionRetry() {
    long now = sampleMonotonicNow();
    evictionRetryNanos = saturatingAdd(now, evictionRetryBackoffNanos);
    evictionRetryBackoffNanos =
        Math.min(EVICTION_RETRY_MAX_NANOS, evictionRetryBackoffNanos << 1);
  }

  private int drainAccesses(int limit) {
    if (!accessHint.getAndSet(false)) {
      return 0;
    }
    int work = 0;
    int attempts = 0;
    ReaderSlot slot;
    while (attempts < limit) {
      slot = dirtyReaderQueue.poll();
      if (slot == null) {
        slot = dirtyReaderOverflowQueue.poll();
      }
      if (slot == null) {
        break;
      }
      attempts++;
      AccessRing access = slot.access;
      // Clear before reading the counters/ring. A producer racing after this point either
      // publishes data into this drain or publishes a second queue token for the next pass.
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
      if (access != null) {
        while (work < limit && access.poll(this)) {
          work++;
        }
      }
      if (access != null && !access.isEmpty()) {
        requeueDirtyReader(slot);
        break;
      }
    }
    if (!dirtyReaderQueue.isEmpty() || !dirtyReaderOverflowQueue.isEmpty()) {
      accessHint.set(true);
    }
    return work;
  }

  private void requeueDirtyReader(ReaderSlot slot) {
    if (slot.markAccessPending()) {
      publishDirtyReader(slot);
    }
    accessHint.set(true);
  }

  private void publishDirtyReader(ReaderSlot slot) {
    if (!dirtyReaderQueue.offer(slot)) {
      dirtyReaderOverflowQueue.offer(slot);
    }
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
    if (!countBounded) {
      budget.release(before - retirements.retiredBytes());
    }
    // An active reader cannot be observed through a write-side queue event. Keep a bounded
    // QSBR deadline while it remains active so its ordinary exit need not perform a WakeGate
    // CAS on every cache operation. This is deadline-driven reclaim work, not idle polling.
    // A reclaim pass can make progress in one stripe and then stop at the first record in a
    // later stripe whose reader is still active. Treat that partial-progress case as blocked
    // too; otherwise a blocked retirement could lose its bounded reclaim retry and the worker
    // could park forever with a full retirement ring.
    boolean pending = retirements.hasPendingReclaim();
    reclaimContinuation = reclaimed == limit && pending;
    reclaimBlocked = !reclaimContinuation && pending;
    if (!pending || reclaimContinuation) {
      resetReclaimRetry();
    } else {
      if (reclaimed != 0) {
        reclaimRetryBackoffNanos = RECLAIM_RETRY_INITIAL_NANOS;
      }
      scheduleReclaimRetry();
    }
    return reclaimed;
  }

  private void scheduleReclaimRetry() {
    reclaimRetryNanos = saturatingAdd(sampleMonotonicNow(), reclaimRetryBackoffNanos);
    reclaimRetryBackoffNanos =
        Math.min(RECLAIM_RETRY_MAX_NANOS, reclaimRetryBackoffNanos << 1);
  }

  private void resetReclaimRetry() {
    reclaimRetryNanos = Long.MAX_VALUE;
    reclaimRetryBackoffNanos = RECLAIM_RETRY_INITIAL_NANOS;
  }

  @Override
  public void expire(Entry entry, long expectedGeneration, long expectedValueAddress) {
    long taggedAddress = entry.valueAddress;
    long address = Entry.rawValueAddress(taggedAddress);
    if (taggedAddress != expectedValueAddress
        || address == 0L
        || !Entry.hasTtl(taggedAddress)) {
      return;
    }
    long expiry = ValueBlock.expireAtMillis(address);
    if (expiry <= 0L || expiry > nowMillis) {
      return;
    }
    timeoutLagMillis = Math.max(timeoutLagMillis, Math.max(0L, nowMillis - expiry));
    if (removeFromMap(
        entry, false, expectedGeneration, expectedValueAddress, RemovalCause.EXPIRED)) {
      physicalExpired++;
    } else {
      if (entry.valueAddress == expectedValueAddress && isCurrent(entry)) {
        wheel.add(entry, expiry);
      }
    }
  }

  private int evictIfNeeded(int limit) {
    int work = 0;
    int scans = 0;
    long target = capacity;
    boolean scanExhausted = false;
    boolean removed = false;
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
      boolean removedCurrent =
          removeFromMap(
              victim,
              true,
              expectedGeneration,
              expectedValueAddress,
              victimHash,
              RemovalCause.SIZE);
      if (!removedCurrent
          && (victim.valueAddress == 0L || !isCurrent(victim))) {
        policy.remove(victim, false);
        policyDirty = true;
        removed = true;
        work++;
      } else {
        if (removedCurrent) {
          removed = true;
        }
        if (!removedCurrent
            && victim.valueAddress != 0L
            && isCurrent(victim)
            && victim.isWriterLocked()) {
          policy.skipLocked(victim);
          scheduleEvictionRetry();
        } else if (!removedCurrent) {
          scheduleEvictionRetry();
          break;
        }
      }
    }
    if (removed || policy.usedWeight() <= target) {
      evictionRetryNanos = Long.MAX_VALUE;
      evictionRetryBackoffNanos = EVICTION_RETRY_INITIAL_NANOS;
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
    Throwable listenerFailure = null;
    if (cause != null && evictionNotifier != null) {
      try {
        evictionNotifier.notify(entry, value, cause);
      } catch (Throwable failure) {
        listenerFailure = failure;
      }
    }
    retireEntryBlocks(entry, value);
    if (listenerFailure != null) {
      throwUnchecked(listenerFailure);
    }
    return true;
  }

  private void retireEntryBlocks(Entry entry, long value) {
    long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
    retireActorValueBlock(value, valueAllocation);
    long keyAllocation = entry.keyAllocationLength();
    retireActorValue(entry.nativeKeyAddress, keyAllocation);
  }

  /** Records one retired value version using the actor's last published maintenance time. */
  private void recordValueResidence(long valueAddress) {
    if (valueAddress == 0L) {
      return;
    }
    long createdAtMillis = ValueBlock.createdAtMillis(valueAddress);
    long residenceMillis = nowMillis - createdAtMillis;
    if (residenceMillis < 0L) {
      residenceMillis = 0L;
    }
    entryResidenceCount.incrementAndGet();
    totalEntryResidenceTimeMillis.addAndGet(residenceMillis);
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
    readers.cleanupCollected(4_096);
    while (hasActiveReaders()) {
      LockSupport.parkNanos(this, WINDOW_NANOS);
    }
    while (reliableRemovals.pollNext(removalRecord)) {
      releaseReliableRemovalRecord();
      removalRecord.clear();
    }
    if (pendingRemovalRetirement) {
      releaseReliableRemovalRecord();
      pendingRemovalRetirement = false;
      pendingRemovalFailure = null;
      removalRecord.clear();
    }
    while (retirements.consumeReadyHint()) {
      int sealed = retirements.seal(Integer.MAX_VALUE, epoch);
      if (sealed != 0) {
        latestRetireEpoch = epoch;
      }
    }
    clearPendingMutationQueues();
    drainWeakValueReferences(Integer.MAX_VALUE);
    dirtyReaderQueue.clear();
    dirtyReaderOverflowQueue.clear();
    accessHint.set(false);
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
    if (!countBounded) {
      budget.reclaimIdleCredits();
      budget.clear();
    }
    readers.clear();
    evictionNotifier = null;
  }

  private void releaseReliableRemovalRecord() {
    Entry entry = removalRecord.entry;
    if (entry == null) {
      return;
    }
    if (removalRecord.valueAddress != 0L) {
      memory.releaseEntry(removalRecord.valueAddress, removalRecord.valueAllocation);
    }
    memory.releaseEntry(entry.nativeKeyAddress, entry.keyAllocationLength());
  }

  private static void throwUnchecked(Throwable failure) {
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    throw new RuntimeException(failure);
  }

  private void clearPendingMutationQueues() {
    while (queue.poll() != null) {
      // Drop the transport reference without touching entry ownership.
    }
    for (ConcurrentLinkedQueue<Entry> repairQueue : repairQueues) {
      repairQueue.clear();
    }
    dirtyRepairShardQueue.clear();
    for (int shard = 0; shard <= repairShardMask; shard++) {
      repairShardDirty.set(shard, 0);
    }
    deferredMutations.clear();
    repairNeeded.set(false);
    repairDebtTotal.set(0L);
  }

  private boolean hasActiveReaders() {
    return readers.hasActiveReader();
  }

  private boolean prepareActorRetirement(int records) {
    int batchRecords =
        Math.max(records, actorRetirementBatchRecords);
    if (actorRetirement.active()) {
      if (retirements.remaining(actorRetirement) >= records) {
        return true;
      }
      retirements.cancel(actorRetirement);
    }
    if (retirements.reserve(actorRetirement, Math.max(records, batchRecords))) {
      return true;
    }
    // Recovery is a separately planned action in the next window. Running seal/reclaim here
    // would let a single eviction candidate bypass both the work-plan budget and the cadence.
    return false;
  }

  private void finishActorRetirementBatch() {
    if (actorRetirement.active()) {
      retirements.cancel(actorRetirement);
    }
  }

  private void retireActorValue(long address, long allocation) {
    retirements.append(actorRetirement, address, allocation);
  }

  private void retireActorValueBlock(long address, long allocation) {
    retirements.appendValue(actorRetirement, address, allocation);
  }

  private void signal() {
    if (wakeGate.signal()) {
      LockSupport.unpark(thread);
    }
  }

  /**
   * A producer publishes only the class of work it made visible. The actor consumes the mask at a
   * window boundary, where it can combine it with actor-owned continuations into one work plan.
   */
  private void requestWork(int work) {
    while (true) {
      int current = requestedWork.get();
      int updated = current | work;
      if (current == updated || requestedWork.compareAndSet(current, updated)) {
        break;
      }
    }
    signal();
  }

  static final class WindowScheduler {
    private long nextPassNanos = Long.MIN_VALUE;

    void request(long nowNanos) {
      if (nextPassNanos == Long.MIN_VALUE) {
        nextPassNanos = saturatingAdd(nowNanos, WINDOW_NANOS);
      }
    }

    boolean isDue(long nowNanos) {
      return nextPassNanos != Long.MIN_VALUE && nowNanos >= nextPassNanos;
    }

    long nextPassNanos() {
      return nextPassNanos;
    }

    void complete(long passEndNanos, boolean continued, long retryDeadlineNanos) {
      if (!continued) {
        nextPassNanos = Long.MIN_VALUE;
        return;
      }
      long next = saturatingAdd(passEndNanos, WINDOW_NANOS);
      if (retryDeadlineNanos != Long.MIN_VALUE && retryDeadlineNanos > next) {
        next = retryDeadlineNanos;
      }
      nextPassNanos = next;
    }

    void clear() {
      nextPassNanos = Long.MIN_VALUE;
    }
  }

  static final class BatchLimits {
    final int general;
    final int access;
    final int eviction;
    final int async;

    private BatchLimits(int general, int access, int eviction, int async) {
      this.general = general;
      this.access = access;
      this.eviction = eviction;
      this.async = async;
    }

    static BatchLimits forCpu(int logicalCpuCount) {
      // The final cap means values above sixteen cores cannot increase a batch. Clamp before
      // rounding so the Caffeine-style power-of-two calculation cannot overflow.
      int cores = Math.min(16, Math.max(1, logicalCpuCount));
      int powerOfTwo = 1;
      while (powerOfTwo < cores) {
        powerOfTwo <<= 1;
      }
      int general = Math.min(16 * powerOfTwo, 256);
      return new BatchLimits(
          general,
          Math.min(general * 2, 256),
          Math.min(general / 4, 64),
          Math.min(general / 4, 64));
    }
  }

  private static long saturatingAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }

  private static final class WorkPlan {
    private int requested;
    private boolean budget;
    private boolean removals;
    private boolean mutations;
    private boolean repair;
    private boolean deferred;
    private boolean access;
    private boolean ttl;
    private boolean seal;
    private boolean reclaim;
    private boolean eviction;
    private boolean allocation;
    private boolean async;
    private boolean weakValues;
    private boolean flush;
    private boolean refreshClock;

    private WorkPlan reset(int requested) {
      this.requested = requested;
      budget = false;
      removals = false;
      mutations = false;
      repair = false;
      deferred = false;
      access = false;
      ttl = false;
      seal = false;
      reclaim = false;
      eviction = false;
      allocation = false;
      async = false;
      weakValues = false;
      flush = false;
      refreshClock = false;
      return this;
    }

    private boolean hasPolicyMutations() {
      return removals || mutations || repair || deferred;
    }

    private boolean hasActions() {
      return budget
          || removals
          || mutations
          || repair
          || deferred
          || access
          || ttl
          || seal
          || reclaim
          || eviction
          || allocation
          || async
          || weakValues
          || flush;
    }
  }

  private static final class RepairShardToken {
    private final int index;

    private RepairShardToken(int index) {
      this.index = index;
    }
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
    public final long evictionCount;
    public final long evictionWeight;
    public final long expirationCount;
    public final long liveWeight;
    public final long timeoutLagMillis;
    public final boolean unhealthy;
    public final long queueDepth;
    public final long ttlBacklog;
    public final long nativeAllocationFailureCount;
    public final long entryResidenceCount;
    public final long totalEntryResidenceTimeMillis;

    Snapshot(
        long hits,
        long misses,
        long evictionCount,
        long evictionWeight,
        long expirationCount,
        long liveWeight,
        long timeoutLagMillis,
        boolean unhealthy,
        long queueDepth,
        long ttlBacklog,
        long nativeAllocationFailureCount,
        long entryResidenceCount,
        long totalEntryResidenceTimeMillis) {
      this.hits = hits;
      this.misses = misses;
      this.evictionCount = evictionCount;
      this.evictionWeight = evictionWeight;
      this.expirationCount = expirationCount;
      this.liveWeight = liveWeight;
      this.timeoutLagMillis = timeoutLagMillis;
      this.unhealthy = unhealthy;
      this.queueDepth = queueDepth;
      this.ttlBacklog = ttlBacklog;
      this.nativeAllocationFailureCount = nativeAllocationFailureCount;
      this.entryResidenceCount = entryResidenceCount;
      this.totalEntryResidenceTimeMillis = totalEntryResidenceTimeMillis;
    }
  }
}
