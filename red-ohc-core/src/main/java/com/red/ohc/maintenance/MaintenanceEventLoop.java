package com.red.ohc.maintenance;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.util.PaddedAtomicLong;

import com.red.ohc.api.CacheMaintenanceException;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.AccessRing;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.runtime.WriterResource;
import com.red.ohc.runtime.WriterResourceRegistry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.SizeClasses;
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

  private static final long NO_DEADLINE = Long.MIN_VALUE;
  private static final long MAX_IDLE_PARK_NANOS = 10_000_000L;
  private static final int TEST_TASK_QUEUE_CHUNK_SIZE = 1_024;
  private static final int TEST_TASK_MAX_PER_TURN = 1_024;
  private static final int LIFECYCLE_MAX_PER_TURN = 1_024;
  private static final int CAPACITY_MAX_PER_TURN = 1_024;
  private static final int ACCESS_MAX_PER_TURN = 1_024;
  private static final int TTL_MAX_PER_TURN = 1_024;
  private static final int RECLAIM_MAX_SEGMENTS = 256;

  /**
   * A signal arriving while the park timer fires within this window skips the unpark: the timer
   * wake collects the batched work instead, without delaying anything past the deadline the actor
   * already committed to.
   */
  private static final long WAKE_SUPPRESSION_WINDOW_NANOS = 1_000_000L;

  /** Trickle-rate writer work waits one batch window so a pass processes it in bulk. */
  private static final long WORK_BATCH_WINDOW_NANOS = 200_000L;

  private static final int RETIREMENT_BATCH_THRESHOLD = 64;
  private volatile long parkDeadlineNanos;
  private boolean mandatoryWorkBatchPass;
  private static final int ADVISORY_MAX_PER_TURN = 4_096;
  private static final long CAPACITY_RETRY_NANOS = 1_000_000L;
  private static final long RECLAIM_RETRY_INITIAL_NANOS = 1_000_000L;
  private static final long RECLAIM_RETRY_MAX_NANOS = 10_000_000L;
  private static final long READER_LIFECYCLE_PERIOD_NANOS = 1_000_000_000L;
  private static final long MAX_CAPACITY_RETRY_BACKOFF_NANOS = 10_000_000L;
  private static final int READER_LIFECYCLE_SCAN_LIMIT = 256;
  private static final int WORK_MUTATION = 1;
  private static final int WORK_REMOVAL = 1 << 1;
  private static final int WORK_ACCESS_SCAN = 1 << 2;
  private static final int WORK_RETIREMENT = 1 << 3;
  private static final int WORK_FLUSH = 1 << 5;
  private static final int WORK_CLOCK = 1 << 6;
  private static final int WORK_READER_LIFECYCLE = 1 << 7;
  private static final int WORK_CAPACITY = 1 << 8;
  private static final int WORK_READER_NOTIFICATION = 1 << 9;
  private static final int RUNNABLE_CHECK_WORK = 1;
  private static final int RUNNABLE_CHECK_READERS = 1 << 1;
  private static final int RUNNABLE_CHECK_ACTIVE_READERS = 1 << 2;

  private enum AccessScanState {
    NONE,
    PENDING,
    URGENT
  }

  private final ConcurrentHashMap<Entry, Entry> data;
  private final NativeMemory.Memory memory;
  private final Ticker ticker;
  private final long capacity;
  private final Eviction eviction;
  private volatile WriterLifecycleJournal writerLifecycleJournal;
  private WriterLifecycleLane actorRetryLane;
  private volatile MpscUnboundedArrayQueue<TestActorTask> testTasks;
  private volatile WriterResourceRegistry writerResources;
  private volatile EvictionNotifier evictionNotifier;
  private final ReaderRegistry readers;
  private long[] readerSeqSnapshot = new long[ReaderRegistry.SLOT_CHUNK_SIZE];
  private final boolean[] readerBlocked = new boolean[2];
  private final RetirementJournal retirements;
  private final long nativeDebtBudgetBytes;
  private long safeReclaimBatches;
  private long oldestSafeHeadTicket;
  private long oldestSafeHeadNanos = Long.MAX_VALUE;
  private volatile long oldestSafeWaitNanos;
  private final RetirementRateSampler retirementRateSampler = new RetirementRateSampler();
  private volatile long maintenancePassWorkNanos;
  private long maintenanceActiveNanosTotal;
  private long maintenanceParkNanosTotal;
  private long maintenanceImmediateContinuationCount;
  private volatile long publishedMaintenanceActiveNanosTotal;
  private volatile long publishedMaintenanceParkNanosTotal;
  private volatile long publishedMaintenanceImmediateContinuationCount;
  private long maintenancePassCount;
  private long maintenanceWakeCount;
  private long maintenanceCollectedRecordsTotal;
  private volatile long publishedMaintenancePassCount;
  private volatile long publishedMaintenanceWakeCount;
  private volatile long publishedMaintenanceCollectedRecordsTotal;
  private volatile long accessRingDroppedCount;

  /** Page usage audit: bounded cursor walk over registered pages, published per completed sweep. */
  private long pageAuditCursor;

  private final long[] pageAuditPagesInUse = new long[SizeClasses.count()];
  private final long[] pageAuditAllocatedSlots = new long[SizeClasses.count()];
  private final long[] pageAuditFreedSlots = new long[SizeClasses.count()];
  private volatile long[] publishedPagesInUseByClass = new long[SizeClasses.count()];
  private volatile double[] publishedPageOccupancyByClass = new double[SizeClasses.count()];
  private final WriterLifecycleLane.Record writerRemovalRecord = new WriterLifecycleLane.Record();
  private final EntryLinks links;

  /** Reusable actor key for identity-safe capacity removal without rereading Entry native keys. */
  private final EvictionKey evictionKey = new EvictionKey();

  private final ThreadContext.IdentityRemoval identityRemoval = new ThreadContext.IdentityRemoval();
  private final TimerWheel wheel;
  private final MaintenancePolicy policy;
  private final Thread thread;
  private final AtomicBoolean actorStarted = new AtomicBoolean();
  private final WakeGate wakeGate = new WakeGate();
  private final IdleBackoff idleBackoff;
  private final AtomicInteger requestedWork = new AtomicInteger();
  private final WorkPlan workPlan = new WorkPlan();
  private final WorkDecision workDecision = new WorkDecision();

  private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
  private final AtomicReference<FlushRequest> flushRequest = new AtomicReference<>();

  /** Serializes control-plane publication without putting ordinary cache writes behind a lock. */
  private final Object flushPublicationLock = new Object();

  private volatile Runnable flushRetirementExtensionHookForTest;
  private final TurnCuts turnCuts;
  private volatile boolean writesUnavailable;
  private volatile boolean parked;

  /** Actor-owned cursor and pending-work marker for the bitmap-backed reader access scan. */
  private int accessScanCursor;

  private boolean accessScanActive;

  /** Keeps a high-watermark drain running until its backlog falls back to the low watermark. */
  private boolean accessUrgentMode;

  /** Actor-clock deadline for low-watermark access/counter work. */
  private long nextAccessWakeNanos = Long.MAX_VALUE;

  private int readerLifecycleCursor;
  private int readerLifecycleStart;
  private long nextReaderLifecycleCheckNanos = Long.MAX_VALUE;
  private boolean readerLifecycleCheckActive;
  private boolean readerLifecycleWrapped;

  /** The flush boundary for which the actor has observed reader lifecycle state. */
  private FlushRequest readerLifecycleFlushRequest;

  /** Actor-local capacity retry state; only the blocked entry release reopens the drain. */
  private volatile boolean capacityBlocked;

  private volatile Entry capacityBlockedEntry;
  private long capacityRetryNanos = Long.MAX_VALUE;

  /** Current capacity-retry backoff; doubles per consecutive failed retry, resets on removal. */
  private long capacityRetryBackoffNanos;

  /** Actor-created retirement records that must be added to the active flush fence. */
  private boolean actorRetirementNeedsFlushFence;

  /** Reader-exit notification sequence; readers only increment it on the marked path. */
  private final AtomicLong readerNotificationSequence = new AtomicLong();

  private long observedReaderNotificationSequence;

  /** Padded stop bit: isStopping is read on every actor turn, readers touch neighbours. */
  private final PaddedAtomicLong stopState = new PaddedAtomicLong();

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
  private final AtomicLong residenceSampleCount = new AtomicLong();
  private final AtomicLong residenceSampleTotalMillis = new AtomicLong();
  private volatile long timeoutLagMillis;
  private final AtomicLong nativeAllocationFailures = new AtomicLong();

  /** Volatile read paths for the actor-owned eviction counters; writes stay plain and unlocked. */
  private static final VarHandle EVICTION_COUNT;

  private static final VarHandle EVICTION_WEIGHT;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      EVICTION_COUNT =
          lookup.findVarHandle(MaintenanceEventLoop.class, "evictionCount", long.class);
      EVICTION_WEIGHT =
          lookup.findVarHandle(MaintenanceEventLoop.class, "evictionWeight", long.class);
    } catch (ReflectiveOperationException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  /** Actor-thread-only SIZE-eviction counters; snapshot() reads them with volatile semantics. */
  private long evictionCount;

  private long evictionWeight;

  private final AtomicBoolean unhealthy = new AtomicBoolean();
  private volatile LogicalAdmission logicalAdmission;

  /** A sealed retirement could not pass QSBR; recheck it on the bounded retry deadline. */
  private volatile boolean reclaimBlocked;

  private long reclaimRetryNanos = Long.MAX_VALUE;
  private long reclaimRetryBackoffNanos;
  private long retirementReclaimBlockedCount;
  private long retirementReclaimBlockedNanos;
  private long retirementReclaimBlockedSinceNanos = Long.MIN_VALUE;

  /** Actor-owned marker: publish the policy weight only after a policy mutation. */
  private boolean policyDirty;

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      ReaderRegistry readers,
      long nativeDebtBudgetBytes) {
    this(
        data,
        memory,
        ticker,
        capacity,
        eviction,
        null,
        readers,
        false,
        new RetirementJournal(memory),
        nativeDebtBudgetBytes,
        new EntryLinks(memory));
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers,
      boolean countBounded,
      RetirementJournal retirementJournal,
      long nativeDebtBudgetBytes,
      EntryLinks links) {
    this.data = data;
    this.memory = memory;
    this.ticker = ticker;
    this.capacity = capacity;
    this.eviction = eviction;
    this.evictionNotifier = evictionNotifier;
    if (retirementJournal == null) {
      throw new NullPointerException("retirementJournal");
    }
    if (nativeDebtBudgetBytes <= 0L) {
      throw new IllegalArgumentException("nativeDebtBudgetBytes must be positive");
    }
    if (links == null) {
      throw new NullPointerException("links");
    }
    this.links = links;
    this.policy = new MaintenancePolicy(eviction, capacity, countBounded, links);
    this.readers = readers;
    this.retirements = retirementJournal;
    this.nativeDebtBudgetBytes = nativeDebtBudgetBytes;
    this.idleBackoff = new IdleBackoff();
    this.reclaimRetryBackoffNanos = RECLAIM_RETRY_INITIAL_NANOS;
    this.turnCuts = new TurnCuts(retirementJournal.laneCount() + 1);
    long initialNowNanos = ticker.nanos();
    long initialNowMillis = ticker.currentTimeMillis();
    this.wheel = new TimerWheel(initialNowNanos, links);
    this.nowMillis = initialNowMillis;
    this.nowNanos = initialNowNanos;
    this.thread = new Thread(this, "red-ohc-maintenance-event-loop");
    this.thread.setDaemon(true);
    retirementJournal.bindOpenProducerSignal(this::signal);
    retirementJournal.bindReadySignal(
        () -> requestWork(WORK_RETIREMENT), this::recordTerminalFailure);
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      ReaderRegistry readers,
      RetirementJournal retirementJournal,
      long nativeDebtBudgetBytes) {
    this(
        data,
        memory,
        ticker,
        capacity,
        eviction,
        null,
        readers,
        false,
        retirementJournal,
        nativeDebtBudgetBytes,
        new EntryLinks(memory));
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers,
      long nativeDebtBudgetBytes) {
    this(
        data,
        memory,
        ticker,
        capacity,
        eviction,
        evictionNotifier,
        readers,
        false,
        new RetirementJournal(memory),
        nativeDebtBudgetBytes,
        new EntryLinks(memory));
  }

  public MaintenanceEventLoop(
      ConcurrentHashMap<Entry, Entry> data,
      NativeMemory.Memory memory,
      Ticker ticker,
      long capacity,
      Eviction eviction,
      EvictionNotifier evictionNotifier,
      ReaderRegistry readers,
      boolean countBounded,
      RetirementJournal retirementJournal,
      long nativeDebtBudgetBytes) {
    this(
        data,
        memory,
        ticker,
        capacity,
        eviction,
        evictionNotifier,
        readers,
        countBounded,
        retirementJournal,
        nativeDebtBudgetBytes,
        new EntryLinks(memory));
  }

  public void start() {
    if (!actorStarted.compareAndSet(false, true)) {
      throw new IllegalStateException("maintenance actor is already started");
    }
    thread.start();
  }

  public void bindWriterLifecycleJournal(WriterLifecycleJournal journal) {
    if (journal == null) {
      throw new NullPointerException("journal");
    }
    if (thread.getState() != Thread.State.NEW) {
      throw new IllegalStateException(
          "writer lifecycle journal must be bound before maintenance starts");
    }
    writerLifecycleJournal = journal;
    actorRetryLane = journal.createLane();
    journal.bindReadySignal(this::requestLifecycleMaintenance, this::recordTerminalFailure);
  }

  public void bindWriterResourceRegistry(WriterResourceRegistry registry) {
    if (registry == null) {
      throw new NullPointerException("registry");
    }
    if (thread.getState() != Thread.State.NEW) {
      throw new IllegalStateException(
          "writer resource registry must be bound before maintenance starts");
    }
    writerResources = registry;
    registry.bindReadySignal(() -> requestWork(WORK_REMOVAL | WORK_RETIREMENT));
  }

  public void bindLogicalAdmission(LogicalAdmission admission) {
    if (admission == null) {
      throw new NullPointerException("admission");
    }
    if (thread.getState() != Thread.State.NEW) {
      throw new IllegalStateException("logical admission must be bound before maintenance starts");
    }
    logicalAdmission = admission;
  }

  /** Reconciles the ledger against the map owned by this actor, not a caller-side test seam. */
  public void assertLogicalAdmissionStable(LogicalAdmission admission) {
    if (admission == null) {
      throw new NullPointerException("admission");
    }
    admission.assertStable(data);
  }

  public void stop() {
    synchronized (flushPublicationLock) {
      writesUnavailable = true;
      stopState.set(1L);
    }
    signal();
    LockSupport.unpark(thread);
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

  public long nowMillis() {
    return nowMillis;
  }

  public Thread thread() {
    return thread;
  }

  public RetirementJournal retirementJournal() {
    return retirements;
  }

  public long retiredBytes() {
    return retirements.retiredBytes();
  }

  public long nativeDebtBudgetBytes() {
    return nativeDebtBudgetBytes;
  }

  public long nativeDebtHeadroomBytes() {
    if (nativeDebtBudgetBytes == Long.MAX_VALUE) {
      return -1L;
    }
    long debt = retirements.retirementDebtBytes();
    return debt >= nativeDebtBudgetBytes ? 0L : nativeDebtBudgetBytes - debt;
  }

  public int retiredEntries() {
    return retirements.retiredEntries();
  }

  public long ghostNativeBytes() {
    return policy.ghostNativeBytes();
  }

  /** Number of sampled accesses intentionally suppressed while their producer snapshot was Skip. */
  public long skipSuppressedAccesses() {
    return policy.skipSuppressedAccesses();
  }

  /** Number of first Ghost reappearances deliberately deferred to Small/Skip. */
  public long ghostDeferredPromotions() {
    return policy.ghostDeferredPromotions();
  }

  public boolean ghostRehashPending() {
    return policy.ghostRehashPending();
  }

  public long ttlBacklog() {
    return wheel.scheduled();
  }

  public long retirementQueueDepth() {
    return retirements.queuedRecords();
  }

  public long retirementSafeSegmentCount() {
    return retirements.safeSegmentDebt();
  }

  public long retirementReclaimBatchCount() {
    return safeReclaimBatches;
  }

  public long retirementOldestSafeWaitNanos() {
    return oldestSafeWaitNanos;
  }

  public int registerReader(ReaderSlot slot) {
    int index = readers.register(slot);
    slot.setAccessSignal(this::requestAccessWork);
    // Registration is cold relative to cache reads. It also starts the first bounded lifecycle
    // sweep immediately; later sweeps are driven by the actor-owned one-second deadline.
    requestWork(WORK_ACCESS_SCAN | WORK_READER_LIFECYCLE);
    return index;
  }

  public void registerReader(ThreadContext context) {
    registerReader(context.slot);
  }

  private void requestAccessWork() {
    // AccessRing publication only needs a coalesced work bit and the existing wake protocol.
    while (true) {
      int current = requestedWork.get();
      if ((current & WORK_ACCESS_SCAN) != 0
          || requestedWork.compareAndSet(current, current | WORK_ACCESS_SCAN)) {
        break;
      }
    }
    signal();
  }

  /** ReaderGuard calls this after a marked reader becomes quiescent or drops value protection. */
  public void readerQuiescent(ReaderSlot slot) {
    if (readers.consumeReaderNotification(slot)) {
      readerNotificationSequence.incrementAndGet();
      requestWork(WORK_READER_NOTIFICATION | WORK_CLOCK);
    }
  }

  public void recordNativeAllocationFailure() {
    nativeAllocationFailures.incrementAndGet();
  }

  /** Records one successful SIZE eviction after its CHM unlink has become durable. */
  public void recordSizeEviction(long keyAllocation, long valueAllocation) {
    if (keyAllocation < 0L || valueAllocation < 0L) {
      throw new IllegalArgumentException("negative logical eviction allocation");
    }
    recordSizeEvictionWeight(saturatingAdd(keyAllocation, valueAllocation));
  }

  private void recordSizeEvictionWeight(long logicalBytes) {
    evictionCount++;
    evictionWeight = saturatingAdd(evictionWeight, logicalBytes);
  }

  public void throwIfUnavailable() {
    if (!writesUnavailable) {
      return;
    }
    Throwable failure = terminalFailure.get();
    if (failure != null) {
      throw new CacheMaintenanceException(failure);
    }
    if (isStopping()) {
      throw new IllegalStateException("cache maintenance is stopping");
    }
  }

  public void recordTerminalFailure(Throwable failure) {
    if (failure == null) {
      throw new NullPointerException("failure");
    }
    FlushRequest request;
    synchronized (flushPublicationLock) {
      writesUnavailable = true;
      if (!terminalFailure.compareAndSet(null, failure)) {
        return;
      }
      unhealthy.set(true);
      request = flushRequest.getAndSet(null);
    }
    CacheMaintenanceException unavailable = new CacheMaintenanceException(failure);
    if (request != null) {
      request.future.completeExceptionally(unavailable);
    }
    // MpscUnboundedArrayQueue has one consumer: only the actor (or the actor-owned teardown
    // path) may poll it. A producer-side failure merely wakes the actor, which drains pending
    // messages in its terminal branch without creating a second consumer.
    if (Thread.currentThread() == thread || !actorStarted.get()) {
      rejectPendingTestTasks(unavailable);
    }
    signal();
    // Terminal cleanup is not ordinary continuation work and must bypass the fixed cadence even
    // when WakeGate is already in REQUIRED state and signal() therefore has nothing to unpark.
    LockSupport.unpark(thread);
  }

  void setFlushRetirementExtensionHookForTest(Runnable hook) {
    flushRetirementExtensionHookForTest = hook;
  }

  /** Marks a mutation before the corresponding pointer or CHM publication is exposed. */
  public boolean prepareMutation(Entry entry, int flags) {
    if ((flags & (Entry.PENDING_ADD | Entry.PENDING_UPDATE)) == 0
        || (flags & ~(Entry.PENDING_ADD | Entry.PENDING_UPDATE)) != 0) {
      throw new IllegalArgumentException("invalid advisory mutation flags: " + flags);
    }
    return entry.publishMutation(flags);
  }

  /** Publishes a writer mutation through its lane slot and the ready-lane wake channel. */
  public void enqueueWriterMutationHint(
      WriterLifecycleLane lane,
      Entry entry,
      int keyHash,
      long valueAllocation,
      long mutationVersion,
      boolean wake) {
    if (lane == null) {
      throw new NullPointerException("lane");
    }
    if (entry == null) {
      throw new NullPointerException("entry");
    }
    long sequence = -1L;
    try {
      sequence = lane.reserve();
      lane.writeMutation(sequence, entry, keyHash, valueAllocation, mutationVersion);
    } catch (Throwable error) {
      if (sequence >= 0L) {
        try {
          lane.cancel(sequence, false);
        } catch (Throwable cancelFailure) {
          error.addSuppressed(cancelFailure);
        }
      }
      recordTerminalFailure(error);
      throwUnchecked(error);
      return;
    }
    commitWriterLifecycle(lane, sequence, wake);
  }

  /** Commits a previously written writer lifecycle slot and wakes the ready-lane channel. */
  public void commitWriterLifecycle(WriterLifecycleLane lane, long sequence, boolean wake) {
    if (lane == null) {
      throw new NullPointerException("lane");
    }
    try {
      lane.commit(sequence, wake);
    } catch (Throwable failure) {
      recordTerminalFailure(failure);
      throwUnchecked(failure);
    }
  }

  /** Cancels a previously reserved writer lifecycle slot and wakes the ready-lane channel. */
  public void cancelWriterLifecycle(WriterLifecycleLane lane, long sequence, boolean wake) {
    if (lane == null) {
      throw new NullPointerException("lane");
    }
    lane.cancel(sequence, wake);
  }

  /** Wakes the actor after the lifecycle commit has become visible. */
  public void requestLifecycleMaintenance() {
    requestWork(WORK_REMOVAL);
  }

  /** Test seam; production work is carried exclusively by journals and work bits. */
  public boolean submitActorTaskForTest(Runnable action, Consumer<Throwable> reject) {
    if (action == null || reject == null) {
      throw new NullPointerException();
    }
    Throwable failure;
    synchronized (flushPublicationLock) {
      failure = terminalFailure.get();
      if (failure == null && !isStopping()) {
        try {
          if (testTasks == null) {
            testTasks = new MpscUnboundedArrayQueue<>(TEST_TASK_QUEUE_CHUNK_SIZE);
          }
          testTasks.offer(new TestActorTask(action, reject));
        } catch (Throwable publicationFailure) {
          recordTerminalFailure(publicationFailure);
          failure = publicationFailure;
        }
        if (failure == null) {
          signal();
          return true;
        }
      }
    }
    reject.accept(failure == null ? new IllegalStateException("maintenance is stopping") : failure);
    return false;
  }

  private boolean hasTestTasks() {
    MpscUnboundedArrayQueue<TestActorTask> tasks = testTasks;
    return tasks != null && !tasks.isEmpty();
  }

  private int rejectPendingTestTasks(Throwable failure) {
    MpscUnboundedArrayQueue<TestActorTask> tasks = testTasks;
    if (tasks == null) {
      return 0;
    }
    int rejected = 0;
    TestActorTask task;
    while ((task = tasks.relaxedPoll()) != null) {
      rejected++;
      try {
        task.reject.accept(failure);
      } catch (Throwable rejectionFailure) {
        if (rejectionFailure != failure) {
          failure.addSuppressed(rejectionFailure);
        }
      }
    }
    return rejected;
  }

  private int drainTestTasks(int limit) {
    MpscUnboundedArrayQueue<TestActorTask> tasks = testTasks;
    if (tasks == null) {
      return 0;
    }
    int processed = 0;
    while (processed < limit) {
      Throwable failure = terminalFailure.get();
      if (failure != null || isStopping()) {
        return processed
            + rejectPendingTestTasks(
                failure == null ? new IllegalStateException("maintenance is stopping") : failure);
      }
      TestActorTask task = tasks.relaxedPoll();
      if (task == null) {
        break;
      }
      processed++;
      try {
        task.action.run();
      } catch (Throwable taskFailure) {
        try {
          task.reject.accept(taskFailure);
        } catch (Throwable rejectionFailure) {
          if (rejectionFailure != taskFailure) {
            taskFailure.addSuppressed(rejectionFailure);
          }
        }
        recordTerminalFailure(taskFailure);
        break;
      }
    }
    return processed;
  }

  private void enqueueActorRetry(Entry entry) {
    enqueueWriterMutationHint(
        actorRetryLane, entry, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION, true);
  }

  private int reclaimSafeSegmentsInline(int maximumSegments) {
    if (maximumSegments <= 0) {
      return 0;
    }
    RetirementJournal.ReclaimResult reclaimed =
        retirements.reclaimActorSafeBatchResult(memory, maximumSegments);
    if (reclaimed.segments != 0) {
      safeReclaimBatches = saturatingAdd(safeReclaimBatches, 1L);
      requestWriterResourceScan();
    }
    return reclaimed.segments;
  }

  /** Publishes one batch-level wake for deferred mutation hints. */
  public void requestMutationMaintenance() {
    requestWork(WORK_MUTATION | WORK_REMOVAL);
    requestCapacityMaintenanceIfOverTarget();
  }

  /** Requests an actor pass after a writer-side resource pressure event. */
  public void requestMaintenance() {
    requestWork(WORK_MUTATION | WORK_RETIREMENT | WORK_CLOCK);
  }

  /** Requests one coalesced capacity drain through the work bits. */
  public void requestCapacityMaintenance() {
    int current;
    do {
      current = requestedWork.get();
      if ((current & WORK_CAPACITY) != 0) {
        return;
      }
    } while (!requestedWork.compareAndSet(current, current | WORK_CAPACITY));
    signal();
  }

  /** Wakes a capacity pass only when the entry it stopped behind has been released. */
  public void requestCapacityMaintenanceIfBlocked(Entry entry) {
    if (capacityBlockedEntry == entry) {
      requestCapacityMaintenance();
    }
  }

  private void requestCapacityMaintenanceIfOverTarget() {
    LogicalAdmission admission = logicalAdmission;
    if (admission != null && admission.needsCapacityWake()) {
      requestCapacityMaintenance();
    }
  }

  /** Completes maintenance through the captured lifecycle and retirement watermarks. */
  public CompletableFuture<Void> flush() {
    synchronized (flushPublicationLock) {
      while (true) {
        Throwable failure = terminalFailure.get();
        if (failure != null) {
          CompletableFuture<Void> failed = new CompletableFuture<>();
          failed.completeExceptionally(new CacheMaintenanceException(failure));
          return failed;
        }
        FlushRequest existing = flushRequest.get();
        WriterLifecycleJournal lifecycle = writerLifecycleJournal;
        long[] lifecycleWatermark = lifecycle == null ? null : lifecycle.captureWatermark();
        FlushRequest created =
            new FlushRequest(
                existing == null ? new CompletableFuture<>() : existing.future,
                lifecycleWatermark,
                retirements.captureAndCutWatermark());
        if (!flushRequest.compareAndSet(existing, created)) {
          // The actor can clear a completed request between the read and publication. Retry from
          // the new snapshot; never dereference a request that may have just been cleared.
          continue;
        }
        try {
          requestWork(WORK_FLUSH | WORK_CLOCK);
          signal();
        } catch (Throwable error) {
          recordTerminalFailure(error);
        }
        return created.future;
      }
    }
  }

  public Snapshot snapshot() {
    WriterLifecycleJournal lifecycle = writerLifecycleJournal;
    long lifecyclePublishedRecords = lifecycle == null ? 0L : lifecycle.publishedRecordsTotal();
    long lifecycleCompletedRecords = lifecycle == null ? 0L : lifecycle.completedRecordsTotal();
    long lifecycleLagRecords = lifecycle == null ? 0L : lifecycle.lagRecords();
    long lifecycleAllocatedSegments = lifecycle == null ? 0L : lifecycle.allocatedSegments();
    long lifecycleHeadOfLineStops = lifecycle == null ? 0L : lifecycle.headOfLineStopCount();
    return new Snapshot(
        hits,
        misses,
        (long) EVICTION_COUNT.getVolatile(this),
        (long) EVICTION_WEIGHT.getVolatile(this),
        physicalExpired,
        publishedLiveWeight,
        timeoutLagMillis,
        unhealthy.get(),
        ttlBacklog(),
        nativeAllocationFailures.get(),
        residenceSampleCount.get(),
        1.0d / ThreadContext.RESIDENCE_SAMPLE_INTERVAL,
        averageResidenceSampleMillis(),
        maintenancePassWorkNanos,
        publishedMaintenanceActiveNanosTotal,
        publishedMaintenanceParkNanosTotal,
        publishedMaintenanceImmediateContinuationCount,
        retirements.lastSealScannedLanes(),
        retirements.lastSealSealedLanes(),
        retirements.lastSealRecords(),
        retirements.sealRecordsTotal(),
        retirements.reclaimRecordsTotal(),
        retirements.sealScannedLanesTotal(),
        retirementReclaimBlockedCount,
        retirementReclaimBlockedNanos,
        publishedMaintenancePassCount,
        publishedMaintenanceWakeCount,
        publishedMaintenanceCollectedRecordsTotal,
        readers.activeReaderCount(),
        accessRingDroppedCount,
        retirementQueueDepth(),
        retirements.publishedRecordsTotal(),
        retirements.completedRecordsTotal(),
        retirements.lagRecords(),
        retirements.unsafeRecords(),
        retirements.unsafeBytes(),
        retirements.safeRecords(),
        retirements.safeBytes(),
        retirements.claimedRecords(),
        retirements.claimedBytes(),
        retirements.actorReclaimedRecordsTotal(),
        retirements.allocatedSegments(),
        retirements.reusedSegments(),
        retirements.trimmedSegments(),
        policy.ghostNativeBytes(),
        policy.ghostAllocationTrims(),
        policy.ghostAllocationDrops(),
        policy.ghostRehashPending(),
        policy.skipSuppressedAccesses(),
        policy.ghostDeferredPromotions(),
        lifecyclePublishedRecords,
        lifecycleCompletedRecords,
        lifecycleLagRecords,
        lifecycleAllocatedSegments,
        lifecycleHeadOfLineStops,
        memory.pageAllocatedCount(),
        memory.pageReusedCount(),
        memory.pageReadyCount(),
        memory.pageTrimmedCount(),
        writerResources == null ? 0L : writerResources.activeCount(),
        writerResources == null ? 0L : writerResources.retiringCount(),
        writerResources == null ? 0L : writerResources.pooledCount(),
        retirements.generatedBytesTotal(),
        retirements.completedBytesTotal(),
        retirementRateSampler.generatedBytesPerSecond(),
        retirementRateSampler.completedBytesPerSecond(),
        nativeDebtBudgetBytes,
        nativeDebtHeadroomBytes(),
        retirements.safeSegmentDebt(),
        safeReclaimBatches,
        oldestSafeWaitNanos,
        readyPagesByClassSnapshot(),
        publishedPagesInUseByClass,
        publishedPageOccupancyByClass,
        memory.retainedPagesInUse(),
        memory.pooledPageCount(),
        memory.trimmedBytesTotal());
  }

  @Override
  public void run() {
    wakeGate.requireProcessing();
    boolean previousBatchCompleted = false;
    while (!isStopping() || hasShutdownWork()) {
      monotonicSampledThisPass = false;
      if (isStopping()) {
        previousBatchCompleted = false;
      }
      if (isStopping() && terminalFailure.get() != null) {
        previousBatchCompleted = false;
        break;
      }
      // Terminal failure rejects pending test tasks and parks until internal shutdown takes
      // responsibility for final native teardown.
      Throwable terminal = terminalFailure.get();
      if (terminal != null) {
        previousBatchCompleted = false;
        rejectPendingTestTasks(new CacheMaintenanceException(terminal));
        parked = true;
        try {
          LockSupport.park(this);
        } finally {
          parked = false;
        }
        continue;
      }
      if (hasTestTasks()) {
        long taskStartNanos = System.nanoTime();
        int tasksProcessed = 0;
        try {
          sampleMonotonicNow();
          tasksProcessed = drainTestTasks(TEST_TASK_MAX_PER_TURN);
          if (tasksProcessed > 0 && previousBatchCompleted) {
            maintenanceImmediateContinuationCount =
                saturatingAdd(maintenanceImmediateContinuationCount, 1L);
            previousBatchCompleted = false;
          }
        } catch (Throwable failure) {
          // Test tasks use the same terminal path as maintenance failures.
          recordTerminalFailure(failure);
        } finally {
          long endNanos = System.nanoTime();
          long elapsed = Math.max(0L, endNanos - taskStartNanos);
          maintenancePassWorkNanos = elapsed;
          maintenanceActiveNanosTotal = saturatingAdd(maintenanceActiveNanosTotal, elapsed);
          advancePageAudit();
        }
        if (tasksProcessed > 0) {
          maintenanceCollectedRecordsTotal =
              saturatingAdd(maintenanceCollectedRecordsTotal, tasksProcessed);
          idleBackoff.reset();
        }
        if (terminalFailure.get() != null) {
          previousBatchCompleted = false;
          continue;
        }
      }
      WorkDecision decision = captureWorkDecision(true);
      if (!decision.isRunnable()) {
        previousBatchCompleted = false;
        parkUntilWork();
        continue;
      }
      if (shouldDeferSmallWorkBatch(decision)) {
        previousBatchCompleted = false;
        parkForWorkBatch();
        continue;
      }
      if (previousBatchCompleted) {
        maintenanceImmediateContinuationCount =
            saturatingAdd(maintenanceImmediateContinuationCount, 1L);
        previousBatchCompleted = false;
      }
      idleBackoff.reset();
      long passStartNanos = System.nanoTime();
      int maintenanceWorkUnits = 0;
      boolean batchCompleted = false;
      try {
        sampleMonotonicNow();
        WorkPlan plan = decision.plan;
        if (!plan.hasActions()) {
          continue;
        }
        maintenanceWorkUnits = maintenancePass(plan);
        mandatoryWorkBatchPass = false;
        batchCompleted = true;
      } catch (Throwable failure) {
        recordTerminalFailure(failure);
      } finally {
        long endNanos = System.nanoTime();
        long elapsed = Math.max(0L, endNanos - passStartNanos);
        maintenancePassWorkNanos = elapsed;
        maintenanceActiveNanosTotal = saturatingAdd(maintenanceActiveNanosTotal, elapsed);
        maintenanceCollectedRecordsTotal =
            saturatingAdd(maintenanceCollectedRecordsTotal, maintenanceWorkUnits);
        advancePageAudit();
        publishActorSnapshots();
      }
      previousBatchCompleted = batchCompleted && !isStopping() && terminalFailure.get() == null;
    }
    shutdownAndFree();
  }

  /** Runs a maintenance turn for actor-focused tests. */
  private int maintenancePass() {
    monotonicSampledThisPass = false;
    sampleMonotonicNow();
    if (retirements.hasOpenProducerRecords()) {
      retirements.cutAllProducersAtWatermark();
      retirements.requestSeal();
      requestWork(WORK_RETIREMENT);
    }
    return maintenancePass(captureWorkDecision(true).plan);
  }

  /** Executes one fair turn. Each source is drained only through its captured cut. */
  private int maintenancePass(WorkPlan plan) {
    maintenancePassCount++;
    refreshClock(plan);
    TurnCuts turn = captureTurnCuts(plan);
    ReaderRegistry.SlotTableSnapshot accessSlots = turn.accessSlots;
    turn.accessSlots = null;
    prepareMaintenanceQuotas(plan, turn);
    int work = 0;
    if (plan.accessUrgent) {
      accessUrgentMode = true;
      nextAccessWakeNanos = Long.MAX_VALUE;
    }
    boolean policyChanged = plan.hasAdvisoryPolicyMutations();
    if (policyChanged) {
      policy.beginWriteBatch();
    }
    if (plan.removals) {
      work += drainWriterLifecycleJournal(turn.lifecycleWatermark, plan.lifecycleQuota);
      policyChanged |= policyDirty;
    }
    extendPendingFlushForActorRetirements();
    // Publish the turn's fixed retirement cut before advisory policy/TTL work. SAFE segments stay
    // on the actor-owned retirement queue and are reclaimed by the bounded turn below.
    work += drainRetirementTurn(plan, turn);
    if (plan.capacity) {
      work += drainCapacityPressure(plan);
      if (extendPendingFlushForActorRetirements()) {
        // Capacity eviction can append actor-owned retirements after the first safe turn. Extend
        // the same flush fence and give those records one bounded reclaim turn before completion.
        work += drainRetirementTurn(plan, turn);
      }
    }
    int advisoryRemaining = plan.advisoryQuota;
    if (advisoryRemaining > 0 && (plan.ghostRehash || policyChanged)) {
      int ghostQuota = Math.min(NativeS3GhostMap.REHASH_PASS_BUDGET, advisoryRemaining);
      int phaseWork = policy.advanceGhostRehash(ghostQuota);
      work += phaseWork;
      advisoryRemaining -= Math.min(advisoryRemaining, phaseWork);
    }
    if (plan.access && plan.accessQuota > 0) {
      work += drainAccesses(accessSlots, Math.min(turn.accessRecords, plan.accessQuota));
    }
    if (plan.readerLifecycle && plan.readerLifecycleQuota > 0) {
      int readerQuota = Math.min(READER_LIFECYCLE_SCAN_LIMIT, plan.readerLifecycleQuota);
      work += scanTerminatedReaders(readerQuota);
    }
    // Expiry/eviction runs after the authoritative mutation transport has been drained so actor
    // policy never observes a stale pointer or pending flag.
    if (plan.ttl && plan.ttlQuota > 0) {
      work += wheel.advance(nowNanos, plan.ttlQuota, this);
      if (extendPendingFlushForActorRetirements()) {
        // Expiry runs after the first retirement turn, so give records created by this same flush
        // pass their own bounded seal/reclaim turn before checking the fence.
        work += drainRetirementTurn(plan, turn);
      }
    }
    WriterResourceRegistry resources = writerResources;
    // A blocked resource is idle until a new request or an earlier phase reports scan progress;
    // advisory quota alone is not a reason to walk every retiring resource again.
    if (advisoryRemaining > 0 && resources != null && resources.hasRetirementWork()) {
      work += resources.processRetirements(turn.resourceVersion, advisoryRemaining);
    }
    policyChanged |= policyDirty;
    publishLiveWeight();
    if (plan.flush) {
      completeFlushIfIdle();
    }
    if (plan.access) {
      if (plan.flush) {
        accessScanActive = false;
        accessUrgentMode = false;
        nextAccessWakeNanos = Long.MAX_VALUE;
      } else {
        int pendingAccess = pendingAccessRecords(readers.slotTableSnapshot());
        if (pendingAccess == 0) {
          accessScanActive = false;
          accessUrgentMode = false;
          nextAccessWakeNanos = Long.MAX_VALUE;
        } else if (accessUrgentMode && pendingAccess > AccessRing.LOW_WATERMARK) {
          accessScanActive = true;
          nextAccessWakeNanos = Long.MAX_VALUE;
          requestWork(WORK_ACCESS_SCAN);
        } else {
          scheduleAccessWake();
        }
      }
    }
    if (plan.ttl && plan.ttlQuota < TTL_MAX_PER_TURN && ttlWorkDue()) {
      requestWork(WORK_CLOCK);
    }
    sampleRetirementRates();
    trimReadyPagesWhenOverCapacity();
    sealIdleArenaRetention();
    return work;
  }

  private static final long READY_TRIM_CHECK_INTERVAL_NANOS = 1_000_000_000L;
  private static final long READY_TRIM_BACKOFF_NANOS = 10_000_000_000L;
  private long nextReadyTrimCheckNanos;

  /** Returns fully-empty ready pages to the allocator once native use exceeds the byte capacity. */
  private void trimReadyPagesWhenOverCapacity() {
    long now = sampleMonotonicNow();
    if (now < nextReadyTrimCheckNanos) {
      return;
    }
    LogicalAdmission admission = logicalAdmission;
    if (admission == null || admission.isCountBounded()) {
      nextReadyTrimCheckNanos = now + READY_TRIM_BACKOFF_NANOS;
      return;
    }
    if (memory.allocated() <= capacity || memory.pageReadyCount() == 0L) {
      nextReadyTrimCheckNanos = now + READY_TRIM_CHECK_INTERVAL_NANOS;
      return;
    }
    long trimmedBytes = memory.trimAvailablePages();
    nextReadyTrimCheckNanos =
        now + (trimmedBytes > 0L ? READY_TRIM_CHECK_INTERVAL_NANOS : READY_TRIM_BACKOFF_NANOS);
  }

  private static final long IDLE_RETENTION_SEAL_NANOS = 60_000_000_000L;
  private static final int RETENTION_PATROL_PER_TURN = 16;
  private static final long RETENTION_PATROL_SWEEP_INTERVAL_NANOS = 10_000_000_000L;
  private int retentionPatrolCursor;
  private long nextRetentionPatrolNanos;
  private long[] retentionPatrolRecords = new long[0];
  private long[] retentionPatrolChangeNanos = new long[0];

  /** Seals the private page retention of writers whose lifecycle lane has been silent for 60s. */
  private void sealIdleArenaRetention() {
    long now = sampleMonotonicNow();
    if (now < nextRetentionPatrolNanos) {
      return;
    }
    WriterResourceRegistry resources = writerResources;
    int count = resources == null ? 0 : resources.resourceCount();
    if (count == 0) {
      nextRetentionPatrolNanos = now + RETENTION_PATROL_SWEEP_INTERVAL_NANOS;
      return;
    }
    if (retentionPatrolRecords.length < count) {
      long[] records = new long[count];
      long[] changeNanos = new long[count];
      System.arraycopy(retentionPatrolRecords, 0, records, 0, retentionPatrolRecords.length);
      System.arraycopy(
          retentionPatrolChangeNanos, 0, changeNanos, 0, retentionPatrolChangeNanos.length);
      for (int index = retentionPatrolChangeNanos.length; index < count; index++) {
        changeNanos[index] = Long.MIN_VALUE;
      }
      retentionPatrolRecords = records;
      retentionPatrolChangeNanos = changeNanos;
    }
    int cursor = retentionPatrolCursor;
    int visited = 0;
    while (visited < RETENTION_PATROL_PER_TURN && cursor < count) {
      WriterResource resource = resources.resourceAt(cursor);
      if (resource != null) {
        long published = resource.lifecycleLane().publishedRecordsTotal();
        if (published == retentionPatrolRecords[cursor]) {
          long changedAt = retentionPatrolChangeNanos[cursor];
          if (changedAt != Long.MIN_VALUE && now - changedAt >= IDLE_RETENTION_SEAL_NANOS) {
            resource.arena().sealRetention();
            retentionPatrolChangeNanos[cursor] = now;
          }
        } else {
          retentionPatrolRecords[cursor] = published;
          retentionPatrolChangeNanos[cursor] = now;
        }
      }
      cursor++;
      visited++;
    }
    if (cursor >= count) {
      retentionPatrolCursor = 0;
      nextRetentionPatrolNanos = now + RETENTION_PATROL_SWEEP_INTERVAL_NANOS;
    } else {
      retentionPatrolCursor = cursor;
    }
  }

  private static final int PAGE_AUDIT_MAX_PER_STEP = 512;
  private static final long PAGE_AUDIT_STEP_INTERVAL_NANOS = 1_000_000L;
  private static final long PAGE_AUDIT_SWEEP_INTERVAL_NANOS = 1_000_000_000L;
  private long nextPageAuditNanos;

  private void advancePageAudit() {
    long now = sampleMonotonicNow();
    if (now < nextPageAuditNanos) {
      return;
    }
    long next =
        memory.auditPageUsage(
            pageAuditCursor,
            PAGE_AUDIT_MAX_PER_STEP,
            pageAuditPagesInUse,
            pageAuditAllocatedSlots,
            pageAuditFreedSlots);
    if (next >= 0L) {
      pageAuditCursor = next;
      nextPageAuditNanos = now + PAGE_AUDIT_STEP_INTERVAL_NANOS;
      return;
    }
    long[] pagesInUse = new long[pageAuditPagesInUse.length];
    double[] occupancy = new double[pagesInUse.length];
    for (int sizeClass = 0; sizeClass < pagesInUse.length; sizeClass++) {
      long pages = pageAuditPagesInUse[sizeClass];
      pagesInUse[sizeClass] = pages;
      long slots =
          pages * (long) (SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass));
      if (slots > 0L) {
        occupancy[sizeClass] =
            (double) (pageAuditAllocatedSlots[sizeClass] - pageAuditFreedSlots[sizeClass]) / slots;
      }
      pageAuditPagesInUse[sizeClass] = 0L;
      pageAuditAllocatedSlots[sizeClass] = 0L;
      pageAuditFreedSlots[sizeClass] = 0L;
    }
    publishedPagesInUseByClass = pagesInUse;
    publishedPageOccupancyByClass = occupancy;
    pageAuditCursor = 0L;
    nextPageAuditNanos = now + PAGE_AUDIT_SWEEP_INTERVAL_NANOS;
  }

  private long[] readyPagesByClassSnapshot() {
    long[] ready = new long[SizeClasses.count()];
    for (int sizeClass = 0; sizeClass < ready.length; sizeClass++) {
      ready[sizeClass] = memory.pageReadyCount(sizeClass);
    }
    return ready;
  }

  private void prepareMaintenanceQuotas(WorkPlan plan, TurnCuts turn) {
    WriterLifecycleJournal lifecycle = writerLifecycleJournal;
    // Fixed per-phase quanta: a phase runs its full bounded cap whenever its source has work.
    // Each drain loop self-limits when its source runs empty; backlog does not scale other phases.
    plan.lifecycleQuota =
        plan.removals
                && lifecycle != null
                && (lifecycle.hasPendingReadyLanes() || lifecycle.hasPendingRecords())
            ? LIFECYCLE_MAX_PER_TURN
            : 0;
    plan.capacityQuota = plan.capacity ? CAPACITY_MAX_PER_TURN : 0;
    boolean retirementPending =
        plan.seal
            || plan.flush
            || plan.safe
            || retirements.hasSealedSegments()
            || retirements.hasSafeSegments();
    plan.retirementQuota =
        retirementPending
            ? Math.min(
                RECLAIM_MAX_SEGMENTS, boundedWorkCount(Math.max(1L, retirements.safeSegmentDebt())))
            : 0;
    plan.accessQuota =
        plan.access ? Math.max(1, Math.min(turn.accessRecords, ACCESS_MAX_PER_TURN)) : 0;
    plan.ttlQuota =
        plan.ttl
            ? Math.max(
                1, Math.min((int) Math.min(Integer.MAX_VALUE, ttlBacklog()), TTL_MAX_PER_TURN))
            : 0;
    plan.readerLifecycleQuota = plan.readerLifecycle ? READER_LIFECYCLE_SCAN_LIMIT : 0;
    WriterResourceRegistry resources = writerResources;
    // Only an unvisited scan or a newly queued resource is demand. Progress discovered by an
    // earlier phase reopens the scan for the next immediate continuation.
    boolean resourcePending = resources != null && resources.hasRetirementWork();
    plan.advisoryQuota =
        plan.ghostRehash || resourcePending
            ? Math.min(
                ADVISORY_MAX_PER_TURN,
                saturatingIntAdd(
                    plan.ghostRehash ? NativeS3GhostMap.REHASH_PASS_BUDGET : 0,
                    resourcePending
                        ? boundedWorkCount(Math.max(1L, resources.retiringCount()))
                        : 0))
            : 0;
  }

  private boolean capacityOverTarget(FlushRequest request) {
    LogicalAdmission admission = logicalAdmission;
    // The policy mirrors applied lifecycle work. Writes beyond a flush cut cannot force
    // that request to wait for occupancy the actor is deliberately leaving to later turns.
    return admission != null
        && admission.isOverTarget()
        && (request == null || policy.usedBytes() > admission.target());
  }

  /**
   * Drains logical occupancy in bounded turns. Capacity is deliberately a lower-priority actor
   * source: lifecycle records are cut and applied before this method runs.
   */
  private int drainCapacityPressure(WorkPlan plan) {
    LogicalAdmission admission = logicalAdmission;
    if (admission != null) {
      admission.actorBeginCapacityPass();
    }
    if (admission == null || !capacityOverTarget(plan.flushRequest)) {
      capacityBlockedEntry = null;
      capacityBlocked = false;
      capacityRetryNanos = Long.MAX_VALUE;
      return 0;
    }

    capacityBlockedEntry = null;
    capacityBlocked = false;
    capacityRetryNanos = Long.MAX_VALUE;
    int removed = 0;
    int attempts = 0;
    int removalBudget = plan.capacityQuota;
    int attemptBudget = Math.max(1, removalBudget) * 4;
    while (capacityOverTarget(plan.flushRequest)
        && removed < removalBudget
        && attempts < attemptBudget) {
      // Kept per attempt: completed writer removals must stop excess eviction mid-pass (see
      // capacityDrainObservesConcurrentRemoval). The flag gate makes the no-reduction case a
      // single volatile read; a removal-heavy window pays the exact sum, which is what lets
      // the loop break early.
      admission.actorReconcileExternalReductions();
      if (!capacityOverTarget(plan.flushRequest)) {
        break;
      }
      MaintenancePolicy.Selection selection = policy.selectVictim(64);
      attempts += Math.max(1, policy.lastVictimScanCount());
      Entry victim = selection.entry;
      if (victim == null) {
        if (selection.kind == MaintenancePolicy.Selection.Kind.SCAN_EXHAUSTED
            && attempts < attemptBudget) {
          continue;
        }
        scheduleCapacityRetry();
        break;
      }
      long taggedValue = victim.valueAddress;
      long value = Entry.rawValueAddress(taggedValue);
      if (!Entry.isAliveTagged(taggedValue) || victim.isLogicallyAbsent() || value == 0L) {
        // A dead victim may still be mapped: its own removal protocol (writer remove, expiry,
        // or a queued lifecycle record) owns the CHM unlink. Unlinking it here would orphan a
        // mapped entry out of the policy - invisible to every future selection - so only
        // unlink once the mapping is verifiably gone; otherwise defer via skipLocked so the
        // tail selection moves on without losing the victim.
        if (isMappedEntry(victim)) {
          policy.skipLocked(victim);
          attempts++;
          continue;
        }
        policy.remove(victim, false);
        continue;
      }
      // One state-word snapshot serves both the writer-lock check and the generation that
      // removeFromMap validates against; the pair stays coherent at a single instant instead
      // of straddling a racing finishWriter.
      long claimState = victim.writerClaimStateWord();
      if ((claimState & Entry.WRITER_LOCK) != 0L) {
        capacityBlockedEntry = victim;
        capacityBlocked = true;
        if (!victim.isWriterLocked()) {
          // The writer released between the check and the record. Publishing the block now
          // would aim the writer's unblock wake at a freed (possibly recycled) reference;
          // clear it and let this turn retry the tail.
          capacityBlockedEntry = null;
          capacityBlocked = false;
          continue;
        }
        scheduleCapacityRetry();
        break;
      }
      long generation = Entry.generationOfStateWord(claimState);
      long victimCharge = admission.chargeOf(victim);
      if (removeFromMap(victim, true, generation, taggedValue, RemovalCause.SIZE)) {
        removed++;
        // The shared LongAdder is updated by the logical-absent transition. Keep this actor-local
        // sample in step without paying for a contended exact sum after every victim.
        admission.actorSubtractReleasedCharge(victimCharge);
        if (!capacityOverTarget(plan.flushRequest)) {
          admission.actorReconcileCapacity();
        }
        continue;
      }
      if (victim.isWriterLocked()) {
        capacityBlockedEntry = victim;
        capacityBlocked = true;
        if (!victim.isWriterLocked()) {
          // Same stale-block race as the selection-time check: the writer released between
          // the failed removal and the record, so the block must not be published.
          capacityBlockedEntry = null;
          capacityBlocked = false;
          continue;
        }
        scheduleCapacityRetry();
        break;
      }
      long retiredTaggedValue = victim.valueAddress;
      if (!Entry.isAliveTagged(retiredTaggedValue)
          || victim.isLogicallyAbsent()
          || Entry.rawValueAddress(retiredTaggedValue) == 0L) {
        // Same rule as the selection-time drop: only unlink once the mapping is verifiably
        // gone; a still-mapped dead victim stays linked and selectable for its own protocol.
        if (isMappedEntry(victim)) {
          policy.skipLocked(victim);
          attempts++;
          continue;
        }
        policy.remove(victim, false);
      } else {
        policy.skipLocked(victim);
        scheduleCapacityRetry();
        break;
      }
    }

    admission.actorReconcileCapacity();

    if (removed > 0) {
      // Any successful removal proves the tail is live: collapse the retry backoff ladder.
      capacityRetryBackoffNanos = 0L;
    }
    if (!capacityOverTarget(plan.flushRequest)) {
      capacityBlockedEntry = null;
      capacityBlocked = false;
      capacityRetryNanos = Long.MAX_VALUE;
    } else if (removed > 0 && !capacityBlocked) {
      // The next bounded turn first applies lifecycle records published during this pass.
      requestCapacityMaintenance();
    } else if (!capacityBlocked && capacityRetryNanos == Long.MAX_VALUE) {
      if (removalBudget < CAPACITY_MAX_PER_TURN) {
        requestCapacityMaintenance();
      } else {
        scheduleCapacityRetry();
      }
    }
    return removed;
  }

  private void scheduleCapacityRetry() {
    // Exponential backoff, so a persistently locked or unremovable tail stops paying a
    // full ledger sample plus a victim walk on every retry. Any successful removal in
    // drainCapacityPressure resets the ladder.
    long backoff =
        Math.min(
            MAX_CAPACITY_RETRY_BACKOFF_NANOS,
            Math.max(CAPACITY_RETRY_NANOS, capacityRetryBackoffNanos << 1));
    capacityRetryBackoffNanos = backoff;
    capacityRetryNanos = saturatingAdd(sampleMonotonicNow(), backoff);
  }

  private void sampleRetirementRates() {
    long sampleNanos = sampleMonotonicNow();
    if (!retirementRateSampler.shouldSample(sampleNanos)) {
      return;
    }
    retirementRateSampler.observe(
        sampleNanos, retirements.generatedBytesTotal(), retirements.completedBytesTotal());
  }

  /** Arms a quiescence cut, growing the snapshot when registrations expanded the table. */
  private void armReaderQuiescence() {
    int marked = readers.armReaderQuiescence(readerSeqSnapshot);
    while (marked < 0) {
      readerSeqSnapshot = new long[readers.slotCapacity()];
      marked = readers.armReaderQuiescence(readerSeqSnapshot);
    }
  }

  private int drainRetirementTurn(WorkPlan plan, TurnCuts turn) {
    FlushRequest flushRequestSnapshot = plan.flushRequest;
    boolean forceCloseRetirementCut = isStopping() && retirements.hasOpenProducerRecords();
    long[] retirementWatermark =
        plan.flush && flushRequestSnapshot != null
            ? flushRequestSnapshot.retirementWatermark
            : turn.retirementWatermark;
    boolean retirementWork =
        plan.seal
            || plan.flush
            || forceCloseRetirementCut
            || retirements.hasSealedSegments()
            || retirements.hasSafeSegments();
    if (!retirementWork) {
      return 0;
    }
    int sealed = 0;
    if (forceCloseRetirementCut) {
      retirements.cutAllProducersAtWatermark();
    }
    if (retirements.hasSealableSnapshot(retirementWatermark)) {
      // Seal only the fixed reservation watermark; QSBR tracks the reader operation sequences
      // protecting this batch.
      sealed = retirements.sealSnapshotSegments(retirementWatermark);
    }
    int published = 0;
    if (retirements.hasSealedSegments()) {
      // The actor owns the wait protocol: arm every odd reader with a wake marker, then confirm.
      // A reader exit only wakes us when it consumes this marker; unchanged odd slots block.
      armReaderQuiescence();
      readers.confirmReaderQuiescence(readerSeqSnapshot, readerBlocked);
      published = retirements.publishSafe(!readerBlocked[0], !readerBlocked[1]);
      if (retirements.hasSealedSegments()) {
        // A reader may consume the first marker, leave, and re-enter between the arm and the
        // confirmation load. Re-arm after publishing so that a reader observed active by this
        // pass cannot leave the actor parked without a notification.
        armReaderQuiescence();
      }
    }
    long safePublishedAt = sampleMonotonicNow();
    retirements.finishReadyDrains();
    int reclaimed = 0;
    // SAFE segments remain actor-owned and are reclaimed in bounded batches.
    if (published != 0 || retirements.hasSafeSegments()) {
      reclaimed = reclaimSafeSegmentsInline(Math.min(RECLAIM_MAX_SEGMENTS, plan.retirementQuota));
    }
    long safeHeadTicket = retirements.oldestSafeTicket();
    if (safeHeadTicket == 0L) {
      oldestSafeHeadTicket = 0L;
      oldestSafeHeadNanos = Long.MAX_VALUE;
      oldestSafeWaitNanos = 0L;
    } else {
      if (safeHeadTicket != oldestSafeHeadTicket) {
        oldestSafeHeadTicket = safeHeadTicket;
        oldestSafeHeadNanos = safePublishedAt;
      }
      oldestSafeWaitNanos =
          oldestSafeHeadNanos == Long.MAX_VALUE
              ? 0L
              : Math.max(0L, safePublishedAt - oldestSafeHeadNanos);
    }
    updateReclaimPublicationState(sampleMonotonicNow());
    return sealed + published + reclaimed;
  }

  /** Policy is actor-owned and is updated asynchronously from mutation hints. */
  private boolean hasWriterLifecycleWork() {
    WriterLifecycleJournal journal = writerLifecycleJournal;
    return journal != null && (journal.hasPendingReadyLanes() || journal.hasPendingRecords());
  }

  private boolean hasWriterLifecycleWork(FlushRequest request) {
    if (request == null) {
      return hasWriterLifecycleWork();
    }
    WriterLifecycleJournal journal = writerLifecycleJournal;
    long[] watermark = request.lifecycleWatermark;
    if (journal == null || watermark == null) {
      return false;
    }
    for (int index = 0; index < watermark.length; index++) {
      WriterLifecycleLane lane = journal.lane(index);
      if (!lane.watermarkComplete(watermark[index]) && lane.hasHeadCommitted()) {
        return true;
      }
    }
    return false;
  }

  private boolean readerLifecycleWork(boolean force) {
    FlushRequest flush = flushRequest.get();
    boolean newFlush = flush != readerLifecycleFlushRequest;
    if (!readers.hasRegisteredSlots()) {
      readerLifecycleFlushRequest = flush;
      readerLifecycleCheckActive = false;
      readerLifecycleWrapped = false;
      nextReaderLifecycleCheckNanos = Long.MAX_VALUE;
      return false;
    }
    long now = sampleMonotonicNow();
    if (!readerLifecycleCheckActive
        && (force
            || newFlush
            || (nextReaderLifecycleCheckNanos != Long.MAX_VALUE
                && nextReaderLifecycleCheckNanos <= now))) {
      // Do not associate a newer flush with a sweep that was already active. Its bounded scan
      // started before the newer watermark boundary and cannot account for the newer boundary's
      // slots.
      readerLifecycleFlushRequest = flush;
      startReaderLifecycleSweep();
    }
    return readerLifecycleCheckActive;
  }

  private void startReaderLifecycleSweep() {
    int capacity = readers.slotCapacity();
    readerLifecycleCursor = capacity == 0 ? 0 : readerLifecycleCursor % capacity;
    readerLifecycleStart = readerLifecycleCursor;
    readerLifecycleWrapped = false;
    readerLifecycleCheckActive = capacity != 0;
  }

  private boolean hasRunnableWork() {
    return captureWorkDecision(false).isRunnable();
  }

  private WorkDecision captureWorkDecision(boolean consumeRequestedWork) {
    boolean stopping = isStopping();
    WorkDecision decision = workDecision;
    decision.readersChecked = false;
    decision.activeReaders = false;
    long notificationSequence = readerNotificationSequence.get();
    decision.readerNotificationWork = notificationSequence != observedReaderNotificationSequence;
    if (consumeRequestedWork) {
      observedReaderNotificationSequence = notificationSequence;
    }
    decision.requested = consumeRequestedWork ? requestedWork.getAndSet(0) : requestedWork.get();
    decision.retirementState = retirements.workState();
    decision.flushRequest = flushRequest.get();
    LogicalAdmission admission = logicalAdmission;
    if (admission != null) {
      admission.refreshActorSnapshot();
    }
    decision.capacityOverTarget = capacityOverTarget(stopping ? null : decision.flushRequest);
    long capacityRetry = capacityRetryNanos;
    boolean capacityRequested = (decision.requested & WORK_CAPACITY) != 0;
    decision.capacityRetryDue =
        decision.capacityOverTarget
            && (capacityRequested
                || capacityRetry == Long.MAX_VALUE
                || capacityRetry <= sampleMonotonicNow());
    decision.writerLifecycleWork = hasWriterLifecycleWork(stopping ? null : decision.flushRequest);
    decision.writerResourceWork = writerResources != null && writerResources.hasRetirementWork();
    decision.ghostRehash = policy.ghostRehashPending();
    boolean accessHintRequested = (decision.requested & WORK_ACCESS_SCAN) != 0;
    boolean accessDeadlineDue = !stopping && accessDeadlineDue();
    boolean accessImmediate = !stopping && accessScanActive;
    if (!accessImmediate && (accessHintRequested || accessDeadlineDue)) {
      accessImmediate = scanAccessState(true) == AccessScanState.URGENT;
    }
    boolean accessUrgent = !stopping && (accessImmediate || accessDeadlineDue);
    decision.accessImmediate = accessImmediate;
    decision.accessScanActive = accessUrgent;
    decision.readerLifecycleWork =
        readerLifecycleWork((decision.requested & WORK_READER_LIFECYCLE) != 0 || stopping);
    decision.testTaskWork = hasTestTasks();
    decision.ttlDue = ttlWorkDue();
    decision.pendingSealed = (decision.retirementState & RetirementJournal.WORK_STATE_SEALED) != 0;
    decision.pendingSafe = (decision.retirementState & RetirementJournal.WORK_STATE_SAFE) != 0;
    decision.pendingWriterLifecycleWatermark =
        decision.flushRequest != null && hasPendingWriterLifecycleWatermark(decision.flushRequest);

    WorkPlan plan = workPlan.reset(decision.requested);
    plan.flush = (plan.requested & WORK_FLUSH) != 0 || decision.flushRequest != null;
    plan.flushRequest = decision.flushRequest;
    plan.capacity =
        (plan.requested & WORK_CAPACITY) != 0
            || decision.capacityRetryDue
            || (plan.flush && decision.capacityOverTarget && !capacityBlocked);
    plan.removals = (plan.requested & WORK_REMOVAL) != 0 || decision.writerLifecycleWork;
    plan.resourceRetirements = decision.writerResourceWork;
    plan.mutations = (plan.requested & WORK_MUTATION) != 0;
    plan.ghostRehash = decision.ghostRehash;
    plan.access = plan.flush || (!stopping && decision.accessScanActive);
    plan.accessUrgent = decision.accessImmediate;
    plan.readerLifecycle =
        (plan.requested & WORK_READER_LIFECYCLE) != 0
            || plan.flush
            || stopping
            || decision.readerLifecycleWork;
    plan.ttl = decision.ttlDue && (!isStopping() || plan.flush);
    plan.seal =
        (plan.requested & WORK_RETIREMENT) != 0
            || (plan.requested & WORK_READER_NOTIFICATION) != 0
            || decision.readerNotificationWork
            || (decision.retirementState
                    & (RetirementJournal.WORK_STATE_READY | RetirementJournal.WORK_STATE_SEALED))
                != 0;
    plan.safe = decision.pendingSafe;
    plan.refreshClock =
        (plan.requested & WORK_CLOCK) != 0
            || plan.flush
            || plan.ttl
            || plan.seal
            || plan.readerLifecycle;
    decision.plan = plan;
    decision.runnableCheck = runnableCheck(decision);
    return decision;
  }

  private int runnableCheck(WorkDecision decision) {
    int requested = decision.requested;
    // WORK_MUTATION opens the next policy write batch even after the lane has drained.
    // Other requested bits must be backed by an authoritative source below.
    int directlyRunnable = WORK_MUTATION;
    if ((requested & directlyRunnable) != 0) {
      return RUNNABLE_CHECK_WORK;
    }
    if (decision.capacityRetryDue) {
      return RUNNABLE_CHECK_WORK;
    }
    if (decision.readerNotificationWork && !reclaimNotificationSuppressed()) {
      return RUNNABLE_CHECK_WORK;
    }
    // Global READY may describe only records beyond a reader-blocked flush's fixed cut.
    if (decision.testTaskWork
        || ((decision.retirementState & RetirementJournal.WORK_STATE_READY) != 0
            && (decision.flushRequest == null
                || isStopping()
                || retirements.hasSealableSnapshot(decision.flushRequest.retirementWatermark)))
        || decision.writerLifecycleWork
        || decision.writerResourceWork
        || decision.ghostRehash
        || decision.accessScanActive
        || decision.readerLifecycleWork) {
      return RUNNABLE_CHECK_WORK;
    }

    boolean pendingSealed = decision.pendingSealed;
    boolean pendingSafe = decision.pendingSafe;
    if (pendingSafe) {
      return RUNNABLE_CHECK_WORK;
    }
    boolean capacityRunnable =
        !decision.capacityOverTarget
            || ((requested & WORK_CAPACITY) != 0 || decision.capacityRetryDue);
    if (decision.flushRequest != null
        && !decision.pendingWriterLifecycleWatermark
        && capacityRunnable) {
      if (retirements.watermarkComplete(decision.flushRequest.retirementWatermark)) {
        return RUNNABLE_CHECK_WORK;
      }
      if (retirements.hasSealableSnapshot(decision.flushRequest.retirementWatermark)) {
        return RUNNABLE_CHECK_WORK;
      }
    }

    if (decision.ttlDue) {
      int result = RUNNABLE_CHECK_WORK;
      if (decision.readersChecked) {
        result |= RUNNABLE_CHECK_READERS;
        if (decision.activeReaders) {
          result |= RUNNABLE_CHECK_ACTIVE_READERS;
        }
      }
      return result;
    }
    if (!pendingSealed) {
      return 0;
    }
    if (!decision.readersChecked) {
      decision.activeReaders = hasActiveReaders();
      decision.readersChecked = true;
    }
    int result =
        RUNNABLE_CHECK_READERS | (decision.activeReaders ? RUNNABLE_CHECK_ACTIVE_READERS : 0);
    // An expired reader-blocked retry must run one bounded pass to re-arm notifications and
    // advance its backoff. Otherwise parkUntilWork keeps seeing a past deadline and spins.
    boolean blockedRetryDue =
        reclaimBlocked
            && reclaimRetryNanos != Long.MAX_VALUE
            && reclaimRetryNanos <= sampleMonotonicNow();
    if (blockedRetryDue
        || (!decision.activeReaders
            && (((decision.retirementState & RetirementJournal.WORK_STATE_SEALED) != 0)
                || reclaimWorkDue(sampleMonotonicNow())))) {
      result |= RUNNABLE_CHECK_WORK;
    }
    return result;
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

  /** Publishes actor-owned counters as one weakly consistent snapshot. */
  private void publishActorSnapshots() {
    publishedMaintenanceActiveNanosTotal = maintenanceActiveNanosTotal;
    publishedMaintenanceParkNanosTotal = maintenanceParkNanosTotal;
    publishedMaintenanceImmediateContinuationCount = maintenanceImmediateContinuationCount;
    publishedMaintenancePassCount = maintenancePassCount;
    publishedMaintenanceWakeCount = maintenanceWakeCount;
    publishedMaintenanceCollectedRecordsTotal = maintenanceCollectedRecordsTotal;
  }

  /** Internal shutdown drains published work without waiting for a future TTL deadline. */
  private boolean hasShutdownWork() {
    int retirementState = retirements.workState();
    boolean pendingSealed = (retirementState & RetirementJournal.WORK_STATE_SEALED) != 0;
    boolean pendingRetirement =
        pendingSealed || (retirementState & RetirementJournal.WORK_STATE_SAFE) != 0;
    if (hasTestTasks()
        || (retirementState & RetirementJournal.WORK_STATE_SAFE) != 0
        || (retirementState & RetirementJournal.WORK_STATE_READY) != 0
        || hasWriterLifecycleWork()
        || (writerLifecycleJournal != null && writerLifecycleJournal.hasReadyRecords())
        || (retirementState & RetirementJournal.WORK_STATE_OPEN) != 0
        || (writerResources != null && writerResources.hasRetirementWork())) {
      return true;
    }

    boolean readersChecked = false;
    boolean activeReaders = false;
    FlushRequest request = flushRequest.get();
    if (request != null && !hasPendingWriterLifecycleWatermark(request)) {
      if (retirements.watermarkComplete(request.retirementWatermark)) {
        return true;
      }
      if (!pendingRetirement) {
        return true;
      }
      activeReaders = hasActiveReaders();
      readersChecked = true;
      if (!activeReaders) {
        return true;
      }
    }
    if (!pendingSealed) {
      return false;
    }
    if (!readersChecked) {
      activeReaders = hasActiveReaders();
    }
    return !activeReaders
        && (((retirementState & RetirementJournal.WORK_STATE_SEALED) != 0)
            || reclaimWorkDue(sampleMonotonicNow()));
  }

  private boolean capacityWorkDue() {
    if (!capacityOverTarget(flushRequest.get())) {
      return false;
    }
    int requested = requestedWork.get();
    return (requested & WORK_CAPACITY) != 0
        || capacityRetryNanos == Long.MAX_VALUE
        || capacityRetryNanos <= sampleMonotonicNow();
  }

  private boolean hasPendingWriterLifecycleWatermark(FlushRequest request) {
    WriterLifecycleJournal journal = writerLifecycleJournal;
    return journal != null
        && request.lifecycleWatermark != null
        && !journal.watermarkComplete(request.lifecycleWatermark);
  }

  private void parkUntilWork() {
    completeFlushIfIdle();
    mandatoryWorkBatchPass = false;
    if (!wakeGate.armIdle()) {
      idleBackoff.reset();
      return;
    }
    clearStaleWakeRequests();
    AccessScanState accessState = scanAccessState(true);
    if (accessState == AccessScanState.URGENT) {
      // A high-watermark publication can follow the last scan while its notification is still
      // coalesced. Hand it to the next turn; the wake gate alone cannot schedule a drain.
      accessScanActive = true;
      accessUrgentMode = true;
      nextAccessWakeNanos = Long.MAX_VALUE;
      wakeGate.requireProcessing();
      idleBackoff.reset();
      return;
    } else if (accessState == AccessScanState.PENDING) {
      scheduleAccessWake();
    } else {
      nextAccessWakeNanos = Long.MAX_VALUE;
    }
    WorkDecision decision = captureWorkDecision(false);
    if (cutIdleRetirementRecords(decision.retirementState)) {
      wakeGate.requireProcessing();
      idleBackoff.reset();
      return;
    }
    if (isStopping() || decision.isRunnable()) {
      wakeGate.requireProcessing();
      idleBackoff.reset();
      return;
    }

    long ttlWakeNanos = nextTtlWakeNanos();
    long retryWakeNanos = nextRetryDeadlineNanos(decision.retirementState, decision.runnableCheck);
    long readerLifecycleWakeNanos = nextReaderLifecycleWakeNanos();
    long accessWakeNanos = nextAccessWakeWallClockNanos();
    long wakeNanos =
        Math.min(
            Math.min(Math.min(ttlWakeNanos, retryWakeNanos), readerLifecycleWakeNanos),
            accessWakeNanos);
    if (wakeNanos == Long.MAX_VALUE) {
      while (idleBackoff.takeSpinTurn()) {
        Thread.onSpinWait();
      }
      while (idleBackoff.takeYieldTurn()) {
        Thread.yield();
      }
    }
    if (!wakeGate.finishIdle()) {
      idleBackoff.reset();
      return;
    }

    long delay;
    if (wakeNanos == Long.MAX_VALUE) {
      // No deadline source: the idle backoff ladder stays clamped so a fully idle cache
      // still wakes periodically for liveness checks.
      delay = Math.min(idleBackoff.nextParkNanos(), MAX_IDLE_PARK_NANOS);
    } else {
      // A real deadline (TTL, reader lifecycle, capacity/retirement retry) parks for its
      // true duration: clamping it to the idle cap turned every deadline into 100Hz
      // polling, each wake paying the stale-bit sweep and a fresh work decision.
      delay = wakeNanos - System.nanoTime();
      if (delay <= 0L) {
        // A deadline can expire between the final source check and the park setup. Return to the
        // runnable check instead of turning the missed handoff into a 1ns polling loop.
        idleBackoff.reset();
        wakeGate.requireProcessing();
        return;
      }
    }

    parked = true;
    parkDeadlineNanos = System.nanoTime() + delay;
    long parkStart = System.nanoTime();
    try {
      LockSupport.parkNanos(this, delay);
    } finally {
      parkDeadlineNanos = 0L;
      maintenanceParkNanosTotal =
          saturatingAdd(maintenanceParkNanosTotal, Math.max(0L, System.nanoTime() - parkStart));
      maintenanceWakeCount++;
      publishActorSnapshots();
      parked = false;
      wakeGate.requireProcessing();
    }
  }

  /** True when the only pending work is trickle-rate writer output that batches better. */
  private boolean shouldDeferSmallWorkBatch(WorkDecision decision) {
    if (mandatoryWorkBatchPass || isStopping() || decision.flushRequest != null) {
      return false;
    }
    if ((decision.requested
            & (WORK_MUTATION | WORK_REMOVAL | WORK_CAPACITY | WORK_READER_NOTIFICATION))
        != 0) {
      return false;
    }
    if (decision.capacityRetryDue
        || decision.readerNotificationWork
        || decision.accessScanActive
        || decision.ttlDue
        || decision.ghostRehash
        || decision.writerLifecycleWork
        || decision.writerResourceWork
        || decision.pendingSealed
        || decision.pendingSafe) {
      return false;
    }
    return !hasTestTasks();
  }

  private void parkForWorkBatch() {
    // One deferred window is the limit: the pass after it must run even if the batch is small.
    mandatoryWorkBatchPass = true;
    if (!wakeGate.armIdle()) {
      return;
    }
    clearStaleWakeRequests();
    if (!wakeGate.finishIdle()) {
      return;
    }
    long parkStart = System.nanoTime();
    parked = true;
    parkDeadlineNanos = parkStart + WORK_BATCH_WINDOW_NANOS;
    try {
      LockSupport.parkNanos(this, WORK_BATCH_WINDOW_NANOS);
    } finally {
      parkDeadlineNanos = 0L;
      maintenanceParkNanosTotal =
          saturatingAdd(maintenanceParkNanosTotal, Math.max(0L, System.nanoTime() - parkStart));
      maintenanceWakeCount++;
      publishActorSnapshots();
      parked = false;
      wakeGate.requireProcessing();
    }
  }

  /** Removes advisory wake bits whose authoritative source is already empty before parking. */
  private void clearStaleWakeRequests() {
    int stale =
        WORK_MUTATION | WORK_REMOVAL | WORK_RETIREMENT | WORK_CLOCK | WORK_READER_NOTIFICATION;
    stale |= WORK_ACCESS_SCAN;
    if (!readers.hasRegisteredSlots()) {
      stale |= WORK_READER_LIFECYCLE;
    }
    if (!capacityOverTarget(flushRequest.get())) {
      stale |= WORK_CAPACITY;
    }
    if (flushRequest.get() == null) {
      stale |= WORK_FLUSH;
    }
    int current;
    do {
      current = requestedWork.get();
      if ((current & stale) == 0) {
        return;
      }
    } while (!requestedWork.compareAndSet(current, current & ~stale));
  }

  private boolean accessDeadlineDue() {
    return nextAccessWakeNanos != Long.MAX_VALUE && nextAccessWakeNanos <= System.nanoTime();
  }

  /** Scans registered readers once and combines counter/ring pending state with ring urgency. */
  private AccessScanState scanAccessState(boolean armNotifications) {
    ReaderRegistry.SlotTableSnapshot slots = readers.slotTableSnapshot();
    AccessScanState state = AccessScanState.NONE;
    for (int chunkIndex = 0; chunkIndex < slots.slotChunkCount(); chunkIndex++) {
      long bits = slots.liveBitmap(chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        int index = (chunkIndex << ReaderRegistry.SLOT_CHUNK_SHIFT) + offset;
        ReaderSlot slot = slots.slotAt(index);
        if (slot != null) {
          AccessRing access = slot.access;
          if (state == AccessScanState.URGENT) {
            // The flush/access turn can drain every reader after this scan. Keep later rings armed
            // so a subsequent producer cannot inherit a stale coalesced notification state.
            if (armNotifications && access != null) {
              access.armNotificationAndCheckPending();
            }
            bits &= bits - 1L;
            continue;
          }
          boolean ringPending =
              access != null
                  && (armNotifications
                      ? access.armNotificationAndCheckPending()
                      : !access.isEmpty());
          if (ringPending) {
            if (armNotifications && access.size() >= AccessRing.HIGH_WATERMARK) {
              state = AccessScanState.URGENT;
            } else {
              state = AccessScanState.PENDING;
            }
          }
          if (state != AccessScanState.URGENT
              && (slot.publishedHits != slots.consumedHits(index)
                  || slot.publishedMisses != slots.consumedMisses(index))) {
            state = AccessScanState.PENDING;
          }
        }
        bits &= bits - 1L;
      }
    }
    return state;
  }

  private boolean hasPendingAccessReadOnly() {
    return scanAccessState(false) != AccessScanState.NONE;
  }

  private void scheduleAccessWake() {
    long deadline = saturatingAdd(System.nanoTime(), MAX_IDLE_PARK_NANOS);
    if (nextAccessWakeNanos == Long.MAX_VALUE || deadline < nextAccessWakeNanos) {
      nextAccessWakeNanos = deadline;
    }
    accessScanActive = false;
    accessUrgentMode = false;
  }

  private long nextAccessWakeWallClockNanos() {
    if (accessUrgentMode) {
      return System.nanoTime();
    }
    if (nextAccessWakeNanos == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    return nextAccessWakeNanos;
  }

  private boolean ttlWorkDue() {
    if (!wheel.hasPending()) {
      return false;
    }
    if (wheel.hasPendingExpiry()) {
      return true;
    }
    long nextTick = wheel.nextWakeTick();
    long nowNanos = sampleMonotonicNow();
    long currentTick = nowNanos / TimerWheel.TICK_NANOS;
    return nextTick != Long.MAX_VALUE && nextTick <= currentTick;
  }

  private long nextTtlWakeNanos() {
    if (!wheel.hasPending() || wheel.hasPendingExpiry()) {
      return Long.MAX_VALUE;
    }
    long nextTick = wheel.nextWakeTick();
    if (nextTick == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long logicalDeadline =
        nextTick > Long.MAX_VALUE / TimerWheel.TICK_NANOS
            ? Long.MAX_VALUE
            : nextTick * TimerWheel.TICK_NANOS;
    long logicalNow = sampleMonotonicNow();
    long parkNow = System.nanoTime();
    if (logicalDeadline <= logicalNow) {
      return parkNow;
    }
    long delay = logicalDeadline - logicalNow;
    return Long.MAX_VALUE - parkNow < delay ? Long.MAX_VALUE : parkNow + delay;
  }

  private long nextReaderLifecycleWakeNanos() {
    if (!readers.hasRegisteredSlots()) {
      return Long.MAX_VALUE;
    }
    if (readerLifecycleCheckActive) {
      return System.nanoTime();
    }
    if (nextReaderLifecycleCheckNanos == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    sampleMonotonicNow();
    return retryDeadlineToWallClock(nextReaderLifecycleCheckNanos);
  }

  private boolean extendPendingFlushForActorRetirements() {
    if (!actorRetirementNeedsFlushFence) {
      return false;
    }
    // Serialize the actor watermark extension with flush() publication so a newer request cannot
    // replace the request after the actor has read it.
    synchronized (flushPublicationLock) {
      if (!actorRetirementNeedsFlushFence) {
        return false;
      }
      FlushRequest request = flushRequest.get();
      if (request == null) {
        // A flush created after this append captures the current actor lane itself.
        actorRetirementNeedsFlushFence = false;
        return false;
      }
      Runnable hook = flushRetirementExtensionHookForTest;
      if (hook != null) {
        hook.run();
      }
      if (!retirements.captureAndCutActorWatermark(request.retirementWatermark)) {
        return false;
      }
      actorRetirementNeedsFlushFence = false;
      return true;
    }
  }

  private void completeFlushIfIdle() {
    if (terminalFailure.get() != null) {
      return;
    }
    FlushRequest request = flushRequest.get();
    if (request == null) {
      return;
    }

    boolean writerLifecyclePending = hasPendingWriterLifecycleWatermark(request);
    boolean readerLifecyclePending =
        readerLifecycleCheckActive || readerLifecycleFlushRequest != request;
    RetirementJournal retirementJournal = retirements;
    boolean retirementWatermarkPending =
        retirementJournal != null
            && !retirementJournal.watermarkComplete(request.retirementWatermark);
    boolean accessPending = hasPendingAccessReadOnly();
    boolean ttlPending = ttlWorkDue();
    boolean capacityPending = capacityOverTarget(request);
    if (writerLifecyclePending
        || readerLifecyclePending
        || retirementWatermarkPending
        || accessPending
        || ttlPending
        || capacityPending) {
      return;
    }
    synchronized (flushPublicationLock) {
      // Flush publication, terminal transition, and actor completion share one control-plane
      // boundary. A producer can therefore either extend this exact request or observe it already
      // completed; it cannot leave a future behind a cleared request pointer.
      if (terminalFailure.get() != null || flushRequest.get() != request) {
        return;
      }
      if (hasPendingWriterLifecycleWatermark(request)
          || readerLifecycleCheckActive
          || readerLifecycleFlushRequest != request
          || (retirementJournal != null
              && !retirementJournal.watermarkComplete(request.retirementWatermark))
          || hasPendingAccessReadOnly()
          || ttlWorkDue()
          || capacityOverTarget(request)) {
        return;
      }
      if (!flushRequest.compareAndSet(request, null)) {
        return;
      }
      readerLifecycleFlushRequest = null;
      request.future.complete(null);
    }
  }

  private int drainWriterLifecycleJournal(long[] watermark, int maximumRecords) {
    WriterLifecycleJournal journal = writerLifecycleJournal;
    if (journal == null || watermark == null || watermark.length == 0 || maximumRecords <= 0) {
      return 0;
    }
    int work = 0;
    // Pass drains visit only lanes whose ready marker is set; the marker is rearmed by
    // finishReadyDrain whenever unconsumed head records remain.
    int budget = journal.readyLaneCount();
    for (int index = 0; index < budget && work < maximumRecords; index++) {
      WriterLifecycleLane lane = journal.pollReadyLane();
      if (lane == null) {
        break;
      }
      int laneIndex = lane.laneIndex();
      if (laneIndex >= watermark.length) {
        // The lane was created after this cut captured its watermark; retry next pass.
        journal.requeueReadyLane(lane);
        continue;
      }
      work += drainLifecycleLane(lane, laneIndex, watermark, maximumRecords - work);
    }
    // Deferred-wake commits never signal a lane into the ready queue; scan the backlog directly.
    int laneCount = Math.min(watermark.length, journal.laneCount());
    for (int laneIndex = 0; laneIndex < laneCount && work < maximumRecords; laneIndex++) {
      if (!journal.lane(laneIndex).watermarkComplete(watermark[laneIndex])) {
        work +=
            drainLifecycleLane(
                journal.lane(laneIndex), laneIndex, watermark, maximumRecords - work);
      }
    }
    if (work != 0) {
      requestWriterResourceScan();
    }
    return work;
  }

  private int drainAllWriterLifecycleJournal(long[] watermark) {
    WriterLifecycleJournal journal = writerLifecycleJournal;
    if (journal == null || watermark == null || watermark.length == 0) {
      return 0;
    }
    int work = 0;
    // Internal stop and terminal cleanup sweep every lane regardless of ready markers.
    for (int laneIndex = 0; laneIndex < watermark.length; laneIndex++) {
      work += drainLifecycleLane(journal.lane(laneIndex), laneIndex, watermark, Integer.MAX_VALUE);
    }
    return work;
  }

  private int drainLifecycleLane(
      WriterLifecycleLane lane, int laneIndex, long[] watermark, int maximumRecords) {
    int work = 0;
    while (!lane.watermarkComplete(watermark[laneIndex])) {
      if (!lane.poll(writerRemovalRecord)) {
        break;
      }
      try {
        processWriterLifecycleRecord(lane);
      } finally {
        lane.release(writerRemovalRecord);
        writerRemovalRecord.clear();
      }
      work++;
      if (work >= maximumRecords) {
        break;
      }
    }
    lane.finishReadyDrain();
    return work;
  }

  private void requestWriterResourceScan() {
    WriterResourceRegistry resources = writerResources;
    if (resources != null) {
      resources.requestRetirementScan();
    }
  }

  /** Applies exactly one already-published lifecycle record in its lane order. */
  private void processWriterLifecycleRecord(WriterLifecycleLane lane) {
    if (writerRemovalRecord.operation == WriterLifecycleLane.REMOVE
        && writerRemovalRecord.entry != null) {
      if (evictionNotifier != null && writerRemovalRecord.cause != null) {
        try {
          evictionNotifier.notify(
              writerRemovalRecord.entry,
              writerRemovalRecord.valueAddress,
              writerRemovalRecord.cause);
        } catch (Throwable ignored) {
          // Listener failures must not prevent native retirement.
        }
      }
      processEntry(writerRemovalRecord.entry, Entry.PENDING_REMOVE);
      // The reliable lifecycle record is the last owner allowed to touch this native metadata.
      // Clear stale advisory queue/retry bits before the key block is retired; otherwise a later
      // stale queue node can keep EntryLinks alive forever.
      writerRemovalRecord.entry.clearStalePendingFlags();
      retireActorValueBlock(writerRemovalRecord.valueAddress, writerRemovalRecord.allocation);
      retireActorValue(
          writerRemovalRecord.entry.nativeKeyAddress,
          writerRemovalRecord.entry.nativeKeyAllocationLength());
      links.maybeRelease(writerRemovalRecord.entry);
    } else if (writerRemovalRecord.operation == WriterLifecycleLane.MUTATION
        && writerRemovalRecord.entry != null) {
      processEntry(
          writerRemovalRecord.entry,
          (int) writerRemovalRecord.valueAddress,
          writerRemovalRecord.allocation,
          writerRemovalRecord.generation);
    }
  }

  private void processEntry(Entry entry) {
    processEntry(entry, false);
  }

  /** Applies a reliable mutation record carrying the actor's primitive publication seed. */
  private void processEntry(
      Entry entry, int seededKeyHash, long seededValueAllocation, long seededMutationVersion) {
    processEntry(entry, false, seededKeyHash, seededValueAllocation, seededMutationVersion, true);
  }

  private void processEntry(Entry entry, boolean allowRetired) {
    processEntry(entry, allowRetired, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION, false);
  }

  private void processEntry(
      Entry entry,
      boolean allowRetired,
      int seededKeyHash,
      long seededValueAllocation,
      long seededMutationVersion,
      boolean hasSeed) {
    if (terminalFailure.get() != null) {
      return;
    }
    if (!allowRetired && !entry.isAlive()) {
      // Advisory queues can retain an Entry after reliable removal has retired its native block.
      // Check the heap lifecycle tag before touching the native state word.
      return;
    }
    int flags = entry.takePending();
    if (flags == 0) {
      entry.tryRolloverMaintenanceVersion();
      return;
    }
    if (entry.isWriterLocked()) {
      // ADD/UPDATE no longer hold a pending claim. A worker may therefore dequeue the
      // advisory hint while the per-entry writer is between allocation and publication.
      // Leave one native retry marker for the writer's publication epilogue. The actor never
      // waits for the writer and never keeps a second Java queue of Entry references.
      entry.requestMutationRetry(flags);
      if (!entry.isWriterLocked()) {
        // The writer may have released the mutex between the probe and the marker CAS. In that
        // race the actor and writer compete for one retry handoff; only the CAS winner publishes.
        if (entry.claimMutationRetry()) {
          enqueueActorRetry(entry);
        }
      }
      return;
    }
    processEntry(
        entry, flags, seededKeyHash, seededValueAllocation, seededMutationVersion, hasSeed);
  }

  private void processEntry(Entry entry, int flags) {
    processEntry(entry, flags, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION, false);
  }

  private void processEntry(
      Entry entry,
      int flags,
      int seededKeyHash,
      long seededValueAllocation,
      long seededMutationVersion,
      boolean hasSeed) {
    if (terminalFailure.get() != null) {
      return;
    }
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
      applyEntry(
          entry,
          version,
          flags,
          seededKeyHash,
          seededValueAllocation,
          seededMutationVersion,
          hasSeed);
      entry.tryRolloverMaintenanceVersion();
    }
  }

  private void applyEntry(Entry entry, long version, int flags) {
    applyEntry(entry, version, flags, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION, false);
  }

  private void applyEntry(
      Entry entry,
      long version,
      int flags,
      int seededKeyHash,
      long seededValueAllocation,
      long seededMutationVersion,
      boolean hasSeed) {
    long taggedAddress = entry.valueAddress;
    long address = Entry.rawValueAddress(taggedAddress);
    long deadlineNanos =
        address != 0L && Entry.hasTtl(taggedAddress)
            ? ValueBlock.deadlineNanos(address)
            : NO_DEADLINE;
    if (address == 0L || !isCurrent(entry) || entry.isLogicallyAbsent()) {
      wheel.remove(entry);
      policy.remove(entry, false);
      policyDirty = true;
      if (deadlineNanos != NO_DEADLINE && isCurrent(entry)) {
        // A reader can mark an expired mapping logically absent before its ADD/UPDATE hint is
        // drained. Keep the timer attached so the actor still owns the physical unlink and
        // retirement; the entry must not contribute to the live policy weight in the meantime.
        wheel.reschedule(entry, deadlineNanos);
      }
      if (!entry.markAppliedVersion(version)) {
        republishMutation(entry, flags);
      }
      return;
    }
    long currentValueAllocation = entry.currentValueAllocation();
    boolean reliableSeed =
        hasSeed && seededMutationVersion != WriterLifecycleLane.UNSEEDED_MUTATION_VERSION;
    // The 32-bit version freezes at saturation while further updates may still coalesce.
    // Equality at that fence cannot establish freshness; the Entry remains authoritative.
    long valueAllocation =
        reliableSeed && seededMutationVersion == version && version != 0xffffffffL
            ? seededValueAllocation
            : currentValueAllocation;
    if (!reliableSeed && valueAllocation == 0L) {
      // Advisory and explicitly unseeded mutations do not carry a writer-owned allocation seed.
      // Keep their historical cold fallback; reliable lane records use the current Entry
      // allocation on a version mismatch.
      valueAllocation = ValueBlock.allocationLength(ValueBlock.length(address));
    }
    int keyHash = reliableSeed ? seededKeyHash : entry.keyHash();
    policy.add(entry, valueAllocation, keyHash);
    int linkId = entry.policyLinkId();
    policyDirty = true;
    if (deadlineNanos != NO_DEADLINE) {
      wheel.reschedule(entry, deadlineNanos, linkId);
    } else {
      wheel.remove(entry, linkId);
    }
    if (!entry.markAppliedVersion(version)) {
      republishMutation(entry, flags);
    }
  }

  /** Applies one current mutation for actor-focused tests. */
  private void applyEntry(Entry entry) {
    applyEntry(entry, entry.mutationVersion(), Entry.PENDING_UPDATE);
  }

  private void republishMutation(Entry entry, int flags) {
    if (!entry.publishMutation(flags)) {
      return;
    }
    enqueueActorRetry(entry);
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
    return !reclaimBlocked || reclaimRetryNanos == Long.MAX_VALUE || reclaimRetryNanos <= nowNanos;
  }

  // While reclaim is reader-blocked, reader-exit notifications are redundant until the retry.
  // A deep backlog keeps the notifications live: publication must not starve under load.
  private boolean reclaimNotificationSuppressed() {
    return reclaimBlocked
        && reclaimRetryNanos != Long.MAX_VALUE
        && reclaimRetryNanos > sampleMonotonicNow()
        && retirements.queuedRecords() < RETIREMENT_BATCH_THRESHOLD;
  }

  private long nextRetryDeadlineNanos(int retirementState, int runnableCheck) {
    long tickerDeadline = Long.MAX_VALUE;
    if (capacityOverTarget(flushRequest.get()) && capacityRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.min(tickerDeadline, capacityRetryNanos);
    }
    // Reader-blocked reclaim parks until the retry: notifications are suppressed while
    // blocked, so the retry deadline is the wake that retries the publication.
    boolean pendingSealed = (retirementState & RetirementJournal.WORK_STATE_SEALED) != 0;
    if (pendingSealed && reclaimRetryNanos != Long.MAX_VALUE) {
      tickerDeadline = Math.min(tickerDeadline, reclaimRetryNanos);
    }
    return tickerDeadline == Long.MAX_VALUE
        ? Long.MAX_VALUE
        : retryDeadlineToWallClock(tickerDeadline);
  }

  private long retryDeadlineToWallClock(long tickerDeadlineNanos) {
    long remaining = tickerDeadlineNanos - nowNanos;
    return remaining <= 0L ? System.nanoTime() : saturatingAdd(System.nanoTime(), remaining);
  }

  private int drainAccesses(ReaderRegistry.SlotTableSnapshot slots, int limit) {
    if (limit <= 0) {
      return 0;
    }
    if (slots == null) {
      throw new IllegalStateException("access turn has no reader slot snapshot");
    }
    int capacity = slots.slotCapacity();
    if (capacity == 0) {
      accessScanCursor = 0;
      accessScanActive = false;
      return 0;
    }
    int start = accessScanCursor >= capacity ? 0 : accessScanCursor;
    int work = drainAccessRange(slots, start, capacity, limit);
    if (work < limit && start != 0) {
      work += drainAccessRange(slots, 0, start, limit - work);
    }
    return work;
  }

  private int drainAccessRange(
      ReaderRegistry.SlotTableSnapshot slots, int startInclusive, int endExclusive, int limit) {
    int work = 0;
    int cursor = startInclusive;
    while (cursor < endExclusive && work < limit) {
      int index = slots.firstLiveSlot(cursor, endExclusive);
      if (index < 0) {
        break;
      }
      accessScanCursor = index + 1;
      if (accessScanCursor >= slots.slotCapacity()) {
        accessScanCursor = 0;
      }
      ReaderSlot slot = slots.slotAt(index);
      if (slot != null) {
        collectAccessCounters(slots, index, slot);
        AccessRing access = slot.access;
        if (access != null) {
          work += access.poll(limit - work, this);
        }
      }
      cursor = index + 1;
    }
    return work;
  }

  /** Checks a bounded number of registered owners and retires slots whose threads have ended. */
  private int scanTerminatedReaders(int limit) {
    if (limit <= 0 || !readerLifecycleCheckActive) {
      return 0;
    }
    if (!readers.hasRegisteredSlots()) {
      finishReaderLifecycleSweep();
      return 0;
    }
    ReaderRegistry.SlotTableSnapshot slots = readers.slotTableSnapshot();
    int capacity = slots.slotCapacity();
    if (capacity == 0) {
      finishReaderLifecycleSweep();
      return 0;
    }
    if (readerLifecycleCursor >= capacity) {
      readerLifecycleCursor = 0;
      readerLifecycleWrapped = true;
    }
    int checked = 0;
    int retired = 0;
    while (checked < limit && readerLifecycleCheckActive) {
      int endExclusive = readerLifecycleWrapped ? readerLifecycleStart : capacity;
      int index = slots.firstLiveSlot(readerLifecycleCursor, endExclusive);
      if (index < 0) {
        if (readerLifecycleWrapped || readerLifecycleStart == 0) {
          finishReaderLifecycleSweep();
        } else {
          readerLifecycleWrapped = true;
          readerLifecycleCursor = 0;
        }
        continue;
      }
      checked++;
      readerLifecycleCursor = index + 1;
      if (readerLifecycleCursor >= capacity) {
        readerLifecycleCursor = 0;
        readerLifecycleWrapped = true;
      }
      ReaderSlot slot = slots.slotAt(index);
      if (slot == null || !readers.isTerminated(slot)) {
        continue;
      }
      collectFinalAccess(slots, index, slot);
      WriterResource resource = readers.detachTerminated(index);
      if (resource != null && writerResources != null) {
        writerResources.requestRetirement(resource);
      }
      retired++;
    }
    if (readerLifecycleCheckActive
        && readerLifecycleWrapped
        && slots.firstLiveSlot(readerLifecycleCursor, readerLifecycleStart) < 0) {
      finishReaderLifecycleSweep();
    }
    // A bounded lifecycle sweep advances its cursor even when every inspected owner is still
    // live. Count that scan as progress so the adaptive controller does not mistake a real
    // registry walk for an empty, immediately-repeatable maintenance pass.
    return Math.max(retired, checked);
  }

  private void collectAccessCounters(
      ReaderRegistry.SlotTableSnapshot slots, int index, ReaderSlot slot) {
    long consumedHits = slots.consumedHits(index);
    long hitDelta = slot.publishedHits - consumedHits;
    if (hitDelta != 0L) {
      hits += hitDelta;
      slots.consumedHits(index, consumedHits + hitDelta);
    }
    long consumedMisses = slots.consumedMisses(index);
    long missDelta = slot.publishedMisses - consumedMisses;
    if (missDelta != 0L) {
      misses += missDelta;
      slots.consumedMisses(index, consumedMisses + missDelta);
    }
  }

  private void collectFinalAccess(
      ReaderRegistry.SlotTableSnapshot slots, int index, ReaderSlot slot) {
    collectAccessCounters(slots, index, slot);
    AccessRing access = slot.access;
    if (access != null) {
      access.poll(Integer.MAX_VALUE, this);
    }
  }

  private void finishReaderLifecycleSweep() {
    readerLifecycleCheckActive = false;
    readerLifecycleWrapped = false;
    nextReaderLifecycleCheckNanos =
        readers.hasRegisteredSlots()
            ? saturatingAdd(sampleMonotonicNow(), READER_LIFECYCLE_PERIOD_NANOS)
            : Long.MAX_VALUE;
  }

  /**
   * Counts enough pending access work to choose immediate continuation versus the deadline path.
   */
  private int pendingAccessRecords(ReaderRegistry.SlotTableSnapshot slots) {
    int pending = 0;
    for (int chunkIndex = 0; chunkIndex < slots.slotChunkCount(); chunkIndex++) {
      long bits = slots.liveBitmap(chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        int index = (chunkIndex << ReaderRegistry.SLOT_CHUNK_SHIFT) + offset;
        ReaderSlot slot = slots.slotAt(index);
        if (slot != null) {
          pending =
              cappedAccessCount(
                  pending, boundedWorkCount(slot.publishedHits - slots.consumedHits(index)));
          pending =
              cappedAccessCount(
                  pending, boundedWorkCount(slot.publishedMisses - slots.consumedMisses(index)));
          AccessRing access = slot.access;
          if (access != null) {
            pending = cappedAccessCount(pending, access.size());
          }
          if (pending > AccessRing.LOW_WATERMARK) {
            return pending;
          }
        }
        bits &= bits - 1L;
      }
    }
    return pending;
  }

  private static int cappedAccessCount(int current, int addition) {
    if (addition <= 0) {
      return current;
    }
    return Math.min(AccessRing.LOW_WATERMARK + 1, saturatingIntAdd(current, addition));
  }

  @Override
  public void accept(
      Entry entry, long observedValueAddress, long observedGeneration, int observedPolicyState) {
    if (isCurrent(entry)
        && Entry.samePublishedValue(entry.valueAddress, observedValueAddress)
        && entry.generation() == observedGeneration) {
      policy.access(entry, observedPolicyState);
      policy.recordAccessHit();
    }
  }

  /** Tracks a QSBR-blocked sealed queue without releasing any SAFE native memory on the actor. */
  private void updateReclaimPublicationState(long blockSample) {
    boolean wasBlocked = reclaimBlocked;
    if (!retirements.hasSealedSegments()) {
      reclaimBlocked = false;
      if (wasBlocked && retirementReclaimBlockedSinceNanos != Long.MIN_VALUE) {
        retirementReclaimBlockedNanos =
            saturatingAdd(
                retirementReclaimBlockedNanos,
                Math.max(0L, blockSample - retirementReclaimBlockedSinceNanos));
        retirementReclaimBlockedSinceNanos = Long.MIN_VALUE;
      }
      resetReclaimRetry();
      return;
    }
    // The actor may not release SAFE segments, but it still owns the QSBR observation that makes
    // a sealed segment eligible for publication. Keep the existing bounded retry/wake protocol.
    reclaimBlocked = true;
    if (!wasBlocked) {
      retirementReclaimBlockedCount = saturatingAdd(retirementReclaimBlockedCount, 1L);
      retirementReclaimBlockedSinceNanos = blockSample;
    }
    scheduleReclaimRetry();
  }

  private void scheduleReclaimRetry() {
    reclaimRetryNanos = saturatingAdd(sampleMonotonicNow(), reclaimRetryBackoffNanos);
    reclaimRetryBackoffNanos = Math.min(RECLAIM_RETRY_MAX_NANOS, reclaimRetryBackoffNanos << 1);
  }

  private void resetReclaimRetry() {
    reclaimRetryNanos = Long.MAX_VALUE;
    reclaimRetryBackoffNanos = RECLAIM_RETRY_INITIAL_NANOS;
  }

  @Override
  public void expire(Entry entry, long expectedGeneration, long expectedValueAddress) {
    long taggedAddress = entry.valueAddress;
    long address = Entry.rawValueAddress(taggedAddress);
    if (taggedAddress != expectedValueAddress || address == 0L || !Entry.hasTtl(taggedAddress)) {
      rescheduleCurrentTimerAfterValueRace(entry, taggedAddress);
      return;
    }
    long deadline = ValueBlock.deadlineNanos(address);
    if (deadline == NO_DEADLINE || deadline > nowNanos) {
      return;
    }
    long lagNanos = Math.max(0L, nowNanos - deadline);
    long lagMillis = lagNanos / 1_000_000L;
    timeoutLagMillis = Math.max(timeoutLagMillis, lagMillis);
    if (removeFromMap(
        entry, false, expectedGeneration, expectedValueAddress, RemovalCause.EXPIRED)) {
      physicalExpired++;
    } else {
      if (Entry.samePublishedValue(entry.valueAddress, expectedValueAddress) && isCurrent(entry)) {
        wheel.add(entry, deadline);
      }
    }
  }

  /**
   * The timer node was detached before expiry validation, so restore a concurrently published
   * value.
   */
  private void rescheduleCurrentTimerAfterValueRace(Entry entry, long taggedAddress) {
    if (!entry.isAlive() || taggedAddress == 0L || !Entry.hasTtl(taggedAddress)) {
      return;
    }
    long address = Entry.rawValueAddress(taggedAddress);
    if (address != 0L) {
      wheel.add(entry, ValueBlock.deadlineNanos(address));
    }
  }

  public boolean removeFromMap(
      Entry entry, boolean eviction, long expectedGeneration, long expectedValueAddress) {
    return removeFromMap(entry, eviction, expectedGeneration, expectedValueAddress, null);
  }

  private boolean removeFromMap(
      Entry entry,
      boolean eviction,
      long expectedGeneration,
      long expectedValueAddress,
      RemovalCause cause) {
    if (expectedValueAddress == 0L) {
      return false;
    }
    boolean writerHeld = false;
    boolean removed = false;
    long value = 0L;
    try {
      if (!entry.claimWriter()) {
        return false;
      }
      writerHeld = true;
      if (entry.generation() != expectedGeneration
          || !Entry.samePublishedValue(entry.valueAddress, expectedValueAddress)) {
        return false;
      }
      entry.markRetired();
      boolean physicallyRemoved;
      int policyLinkId = entry.policyLinkId();
      if (eviction) {
        // Capacity victims are policy-owned.  A missing or timer-only link has no reliable hash
        // seed, so fail the conditional removal instead of falling back to Entry.hashCode().
        physicallyRemoved =
            policyLinkId != 0
                && links.policyState(policyLinkId) != Entry.POLICY_NONE
                && data.remove(evictionKey.reset(entry, links.keyHash(policyLinkId))) == entry;
      } else {
        // Expiry can encounter a timer-only record before its policy mutation is applied.  Keep
        // the existing identity helper for that cold path; it is not the capacity eviction hot
        // path and does not assume an initialized policy mirror.
        physicallyRemoved = identityRemoval.remove(data, entry);
      }
      if (!physicallyRemoved) {
        entry.restoreAlive();
        releaseWriter(entry);
        writerHeld = false;
        return false;
      }
      removed = true;
      value = Entry.rawValueAddress(entry.valueAddress);
      if (eviction) {
        long valueAllocation =
            value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
        recordSizeEviction(entry.keyAllocationLength(), valueAllocation);
      }
      markLogicallyAbsentByActor(entry);
      clearValue(entry);
      releaseWriter(entry);
      writerHeld = false;
    } catch (Throwable failure) {
      // A CHM implementation may unlink the node and throw afterwards. The unlink is already
      // durable in that case: finish the victim's accounting and retirement, then propagate the
      // original failure so the cache enters its terminal state. Never restore this victim.
      if (writerHeld && physicallyUnlinked(entry)) {
        try {
          removed = true;
          value = Entry.rawValueAddress(entry.valueAddress);
          if (eviction) {
            long valueAllocation =
                value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
            recordSizeEviction(entry.keyAllocationLength(), valueAllocation);
          }
          markLogicallyAbsentByActor(entry);
          clearValue(entry);
          releaseWriter(entry);
          writerHeld = false;
          wheel.remove(entry);
          policy.remove(entry, eviction);
          policyDirty = true;
          retireEntryBlocks(entry, value);
        } catch (Throwable cleanupFailure) {
          if (cleanupFailure != failure) {
            failure.addSuppressed(cleanupFailure);
          }
          recordTerminalFailure(cleanupFailure);
        }
      }
      throw failure;
    } finally {
      if (writerHeld) {
        releaseWriter(entry);
      }
    }
    if (!removed) {
      return false;
    }
    wheel.remove(entry);
    policy.remove(entry, eviction);
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

  private boolean physicallyUnlinked(Entry entry) {
    try {
      return !isMappedEntry(entry);
    } catch (Throwable ignored) {
      return false;
    }
  }

  /** Checks map ownership with one identity-safe CHM lookup and no table-wide scan. */
  private boolean isMappedEntry(Entry target) {
    if (target == null) {
      return false;
    }
    try {
      int linkId = target.policyLinkId();
      int keyHash =
          linkId != 0 && links.policyState(linkId) != Entry.POLICY_NONE
              ? links.keyHash(linkId)
              : target.keyHash();
      return data.get(evictionKey.reset(target, keyHash)) == target;
    } catch (Throwable ignored) {
      // A failure during exceptional cleanup must not free a possibly still-mapped native entry.
      return true;
    }
  }

  private void retireEntryBlocks(Entry entry, long value) {
    long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
    retireActorValueBlock(value, valueAllocation);
    long keyAllocation = entry.nativeKeyAllocationLength();
    retireActorValue(entry.nativeKeyAddress, keyAllocation);
    links.maybeRelease(entry);
  }

  /** Records a low-frequency writer-side replacement sample at pointer publication time. */
  public void recordResidenceSample(long createdAtMillis, long publishedAtMillis) {
    long residenceMillis = publishedAtMillis - createdAtMillis;
    if (residenceMillis < 0L) {
      residenceMillis = 0L;
    }
    residenceSampleCount.incrementAndGet();
    residenceSampleTotalMillis.addAndGet(residenceMillis);
  }

  private double averageResidenceSampleMillis() {
    long count = residenceSampleCount.get();
    return count == 0L ? 0.0d : (double) residenceSampleTotalMillis.get() / count;
  }

  private boolean isCurrent(Entry entry) {
    return entry.isAlive();
  }

  private void markLogicallyAbsent(Entry entry) {
    LogicalAdmission admission = logicalAdmission;
    if (admission != null) {
      admission.markAbsent(entry);
      return;
    }
    entry.markLogicallyAbsent();
  }

  /** Completes the actor-owned logical transition after a successful identity unlink. */
  private long markLogicallyAbsentByActor(Entry entry) {
    LogicalAdmission admission = logicalAdmission;
    if (admission == null) {
      entry.markLogicallyAbsentAfterWriterClaim();
      return 0L;
    }
    return admission.markAbsentByActor(entry);
  }

  /** Completes the native writer transition and wakes an application waiter, if any. */
  private void releaseWriter(Entry entry) {
    entry.finishWriter();
    if (isStopping()) {
      // The normal live-entry handoff uses notify(). Once shutdown starts, every waiter must
      // observe the terminal state and leave rather than relying on another writer turnover.
      synchronized (entry) {
        entry.notifyAll();
      }
    }
  }

  private void shutdownAndFree() {
    Throwable failure = null;
    try {
      rejectPendingTestTasks(new IllegalStateException("maintenance is stopping"));
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    WriterResourceRegistry resources = writerResources;
    if (resources != null) {
      try {
        resources.detachAll();
      } catch (Throwable cleanupFailure) {
        failure = appendShutdownFailure(failure, cleanupFailure);
      }
      try {
        resources.processRetirements();
      } catch (Throwable cleanupFailure) {
        failure = appendShutdownFailure(failure, cleanupFailure);
      }
    }

    try {
      startReaderLifecycleSweep();
      scanTerminatedReaders(READER_LIFECYCLE_SCAN_LIMIT);
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      awaitActiveReaders();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      while (readerLifecycleCheckActive) {
        scanTerminatedReaders(READER_LIFECYCLE_SCAN_LIMIT);
        if (resources != null) {
          resources.processRetirements();
        }
      }
      drainAllAccesses();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    WriterLifecycleJournal writerJournal = writerLifecycleJournal;
    if (writerJournal != null) {
      try {
        while (writerJournal.hasCommittedRecords()) {
          long[] watermark = writerJournal.captureWatermark();
          if (drainAllWriterLifecycleJournal(watermark) == 0) {
            break;
          }
        }
      } catch (Throwable cleanupFailure) {
        failure = appendShutdownFailure(failure, cleanupFailure);
      }
    }

    try {
      retirements.cutAllProducersAtWatermark();
      retirements.sealReadySegments();
      retirements.publishSafe(true, true);
      retirements.reclaimActorResult(memory, Integer.MAX_VALUE);
      while (retirements.consumeReadyHint()) {
        int sealed = retirements.sealReadySegments();
      }
      if (resources != null) {
        resources.processRetirements();
      }
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      rejectPendingTestTasks(new IllegalStateException("maintenance is stopping"));
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    for (Entry entry : data.values()) {
      long value = Entry.rawValueAddress(entry.valueAddress);
      try {
        // Internal shutdown owns the final ledger transition before clearing the Java map. This
        // keeps the LongAdder pair quiescent and lets the cache-side cold assertion reconcile to
        // an empty mapping set.
        markLogicallyAbsent(entry);
        clearValue(entry);
      } catch (Throwable cleanupFailure) {
        failure = appendShutdownFailure(failure, cleanupFailure);
      }
      if (value != 0L) {
        try {
          memory.releaseEntry(value, ValueBlock.allocationLength(ValueBlock.length(value)));
        } catch (Throwable cleanupFailure) {
          failure = appendShutdownFailure(failure, cleanupFailure);
        }
      }
      try {
        memory.releaseEntry(entry.nativeKeyAddress, entry.nativeKeyAllocationLength());
      } catch (Throwable cleanupFailure) {
        failure = appendShutdownFailure(failure, cleanupFailure);
      }
    }
    try {
      data.clear();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    // Every teardown step is independent. In particular, a failed final retirement release must
    // not prevent closing the journal, policy, links, and native arenas below.
    try {
      retirements.close();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      policy.close();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      links.close();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      readers.clear();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      readers.close();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    try {
      memory.closeArenas();
    } catch (Throwable cleanupFailure) {
      failure = appendShutdownFailure(failure, cleanupFailure);
    }
    evictionNotifier = null;
    if (failure != null) {
      recordTerminalFailure(failure);
    }
  }

  private static Throwable appendShutdownFailure(Throwable first, Throwable next) {
    if (first == null) {
      return next;
    }
    if (first != next) {
      first.addSuppressed(next);
    }
    return first;
  }

  private void awaitActiveReaders() {
    // Internal stop expects quiesced callers; also cover remaining protected reads.
    while (hasActiveReaders()) {
      if (!readerLifecycleCheckActive) {
        startReaderLifecycleSweep();
      }
      scanTerminatedReaders(READER_LIFECYCLE_SCAN_LIMIT);
      WriterResourceRegistry resources = writerResources;
      if (resources != null) {
        resources.processRetirements();
      }
      if (!hasActiveReaders()) {
        break;
      }
      LockSupport.parkNanos(1_000_000L);
    }
  }

  private void drainAllAccesses() {
    while (hasPendingAccessReadOnly()) {
      drainAccesses(readers.slotTableSnapshot(), Integer.MAX_VALUE);
    }
  }

  private void clearValue(Entry entry) {
    entry.clearValue();
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

  private boolean hasActiveReaders() {
    return readers.hasActiveReader();
  }

  private void retireActorValue(long address, long allocation) {
    retirements.appendStructural(address, allocation);
    actorRetirementNeedsFlushFence |= flushRequest.get() != null;
  }

  private void retireActorValueBlock(long address, long allocation) {
    retirements.append(address, allocation);
    actorRetirementNeedsFlushFence |= flushRequest.get() != null;
  }

  private void signal() {
    if (wakeGate.signal()) {
      if (wakeSuppressedByParkTimer(System.nanoTime())) {
        return;
      }
      LockSupport.unpark(thread);
    }
  }

  /** True when the pending park timer wakes the actor within the batching window. */
  boolean wakeSuppressedByParkTimer(long nowNanos) {
    long remaining = parkDeadlineNanos - nowNanos;
    return remaining > 0L && remaining <= WAKE_SUPPRESSION_WINDOW_NANOS;
  }

  private boolean isStopping() {
    return stopState.get() != 0L;
  }

  /** Publishes coalesced work bits and wakes the actor. */
  private void requestWork(int work) {
    if (work == 0) {
      return;
    }
    int current;
    do {
      current = requestedWork.get();
      if ((current | work) == current) {
        break;
      }
    } while (!requestedWork.compareAndSet(current, current | work));
    signal();
  }

  /** Captures reusable source cuts once so one actor turn cannot chase a growing source. */
  private TurnCuts captureTurnCuts(WorkPlan plan) {
    WriterLifecycleJournal lifecycle = writerLifecycleJournal;
    turnCuts.ensureLifecycleLanes(lifecycle == null ? 0 : lifecycle.laneCount());
    if (lifecycle != null && (plan.removals || plan.flush)) {
      lifecycle.captureWatermark(turnCuts.lifecycleWatermark);
      FlushRequest request = plan.flushRequest;
      if (!isStopping() && request != null && request.lifecycleWatermark != null) {
        long[] flushCut = request.lifecycleWatermark;
        for (int index = 0; index < turnCuts.lifecycleWatermark.length; index++) {
          turnCuts.lifecycleWatermark[index] =
              index < flushCut.length
                  ? Math.min(turnCuts.lifecycleWatermark[index], flushCut[index])
                  : 0L;
        }
      }
    }
    if (plan.access || plan.flush) {
      turnCuts.accessSlots = readers.slotTableSnapshot();
      captureAccessTurnCut(turnCuts.accessSlots, turnCuts);
    } else {
      turnCuts.accessSlots = null;
      turnCuts.accessRecords = 0;
    }
    turnCuts.ensureRetirementLanes(retirements.laneCount() + 1);
    boolean retirementWork =
        plan.seal
            || plan.flush
            || plan.safe
            || retirements.hasSealedSegments()
            || retirements.hasSafeSegments();
    if (isStopping()) {
      retirements.captureAndCutWatermark(turnCuts.retirementWatermark);
    } else if (plan.seal) {
      retirements.captureAndCutReadyWatermark(turnCuts.retirementWatermark);
    } else if (retirementWork) {
      retirements.captureTurnWatermark(turnCuts.retirementWatermark);
    }
    WriterResourceRegistry resources = writerResources;
    turnCuts.resourceVersion =
        resources == null ? Long.MAX_VALUE : resources.captureRegistrationVersion();
    return turnCuts;
  }

  /** Cuts partial producers at idle after the active flush's fixed boundary has completed. */
  private boolean cutIdleRetirementRecords(int retirementState) {
    if ((retirementState & RetirementJournal.WORK_STATE_OPEN) == 0
        || (!isStopping() && flushRequest.get() != null)) {
      return false;
    }
    retirements.cutAllProducersAtWatermark();
    retirements.requestSeal();
    requestWork(WORK_RETIREMENT);
    return true;
  }

  private void captureAccessTurnCut(ReaderRegistry.SlotTableSnapshot slots, TurnCuts turn) {
    int pendingRecords = 0;
    long dropped = 0L;
    for (int chunkIndex = 0; chunkIndex < slots.slotChunkCount(); chunkIndex++) {
      long bits = slots.liveBitmap(chunkIndex);
      while (bits != 0L) {
        int offset = Long.numberOfTrailingZeros(bits);
        int index = (chunkIndex << ReaderRegistry.SLOT_CHUNK_SHIFT) + offset;
        ReaderSlot slot = slots.slotAt(index);
        if (slot != null) {
          pendingRecords =
              saturatingIntAdd(
                  pendingRecords,
                  boundedWorkCount(Math.max(0L, slot.publishedHits - slots.consumedHits(index))));
          pendingRecords =
              saturatingIntAdd(
                  pendingRecords,
                  boundedWorkCount(
                      Math.max(0L, slot.publishedMisses - slots.consumedMisses(index))));
          AccessRing access = slot.access;
          if (access != null) {
            int depth = access.size();
            pendingRecords = saturatingIntAdd(pendingRecords, depth);
            dropped = saturatingAdd(dropped, access.droppedCount());
          }
        }
        bits &= bits - 1L;
      }
    }
    turn.accessRecords = pendingRecords;
    turn.accessDropped = dropped;
    accessRingDroppedCount = turn.accessDropped;
  }

  private static int boundedWorkCount(long count) {
    return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, count));
  }

  private static int saturatingIntAdd(int left, int right) {
    if (right > 0 && left > Integer.MAX_VALUE - right) {
      return Integer.MAX_VALUE;
    }
    return left + right;
  }

  private static long saturatingAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }

  /** Actor-local CHM key adapter that compares by Entry identity and uses the mirrored hash. */
  private static final class EvictionKey {
    private Entry victim;
    private int hash;

    private EvictionKey reset(Entry victim, int keyHash) {
      this.victim = victim;
      this.hash = keyHash;
      return this;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object other) {
      return other == victim;
    }
  }

  private static final class WorkDecision {
    private int requested;
    private int retirementState;
    private int runnableCheck;
    private FlushRequest flushRequest;
    private boolean capacityOverTarget;
    private boolean capacityRetryDue;
    private boolean writerLifecycleWork;
    private boolean writerResourceWork;
    private boolean ghostRehash;
    private boolean accessImmediate;
    private boolean accessScanActive;
    private boolean readerLifecycleWork;
    private boolean readerNotificationWork;
    private boolean testTaskWork;
    private boolean ttlDue;
    private boolean pendingSealed;
    private boolean pendingSafe;
    private boolean pendingWriterLifecycleWatermark;
    private boolean readersChecked;
    private boolean activeReaders;
    private WorkPlan plan;

    private boolean isRunnable() {
      return (runnableCheck & RUNNABLE_CHECK_WORK) != 0;
    }
  }

  private static final class WorkPlan {
    private int requested;
    private FlushRequest flushRequest;
    private boolean removals;
    private boolean resourceRetirements;
    private boolean mutations;
    private boolean capacity;
    private boolean access;
    private boolean accessUrgent;
    private boolean readerLifecycle;
    private boolean ttl;
    private boolean seal;
    private boolean safe;
    private boolean ghostRehash;
    private boolean flush;
    private boolean refreshClock;
    private int lifecycleQuota;
    private int capacityQuota;
    private int retirementQuota;
    private int accessQuota;
    private int ttlQuota;
    private int readerLifecycleQuota;
    private int advisoryQuota;

    private WorkPlan reset(int requested) {
      this.requested = requested;
      flushRequest = null;
      removals = false;
      resourceRetirements = false;
      mutations = false;
      capacity = false;
      access = false;
      accessUrgent = false;
      readerLifecycle = false;
      ttl = false;
      seal = false;
      safe = false;
      ghostRehash = false;
      flush = false;
      refreshClock = false;
      lifecycleQuota = 0;
      capacityQuota = 0;
      retirementQuota = 0;
      accessQuota = 0;
      ttlQuota = 0;
      readerLifecycleQuota = 0;
      advisoryQuota = 0;
      return this;
    }

    private boolean hasAdvisoryPolicyMutations() {
      return mutations;
    }

    private boolean hasActions() {
      return removals
          || resourceRetirements
          || mutations
          || capacity
          || access
          || readerLifecycle
          || ttl
          || seal
          || safe
          || ghostRehash
          || flush;
    }
  }

  private static final class TurnCuts {
    private long[] lifecycleWatermark;
    private ReaderRegistry.SlotTableSnapshot accessSlots;
    private int accessRecords;
    private long accessDropped;
    private long resourceVersion;
    private long[] retirementWatermark;

    private TurnCuts(int laneCount) {
      this.lifecycleWatermark = new long[0];
      this.retirementWatermark = new long[laneCount];
    }

    private void ensureLifecycleLanes(int laneCount) {
      if (lifecycleWatermark.length != laneCount) {
        lifecycleWatermark = new long[laneCount];
      }
    }

    private void ensureRetirementLanes(int laneCount) {
      if (retirementWatermark.length != laneCount) {
        retirementWatermark = new long[laneCount];
      }
    }
  }

  private static final class TestActorTask {
    private final Runnable action;
    private final Consumer<Throwable> reject;

    private TestActorTask(Runnable action, Consumer<Throwable> reject) {
      this.action = action;
      this.reject = reject;
    }
  }

  private static final class FlushRequest {
    private final CompletableFuture<Void> future;
    private final long[] lifecycleWatermark;
    private final long[] retirementWatermark;

    private FlushRequest(
        CompletableFuture<Void> future, long[] lifecycleWatermark, long[] retirementWatermark) {
      this.future = future;
      this.lifecycleWatermark = lifecycleWatermark;
      this.retirementWatermark = retirementWatermark;
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
    public final long ttlBacklog;
    public final long nativeAllocationFailureCount;
    public final long residenceSampleCount;
    public final double residenceSampleRate;
    public final double sampledAverageResidenceTimeMillis;
    public final long maintenancePassWorkNanos;
    public final long maintenanceActiveNanosTotal;
    public final long maintenanceParkNanosTotal;
    public final long maintenanceImmediateContinuationCount;
    public final long retirementSealScannedLanes;
    public final long retirementSealSealedLanes;
    public final long retirementSealRecords;
    public final long retirementSealRecordsTotal;
    public final long retirementReclaimRecordsTotal;
    public final long retirementSealScannedLanesTotal;
    public final long retirementReclaimBlockedCount;
    public final long retirementReclaimBlockedNanos;
    public final long maintenancePassCount;
    public final long maintenanceWakeCount;
    public final long maintenanceCollectedRecordsTotal;
    public final long activeReaderCount;
    public final long accessRingDroppedCount;
    public final long retirementQueueDepth;
    public final long retirementPublishedRecordsTotal;
    public final long retirementCompletedRecordsTotal;
    public final long retirementLagRecords;
    public final long retirementUnsafeRecords;
    public final long retirementUnsafeBytes;
    public final long retirementSafeRecords;
    public final long retirementSafeBytes;
    public final long retirementClaimedRecords;
    public final long retirementClaimedBytes;
    public final long retirementActorReclaimedRecords;
    public final long retirementAllocatedSegments;
    public final long retirementReusedSegments;
    public final long retirementTrimmedSegments;
    public final long ghostNativeBytes;
    public final long ghostAllocationTrimCount;
    public final long ghostAllocationDropCount;
    public final boolean ghostRehashPending;
    public final long skipSuppressedAccessCount;
    public final long ghostDeferredPromotionCount;
    public final long lifecycleJournalPublishedRecords;
    public final long lifecycleJournalCompletedRecords;
    public final long lifecycleJournalLagRecords;
    public final long lifecycleJournalAllocatedSegments;
    public final long lifecycleJournalHeadOfLineStopCount;
    public final long allocatorPageAllocatedCount;
    public final long allocatorPageReusedCount;
    public final long allocatorPageReadyCount;
    public final long allocatorPageTrimmedCount;
    public final long writerResourceActiveCount;
    public final long writerResourceRetiringCount;
    public final long writerResourcePooledCount;
    public final long retirementGeneratedBytesTotal;
    public final long retirementCompletedBytesTotal;
    public final double retirementGeneratedBytesPerSecond;
    public final double retirementCompletedBytesPerSecond;
    public final long nativeDebtBudgetBytes;
    public final long nativeDebtHeadroomBytes;
    public final long retirementSafeSegmentCount;
    public final long retirementReclaimBatchCount;
    public final long retirementOldestSafeWaitNanos;
    public final long[] allocatorReadyPagesByClass;
    public final long[] allocatorPagesInUseByClass;
    public final double[] allocatorPageOccupancyByClass;
    public final long allocatorRetainedPagesCurrent;
    public final long allocatorPooledPageCount;
    public final long allocatorTrimmedBytesTotal;

    Snapshot(
        long hits,
        long misses,
        long evictionCount,
        long evictionWeight,
        long expirationCount,
        long liveWeight,
        long timeoutLagMillis,
        boolean unhealthy,
        long ttlBacklog,
        long nativeAllocationFailureCount,
        long residenceSampleCount,
        double residenceSampleRate,
        double sampledAverageResidenceTimeMillis,
        long maintenancePassWorkNanos,
        long maintenanceActiveNanosTotal,
        long maintenanceParkNanosTotal,
        long maintenanceImmediateContinuationCount,
        long retirementSealScannedLanes,
        long retirementSealSealedLanes,
        long retirementSealRecords,
        long retirementSealRecordsTotal,
        long retirementReclaimRecordsTotal,
        long retirementSealScannedLanesTotal,
        long retirementReclaimBlockedCount,
        long retirementReclaimBlockedNanos,
        long maintenancePassCount,
        long maintenanceWakeCount,
        long maintenanceCollectedRecordsTotal,
        long activeReaderCount,
        long accessRingDroppedCount,
        long retirementQueueDepth,
        long retirementPublishedRecordsTotal,
        long retirementCompletedRecordsTotal,
        long retirementLagRecords,
        long retirementUnsafeRecords,
        long retirementUnsafeBytes,
        long retirementSafeRecords,
        long retirementSafeBytes,
        long retirementClaimedRecords,
        long retirementClaimedBytes,
        long retirementActorReclaimedRecords,
        long retirementAllocatedSegments,
        long retirementReusedSegments,
        long retirementTrimmedSegments,
        long ghostNativeBytes,
        long ghostAllocationTrimCount,
        long ghostAllocationDropCount,
        boolean ghostRehashPending,
        long skipSuppressedAccessCount,
        long ghostDeferredPromotionCount,
        long lifecycleJournalPublishedRecords,
        long lifecycleJournalCompletedRecords,
        long lifecycleJournalLagRecords,
        long lifecycleJournalAllocatedSegments,
        long lifecycleJournalHeadOfLineStopCount,
        long allocatorPageAllocatedCount,
        long allocatorPageReusedCount,
        long allocatorPageReadyCount,
        long allocatorPageTrimmedCount,
        long writerResourceActiveCount,
        long writerResourceRetiringCount,
        long writerResourcePooledCount,
        long retirementGeneratedBytesTotal,
        long retirementCompletedBytesTotal,
        double retirementGeneratedBytesPerSecond,
        double retirementCompletedBytesPerSecond,
        long nativeDebtBudgetBytes,
        long nativeDebtHeadroomBytes,
        long retirementSafeSegmentCount,
        long retirementReclaimBatchCount,
        long retirementOldestSafeWaitNanos,
        long[] allocatorReadyPagesByClass,
        long[] allocatorPagesInUseByClass,
        double[] allocatorPageOccupancyByClass,
        long allocatorRetainedPagesCurrent,
        long allocatorPooledPageCount,
        long allocatorTrimmedBytesTotal) {
      this.hits = hits;
      this.misses = misses;
      this.evictionCount = evictionCount;
      this.evictionWeight = evictionWeight;
      this.expirationCount = expirationCount;
      this.liveWeight = liveWeight;
      this.timeoutLagMillis = timeoutLagMillis;
      this.unhealthy = unhealthy;
      this.ttlBacklog = ttlBacklog;
      this.nativeAllocationFailureCount = nativeAllocationFailureCount;
      this.residenceSampleCount = residenceSampleCount;
      this.residenceSampleRate = residenceSampleRate;
      this.sampledAverageResidenceTimeMillis = sampledAverageResidenceTimeMillis;
      this.maintenancePassWorkNanos = maintenancePassWorkNanos;
      this.maintenanceActiveNanosTotal = maintenanceActiveNanosTotal;
      this.maintenanceParkNanosTotal = maintenanceParkNanosTotal;
      this.maintenanceImmediateContinuationCount = maintenanceImmediateContinuationCount;
      this.retirementSealScannedLanes = retirementSealScannedLanes;
      this.retirementSealSealedLanes = retirementSealSealedLanes;
      this.retirementSealRecords = retirementSealRecords;
      this.retirementSealRecordsTotal = retirementSealRecordsTotal;
      this.retirementReclaimRecordsTotal = retirementReclaimRecordsTotal;
      this.retirementSealScannedLanesTotal = retirementSealScannedLanesTotal;
      this.retirementReclaimBlockedCount = retirementReclaimBlockedCount;
      this.retirementReclaimBlockedNanos = retirementReclaimBlockedNanos;
      this.maintenancePassCount = maintenancePassCount;
      this.maintenanceWakeCount = maintenanceWakeCount;
      this.maintenanceCollectedRecordsTotal = maintenanceCollectedRecordsTotal;
      this.activeReaderCount = activeReaderCount;
      this.accessRingDroppedCount = accessRingDroppedCount;
      this.retirementQueueDepth = retirementQueueDepth;
      this.retirementPublishedRecordsTotal = retirementPublishedRecordsTotal;
      this.retirementCompletedRecordsTotal = retirementCompletedRecordsTotal;
      this.retirementLagRecords = retirementLagRecords;
      this.retirementUnsafeRecords = retirementUnsafeRecords;
      this.retirementUnsafeBytes = retirementUnsafeBytes;
      this.retirementSafeRecords = retirementSafeRecords;
      this.retirementSafeBytes = retirementSafeBytes;
      this.retirementClaimedRecords = retirementClaimedRecords;
      this.retirementClaimedBytes = retirementClaimedBytes;
      this.retirementActorReclaimedRecords = retirementActorReclaimedRecords;
      this.retirementAllocatedSegments = retirementAllocatedSegments;
      this.retirementReusedSegments = retirementReusedSegments;
      this.retirementTrimmedSegments = retirementTrimmedSegments;
      this.ghostNativeBytes = ghostNativeBytes;
      this.ghostAllocationTrimCount = ghostAllocationTrimCount;
      this.ghostAllocationDropCount = ghostAllocationDropCount;
      this.ghostRehashPending = ghostRehashPending;
      this.skipSuppressedAccessCount = skipSuppressedAccessCount;
      this.ghostDeferredPromotionCount = ghostDeferredPromotionCount;
      this.lifecycleJournalPublishedRecords = lifecycleJournalPublishedRecords;
      this.lifecycleJournalCompletedRecords = lifecycleJournalCompletedRecords;
      this.lifecycleJournalLagRecords = lifecycleJournalLagRecords;
      this.lifecycleJournalAllocatedSegments = lifecycleJournalAllocatedSegments;
      this.lifecycleJournalHeadOfLineStopCount = lifecycleJournalHeadOfLineStopCount;
      this.allocatorPageAllocatedCount = allocatorPageAllocatedCount;
      this.allocatorPageReusedCount = allocatorPageReusedCount;
      this.allocatorPageReadyCount = allocatorPageReadyCount;
      this.allocatorPageTrimmedCount = allocatorPageTrimmedCount;
      this.writerResourceActiveCount = writerResourceActiveCount;
      this.writerResourceRetiringCount = writerResourceRetiringCount;
      this.writerResourcePooledCount = writerResourcePooledCount;
      this.retirementGeneratedBytesTotal = retirementGeneratedBytesTotal;
      this.retirementCompletedBytesTotal = retirementCompletedBytesTotal;
      this.retirementGeneratedBytesPerSecond = retirementGeneratedBytesPerSecond;
      this.retirementCompletedBytesPerSecond = retirementCompletedBytesPerSecond;
      this.nativeDebtBudgetBytes = nativeDebtBudgetBytes;
      this.nativeDebtHeadroomBytes = nativeDebtHeadroomBytes;
      this.retirementSafeSegmentCount = retirementSafeSegmentCount;
      this.retirementReclaimBatchCount = retirementReclaimBatchCount;
      this.retirementOldestSafeWaitNanos = retirementOldestSafeWaitNanos;
      this.allocatorReadyPagesByClass = allocatorReadyPagesByClass;
      this.allocatorPagesInUseByClass = allocatorPagesInUseByClass;
      this.allocatorPageOccupancyByClass = allocatorPageOccupancyByClass;
      this.allocatorRetainedPagesCurrent = allocatorRetainedPagesCurrent;
      this.allocatorPooledPageCount = allocatorPooledPageCount;
      this.allocatorTrimmedBytesTotal = allocatorTrimmedBytesTotal;
    }
  }
}
