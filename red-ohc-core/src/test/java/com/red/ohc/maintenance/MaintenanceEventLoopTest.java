package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.jctools.queues.MpscUnboundedArrayQueue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.AccessRing;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.runtime.WriterResource;
import com.red.ohc.runtime.WriterResourceRegistry;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class MaintenanceEventLoopTest {
  private final List<ReaderRegistry> testReaders = new ArrayList<>();

  private ReaderRegistry newReaderRegistry(NativeMemory.Memory memory) {
    ReaderRegistry readers = new ReaderRegistry(memory);
    synchronized (testReaders) {
      testReaders.add(readers);
    }
    return readers;
  }

  @AfterMethod(alwaysRun = true)
  public void closeTestReaderRegistries() {
    List<ReaderRegistry> readers;
    synchronized (testReaders) {
      readers = new ArrayList<>(testReaders);
      testReaders.clear();
    }
    for (ReaderRegistry registry : readers) {
      registry.clear();
      registry.close();
    }
  }

  @Test
  public void sourceCutsAreReusableAndDoNotExposeRecordBudgets() throws Exception {
    assertMissingField(MaintenanceEventLoop.class, "LIFECYCLE_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "MUTATION_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "ACCESS_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "TTL_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "RETIREMENT_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "ASYNC_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "WRITER_NATIVE_DEBT_WAIT_NANOS");
    assertMissingField(MaintenanceEventLoop.class, "writerAssistRunning");
    assertMissingField(MaintenanceEventLoop.class, "writerDebtWaiters");
    assertMissingField(MaintenanceEventLoop.class, "writerDebtProgressVersion");
    assertMissingField(MaintenanceEventLoop.class, "writerDebtTransferVersion");
    assertMissingField(MaintenanceEventLoop.class, "writerDebtTransfersInFlight");
    assertMissingField(MaintenanceEventLoop.class, "writerDebtWaiterBytes");
    assertMissingNestedClass(MaintenanceEventLoop.class, "TurnSnapshot");
    assertTrue(MaintenanceEventLoop.class.getDeclaredField("turnCuts") != null);
  }

  @Test
  public void constructorsRequireAnExplicitNativeDebtBudget() {
    for (java.lang.reflect.Constructor<?> constructor : MaintenanceEventLoop.class.getConstructors()) {
      Class<?>[] parameterTypes = constructor.getParameterTypes();
      assertTrue(
          java.util.Arrays.stream(parameterTypes).anyMatch(type -> type == long.class),
          "every constructor must require an explicit native retirement-debt budget");
    }
  }

  @Test
  public void lifecycleTurnDrainsOnlyRecordsReservedBeforeItsPerLaneWatermark()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal(2);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.bindWriterLifecycleJournal(lifecycle);
      WriterLifecycleLane first = lifecycle.lane(0);
      WriterLifecycleLane second = lifecycle.lane(1);
      first.cancel(first.reserve());
      second.cancel(second.reserve());
      long[] watermark = lifecycle.captureWatermark();
      first.cancel(first.reserve());

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "drainWriterLifecycleJournal", long[].class, int.class, boolean.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, watermark, Integer.MAX_VALUE, false), 2);
      assertEquals(first.reservedRecords(), 1L);
      assertEquals(second.reservedRecords(), 0L);

      assertEquals(drain.invoke(loop, lifecycle.captureWatermark(), Integer.MAX_VALUE, false), 1);
      assertEquals(first.reservedRecords(), 0L);
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void lifecycleTurnRotatesLanesWhenTheRecordLimitIsReached() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal(2);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    try {
      loop.bindWriterLifecycleJournal(lifecycle);
      lifecycle.lane(0).cancel(lifecycle.lane(0).reserve());
      lifecycle.lane(1).cancel(lifecycle.lane(1).reserve());
      long[] watermark = lifecycle.captureWatermark();
      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "drainWriterLifecycleJournal", long[].class, int.class, boolean.class);
      drain.setAccessible(true);

      assertEquals(drain.invoke(loop, watermark, 1, false), 1);
      assertEquals(lifecycle.lane(0).reservedRecords(), 0L);
      assertEquals(lifecycle.lane(1).reservedRecords(), 1L);

      assertEquals(drain.invoke(loop, watermark, 1, false), 1);
      assertEquals(lifecycle.lane(1).reservedRecords(), 0L);
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void accessRingReportsOccupancyAndDroppedSamples() {
    AccessRing ring = new AccessRing();
    for (int index = 0; index < 8_192; index++) {
      assertTrue(ring.offer(null, 0L, 0L, Entry.POLICY_NONE));
    }
    assertEquals(ring.size(), 8_192);
    assertFalse(ring.offer(null, 0L, 0L, Entry.POLICY_NONE));
    assertEquals(ring.droppedCount(), 1L);

    int drained = 0;
    while (ring.poll((entry, address, generation, policyState) -> {})) {
      drained++;
    }
    assertEquals(drained, 8_192);
    assertEquals(ring.size(), 0);
  }

  @Test
  public void actorDoesNotExposeDynamicBatchPressureState() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      assertMissingField(MaintenanceEventLoop.class, "maintenanceBatchFactor");
      assertMissingField(MaintenanceEventLoop.class, "maintenancePressurePermille");
      assertMissingField(MaintenanceEventLoop.class, "windowScheduler");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void sourceCutTurnDoesNotUseFixedRecordBudgets() {
    assertTrue(hasField(MaintenanceEventLoop.class, "turnCuts"));
  }

  @Test
  public void lifecycleTurnCleansTerminatedReaderSlotsWithoutAccessRecords() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    Thread owner =
        new Thread(
            () -> {
              readers.register(new ReaderSlot());
            });
    owner.start();
    owner.join();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      Method start = MaintenanceEventLoop.class.getDeclaredMethod("startReaderLifecycleSweep");
      start.setAccessible(true);
      start.invoke(loop);
      Method scan =
          MaintenanceEventLoop.class.getDeclaredMethod("scanTerminatedReaders", int.class);
      scan.setAccessible(true);
      assertEquals(scan.invoke(loop, 256), 1);
      assertEquals(readerCount(readers), 0);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void lifecycleScanCountsInspectedReadersAsMaintenanceProgress() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    readers.register(new ReaderSlot());
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      Method start = MaintenanceEventLoop.class.getDeclaredMethod("startReaderLifecycleSweep");
      start.setAccessible(true);
      start.invoke(loop);
      Method scan =
          MaintenanceEventLoop.class.getDeclaredMethod("scanTerminatedReaders", int.class);
      scan.setAccessible(true);

      assertEquals(
          scan.invoke(loop, 1),
          1,
          "checking a live reader is progress and must not look like an empty maintenance pass");
    } finally {
      readers.clear();
      memory.closeArenas();
    }
  }

  @Test
  public void eventLoopUsesInjectedRetryAndLifecycleTuning() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    RetirementJournal journal = new RetirementJournal(memory);
    EntryLinks links = new EntryLinks(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            ticker,
            1 << 20,
            Eviction.LRU,
            null,
            readers,
            false,
            journal,
            Long.MAX_VALUE,
            links);
    try {
      setBooleanField(loop, "reclaimBlocked", true);

      Method schedule = MaintenanceEventLoop.class.getDeclaredMethod("scheduleReclaimRetry");
      schedule.setAccessible(true);
      schedule.invoke(loop);
      assertEquals(getLongField(loop, "reclaimRetryNanos"), 1_000_000L);
      assertEquals(
          getLongField(loop, "reclaimRetryBackoffNanos"),
          1_000_000L * 2L);

      readers.register(new ReaderSlot());
      Method finish = MaintenanceEventLoop.class.getDeclaredMethod("finishReaderLifecycleSweep");
      finish.setAccessible(true);
      finish.invoke(loop);
      assertEquals(
          getLongField(loop, "nextReaderLifecycleCheckNanos"),
          1_000_000_000L);
    } finally {
      journal.close();
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorActivityCountersAreIndependentOfPolicyPressure() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      MaintenanceEventLoop.Snapshot snapshot = loop.snapshot();
      assertEquals(snapshot.maintenanceActiveNanosTotal, 0L);
      assertEquals(snapshot.maintenanceParkNanosTotal, 0L);
      assertEquals(snapshot.maintenanceImmediateContinuationCount, 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void retirementSegmentCapacityIsOnlyAStorageLayoutUnit() {
    assertEquals(RetirementSegment.CAPACITY, 256);
  }

  @Test
  public void readySegmentsRemainUnpublishedUntilTheActorRuns() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal, Long.MAX_VALUE);
    ThreadContext context = writerContext(memory, journal);
    try {
      appendRetirements(context.retirementLane(), RetirementSegment.CAPACITY);
      assertEquals(loop.retirementJournal().completedRecordsTotal(), 0L);
      assertEquals(loop.retirementJournal().safeSegmentDebt(), 0L);

      loop.requestMaintenance();

      assertEquals(
          loop.retirementJournal().completedRecordsTotal(),
          0L,
          "a ready segment must remain unpublished until the actor gets a turn");
      assertEquals(loop.retirementJournal().safeSegmentDebt(), 0L);
    } finally {
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void sealedRetirementIsReclaimedByTheActorMailbox() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal,
            Long.MAX_VALUE);
    boolean started = false;
    try {
      appendRetirements(journal.actorLane(), RetirementSegment.CAPACITY);
      journal.cutAllProducersAtWatermark();
      assertEquals(
          journal.sealReadySegments(1L),
          RetirementSegment.CAPACITY,
          "the test must publish one complete sealed segment");

      loop.start();
      started = true;
      loop.requestMaintenance();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
      while (journal.completedRecordsTotal() < RetirementSegment.CAPACITY
          && System.nanoTime() < deadline) {
        Thread.yield();
      }

      assertEquals(journal.completedRecordsTotal(), RetirementSegment.CAPACITY);
      assertEquals(
          journal.actorReclaimedRecordsTotal(),
          (long) RetirementSegment.CAPACITY,
          "SAFE retirement must be physically reclaimed by the actor");
    } finally {
      if (started) {
        loop.stop();
        loop.join(2_000L);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void actorReclaimDrainsSafeSegmentsWithoutWriterAssistance()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    int segmentCount = 8;
    long allocation = ValueBlock.allocationLength(1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal,
            Long.MAX_VALUE);
    boolean started = false;
    try {
      WriterArena arena = memory.newWriterArena();
      for (int index = 0; index < RetirementSegment.CAPACITY * segmentCount; index++) {
        long value = arena.allocate(allocation);
        ValueBlock.initialize(value, 0L, 1, 0L);
        journal.append(value, allocation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(
          journal.sealReadySegments(1L), RetirementSegment.CAPACITY * segmentCount);

      loop.start();
      started = true;
      assertTrue(
          awaitCompletedRecords(journal, RetirementSegment.CAPACITY * segmentCount, 3_000L),
          "every published SAFE segment must eventually be reclaimed by the actor");
      assertEquals(
          journal.actorReclaimedRecordsTotal(),
          (long) RetirementSegment.CAPACITY * segmentCount);
      assertEquals(journal.safeSegmentDebt(), 0L);
      long queueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
      while (loop.queueDepth() != 0L && System.nanoTime() < queueDeadline) {
        Thread.yield();
      }
      assertEquals(loop.queueDepth(), 0L, "the actor mailbox must be empty after SAFE reclaim drains");
    } finally {
      if (started) {
        loop.stop();
        loop.join(3_000L);
      } else {
        journal.close();
      }
      memory.closeArenas();
    }
  }

  @Test
  public void blockedRetirementIsNotRunnable() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      loop.retirementJournal().append(0L, 0L);
      assertTrue(loop.retirementJournal().consumeReadyHint());
      loop.retirementJournal().cutAllProducersAtWatermark();
      assertEquals(loop.retirementJournal().sealReadySegments(1L), 1);
      assertTrue(loop.retirementJournal().hasPendingReclaim());
      loop.retirementJournal().finishReadyDrains();
      ((AtomicInteger) getField(loop, "requestedWork")).set(0);

      Method method = MaintenanceEventLoop.class.getDeclaredMethod("hasRunnableWork");
      method.setAccessible(true);
      assertTrue(!(Boolean) method.invoke(loop));
    } finally {
      readers.endOpForTest(activeReader);
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void blockedRetirementExposesRetryDeadlineWhileReaderIsActive()
      throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, ticker, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      retireOne(loop, memory);
      invokeMaintenancePass(loop);

      nowNanos.set(1_000_000L);
      invokeMaintenancePass(loop);

      Method captureWorkDecision =
          MaintenanceEventLoop.class.getDeclaredMethod("captureWorkDecision", boolean.class);
      captureWorkDecision.setAccessible(true);
      Object decision = captureWorkDecision.invoke(loop, false);
      Field decisionRetirementState = decision.getClass().getDeclaredField("retirementState");
      decisionRetirementState.setAccessible(true);
      Field decisionRunnableCheck = decision.getClass().getDeclaredField("runnableCheck");
      decisionRunnableCheck.setAccessible(true);
      Method method =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "nextRetryDeadlineNanos", int.class, int.class);
      method.setAccessible(true);
      assertTrue(
          (Long)
                  method.invoke(
                      loop,
                      decisionRetirementState.getInt(decision),
                      decisionRunnableCheck.getInt(decision))
              != Long.MAX_VALUE,
          "a blocked reader must still expose the retry deadline: notifications are"
              + " suppressed while blocked, so the retry is the wake that retries publication");

      readers.endOpForTest(activeReader);
      decision = captureWorkDecision.invoke(loop, false);
      assertTrue(
          (Long)
                  method.invoke(
                      loop,
                      decisionRetirementState.getInt(decision),
                      decisionRunnableCheck.getInt(decision))
              != Long.MAX_VALUE,
          "the retry deadline must become visible once reclaim can run");
    } finally {
      readers.endOpForTest(activeReader);
      memory.closeArenas();
    }
  }

  @Test
  public void idleRetryDeadlineExposesTheScheduledRetryWhileReaderActive() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot reader = new ReaderSlot();
    int readerIndex = readers.register(reader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers,
            journal,
            Long.MAX_VALUE);
    try {
      readers.beginOpForTest(readerIndex, true);
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      journal.finishReadyDrains();
      setLongField(loop, "reclaimRetryNanos", 1L);

      Method captureWorkDecision =
          MaintenanceEventLoop.class.getDeclaredMethod("captureWorkDecision", boolean.class);
      captureWorkDecision.setAccessible(true);
      Object decision = captureWorkDecision.invoke(loop, false);
      Field decisionRunnableCheck = decision.getClass().getDeclaredField("runnableCheck");
      decisionRunnableCheck.setAccessible(true);
      int check = decisionRunnableCheck.getInt(decision);
      Field decisionRetirementState = decision.getClass().getDeclaredField("retirementState");
      decisionRetirementState.setAccessible(true);
      int retirementState = decisionRetirementState.getInt(decision);
      int runnableWork = intField(MaintenanceEventLoop.class, "RUNNABLE_CHECK_WORK");
      int readersChecked = intField(MaintenanceEventLoop.class, "RUNNABLE_CHECK_READERS");
      int activeReaders =
          intField(MaintenanceEventLoop.class, "RUNNABLE_CHECK_ACTIVE_READERS");
      assertEquals(check & runnableWork, 0);
      assertTrue((check & readersChecked) != 0);
      assertTrue((check & activeReaders) != 0);

      Method retryDeadline =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "nextRetryDeadlineNanos", int.class, int.class);
      retryDeadline.setAccessible(true);
      assertTrue(
          (Long) retryDeadline.invoke(loop, retirementState, check) != Long.MAX_VALUE,
          "a scheduled reclaim retry must expose its deadline even while a reader is active");
    } finally {
      readers.endOpForTest(readerIndex);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void quiescentSealedRetirementAlwaysProducesAPublishPlan() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal,
            Long.MAX_VALUE);
    try {
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      journal.finishReadyDrains();
      setBooleanField(loop, "reclaimBlocked", true);
      setLongField(loop, "reclaimRetryNanos", Long.MAX_VALUE - 1L);

      Method captureWorkDecision =
          MaintenanceEventLoop.class.getDeclaredMethod("captureWorkDecision", boolean.class);
      captureWorkDecision.setAccessible(true);
      Object decision = captureWorkDecision.invoke(loop, false);
      Field decisionRetirementState = decision.getClass().getDeclaredField("retirementState");
      decisionRetirementState.setAccessible(true);
      int retirementState = decisionRetirementState.getInt(decision);
      Field decisionRunnableCheck = decision.getClass().getDeclaredField("runnableCheck");
      decisionRunnableCheck.setAccessible(true);
      int check = decisionRunnableCheck.getInt(decision);
      assertTrue(
          (check & intField(MaintenanceEventLoop.class, "RUNNABLE_CHECK_WORK")) != 0);

      Field planField = decision.getClass().getDeclaredField("plan");
      planField.setAccessible(true);
      Object plan = planField.get(decision);
      Field seal = plan.getClass().getDeclaredField("seal");
      seal.setAccessible(true);
      assertTrue(
          seal.getBoolean(plan),
          "a quiescent sealed segment must enter a runnable loop for SAFE publication");
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void flushDoesNotBusyLoopWhileReaderRetirementIsPinned() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      loop.retirementJournal().append(0L, 0L);
      CompletableFuture<Void> flush = loop.flush();
      invokeMaintenancePassWork(loop);
      Class<?> decisionClass =
          Class.forName(
              "com.red.ohc.maintenance.MaintenanceEventLoop$WorkDecision");
      Method method =
          MaintenanceEventLoop.class.getDeclaredMethod("flushWorkDue", int.class, decisionClass);
      method.setAccessible(true);
      assertFalse(
          (Boolean) method.invoke(loop, loop.retirementJournal().workState(), null),
          "a flush must park behind a pinned reader instead of spinning");

      readers.endOpForTest(activeReader);
      assertTrue((Boolean) method.invoke(loop, loop.retirementJournal().workState(), null));
      flush.cancel(false);
    } finally {
      readers.endOpForTest(activeReader);
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void closingFlushKeepsAccessInMaintenancePlan() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot slot = new ReaderSlot();
    slot.access = new AccessRing();
    slot.access.offer(
        EntryTestSupport.entry(memory, 0, 92, 0L), 1L, 0L, Entry.POLICY_NONE);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    CompletableFuture<Void> flush = null;
    try {
      loop.registerReader(slot);
      flush = loop.flush();
      loop.stop();

      invokeCaptureWorkDecision(loop, true);
      Object plan = getField(getField(loop, "workDecision"), "plan");

      assertFalse(slot.access.isEmpty(), "the regression setup must retain pending access records");
      assertTrue(getBooleanField(plan, "flush"));
      assertTrue(
          getBooleanField(plan, "access"),
          "a closing flush must retain access draining as a flush dependency");
    } finally {
      if (flush != null && !flush.isDone()) {
        flush.cancel(false);
      }
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void flushDoesNotRestartACompletedReaderLifecycleSweepWhileQsbrIsBlocked()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      loop.retirementJournal().append(0L, 0L);
      CompletableFuture<Void> flush = loop.flush();
      invokeMaintenancePassWork(loop);

      assertFalse(flush.isDone());
      assertFalse(
          getBooleanField(loop, "readerLifecycleCheckActive"),
          "the bounded sweep should finish before the flush waits for QSBR");

      invokeCaptureWorkDecision(loop, false);

      assertFalse(
          getBooleanField(loop, "readerLifecycleCheckActive"),
          "a pinned QSBR flush must park instead of restarting lifecycle cleanup");
      flush.cancel(false);
    } finally {
      readers.endOpForTest(activeReader);
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void newerFlushMustReceiveAReaderLifecycleSweepStartedAfterItsPublication()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    readers.register(new ReaderSlot());
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      Method start = MaintenanceEventLoop.class.getDeclaredMethod("startReaderLifecycleSweep");
      start.setAccessible(true);
      start.invoke(loop);

      CompletableFuture<Void> flush = loop.flush();
      loop.flush();
      invokeMaintenancePassWork(loop);

      assertFalse(
          flush.isDone(),
          "a newer flush must not borrow the lifecycle sweep that was already in progress");

      invokeCaptureWorkDecision(loop, false);
      assertTrue(
          getBooleanField(loop, "readerLifecycleCheckActive"),
          "the newer flush must start a fresh lifecycle sweep");
      invokeMaintenancePassWork(loop);
      assertTrue(flush.isDone());
    } finally {
      readers.clear();
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void readerAdmittedAfterRetirementCutDoesNotArmAReclaimNotification()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot oldReader = new ReaderSlot();
    readers.register(oldReader);
    readers.beginOpForTest(oldReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext newReader = new ThreadContext(null);
    try {
      retireOne(loop, memory);
      invokeMaintenancePassWork(loop);
      assertEquals(loop.epoch(), 2L);

      assertTrue(guard.enter(newReader));
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(0);
      loop.requestMaintenance();
      invokeMaintenancePassWork(loop);

      assertTrue(
          readers.consumeReaderNotification(oldReader),
          "the reader from the retirement epoch must remain armed");
      requestedWork.set(0);
      // The new reader straddled this pass's arm, so the actor armed it too: its exit must
      // drive the retry because it may pin the sealed batch for one extra turn.
      guard.exit(newReader);
      assertTrue(
          requestedWork.get() != 0,
          "a straddling reader's exit must wake the blocked reclaim retry");
    } finally {
      if (newReader.readerDepth() != 0) {
        guard.exit(newReader);
      }
      readers.endOpForTest(oldReader);
      readers.clear();
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void flushCannotCompleteBeforeItsReaderLifecycleSweepIsObserved() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    AtomicReference<ReaderSlot> slotReference = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              ReaderSlot slot = new ReaderSlot();
              readers.register(slot);
              slotReference.set(slot);
            });
    owner.start();
    owner.join();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      CompletableFuture<Void> flush = loop.flush();
      invokeCompleteFlushIfIdle(loop);

      assertFalse(
          flush.isDone(), "the fast completion path must not skip the dead-reader sweep");

      invokeMaintenancePassWork(loop);

      assertTrue(flush.isDone());
      assertEquals(readerCount(readers), 0);
      assertTrue(readers.slotAt(0) == null);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushCompletesAfterEveryCoveredAsyncBatchRunsInOrder() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            ticker,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    ticker.reset();
    int taskCount = 2_049;
    List<Integer> executed = new ArrayList<>(taskCount);
    try {
      for (int index = 0; index < taskCount; index++) {
        int taskIndex = index;
        assertTrue(
            loop.submitAsyncMutation(
                () -> executed.add(taskIndex), Throwable::printStackTrace));
      }
      CompletableFuture<Void> flush = loop.flush();

      loop.start();
      flush.get(3L, TimeUnit.SECONDS);

      assertEquals(executed.size(), taskCount);
      for (int index = 0; index < taskCount; index++) {
        assertEquals(executed.get(index).intValue(), index);
      }
      assertTrue(ticker.monotonicCalls.get() > 0);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 15_000L)
  public void asyncFlushFenceSurvivesConcurrentPublishers() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    int producerCount = 16;
    int mutationsPerProducer = 512;
    ExecutorService producers = Executors.newFixedThreadPool(producerCount);
    CountDownLatch ready = new CountDownLatch(producerCount);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger executed = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>(producerCount);
    try {
      loop.start();
      for (int producer = 0; producer < producerCount; producer++) {
        futures.add(
            producers.submit(
                () -> {
                  ready.countDown();
                  try {
                    assertTrue(start.await(2L, TimeUnit.SECONDS));
                    for (int mutation = 0; mutation < mutationsPerProducer; mutation++) {
                      assertTrue(
                          loop.submitAsyncMutation(
                              executed::incrementAndGet,
                              failure -> {
                                throw new AssertionError(failure);
                              }));
                    }
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                  }
                }));
      }
      assertTrue(ready.await(2L, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) {
        future.get(5L, TimeUnit.SECONDS);
      }

      loop.flush().get(5L, TimeUnit.SECONDS);

      assertEquals(executed.get(), producerCount * mutationsPerProducer);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
      assertEquals(loop.asyncMutationLagRecords(), 0L);
    } finally {
      producers.shutdownNow();
      loop.stop();
      loop.join(2_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void asyncSequenceReservationAndMailboxOfferAreOnePublication() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    ExecutorService producers = Executors.newFixedThreadPool(2);
    CountDownLatch firstSequenceAllocated = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondSubmitStarted = new CountDownLatch(1);
    AtomicBoolean firstReservation = new AtomicBoolean(true);
    List<Integer> executed = new ArrayList<>(2);
    loop.setAsyncSequenceAllocatedHookForTest(
        () -> {
          if (firstReservation.compareAndSet(true, false)) {
            firstSequenceAllocated.countDown();
            await(releaseFirst);
          }
        });
    try {
      loop.start();
      Future<?> first =
          producers.submit(
              () ->
                  assertTrue(
                      loop.submitAsyncMutation(() -> executed.add(1), Throwable::printStackTrace)));
      assertTrue(firstSequenceAllocated.await(2L, TimeUnit.SECONDS));
      Future<?> second =
          producers.submit(
              () -> {
                secondSubmitStarted.countDown();
                assertTrue(
                    loop.submitAsyncMutation(() -> executed.add(2), Throwable::printStackTrace));
              });
      assertTrue(secondSubmitStarted.await(2L, TimeUnit.SECONDS));
      releaseFirst.countDown();
      first.get(2L, TimeUnit.SECONDS);
      second.get(2L, TimeUnit.SECONDS);
      loop.flush().get(2L, TimeUnit.SECONDS);

      assertEquals(executed.size(), 2);
      assertEquals(executed.get(0).intValue(), 1);
      assertEquals(executed.get(1).intValue(), 2);
    } finally {
      releaseFirst.countDown();
      producers.shutdownNow();
      loop.stop();
      loop.join(2_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentFlushCannotLoseActorRetirementWatermark() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal,
            Long.MAX_VALUE);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    CountDownLatch extensionRead = new CountDownLatch(1);
    CountDownLatch releaseExtension = new CountDownLatch(1);
    CountDownLatch secondFlushStarted = new CountDownLatch(1);
    Future<?> extension = null;
    Future<CompletableFuture<Void>> secondFlush = null;
    try {
      loop.flush();
      setBooleanField(loop, "actorRetirementNeedsFlushFence", true);

      loop.setFlushRetirementExtensionHookForTest(
          (Runnable)
              () -> {
                extensionRead.countDown();
                await(releaseExtension);
              });
      Method extend =
          MaintenanceEventLoop.class.getDeclaredMethod("extendPendingFlushForActorRetirements");
      extend.setAccessible(true);
      extension =
          callers.submit(
              () -> {
                try {
                  extend.invoke(loop);
                } catch (Exception failure) {
                  throw new AssertionError(failure);
                }
              });
      assertTrue(extensionRead.await(2L, TimeUnit.SECONDS));

      secondFlush =
          callers.submit(
              () -> {
                secondFlushStarted.countDown();
                return loop.flush();
              });
      assertTrue(secondFlushStarted.await(2L, TimeUnit.SECONDS));
      try {
        secondFlush.get(250L, TimeUnit.MILLISECONDS);
        throw new AssertionError("a concurrent flush published behind an actor watermark update");
      } catch (TimeoutException expected) {
        // The actor's request extension must share the flush publication lock.
      }

      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      assertTrue(journal.actorLane().reserve(reservation));
      journal.actorLane().write(reservation, 0L, 0L);
      journal.actorLane().commit(reservation);

      releaseExtension.countDown();
      extension.get(2L, TimeUnit.SECONDS);
      secondFlush.get(2L, TimeUnit.SECONDS);

      Object request = ((AtomicReference<?>) getField(loop, "flushRequest")).get();
      long[] retirementWatermark = (long[]) getField(request, "retirementWatermark");
      assertTrue(
          retirementWatermark[0] >= 1L,
          "the published flush must retain the actor retirement record cut during the race");
    } finally {
      releaseExtension.countDown();
      if (extension != null) {
        try {
          extension.get(2L, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Preserve the original assertion while ensuring the hook cannot strand the test.
        }
      }
      if (secondFlush != null) {
        try {
          secondFlush.get(2L, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Preserve the original assertion while ensuring the flush caller cannot remain blocked.
        }
      }
      loop.setFlushRetirementExtensionHookForTest(null);
      callers.shutdownNow();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentFlushCallsPublishACompleteFence() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    ExecutorService callers = Executors.newFixedThreadPool(12);
    try {
      loop.start();
      for (int round = 0; round < 40; round++) {
        CountDownLatch ready = new CountDownLatch(12);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<CompletableFuture<Void>>> futures = new ArrayList<>(12);
        for (int caller = 0; caller < 12; caller++) {
          futures.add(
              callers.submit(
                  () -> {
                    ready.countDown();
                    try {
                      assertTrue(start.await(2L, TimeUnit.SECONDS));
                      return loop.flush();
                    } catch (InterruptedException interrupted) {
                      Thread.currentThread().interrupt();
                      throw new AssertionError(interrupted);
                    }
                  }));
        }
        assertTrue(ready.await(2L, TimeUnit.SECONDS));
        start.countDown();
        for (Future<CompletableFuture<Void>> future : futures) {
          CompletableFuture<Void> flush = future.get(2L, TimeUnit.SECONDS);
          assertTrue(flush != null, "a racing CAS must never return a cleared flush request");
          flush.get(2L, TimeUnit.SECONDS);
        }
      }
    } finally {
      callers.shutdownNow();
      loop.stop();
      loop.join(2_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushCutsEveryWriterRetirementProducerBeforeItsRecordBudget() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 4);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), journal, Long.MAX_VALUE);
    try {
      RetirementJournal.Lane fullLane = journal.lane(0);
      for (int index = 0; index < 2_048; index++) {
        RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
        assertTrue(fullLane.reserve(reservation));
        fullLane.write(reservation, 0L, 0L);
        fullLane.commit(reservation);
      }
      RetirementJournal.Lane partialLane = journal.lane(1);
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      assertTrue(partialLane.reserve(reservation));
      partialLane.write(reservation, 0L, 0L);
      partialLane.commit(reservation);

      loop.start();
      loop.flush().get(3L, TimeUnit.SECONDS);
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushCompletesWithoutWaitingForPostWatermarkRetirementReservation() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), journal, Long.MAX_VALUE);
    RetirementSegment.Reservation postFlush = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane lane = journal.lane(0);
      RetirementSegment.Reservation preFlush = new RetirementSegment.Reservation();
      assertTrue(lane.reserve(preFlush));
      lane.write(preFlush, 0L, 0L);
      lane.commit(preFlush);

      CompletableFuture<Void> flush = loop.flush();

      // This reservation is after the flush's producer-close linearization and deliberately stays
      // uncommitted. It must not strand the already captured watermark.
      assertTrue(lane.reserve(postFlush));
      lane.write(postFlush, 0L, 0L);
      loop.start();
      flush.get(3L, TimeUnit.SECONDS);
      assertFalse(flush.isCompletedExceptionally());

      lane.commit(postFlush);
    } finally {
      if (postFlush.segment() != null) {
        journal.lane(0).cancel(postFlush);
      }
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushCompletesWithoutWaitingForPostFlushLifecycleReservation() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal(1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.bindWriterLifecycleJournal(lifecycle);
      CompletableFuture<Void> flush = loop.flush();
      long postFlushSequence = lifecycle.lane(0).reserve();

      loop.start();
      flush.get(3L, TimeUnit.SECONDS);

      assertTrue(flush.isDone());
      lifecycle.lane(0).cancel(postFlushSequence);
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushWaitsForLifecycleReservationCapturedBeforeIt() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal(1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    long preFlushSequence = -1L;
    try {
      loop.bindWriterLifecycleJournal(lifecycle);
      preFlushSequence = lifecycle.lane(0).reserve();
      CompletableFuture<Void> flush = loop.flush();

      loop.start();
      Thread.sleep(100L);
      assertFalse(flush.isDone(), "flush must wait for a lifecycle reservation in its watermark");

      lifecycle.lane(0).cancel(preFlushSequence);
      preFlushSequence = -1L;
      flush.get(3L, TimeUnit.SECONDS);
    } finally {
      if (preFlushSequence >= 0L) {
        lifecycle.lane(0).cancel(preFlushSequence);
      }
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void ordinaryActorDoesNotCutAProducerWhileReservationsKeepArriving() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), journal, Long.MAX_VALUE);
    try {
      RetirementJournal.Lane lane = journal.lane(0);
      RetirementSegment.Reservation first = new RetirementSegment.Reservation();
      assertTrue(lane.reserve(first));
      long firstSequence = first.sequence();
      lane.write(first, 0L, 0L);
      lane.commit(first);

      invokeMaintenancePassWork(loop);

      RetirementSegment.Reservation second = new RetirementSegment.Reservation();
      assertTrue(lane.reserve(second));
      assertEquals(second.sequence(), firstSequence + 1L);
      lane.write(second, 0L, 0L);
      lane.commit(second);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void closingAStartedWorkerRejectsRemainingAsyncBatchesAndClearsDepth()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    int pendingCount = 2_049;
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    AtomicInteger executed = new AtomicInteger();
    AtomicInteger rejected = new AtomicInteger();
    try {
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                firstStarted.countDown();
                await(releaseFirst);
                executed.incrementAndGet();
              },
              failure -> rejected.incrementAndGet()));
      for (int index = 0; index < pendingCount; index++) {
        assertTrue(
            loop.submitAsyncMutation(
                executed::incrementAndGet, failure -> rejected.incrementAndGet()));
      }
      loop.start();
      assertTrue(firstStarted.await(1L, TimeUnit.SECONDS));

      loop.beginClosing();
      loop.stop();
      releaseFirst.countDown();
      loop.join(3_000L);

      assertFalse(loop.isAlive());
      assertEquals(executed.get(), 1);
      assertEquals(rejected.get(), pendingCount);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      releaseFirst.countDown();
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void prePublicationAsyncRejectionsAreCounted() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.beginClosing();
      assertFalse(loop.submitAsyncMutation(() -> {}, failure -> {}));
      assertEquals(loop.asyncMutationRejectedCount(), 1L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void shutdownRejectsEveryQueuedAsyncBatchAndClearsDepth() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            ticker,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    ticker.reset();
    int taskCount = 2_049;
    AtomicInteger executed = new AtomicInteger();
    AtomicInteger rejected = new AtomicInteger();
    try {
      for (int index = 0; index < taskCount; index++) {
        assertTrue(
            loop.submitAsyncMutation(
                executed::incrementAndGet, failure -> rejected.incrementAndGet()));
      }

      loop.beginClosing();
      loop.stop();
      loop.start();
      loop.join(3_000L);

      assertFalse(loop.isAlive());
      assertEquals(executed.get(), 0);
      assertEquals(rejected.get(), taskCount);
      assertTrue(ticker.monotonicCalls.get() > 0);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void idleWorkerStopsWithoutWaitingForAWorkWindow() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.start();
      waitUntilParked(loop);

      loop.stop();
      loop.join(1_000L);

      assertFalse(loop.isAlive());
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void asyncDepthReportsPhysicalQueueSize() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                started.countDown();
                await(release);
              },
              Throwable::printStackTrace));
      assertEquals(loop.asyncMutationQueueDepth(), 1L);

      loop.start();
      assertTrue(started.await(2L, TimeUnit.SECONDS));
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
      release.countDown();
      loop.flush().join();
    } finally {
      release.countDown();
      loop.stop();
      loop.join(1_000L);
      ((NativeMemory.Memory) getField(loop, "memory")).closeArenas();
    }
  }

  @Test
  public void asyncDepthDoesNotTraverseTheMutationQueue() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      assertTrue(
          getField(loop, "mailbox") instanceof MpscUnboundedArrayQueue,
          "all actor messages must use the single growable FIFO mailbox");
      assertEquals(
          loop.asyncMutationQueueDepth(),
          0L,
          "an eventually consistent statistic must not traverse an unbounded concurrent queue");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void mailboxWorkContributesToActorActivityCounters() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      assertTrue(loop.submitAsyncMutation(() -> {}, Throwable::printStackTrace));
      loop.start();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
      // Completion is published inside drainMailbox; activity is published by the enclosing
      // turn's finally block. The first counter is not a barrier for the second snapshot.
      while ((loop.asyncMutationCompletedRecords() < 1L
              || loop.snapshot().maintenanceActiveNanosTotal == 0L)
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertEquals(loop.asyncMutationCompletedRecords(), 1L);
      assertTrue(
          loop.snapshot().maintenanceActiveNanosTotal > 0L,
          "actor work drained from the mailbox must be included in activity statistics");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void mailboxDrainHonorsTheActorTurnLimit() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    AtomicInteger executed = new AtomicInteger();
    AtomicReference<Runnable> resubmitting = new AtomicReference<>();
    resubmitting.set(
        () -> {
          if (executed.incrementAndGet() < 10_000) {
            assertTrue(loop.submitAsyncMutation(resubmitting.get(), Throwable::printStackTrace));
          }
        });
    try {
      assertTrue(loop.submitAsyncMutation(resubmitting.get(), Throwable::printStackTrace));

      int processed = invokeDrainAsyncMutations(loop, Integer.MAX_VALUE);

      assertTrue(processed <= 1_024, "one mailbox drain must be bounded to one actor turn");
      assertTrue(executed.get() < 10_000, "a producer must not monopolize the actor drain");
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void publishedMailboxWorkCannotBeMaskedByAnOlderUnpublishedHead() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    AtomicInteger completed = new AtomicInteger();
    try {
      ((WakeGate) getField(loop, "wakeGate")).requireProcessing();
      assertTrue(loop.submitAsyncMutation(completed::incrementAndGet, Throwable::printStackTrace));
      assertEquals(invokeDrainMutations(loop, Integer.MAX_VALUE), 1);
      assertTrue(
          (Boolean) getField(loop, "mailboxHeadUnpublished"),
          "the first bounded drain must record the transient publication gap");

      assertTrue(loop.submitAsyncMutation(completed::incrementAndGet, Throwable::printStackTrace));
      Thread idleCheck =
          new Thread(
              () -> {
                try {
                  invokeParkUntilWork(loop);
                } catch (Exception failure) {
                  throw new AssertionError(failure);
                }
              },
              "mailbox-publication-idle-check");
      idleCheck.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
      while (idleCheck.isAlive()
          && idleCheck.getState() != Thread.State.WAITING
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      boolean parked = idleCheck.getState() == Thread.State.WAITING;
      if (parked) {
        LockSupport.unpark(idleCheck);
      }
      idleCheck.join(1_000L);
      assertFalse(parked, "a published mailbox item must keep the actor runnable");
      assertTrue(idleCheck.getState() == Thread.State.TERMINATED, "idle check must finish");
      assertEquals(
          completed.get(), 1, "the controlled idle check must not execute the second task");
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void asyncMutationQueueGrowsWithoutRejectingAdmittedWork() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    boolean started = false;
    try {
      for (int index = 0; index < 1_025; index++) {
        assertTrue(
            loop.submitAsyncMutation(
                () -> {},
                failure -> {
                  throw new AssertionError(failure);
                }));
      }
      assertEquals(loop.asyncMutationQueueDepth(), 1_025L);

      loop.start();
      started = true;
      loop.stop();
      loop.join(1_000L);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      if (started && loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void terminalFailureDrainsAdmittedAsyncMessagesWithoutASequenceHole() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CompletableFuture<Void> rejected = new CompletableFuture<>();
    try {
      assertTrue(loop.submitAsyncMutation(() -> {}, rejected::completeExceptionally));
      assertEquals(loop.asyncMutationQueueDepth(), 1L);
      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      assertTrue(rejected.isCompletedExceptionally());
      assertEquals(((AtomicLong) getField(loop, "asyncSubmitted")).get(), 1L);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void oneBrokenRejectCallbackCannotStrandLaterAsyncTasks() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CompletableFuture<Void> later = new CompletableFuture<>();
    try {
      assertTrue(
          loop.submitAsyncMutation(
              () -> {},
              failure -> {
                throw new IllegalStateException("broken rejection callback");
              }));
      assertTrue(loop.submitAsyncMutation(() -> {}, later::completeExceptionally));

      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));

      assertTrue(
          later.isCompletedExceptionally(),
          "one rejection callback must not prevent later tasks from reaching a terminal state");
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void terminalFailurePublishedBeforeAsyncExecutionRejectsTheTask() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    AtomicBoolean executed = new AtomicBoolean();
    CompletableFuture<Void> rejected = new CompletableFuture<>();
    try {
      assertTrue(
          loop.submitAsyncMutation(
              () -> executed.set(true), rejected::completeExceptionally));
      ((AtomicReference<Throwable>) getField(loop, "terminalFailure"))
          .set(new IllegalStateException("maintenance boom"));

      assertEquals(invokeDrainAsyncMutations(loop, 1), 1);

      assertFalse(executed.get(), "an unavailable cache must not execute a queued mutation");
      assertTrue(rejected.isCompletedExceptionally());
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void brokenAsyncRejectCallbackDoesNotBecomeAMaintenanceFailure() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                throw new IllegalStateException("mutation failed");
              },
              failure -> {
                throw new IllegalStateException("broken rejection callback");
              }));

      assertEquals(invokeDrainAsyncMutations(loop, 1), 1);

      assertEquals(((AtomicReference<?>) getField(loop, "terminalFailure")).get(), null);
      assertEquals(loop.asyncMutationFailedCount(), 1L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushBarrierOrdersAsyncSequencesAcrossSignedLongWrap() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    AtomicLong submitted = (AtomicLong) getField(loop, "asyncSubmitted");
    submitted.set(Long.MAX_VALUE - 1L);
    setLongField(loop, "asyncCompletedSequence", Long.MAX_VALUE - 1L);
    AtomicInteger executed = new AtomicInteger();
    try {
      assertTrue(loop.submitAsyncMutation(executed::incrementAndGet, Throwable::printStackTrace));
      assertTrue(loop.submitAsyncMutation(executed::incrementAndGet, Throwable::printStackTrace));
      CompletableFuture<Void> flush = loop.flush();

      loop.start();
      flush.get(2L, TimeUnit.SECONDS);

      assertEquals(
          executed.get(),
          2,
          "a wrapped flush sequence must wait for every task included by its barrier");
    } finally {
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void removalNotificationObservesValueBeforeNativeRetirement() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterArena arena = memory.newWriterArena();
    ConcurrentHashMap<Entry, Entry> data = index();
    AtomicInteger observedPayload = new AtomicInteger(-1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1L << 20,
            Eviction.LRU,
            (entry, address, cause) ->
                observedPayload.set(
                    NativeMemory.getByte(ValueBlock.payloadAddress(address)) & 0xff),
            newReaderRegistry(memory), Long.MAX_VALUE);
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(8);
    long valueAllocation = ValueBlock.allocationLength(8);
    try {
      long keyAddress = arena.allocate(keyAllocation);
      long valueAddress = arena.allocate(valueAllocation);
      ValueBlock.initialize(valueAddress, TimerWheel.TICK_NANOS, 8, 0L);
      NativeMemory.putByte(ValueBlock.payloadAddress(valueAddress), (byte) 0x11);
      Entry entry =
          new Entry(
              keyAddress,
              8,
              Entry.tagValueAddress(valueAddress, true));
      entry.initializeNativeMetadata();
      data.put(entry, entry);

      assertTrue(entry.claimWriter());
      entry.markRetired();
      assertTrue(data.remove(entry, entry));
      entry.valueAddress = 0L;
      entry.finishWriter();

      WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
      loop.bindWriterLifecycleJournal(journal);
      WriterLifecycleLane lane = journal.lane(0);
      long sequence = lane.reserve();
      lane.writeRemoval(
          sequence, entry, valueAddress, valueAllocation, entry.generation(), RemovalCause.EXPIRED);
      lane.commit(sequence);
      invokeMaintenancePass(loop);

      assertEquals(observedPayload.get(), 0x11);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void actorTransportUsesOneGrowableQueueWithoutRescanState() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      Field queue = MaintenanceEventLoop.class.getDeclaredField("mailbox");
      queue.setAccessible(true);
      assertTrue(
          queue.get(loop) instanceof MpscUnboundedArrayQueue,
          "all actor messages must use a growable segmented queue");
      assertMissingField(MaintenanceEventLoop.class, "rescanRequired");
      assertMissingField(MaintenanceEventLoop.class, "rescanGeneration");
      assertMissingField(MaintenanceEventLoop.class, "rescanIterator");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void growableMutationTransportAcceptsMoreThanTheLegacyQueueCapacity() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    List<Entry> entries = new ArrayList<>(4_097);
    try {
      for (int index = 0; index < 4_097; index++) {
        Entry entry = EntryTestSupport.entry(memory, 0, index + 1, 0L);
        entries.add(entry);
        loop.publishMutation(entry, Entry.PENDING_ADD);
      }
      assertEquals(loop.queueDepth(), 4_097L);
      loop.start();
      loop.flush().join();
      assertEquals(loop.queueDepth(), 0L);
      for (Entry entry : entries) {
        assertEquals(entry.pendingFlags(), 0);
      }
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void eventLoopOwnsTheExpiryConsumerInsteadOfAllocatingAMethodReferencePerPass() {
    assertTrue(
        TimerWheel.TimerConsumer.class.isAssignableFrom(MaintenanceEventLoop.class),
        "the actor itself must be the stable timer consumer");
  }

  @Test
  public void eventLoopOwnsTheAccessConsumerInsteadOfCreatingOneForEveryReaderScan() {
    assertTrue(
        AccessConsumer.class.isAssignableFrom(MaintenanceEventLoop.class),
        "the actor itself must be the stable access-ring consumer");
  }

  @Test
  public void accessScanRejectsAnOldGenerationEvenWhenTheValueAddressIsUnchanged()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1_000L, Eviction.S3_FIFO,
            newReaderRegistry(memory), Long.MAX_VALUE);
    Entry entry = EntryTestSupport.entry(memory, 0, 90, 0L);
    ((MaintenancePolicy) getField(loop, "policy")).add(entry);
    try {
      loop.accept(entry, 0L, 0L, Entry.POLICY_S3_SMALL);
      assertEquals(entry.policyAccessCount(), 1);

      assertTrue(entry.claimWriter());
      entry.finishWriter();
      assertEquals(entry.generation(), 1L);

      loop.accept(entry, 0L, 0L, Entry.POLICY_S3_SMALL);
      assertEquals(
          entry.policyAccessCount(),
          1,
          "a recycled value address must not make a stale access event current");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void accessScanUsesThePolicyStateCapturedAtHitTime() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 4_000L, Eviction.S3_FIFO,
            newReaderRegistry(memory), Long.MAX_VALUE);
    Entry entry = EntryTestSupport.entry(memory, 0, 91, 0L);
    MaintenancePolicy policy = (MaintenancePolicy) getField(loop, "policy");
    policy.add(entry);
    Entry trigger = EntryTestSupport.entry(memory, 0, 92, 0L);
    policy.add(trigger);
    try {
      loop.accept(entry, 0L, 0L, Entry.POLICY_S4_SKIP);
      assertEquals(entry.policyAccessCount(), 0);

      // The producer snapshot remains Skip even though the actor policy state is now Small.
      loop.accept(entry, 0L, 0L, Entry.POLICY_S4_SKIP);
      assertEquals(
          entry.policyAccessCount(),
          0,
          "a delayed Skip event must not increment the Small access counter");

      loop.accept(entry, 0L, 0L, Entry.POLICY_S3_SMALL);
      assertEquals(entry.policyAccessCount(), 1);
      loop.accept(entry, 0L, 0L, Entry.POLICY_S3_SMALL);
      assertEquals(entry.policyAccessCount(), 2);
      assertEquals(loop.snapshot().skipSuppressedAccessCount, 2L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void accessScanDrainsMultipleReaderRingsAndRotatesAfterTheBatchLimit() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderSlot first = new ReaderSlot();
    ReaderSlot second = new ReaderSlot();
    first.access = new AccessRing();
    second.access = new AccessRing();
    first.access.offer(EntryTestSupport.entry(memory, 0, 1, 0L), 1L, 0L, Entry.POLICY_NONE);
    first.access.offer(EntryTestSupport.entry(memory, 0, 2, 0L), 1L, 0L, Entry.POLICY_NONE);
    second.access.offer(EntryTestSupport.entry(memory, 0, 3, 0L), 1L, 0L, Entry.POLICY_NONE);
    second.access.offer(EntryTestSupport.entry(memory, 0, 4, 0L), 1L, 0L, Entry.POLICY_NONE);
    try {
      loop.registerReader(first);
      loop.registerReader(second);

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "drainAccesses", ReaderRegistry.SlotTableSnapshot.class, int.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, readers.slotTableSnapshot(), 2), 2);
      assertEquals(drain.invoke(loop, readers.slotTableSnapshot(), 2), 2);
      assertTrue(first.access.isEmpty());
      assertTrue(second.access.isEmpty());
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readerRegistrationRequestsTheColdAccessScanWake() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.registerReader(new ReaderSlot());
      Field requestedWork = MaintenanceEventLoop.class.getDeclaredField("requestedWork");
      requestedWork.setAccessible(true);
      AtomicInteger requested = (AtomicInteger) requestedWork.get(loop);
      assertTrue(requested.get() != 0, "the first reader must cold-wake the maintenance actor");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void aPublishedQueueWorkBitMakesTheRunnableCheckConstantTime() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      Field workMutation = MaintenanceEventLoop.class.getDeclaredField("WORK_MUTATION");
      workMutation.setAccessible(true);
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(workMutation.getInt(null));

      assertTrue(
          invokeBooleanMethod(loop, "hasRunnableWork"),
          "published queue work must avoid scanning every authoritative source");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void staleAccessHintDoesNotMakeTheActorRunnable() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(intField(MaintenanceEventLoop.class, "WORK_ACCESS_SCAN"));

      assertFalse(
          invokeBooleanMethod(loop, "hasRunnableWork"),
          "a low-watermark access hint must remain advisory instead of making the actor runnable");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void continuationCounterAdvancesWhenTheNextCapturedAccessTurnActuallyStarts()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderSlot slot = new ReaderSlot();
    slot.access = new AccessRing(slot::signalAccess);
    Entry entry = EntryTestSupport.entry(memory, 0, 211, 0L);
    loop.registerReader(slot);
    for (int index = 0; index < 2_048; index++) {
      assertTrue(slot.access.offer(entry, 1L, 1L, Entry.POLICY_NONE));
    }
    loop.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
      while ((!slot.access.isEmpty()
              || loop.snapshot().maintenanceImmediateContinuationCount == 0L)
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(slot.access.isEmpty());
      assertTrue(
          loop.snapshot().maintenanceImmediateContinuationCount > 0L,
          "a bounded access batch must count the next runnable turn only when it starts");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void readerRegistrationDoesNotKeepAnEmptyAccessScanActive() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      ReaderSlot slot = new ReaderSlot();
      loop.registerReader(slot);
      invokeMaintenancePass(loop);
      assertFalse(
          (Boolean) getField(loop, "accessScanActive"),
          "an empty access ring must not keep the actor access scan active");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readerRegistrationDoesNotSchedulePeriodicAccessScan() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    ReaderSlot slot = new ReaderSlot();
    try {
      loop.registerReader(slot);
      invokeMaintenancePass(loop);
      assertFalse((Boolean) getField(loop, "accessScanActive"));
      assertMissingField(MaintenanceEventLoop.class, "ACCESS_SCAN_IDLE_INTERVAL_NANOS");
      assertMissingField(MaintenanceEventLoop.class, "nextAccessScanNanos");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void idleParkingDoesNotReconcileTheWholeMappingTable() throws Exception {
    CountingIndex data = new CountingIndex();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    loop.bindLogicalAdmission(new LogicalAdmission(1 << 20, false));
    try {
      // Interrupt makes the final park return immediately while still exercising the idle-boundary
      // work checks. The mapping table must not be touched by an idle diagnostic.
      ((WakeGate) getField(loop, "wakeGate")).requireProcessing();
      Thread.currentThread().interrupt();
      invokeParkUntilWork(loop);
      assertEquals(
          data.keySetCalls.get(),
          0,
          "entering idle must not scan every mapping just to reconcile a diagnostic ledger");
    } finally {
      Thread.interrupted();
      memory.closeArenas();
    }
  }

  @Test
  public void mapOwnershipProbeUsesOneMirroredHashLookup() throws Exception {
    CountingIndex data = new CountingIndex();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    Entry entry = EntryTestSupport.entry(memory, 0, 17, 0x1717L, 0L);
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    try {
      ((MaintenancePolicy) getField(loop, "policy")).add(entry);
      Method probe = MaintenanceEventLoop.class.getDeclaredMethod("isMappedEntry", Entry.class);
      probe.setAccessible(true);

      assertTrue((Boolean) probe.invoke(loop, entry));
      assertEquals(data.getCalls.get(), 1);
      assertEquals(
          data.valuesCalls.get(),
          0,
          "map ownership confirmation must not scan every mapped value");
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void sampledAccessSignalsTheActorWithoutPeriodicScan() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    ThreadContext context = new ThreadContext(null);
    try {
      loop.registerReader(context.slot);
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(0);
      Entry entry = EntryTestSupport.entry(0, 1, 0L);

      for (int index = 0; index < 16; index++) {
        context.access(entry);
      }

      assertTrue(
          requestedWork.get() != 0,
          "the first sampled access after an idle scan must wake the maintenance actor");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void publishedReadCountersRemainAdvisoryUntilTheIdleDeadline() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    ThreadContext context = new ThreadContext(null);
    try {
      loop.registerReader(context.slot);
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(0);

      for (int index = 0; index < 1_024; index++) {
        long sequence = context.miss();
        context.finishRead(sequence);
      }

      assertFalse(
          requestedWork.get() != 0,
          "a counter publication must not force an actor wake for every read batch");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void idleWorkerDoesNotReadAClockWithoutTimerOrRetirementWork() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            ticker,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    int constructorCalls = ticker.calls();
    loop.start();
    try {
      waitUntilParked(loop);
      Thread.sleep(20L);
      assertEquals(
          ticker.calls(),
          constructorCalls,
          "an idle worker must not sample a clock just to decide to park");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void forcedPhysicalClockSampleIsReusedForRequestedWork() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterArena arena = memory.newWriterArena();
    long key = arena.allocate(Entry.keyAllocationLengthForKeyLength(0));
    long allocation = ValueBlock.allocationLength(1);
    long value = arena.allocate(allocation);
    NativeMemory.putLong(key, 129L);
    ValueBlock.initialize(value, 0L, 1, 0L);
    Entry entry = new Entry(key, 0, value);
    entry.initializeNativeMetadata();
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, ticker, 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeApplyEntry(loop, entry);
      assertTrue(entry.claimWriter());
      loop.requestMaintenance();
      ticker.reset();

      invokeMaintenancePass(loop);

      assertEquals(
          ticker.monotonicCalls.get(),
          1,
          "one maintenance pass must reuse one monotonic sample for every deadline check");
      assertEquals(ticker.wallCalls.get(), 1);
    } finally {
      if (entry.isWriterLocked()) {
        entry.finishWriter();
      }
      memory.closeArenas();
    }
  }

  @Test
  public void ttlMaintenanceRefreshesWallSnapshotForLaterWriterMetadata() throws Exception {
    AtomicLong monotonicNow = new AtomicLong();
    AtomicLong wallNow = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return monotonicNow.get();
          }

          @Override
          public long currentTimeMillis() {
            return wallNow.get();
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long value = memory.newWriterArena().allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(value, TimerWheel.TICK_NANOS, 1, 0L);
    Entry entry =
        EntryTestSupport.entry(memory, 0, 94, 0L, Entry.tagValueAddress(value, true));
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, ticker, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeApplyEntry(loop, entry);
      monotonicNow.set(TimerWheel.TICK_NANOS);
      wallNow.set(1_000L);

      invokeMaintenancePass(loop);

      assertEquals(
          loop.nowMillis(),
          1_000L,
          "a due TTL pass must refresh the wall snapshot used by subsequent writers");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void maintenancePassDoesNotResamplePhysicalTtlClockBeforeOneSecond() throws Exception {
    AtomicInteger wallCalls = new AtomicInteger();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return 0L;
          }

          @Override
          public long currentTimeMillis() {
            wallCalls.incrementAndGet();
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, ticker, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      for (int pass = 0; pass < 17; pass++) {
        invokeMaintenancePass(loop);
      }

      assertEquals(
          wallCalls.get(),
          1,
          "a busy actor must not sample the physical TTL wall clock more than once per second");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void futureTtlSleepsByWheelTickWithoutEarlyExpiry() throws Exception {
    AtomicInteger monotonicCalls = new AtomicInteger();
    AtomicInteger wallCalls = new AtomicInteger();
    CountDownLatch scheduledWake = new CountDownLatch(1);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            if (monotonicCalls.incrementAndGet() >= 2) {
              scheduledWake.countDown();
            }
            return 0L;
          }

          @Override
          public long currentTimeMillis() {
            wallCalls.incrementAndGet();
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long value = memory.newWriterArena().allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(value, TimerWheel.TICK_NANOS, 1, 0L);
    Entry entry =
        EntryTestSupport.entry(memory, 0, 93, 0L, Entry.tagValueAddress(value, true));
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, ticker, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeApplyEntry(loop, entry);
      monotonicCalls.set(0);
      loop.start();
      waitUntilParked(loop);

      assertTrue(
          scheduledWake.await(1L, TimeUnit.SECONDS),
          "a scheduled TTL must wake the actor at least once for its wheel tick");
      assertTrue(
          monotonicCalls.get() <= 12,
          "a future TTL must not drive tight maintenance passes: " + monotonicCalls.get());
      assertEquals(
          wallCalls.get(),
          1,
          "TTL scheduling must not resample the wall clock after construction");
      assertTrue(
          data.containsKey(entry),
          "physical TTL cleanup must not remove an entry before the semantic ticker reaches expiry");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void realPassCleansQueuedReaderRegistrationsWithoutCreatingWork() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    Thread owner =
        new Thread(
            () -> {
              readers.register(new ReaderSlot());
            });
    owner.start();
    owner.join();

    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      CompletableFuture<Void> flush = loop.flush();
      invokeMaintenancePassWork(loop);
      assertTrue(flush.isDone(), "the flush pass must consume the queued reader cleanup");
      assertEquals(readerCount(readers), 0);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void boundedTransportAcceptsVisibleHintsWithoutMakingTheCacheUnhealthy() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    Entry[] entries = new Entry[4];
    try {
      for (int i = 0; i < entries.length; i++) {
        entries[i] = EntryTestSupport.entry(memory, 0, i + 1, 0L);
        loop.publishMutation(entries[i], Entry.PENDING_ADD);
        loop.requestMutationMaintenance();
      }
      assertEquals(loop.queueDepth(), 4L);
      assertEquals(
          loop.snapshot().unhealthy,
          false,
          "a visible mutation must not make maintenance unhealthy");

      loop.start();
      loop.flush().join();
      for (Entry entry : entries) {
        assertEquals(entry.pendingFlags(), 0);
      }
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void actorAccessScanDrainsCountersWithoutReaderSideSignal() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ThreadContext context = new ThreadContext(null);
    loop.registerReader(context.slot);
    loop.start();
    try {
      Entry entry = EntryTestSupport.entry(memory, 0, 91, 0L);
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
  public void accessCounterConsumptionUsesModularDeltasAcrossSignedWrap() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderSlot slot = new ReaderSlot();
    int index = loop.registerReader(slot);
    ReaderRegistry.SlotTableSnapshot slots = readers.slotTableSnapshot();
    slots.consumedHits(index, Long.MAX_VALUE - 2L);
    slots.consumedMisses(index, Long.MAX_VALUE - 4L);
    slot.publishedHits = Long.MIN_VALUE + 1L;
    slot.publishedMisses = Long.MIN_VALUE + 2L;
    try {
      setLongField(loop, "nextAccessWakeNanos", 0L);
      invokeMaintenancePass(loop);
      assertEquals(loop.snapshot().hits, 4L);
      assertEquals(loop.snapshot().misses, 7L);
      assertEquals(slots.consumedHits(index), slot.publishedHits);
      assertEquals(slots.consumedMisses(index), slot.publishedMisses);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void flushWaitsForEveryRegisteredReaderRingToDrain() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderSlot first = new ReaderSlot();
    ReaderSlot second = new ReaderSlot();
    first.access = new AccessRing();
    second.access = new AccessRing();
    Entry entry = EntryTestSupport.entry(memory, 0, 92, 0L);
    for (int i = 0; i < 256; i++) {
      assertTrue(first.access.offer(entry, 1L, 0L, Entry.POLICY_NONE));
      assertTrue(second.access.offer(entry, 1L, 0L, Entry.POLICY_NONE));
    }
    loop.registerReader(first);
    loop.registerReader(second);
    loop.start();
    try {
      loop.flush().join();
      assertTrue(first.access.isEmpty(), "flush must drain the first reader ring");
      assertTrue(second.access.isEmpty(), "flush must drain the second reader ring");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void closeFinalDrainConsumesPublishedReaderRingData() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderSlot slot = new ReaderSlot();
    slot.access = new AccessRing();
    Entry entry = EntryTestSupport.entry(memory, 0, 93, 0L);
    for (int i = 0; i < 256; i++) {
      assertTrue(slot.access.offer(entry, 1L, 0L, Entry.POLICY_NONE));
    }
    loop.registerReader(slot);
    loop.start();
    try {
      loop.stop();
      loop.join(1_000L);
      assertTrue(slot.access.isEmpty(), "close must final-drain already-published access data");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void uncommittedLifecycleSlotDoesNotPublishAFlushHint() {
    WriterLifecycleLane journal = new WriterLifecycleLane(2);
    long sequence = journal.reserve();
    assertFalse(journal.hasCommittedRecords());
    journal.cancel(sequence);
    assertTrue(journal.hasCommittedRecords());
  }

  @Test
  public void actorMarksQueuedMutationForTheNextWriterPublication() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    Entry entry = EntryTestSupport.entry(memory, 0, 77, 0L);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      // This is the deterministic version of the interleaving: the actor has taken the
      // old transport item while a later writer has claimed the Entry but not yet made
      // its new pointer visible.
      assertTrue(entry.claimWriter());
      invokeProcessEntry(loop, entry);

      assertTrue(
          entry.isMutationRetryRequested(),
          "the actor must leave one retry marker while a writer owns the Entry");
    } finally {
      if (entry.isWriterLocked()) {
        entry.finishWriter();
      }
      memory.closeArenas();
    }
  }

  @Test
  public void actorRetryRetainsFlagsPublishedBeforeTheWriterFinishes() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    Entry entry = EntryTestSupport.entry(memory, 0, 78, 0L);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      assertTrue(entry.claimWriter());
      // The writer publishes the current UPDATE after the old hint was queued, but before it
      // releases the entry. The actor then consumes that coalesced hint while the writer is
      // still locked and must carry the UPDATE into its retry handoff.
      assertFalse(entry.publishMutation(Entry.PENDING_UPDATE));
      assertEquals(invokeDrainMutations(loop, 1), 1);
      assertTrue(entry.isMutationRetryRequested());

      entry.finishWriter();
      loop.enqueueMutationHint(entry, true);
      assertEquals(invokeDrainMutations(loop, 1), 1);
      assertEquals(
          entry.appliedVersion(),
          entry.mutationVersion(),
          "the retry must re-apply the publication that was coalesced before writer release");
    } finally {
      if (entry.isWriterLocked()) {
        entry.finishWriter();
      }
      memory.closeArenas();
    }
  }

  @Test
  public void coalescedMutationAppliesTheLatestValueMetadata() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long firstAllocation = ValueBlock.allocationLength(1);
    long latestAllocation = ValueBlock.allocationLength(32);
    long firstValue = memory.newWriterArena().allocate(firstAllocation);
    long latestValue = memory.newWriterArena().allocate(latestAllocation);
    ValueBlock.initialize(firstValue, Long.MIN_VALUE, 1, 0L);
    ValueBlock.initialize(latestValue, Ticker.DEFAULT.nanos() + 60_000_000_000L, 32, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 80, 0x8080L, firstValue);
    entry.currentValueAllocation(firstAllocation);
    assertTrue(entry.markLogicallyPresent());
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    try {
      loop.publishMutation(entry, Entry.PENDING_ADD);
      assertEquals(loop.queueDepth(), 1L);

      assertTrue(entry.claimWriter());
      entry.currentValueAllocation(latestAllocation);
      entry.valueAddress = Entry.tagValueAddress(latestValue, true);
      entry.finishWriter();
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      assertEquals(
          loop.queueDepth(),
          1L,
          "an update coalesced into the queued hint must not allocate another lane record");

      assertEquals(invokeDrainMutations(loop, 1), 1);
      long expectedBytes =
          CacheMath.logicalEntryBytes(entry.keyAllocationLength(), latestAllocation);
      MaintenancePolicy policy = (MaintenancePolicy) getField(loop, "policy");
      assertEquals(policy.usedBytes(), expectedBytes);
      assertEquals(entry.policyByteWeight(), expectedBytes);
      assertEquals(loop.snapshot().ttlBacklog, 1L);
      assertEquals(entry.appliedVersion(), entry.mutationVersion());
      assertEquals(loop.queueDepth(), 0L);
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void saturatedSeededMutationUsesTheLatestAllocation() throws Exception {
    assertSeededMutationUsesTheLatestAllocation(true);
  }

  @Test
  public void coalescedSeededMutationUsesTheLatestAllocation() throws Exception {
    assertSeededMutationUsesTheLatestAllocation(false);
  }

  private void assertSeededMutationUsesTheLatestAllocation(boolean saturated) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long firstAllocation = ValueBlock.allocationLength(1);
    long latestAllocation = ValueBlock.allocationLength(32);
    long firstValue = memory.newWriterArena().allocate(firstAllocation);
    long latestValue = memory.newWriterArena().allocate(latestAllocation);
    ValueBlock.initialize(firstValue, Long.MIN_VALUE, 1, 0L);
    ValueBlock.initialize(latestValue, Long.MIN_VALUE, 32, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 80, 0x8080L, firstValue);
    entry.currentValueAllocation(firstAllocation);
    assertTrue(entry.markLogicallyPresent());
    if (saturated) {
      EntryTestSupport.maintenanceMeta(entry, (0xffffffffL << 32) | 0xfffffffeL);
    }
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    WriterLifecycleLane lane = new WriterLifecycleLane(2);
    try {
      assertTrue(entry.claimWriter());
      assertTrue(loop.prepareMutation(entry, Entry.PENDING_ADD));
      long version = entry.mutationVersion();
      entry.finishWriter();
      loop.enqueueWriterMutationHint(lane, entry, entry.keyHash(), firstAllocation, version, false);

      assertTrue(entry.claimWriter());
      entry.currentValueAllocation(latestAllocation);
      entry.valueAddress = latestValue;
      assertFalse(loop.prepareMutation(entry, Entry.PENDING_UPDATE));
      entry.finishWriter();
      assertEquals(entry.mutationVersion(), saturated ? version : version + 1L);

      assertEquals(invokeDrainMutations(loop, 1), 1);
      long expected = CacheMath.logicalEntryBytes(entry.keyAllocationLength(), latestAllocation);
      MaintenancePolicy policy = (MaintenancePolicy) getField(loop, "policy");
      assertEquals(policy.usedBytes(), expected, "a coalesced value must supersede the lane seed");
      assertEquals(entry.policyByteWeight(), expected);
      assertEquals(entry.mutationVersion(), saturated ? 0L : version + 1L);
      assertEquals(entry.appliedVersion(), entry.mutationVersion());
      assertEquals(loop.queueDepth(), 0L);
    } finally {
      if (entry.isWriterLocked()) {
        entry.finishWriter();
      }
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void staleQueuedMutationCannotTouchAReusedNativeKeyBlock() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    WriterArena arena = memory.newWriterArena();
    Entry stale = EntryTestSupport.entry(arena, 0, 177, 0L);
    long allocation = stale.nativeKeyAllocationLength();
    try {
      loop.publishMutation(stale, Entry.PENDING_UPDATE);
      assertTrue(stale.claimWriter());
      stale.markRetired();
      stale.finishWriter();
      long retiredAddress = stale.nativeKeyAddress;
      memory.releaseEntry(retiredAddress, allocation);

      long reusedAddress = arena.allocate(allocation);
      try {
        assertEquals(reusedAddress, retiredAddress, "the test must exercise native slot reuse");
        Entry reused = new Entry(reusedAddress, 0, 0L);
        reused.initializeNativeMetadata();
        long sentinel = (9L << 32) | 321L;
        EntryTestSupport.maintenanceMeta(reused, sentinel);

        assertEquals(invokeDrainMutations(loop, 1), 1);
        assertEquals(
            NativeMemory.getLong(
                reused.nativeKeyAddress - Entry.NATIVE_METADATA_BYTES + 16L),
            sentinel,
            "a retired queue item must not access a reused native metadata block");
      } finally {
        memory.releaseEntry(reusedAddress, allocation);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void writerRetryAppliesThePointerPublishedByTheLaterWriter() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, Ticker.DEFAULT.nanos() + 60_000_000_000L, 1, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 78, 0L);
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      assertTrue(entry.claimWriter());
      invokeProcessEntry(loop, entry);
      assertTrue(entry.isMutationRetryRequested());

      entry.valueAddress = Entry.tagValueAddress(value, true);
      entry.finishWriter();
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      loop.start();
      loop.flush().join();

      assertEquals(
          loop.snapshot().ttlBacklog,
          1L,
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
  public void actorLeavesRetryMarkerBeforeTheConcurrentWriterPublishes()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, Ticker.DEFAULT.nanos() + 60_000_000_000L, 1, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 79, 0L);
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CountDownLatch writerClaimed = new CountDownLatch(1);
    CountDownLatch allowPublication = new CountDownLatch(1);
    AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                assertTrue(entry.claimWriter());
                writerClaimed.countDown();
                assertTrue(allowPublication.await(1L, TimeUnit.SECONDS));
                entry.valueAddress = Entry.tagValueAddress(value, true);
                entry.finishWriter();
                loop.publishMutation(entry, Entry.PENDING_UPDATE);
              } catch (Throwable failure) {
                writerFailure.set(failure);
              }
            },
            "pending-publication-writer");
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      writer.start();
      assertTrue(writerClaimed.await(1L, TimeUnit.SECONDS));

      // This invocation represents the actor consuming the old queued transport item while
      // the second writer owns the pending claim but has not published its pointer yet.
      invokeProcessEntry(loop, entry);
      assertTrue(
          entry.isMutationRetryRequested(),
          "the old queue item must leave a retry marker until the writer publishes");

      allowPublication.countDown();
      writer.join(1_000L);
      assertFalse(writer.isAlive(), "the writer did not publish its protected update");
      assertEquals(writerFailure.get(), null);
      loop.start();
      loop.flush().join();

      assertEquals(
          loop.snapshot().ttlBacklog,
          1L,
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

  @Test
  public void mutationPassDoesNotOwnLogicalCapacityEviction() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterArena arena = memory.newWriterArena();
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(0);
    long valueAllocation = ValueBlock.allocationLength(1);
    long logicalWeight = CacheMath.logicalEntryBytes(keyAllocation, valueAllocation);
    long keyOne = arena.allocate(keyAllocation);
    long keyTwo = arena.allocate(keyAllocation);
    long valueOne = arena.allocate(valueAllocation);
    long valueTwo = arena.allocate(valueAllocation);
    ValueBlock.initialize(valueOne, 0L, 1, 0L);
    ValueBlock.initialize(valueTwo, 0L, 1, 0L);
    NativeMemory.putLong(keyOne, 141L);
    NativeMemory.putLong(keyTwo, 142L);
    Entry one = new Entry(keyOne, 0, valueOne);
    NativeMemory.putInt(keyOne - 16L, 141);
    Entry two = new Entry(keyTwo, 0, valueTwo);
    NativeMemory.putInt(keyTwo - 16L, 142);
    one.initializeNativeMetadata();
    two.initializeNativeMetadata();
    one.currentValueAllocation(valueAllocation);
    two.currentValueAllocation(valueAllocation);
    one.markLogicallyPresent();
    two.markLogicallyPresent();
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(one, one);
    data.put(two, two);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new FrozenTicker(), logicalWeight, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.publishMutation(one, Entry.PENDING_ADD);
      loop.publishMutation(two, Entry.PENDING_ADD);

      invokeDrainAsyncMutations(loop, 2);

      assertEquals(data.size(), 2, "logical capacity admission belongs to the writer path");
      assertEquals(
          ((MaintenancePolicy) getField(loop, "policy")).usedWeight(), logicalWeight * 2L);
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void actorDoesNotEvictWhenPolicyWeightExceedsTarget() throws Exception {
    FrozenTicker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    com.red.ohc.storage.WriterArena arena = memory.newWriterArena();
    long key = arena.allocate(Entry.keyAllocationLengthForKeyLength(0));
    long value = arena.allocate(ValueBlock.allocationLength(1));
    NativeMemory.putLong(key, 121L);
    ValueBlock.initialize(value, 0L, 1, 0L);
    Entry entry = new Entry(key, 0, value);
    entry.initializeNativeMetadata();
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, ticker, 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeApplyEntry(loop, entry);
      invokeMaintenancePass(loop);
      assertEquals(data.size(), 1, "the actor must not enforce logical capacity");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void policyRetainsMappingsWithoutWriterAdmission() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterArena arena = memory.newWriterArena();
    long allocation = ValueBlock.allocationLength(1);
    long keyOne = arena.allocate(Entry.keyAllocationLengthForKeyLength(0));
    long keyTwo = arena.allocate(Entry.keyAllocationLengthForKeyLength(0));
    long valueOne = arena.allocate(allocation);
    long valueTwo = arena.allocate(allocation);
    NativeMemory.putLong(keyOne, 131L);
    NativeMemory.putLong(keyTwo, 132L);
    ValueBlock.initialize(valueOne, 0L, 1, 0L);
    ValueBlock.initialize(valueTwo, 0L, 1, 0L);
    Entry one = new Entry(keyOne, 0, valueOne);
    NativeMemory.putInt(keyOne - 16L, 141);
    Entry two = new Entry(keyTwo, 0, valueTwo);
    NativeMemory.putInt(keyTwo - 16L, 142);
    one.initializeNativeMetadata();
    two.initializeNativeMetadata();
    one.currentValueAllocation(allocation);
    two.currentValueAllocation(allocation);
    one.markLogicallyPresent();
    two.markLogicallyPresent();
    long logicalWeight =
        CacheMath.logicalEntryBytes(Entry.keyAllocationLengthForKeyLength(0), allocation);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(one, one);
    data.put(two, two);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new FrozenTicker(), 0L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeApplyEntry(loop, one);
      invokeApplyEntry(loop, two);
      assertEquals(((MaintenancePolicy) getField(loop, "policy")).usedWeight(), logicalWeight * 2L);
      invokeMaintenancePass(loop);
      assertEquals(data.size(), 2);
      assertEquals(((MaintenancePolicy) getField(loop, "policy")).usedWeight(), logicalWeight * 2L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void actorDoesNotScheduleCapacityRetryBackoff() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new FrozenTicker(), 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      invokeMaintenancePass(loop);
      assertEquals(data.size(), 0);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void capacityDrainUsesActorPolicyAndLeavesTargetCharge() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, Ticker.DEFAULT, 1 << 20, Eviction.S3_FIFO, newReaderRegistry(memory), Long.MAX_VALUE);
    long valueAllocation = ValueBlock.allocationLength(1);
    long valueAddress = memory.newWriterArena().allocate(valueAllocation);
    ValueBlock.initialize(valueAddress, 0L, 1, 0L);
    Entry victim = EntryTestSupport.entry(memory, 1, 17, 17L, valueAddress);
    long logicalWeight = CacheMath.logicalEntryBytes(victim.keyAllocationLength(), valueAllocation);
    victim.currentValueAllocation(valueAllocation);
    assertTrue(victim.markLogicallyPresent());
    data.put(victim, victim);
    LogicalAdmission admission = new LogicalAdmission(logicalWeight, false);
    assertTrue(admission.tryChargeDelta(logicalWeight + 1L));

    try {
      loop.bindLogicalAdmission(admission);
      MaintenancePolicy policy = (MaintenancePolicy) getField(loop, "policy");
      policy.add(victim);
      loop.requestCapacityMaintenance();
      invokeMaintenancePass(loop);
      assertTrue(data.isEmpty());
      assertEquals(admission.logicalCharge(), 1L);
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test
  public void capacityDrainObservesConcurrentRemoval() throws Exception {
    for (boolean countBounded : new boolean[] {false, true}) {
      NativeMemory.Memory memory = new NativeMemory.Memory();
      AtomicReference<Runnable> afterRemoval = new AtomicReference<>();
      ConcurrentHashMap<Entry, Entry> data =
          new ConcurrentHashMap<Entry, Entry>() {
            @Override
            public com.red.ohc.index.Entry remove(Object key) {
              com.red.ohc.index.Entry removed = super.remove(key);
              Runnable action = afterRemoval.getAndSet(null);
              if (removed != null && action != null) {
                action.run();
              }
              return removed;
            }
          };
      MaintenanceEventLoop loop =
          new MaintenanceEventLoop(
              data, memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
      long allocation = ValueBlock.allocationLength(1);
      long weight =
          countBounded
              ? 1L
              : CacheMath.logicalEntryBytes(Entry.keyAllocationLengthForKeyLength(1), allocation);
      LogicalAdmission admission = new LogicalAdmission(2L * weight, countBounded);
      loop.bindLogicalAdmission(admission);
      try {
        MaintenancePolicy policy = (MaintenancePolicy) getField(loop, "policy");
        Entry[] entries = new Entry[5];
        for (int i = 0; i < entries.length; i++) {
          long value = memory.newWriterArena().allocate(allocation);
          ValueBlock.initialize(value, 0L, 1, 0L);
          Entry entry = EntryTestSupport.entry(memory, 1, i, i, value);
          entries[i] = entry;
          entry.currentValueAllocation(allocation);
          admission.tryChargeDelta(weight);
          assertTrue(admission.markPresentAfterCharge(entry));
          data.put(entry, entry);
          policy.add(entry);
        }
        // Model two writer removals completing after the actor chose its first victim.
        afterRemoval.set(
            () -> {
              for (int i = 3; i < 5; i++) {
                Entry entry = entries[i];
                assertTrue(entry.claimWriter());
                assertTrue(data.remove(entry, entry));
                admission.markAbsent(entry);
                entry.markRetired();
                entry.finishWriter();
              }
            });
        Class<?> planType = Class.forName(MaintenanceEventLoop.class.getName() + "$WorkPlan");
        java.lang.reflect.Constructor<?> constructor = planType.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object plan = constructor.newInstance();
        Field quota = planType.getDeclaredField("capacityQuota");
        quota.setAccessible(true);
        quota.setInt(plan, 8);
        Method drain = MaintenanceEventLoop.class.getDeclaredMethod("drainCapacityPressure", planType);
        drain.setAccessible(true);

        assertEquals(drain.invoke(loop, plan), 1, "completed writer removals must stop excess eviction");
        assertEquals(data.size(), 2);
        assertEquals(admission.logicalCharge(), 2L * weight);
        assertFalse(admission.isOverTarget());
      } finally {
        loop.stop();
        memory.closeArenas();
      }
    }
  }

  @Test
  public void explicitCapacityWakeReopensAWriterBlockedDrain() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    LogicalAdmission admission = new LogicalAdmission(1L, false);
    assertTrue(admission.tryChargeDelta(2L));
    loop.bindLogicalAdmission(admission);
    try {
      setBooleanField(loop, "capacityBlocked", true);
      ((AtomicInteger) getField(loop, "requestedWork"))
          .set(intField(MaintenanceEventLoop.class, "WORK_CAPACITY"));

      assertTrue(
          invokeBooleanMethod(loop, "hasRunnableWork"),
          "a writer release must make a previously blocked capacity drain runnable");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void capacityWakeOnlyReopensTheDrainForTheBlockedWriter() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    Entry blocked = EntryTestSupport.entry(0, 201, 0L);
    Entry unrelated = EntryTestSupport.entry(0, 202, 0L);
    try {
      Method wake =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "requestCapacityMaintenanceIfBlocked", Entry.class);
      wake.setAccessible(true);
      setBooleanField(loop, "capacityBlocked", true);
      Field blockedEntry = MaintenanceEventLoop.class.getDeclaredField("capacityBlockedEntry");
      blockedEntry.setAccessible(true);
      blockedEntry.set(loop, blocked);
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");

      requestedWork.set(0);
      wake.invoke(loop, blocked);
      assertTrue(
          (requestedWork.get() & intField(MaintenanceEventLoop.class, "WORK_CAPACITY")) != 0,
          "releasing the blocked writer must reopen the capacity drain");

      requestedWork.set(0);
      wake.invoke(loop, unrelated);
      assertEquals(
          requestedWork.get() & intField(MaintenanceEventLoop.class, "WORK_CAPACITY"),
          0,
          "an unrelated writer must not bypass the capacity retry backoff");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void partialCapacityQuotaPreservesRetryBackoffInsteadOfRearmingImmediately()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new FrozenTicker(), 1L, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    LogicalAdmission admission = new LogicalAdmission(1L, false);
    assertTrue(admission.tryChargeDelta(2L));
    loop.bindLogicalAdmission(admission);
    try {
      Class<?> workPlanType =
          Class.forName(
              "com.red.ohc.maintenance.MaintenanceEventLoop$WorkPlan");
      java.lang.reflect.Constructor<?> constructor = workPlanType.getDeclaredConstructor();
      constructor.setAccessible(true);
      Object plan = constructor.newInstance();
      Field capacityQuota = workPlanType.getDeclaredField("capacityQuota");
      capacityQuota.setAccessible(true);
      capacityQuota.setInt(plan, 1);

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod("drainCapacityPressure", workPlanType);
      drain.setAccessible(true);
      AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
      requestedWork.set(0);

      assertEquals(drain.invoke(loop, plan), 0);
      assertEquals(
          requestedWork.get() & intField(MaintenanceEventLoop.class, "WORK_CAPACITY"),
          0,
          "a partial quota must not bypass the capacity retry backoff when no victim progressed");
      assertTrue(
          getLongField(loop, "capacityRetryNanos") != Long.MAX_VALUE,
          "the no-victim path must retain its retry deadline");
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }


  @Test
  public void minimumBudgetStillExpiresEntriesAndCompletesFlush() throws Exception {
    AtomicLong now = new AtomicLong();
    Ticker ticker = new Ticker() {
      @Override
      public long nanos() {
        return now.get();
      }

      @Override
      public long currentTimeMillis() {
        return 0L;
      }
    };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        data, memory, ticker, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      for (int index = 0; index < 2; index++) {
        long value = memory.newWriterArena().allocate(ValueBlock.allocationLength(1));
        ValueBlock.initialize(value, TimerWheel.TICK_NANOS, 1, 0L);
        Entry entry = EntryTestSupport.entry(
            memory, 0, index + 1, 0L, Entry.tagValueAddress(value, true));
        data.put(entry, entry);
        invokeApplyEntry(loop, entry);
      }
      now.set(TimerWheel.TICK_NANOS);
      CompletableFuture<Void> flush = loop.flush();
      for (int pass = 0; pass < 8 && !flush.isDone(); pass++) {
        invokeMaintenancePass(loop);
      }
      assertTrue(data.isEmpty(), "due TTL entries must receive service even at quota one");
      assertTrue(flush.isDone(), "due TTL work must not prevent flush from converging");
      assertFalse(flush.isCompletedExceptionally());
      assertEquals(loop.retirementQueueDepth(), 0L);
    } finally {
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void resourceSweepCompletesTheReadySetWithoutAnotherWake() throws Exception {
    assertResourceSweepContinuation(false);
  }

  @Test
  public void resourceSweepVisitsReadyResourcesBehindABlockedHeadThenBecomesIdle()
      throws Exception {
    assertResourceSweepContinuation(true);
  }

  private void assertResourceSweepContinuation(boolean blockedHead) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        index(), memory, new FrozenTicker(), 1 << 20, Eviction.LRU,
        newReaderRegistry(memory), Long.MAX_VALUE);
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, lifecycle, loop.retirementJournal());
    loop.bindWriterLifecycleJournal(lifecycle);
    loop.bindWriterResourceRegistry(resources);
    try {
      WriterResource first = resources.acquire();
      WriterResource second = resources.acquire();
      long firstSequence = first.lifecycleLane().reserve();
      long secondSequence = second.lifecycleLane().reserve();
      resources.requestRetirement(first);
      resources.requestRetirement(second);
      assertEquals(resources.processRetirements(), 0);
      assertFalse(invokeBooleanMethod(loop, "hasRunnableWork"),
          "a completed sweep of blocked resources must allow the actor to sleep");
      if (!blockedHead) {
        consumeCancelledLifecycle(first, firstSequence);
      }
      consumeCancelledLifecycle(second, secondSequence);
      resources.requestRetirementScan();

      invokeMaintenancePass(loop);
      // Fixed per-phase quanta: one bounded sweep covers the whole ready set, so the
      // resources behind a blocked head pool in the same pass instead of waiting for a
      // continuation turn. A partial sweep now requires more retiring resources than the
      // advisory hard cap.
      assertEquals(resources.pooledCount(), blockedHead ? 1 : 2);
      // Resume through the real run loop: it must drain the resource-owned retirement work
      // from the pooled resources and park once the registry is quiet.
      loop.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
      while (!loop.isParked() && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(loop.isParked(), "the actor must park after a completed or fully blocked sweep");
      if (blockedHead) {
        first.lifecycleLane().cancel(firstSequence);
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (resources.pooledCount() != 2 && System.nanoTime() < deadline) {
          Thread.yield();
        }
        assertEquals(resources.pooledCount(), 2);
      }
    } finally {
      loop.stop();
      loop.join(1_000L);
      assertFalse(loop.isAlive(), "resource continuation must also converge during shutdown");
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void lifecycleProgressRevisitsResourcesAlreadyCheckedByAPartialSweep() throws Exception {
    assertLifecycleProgressRescansResources(false);
  }

  @Test
  public void mailboxProgressRevisitsResourcesAlreadyCheckedByAPartialSweep() throws Exception {
    assertLifecycleProgressRescansResources(true);
  }

  @Test
  public void ordinaryMailboxProgressDoesNotReopenBlockedResourceSweep() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        index(), memory, new FrozenTicker(), 1 << 20, Eviction.LRU,
        newReaderRegistry(memory), Long.MAX_VALUE);
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, lifecycle, loop.retirementJournal());
    loop.bindWriterLifecycleJournal(lifecycle);
    loop.bindWriterResourceRegistry(resources);
    WriterResource resource = null;
    long sequence = 0L;
    try {
      resource = resources.acquire();
      sequence = resource.lifecycleLane().reserve();
      resources.requestRetirement(resource);
      assertEquals(resources.processRetirements(), 0);
      assertFalse(resources.hasRetirementWork());
      ((AtomicInteger) getField(loop, "requestedWork")).set(0);

      assertTrue(loop.submitAsyncMutation(() -> {}, failure -> {
        throw new AssertionError(failure);
      }));
      assertEquals(invokeDrainAsyncMutations(loop, Integer.MAX_VALUE), 1);

      assertFalse(
          resources.hasRetirementWork(),
          "ordinary mailbox progress must not reopen a completed blocked resource sweep");
    } finally {
      if (resource != null && resource.lifecycleLane().reservedRecords() != 0L) {
        consumeCancelledLifecycle(resource, sequence);
      }
      if (resources.hasRetiringResources()) {
        resources.processRetirements();
      }
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void advisoryMaintenanceDoesNotReopenBlockedResourceSweep() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        index(), memory, new FrozenTicker(), 1 << 20, Eviction.LRU,
        newReaderRegistry(memory), Long.MAX_VALUE);
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, lifecycle, loop.retirementJournal());
    loop.bindWriterLifecycleJournal(lifecycle);
    loop.bindWriterResourceRegistry(resources);
    WriterResource first = null;
    WriterResource second = null;
    long firstSequence = 0L;
    long secondSequence = 0L;
    try {
      first = resources.acquire();
      second = resources.acquire();
      firstSequence = first.lifecycleLane().reserve();
      secondSequence = second.lifecycleLane().reserve();
      resources.requestRetirement(first);
      resources.requestRetirement(second);
      assertEquals(resources.processRetirements(), 0);
      ((AtomicInteger) getField(loop, "requestedWork"))
          .set(intField(MaintenanceEventLoop.class, "WORK_MUTATION"));

      invokeMaintenancePass(loop);

      assertFalse(
          resources.hasRetirementWork(),
          "advisory-only maintenance must not reopen a completed blocked resource sweep");
    } finally {
      if (first != null && first.lifecycleLane().reservedRecords() != 0L) {
        consumeCancelledLifecycle(first, firstSequence);
      }
      if (second != null && second.lifecycleLane().reservedRecords() != 0L) {
        consumeCancelledLifecycle(second, secondSequence);
      }
      if (resources.hasRetiringResources()) {
        resources.processRetirements();
      }
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  private void assertLifecycleProgressRescansResources(boolean mailboxOwned) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        index(), memory, new FrozenTicker(), 1 << 20, Eviction.LRU,
        newReaderRegistry(memory), Long.MAX_VALUE);
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, lifecycle, loop.retirementJournal());
    loop.bindWriterLifecycleJournal(lifecycle);
    loop.bindWriterResourceRegistry(resources);
    try {
      WriterResource first = resources.acquire();
      WriterResource second = resources.acquire();
      long firstSequence = first.lifecycleLane().reserve();
      long secondSequence = second.lifecycleLane().reserve();
      resources.requestRetirement(first);
      resources.requestRetirement(second);
      assertEquals(resources.processRetirements(), 0);
      invokeMaintenancePass(loop);
      if (mailboxOwned) {
        loop.cancelWriterLifecycle(first.lifecycleLane(), firstSequence, true);
        assertEquals(invokeDrainMutations(loop, 1), 1);
      } else {
        first.lifecycleLane().cancel(firstSequence);
      }

      for (int pass = 0; pass < 4 && invokeBooleanMethod(loop, "hasRunnableWork"); pass++) {
        invokeMaintenancePass(loop);
      }
      assertEquals(resources.pooledCount(), 1,
          "lifecycle progress must recheck a resource visited before its watermark completed");
      assertFalse(invokeBooleanMethod(loop, "hasRunnableWork"));
      consumeCancelledLifecycle(second, secondSequence);
      assertEquals(resources.processRetirements(), 1);
    } finally {
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void safeReclaimRevisitsResourcesAlreadyCheckedByAPartialSweep() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot reader = new ReaderSlot();
    readers.register(reader);
    readers.beginOpForTest(reader, true);
    MaintenanceEventLoop loop = new MaintenanceEventLoop(
        index(), memory, new FrozenTicker(), 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, lifecycle, loop.retirementJournal());
    loop.bindWriterLifecycleJournal(lifecycle);
    loop.bindWriterResourceRegistry(resources);
    try {
      WriterResource first = resources.acquire();
      WriterResource second = resources.acquire();
      appendRetirements(first.retirementLane(), 1);
      appendRetirements(second.retirementLane(), 1);
      resources.requestRetirement(first);
      resources.requestRetirement(second);
      assertEquals(resources.processRetirements(), 0);
      invokeMaintenancePass(loop);
      assertEquals(resources.pooledCount(), 0);
      readers.endOpForTest(reader);
      loop.readerQuiescent(reader);

      for (int pass = 0; pass < 8 && invokeBooleanMethod(loop, "hasRunnableWork"); pass++) {
        invokeMaintenancePass(loop);
      }
      assertEquals(loop.retirementJournal().completedRecordsTotal(), 2L);
      assertEquals(resources.pooledCount(), 2,
          "SAFE reclaim must recheck all resources whose captured watermark completed");
      assertFalse(invokeBooleanMethod(loop, "hasRunnableWork"));
    } finally {
      readers.endOpForTest(reader);
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  private static void consumeCancelledLifecycle(WriterResource resource, long sequence) {
    resource.lifecycleLane().cancel(sequence);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    assertTrue(resource.lifecycleLane().poll(record));
    resource.lifecycleLane().release(record);
  }


  @Test
  public void advisoryQuotaIsNotCountedTwiceAcrossMaintenancePhases() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      Class<?> workPlanType =
          Class.forName(
              "com.red.ohc.maintenance.MaintenanceEventLoop$WorkPlan");
      java.lang.reflect.Constructor<?> workPlanConstructor =
          workPlanType.getDeclaredConstructor();
      workPlanConstructor.setAccessible(true);
      Object plan = workPlanConstructor.newInstance();
      setBooleanField(plan, "access", true);
      setBooleanField(plan, "ttl", true);
      setBooleanField(plan, "ghostRehash", true);
      setBooleanField(plan, "readerLifecycle", true);

      Class<?> turnCutsType =
          Class.forName(
              "com.red.ohc.maintenance.MaintenanceEventLoop$TurnCuts");
      java.lang.reflect.Constructor<?> constructor = turnCutsType.getDeclaredConstructor(int.class);
      constructor.setAccessible(true);
      Object turn = constructor.newInstance(0);
      Field accessRecords = turnCutsType.getDeclaredField("accessRecords");
      accessRecords.setAccessible(true);
      accessRecords.setInt(turn, 1_024);

      Method prepare =
          MaintenanceEventLoop.class.getDeclaredMethod(
              "prepareMaintenanceQuotas", workPlanType, turnCutsType);
      prepare.setAccessible(true);
      prepare.invoke(loop, plan, turn);

      int accessQuota = intField(plan, "accessQuota");
      int ttlQuota = intField(plan, "ttlQuota");
      int readerLifecycleQuota = intField(plan, "readerLifecycleQuota");
      int advisoryQuota = intField(plan, "advisoryQuota");
      assertTrue(accessQuota > 0, "access keeps its own bounded quota");
      assertTrue(ttlQuota > 0, "ttl keeps its own bounded quota");
      assertTrue(
          readerLifecycleQuota >= 1,
          "reader lifecycle must retain a minimum quota while its sweep is active");
      Object ghostOnlyPlan = workPlanConstructor.newInstance();
      setBooleanField(ghostOnlyPlan, "ghostRehash", true);
      prepare.invoke(loop, ghostOnlyPlan, turn);
      assertEquals(
          advisoryQuota,
          intField(ghostOnlyPlan, "advisoryQuota"),
          "access and TTL must not be counted again inside the shared advisory quota");

      Object readerOnlyPlan = workPlanConstructor.newInstance();
      setBooleanField(readerOnlyPlan, "readerLifecycle", true);
      prepare.invoke(loop, readerOnlyPlan, turn);
      assertEquals(
          intField(readerOnlyPlan, "advisoryQuota"),
          0,
          "reader lifecycle must not consume the ghost/resource advisory quota twice");
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void idleLoopParksInsteadOfBusySpinning() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    loop.start();
    try {
      long deadline = System.nanoTime() + 1_000_000_000L;
      while (!loop.isParked() && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(loop.isParked(), "maintenance loop did not park while idle");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void terminalFailureKeepsActorAliveUntilExplicitStop() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    loop.start();
    try {
      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      long deadline = System.nanoTime() + 1_000_000_000L;
      while (!loop.snapshot().unhealthy && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(loop.snapshot().unhealthy);
      assertTrue(loop.isAlive(), "fatal maintenance must not free native state before close");
      try {
        loop.flush().join();
        throw new AssertionError("flush should preserve the terminal maintenance failure");
      } catch (java.util.concurrent.CompletionException expected) {
        assertTrue(
            expected.getCause() instanceof com.red.ohc.api.CacheMaintenanceException);
      }
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void terminalFailureUnparksEvenWhenTheWakeGateAlreadyRequiresProcessing()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    CountDownLatch parkEntered = new CountDownLatch(1);
    CountDownLatch parkReturned = new CountDownLatch(1);
    Thread parkedThread =
        new Thread(
            () -> {
              parkEntered.countDown();
              LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5L));
              parkReturned.countDown();
            });
    Field threadField = MaintenanceEventLoop.class.getDeclaredField("thread");
    long threadOffset = NativeMemory.unsafe().objectFieldOffset(threadField);
    NativeMemory.unsafe().putObjectVolatile(loop, threadOffset, parkedThread);
    ((WakeGate) getField(loop, "wakeGate")).requireProcessing();
    try {
      parkedThread.start();
      assertTrue(parkEntered.await(1L, TimeUnit.SECONDS));

      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));

      assertTrue(
          parkReturned.await(250L, TimeUnit.MILLISECONDS),
          "terminal failure must bypass the continuation cadence and unpark the actor");
    } finally {
      LockSupport.unpark(parkedThread);
      parkedThread.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void failedActorRemovalCancelsTheUnusedActorRetirementBatch() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            16L << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    try {
      Entry entry = EntryTestSupport.entry(memory, 0, 127, 8L);
      data.put(entry, EntryTestSupport.entry(memory, 0, 128, 0L));

      assertFalse(loop.removeFromMap(entry, false, entry.generation(), 8L));
      assertTrue(data.containsKey(entry), "a failed actor removal must preserve the mapping");
    } finally {
      readers.endOpForTest(activeReader);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushBarrierDoesNotWaitForAsyncMutationsSubmittedAfterIt() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch releaseSecond = new CountDownLatch(1);
    CompletableFuture<Void> secondResult = new CompletableFuture<>();
    try {
      loop.start();
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                firstStarted.countDown();
                await(releaseFirst);
              },
              Throwable::printStackTrace));
      for (int index = 1; index < 64; index++) {
        assertTrue(loop.submitAsyncMutation(() -> {}, Throwable::printStackTrace));
      }
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));

      CompletableFuture<Void> flush = loop.flush();
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                await(releaseSecond);
                secondResult.complete(null);
              },
              secondResult::completeExceptionally));
      releaseFirst.countDown();

      flush.get(2, TimeUnit.SECONDS);
    } finally {
      releaseFirst.countDown();
      releaseSecond.countDown();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushFenceDefersPostFenceAsyncWhileRetirementIsPinned() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    CountDownLatch postFenceStarted = new CountDownLatch(1);
    CountDownLatch releasePostFence = new CountDownLatch(1);
    try {
      retireOne(loop, memory);
      CompletableFuture<Void> flush = loop.flush();
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                postFenceStarted.countDown();
                await(releasePostFence);
              },
              failure -> {
                throw new AssertionError(failure);
              }));

      loop.start();

      assertFalse(
          postFenceStarted.await(1L, TimeUnit.SECONDS),
          "async work published after the flush fence must not run before the fence completes");
      readers.armReaderQuiescence(new long[readers.slotCapacity()]);
      readers.endOpForTest(activeReader);
      loop.readerQuiescent(activeReader);
      flush.get(2L, TimeUnit.SECONDS);

      releasePostFence.countDown();
      assertTrue(postFenceStarted.await(1L, TimeUnit.SECONDS));
    } finally {
      releasePostFence.countDown();
      readers.endOpForTest(activeReader);
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushWaitsForRetirementReclaimWhileAReaderIsActive() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      loop.start();
      retireOne(loop, memory);
      waitForRetiredEntries(loop, 1);

      CompletableFuture<Void> flush = loop.flush();
      Thread.sleep(100L);
      assertFalse(flush.isDone(), "flush must include pending QSBR retirement reclaim");

      readers.armReaderQuiescence(new long[readers.slotCapacity()]);
      readers.endOpForTest(activeReader);
      loop.readerQuiescent(activeReader);
      flush.get(3, TimeUnit.SECONDS);
    } finally {
      readers.endOpForTest(activeReader);
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void flushDoesNotChaseRetirementPublishedAfterItsCapturedWatermark() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      CompletableFuture<Void> flush = loop.flush();
      loop.retirementJournal().append(0L, 0L);

      invokeMaintenancePass(loop);

      assertTrue(
          flush.isDone(),
          "retirement published after the flush cut must belong to the next boundary");
    } finally {
      readers.endOpForTest(activeReader);
      loop.retirementJournal().close();
      memory.closeArenas();
    }
  }

  @Test
  public void blockedReclaimBackoffDoublesWithoutReaderProgress() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, ticker, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      retireOne(loop, memory);
      invokeMaintenancePass(loop);

      nowNanos.set(1_000_000L);
      invokeMaintenancePass(loop);

      assertEquals(
          getLongField(loop, "reclaimRetryNanos"),
          3_000_000L,
          "the first blocked QSBR reclaim must wait one maintenance window before rescanning readers");
    } finally {
      readers.endOpForTest(activeReader);
      memory.closeArenas();
    }
  }

  @Test
  public void newerReaderQuiescenceDoesNotBypassBlockedReclaimBackoff() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, ticker, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext pinned = new ThreadContext(null);
    AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
    try {
      assertTrue(guard.enter(pinned));

      retireOne(loop, memory);
      invokeMaintenancePass(loop);
      nowNanos.set(1_000_000L);
      invokeMaintenancePass(loop);
      requestedWork.set(0);

      for (int index = 0; index < 64; index++) {
        ThreadContext newer = new ThreadContext(null);
        assertTrue(guard.enter(newer));
        assertTrue(
            (readers.readerSequence(newer.slot) & 1L) != 0L,
            "each newer reader publishes its own odd sequence word");
        guard.exit(newer);
        invokeMaintenancePass(loop);
      }

      assertEquals(
          requestedWork.get(),
          0,
          "newer reader exits must not bypass the blocked reclaim retry backoff");
      guard.exit(pinned);
      assertTrue(
          requestedWork.get() != 0,
          "the marked reader that may advance the blocking minimum must wake reclaim");
    } finally {
      if (pinned.readerDepth() != 0) {
        guard.exit(pinned);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void reclaimClearsWakeSetByTheLastBlockingReaderExit() throws Exception {
    AtomicBoolean releaseOnReclaimSample = new AtomicBoolean();
    AtomicReference<ReaderGuard> guardReference = new AtomicReference<>();
    AtomicReference<ThreadContext> contextReference = new AtomicReference<>();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            if (releaseOnReclaimSample.compareAndSet(true, false)) {
              guardReference.get().exit(contextReference.get());
            }
            return 0L;
          }

          @Override
          public long currentTimeMillis() {
            return 0L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, ticker, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext pinned = new ThreadContext(null);
    guardReference.set(guard);
    contextReference.set(pinned);
    AtomicInteger requestedWork = (AtomicInteger) getField(loop, "requestedWork");
    try {
      assertTrue(guard.enter(pinned));
      retireOne(loop, memory);
      invokeMaintenancePass(loop);
      assertTrue((Boolean) getField(loop, "reclaimBlocked"));
      requestedWork.set(0);

      releaseOnReclaimSample.set(true);
      setBooleanField(loop, "monotonicSampledThisPass", false);
      invokeMaintenancePass(loop);
      invokeDrainAsyncMutations(loop, 1);
      assertTrue(awaitCompletedRecords(loop.retirementJournal(), 1L, 3_000L));
      assertEquals(loop.retiredEntries(), 0);
      // The direct compatibility pass can leave an advisory retirement bit published by the
      // reentrant ticker callback; an actor turn would consume it before parking.
      requestedWork.set(0);
      assertFalse(invokeBooleanMethod(loop, "hasRunnableWork"));
      assertFalse(invokeBooleanMethod(loop, "hasShutdownWork"));
    } finally {
      if (pinned.readerDepth() != 0) {
        guard.exit(pinned);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void terminalFailureRejectsQueuedAsyncMutationsBeforeClose() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CompletableFuture<Void> pending = new CompletableFuture<>();
    try {
      loop.start();
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                firstStarted.countDown();
                await(releaseFirst);
              },
              Throwable::printStackTrace));
      assertTrue(firstStarted.await(1, TimeUnit.SECONDS));
      assertTrue(
          loop.submitAsyncMutation(() -> {}, pending::completeExceptionally));

      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      // The actor is still executing the first FIFO item. Release that item so the single
      // mailbox consumer can drain the terminal tail; the producer must not poll the MPSC queue.
      releaseFirst.countDown();

      assertTrue(
          awaitCompletedExceptionally(pending, 1_000L),
          "terminal failure must complete queued async mutations exceptionally");
    } finally {
      releaseFirst.countDown();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void terminalFailureStopsFollowingLifecycleMessages() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ConcurrentHashMap<Entry, Entry> data = index();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal(1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            Long.MAX_VALUE);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    Entry lifecycleEntry = EntryTestSupport.entry(memory, 0, 211, 0L);
    try {
      loop.bindWriterLifecycleJournal(lifecycle);
      assertTrue(lifecycleEntry.publishMutation(Entry.PENDING_UPDATE));
      loop.start();
      assertTrue(
          loop.submitAsyncMutation(
              () -> {
                firstStarted.countDown();
                await(releaseFirst);
              },
              failure -> {
                throw new AssertionError(failure);
              }));
      assertTrue(firstStarted.await(1L, TimeUnit.SECONDS));

      loop.enqueueWriterMutationHint(
          lifecycle.lane(0),
          lifecycleEntry,
          0,
          0L,
          WriterLifecycleLane.UNSEEDED_MUTATION_VERSION,
          false);

      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      releaseFirst.countDown();

      assertTrue(
          lifecycleEntry.pendingFlags() != 0,
          "a lifecycle message behind a terminal failure must not consume its Entry state");
    } finally {
      releaseFirst.countDown();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void terminalFailureCloseClearsPendingMutationQueues() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory), Long.MAX_VALUE);
    Entry entry = EntryTestSupport.entry(memory, 0, 128, 0L);
    try {
      loop.publishMutation(entry, Entry.PENDING_ADD);

      loop.start();
      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      loop.stop();
      loop.join(1_000L);

      assertTrue(((MpscUnboundedArrayQueue<?>) getField(loop, "mailbox")).isEmpty());
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void eachRetirementCutAdvancesTheReaderEpoch() throws Exception {
    Ticker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, ticker, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    loop.start();
    try {
      retireOne(loop, memory);
      waitForEpoch(loop, 2L);
      long firstEpoch = loop.epoch();
      waitForRetiredEntries(loop, 1);

      retireOne(loop, memory);
      waitForRetiredEntries(loop, 2);
      waitForEpoch(loop, firstEpoch + 1L);

      assertTrue(
          loop.epoch() > firstEpoch,
          "each completed retirement cut must advance the reader epoch immediately");
    } finally {
      readers.endOpForTest(activeReader);
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void oldestSafeWaitFollowsTheCurrentSafeHead() throws Exception {
    AtomicLong nowNanos = new AtomicLong(1L);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 1_000L + nowNanos.get() / 1_000_000L;
          }
        };
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, ticker, 1 << 20, Eviction.S3_FIFO, newReaderRegistry(memory), Long.MAX_VALUE);
    RetirementJournal journal = loop.retirementJournal();
    try {
      for (int record = 0; record < 33 * RetirementSegment.CAPACITY; record++) {
        journal.append(0L, 0L);
      }
      invokeMaintenancePassWork(loop);
      assertTrue(journal.hasSafeSegments());
      long previousHead = journal.oldestSafeTicket();

      for (int turn = 1; turn <= 3; turn++) {
        nowNanos.set(1L + turn * 1_000_000_000L);
        for (int record = 0; record < 32 * RetirementSegment.CAPACITY; record++) {
          journal.append(0L, 0L);
        }
        invokeMaintenancePassWork(loop);

        long currentHead = journal.oldestSafeTicket();
        assertTrue(currentHead != previousHead, "the bounded reclaim must advance the SAFE head");
        assertEquals(
            loop.retirementOldestSafeWaitNanos(),
            0L,
            "a newly published SAFE head must not inherit the retired head's age");
        previousHead = currentHead;
      }
    } finally {
      loop.stop();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void ordinaryReaderQuiescenceUsesTheEpochDeadlineInsteadOfAnImmediateWake()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot reader = new ReaderSlot();
    readers.register(reader);
    readers.beginOpForTest(reader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    loop.start();
    try {
      retireOne(loop, memory);
      waitForRetiredEntries(loop, 1);

      readers.armReaderQuiescence(new long[readers.slotCapacity()]);
      readers.endOpForTest(reader);
      loop.readerQuiescent(reader);

      waitForNoRetiredEntries(loop);
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void fullRetirementRingDoesNotBlockWriterAndLaterReclaimsAfterReaderQuiesces()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = newReaderRegistry(memory);
    ReaderSlot activeReader = new ReaderSlot();
    readers.register(activeReader);
    readers.beginOpForTest(activeReader, true);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 16L << 20, Eviction.LRU, readers, Long.MAX_VALUE);
    try {
      retireOne(loop, memory);

      loop.start();
      waitForRetiredEntries(loop, 1);
      readers.armReaderQuiescence(new long[readers.slotCapacity()]);
      readers.endOpForTest(activeReader);

      loop.readerQuiescent(activeReader);
      waitForNoRetiredEntries(loop);
    } finally {
      readers.endOpForTest(activeReader);
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void workerDoesNotParkWithMoreThanOneReclaimBatchStillPending() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 16L << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    long allocation = ValueBlock.allocationLength(1);
    int recordCount = 2_048;
    try {
      for (int index = 0; index < recordCount; index++) {
        long address = memory.newWriterArena().allocate(allocation);
        ValueBlock.initialize(address, 0L, 1, 0L);
        loop.retirementJournal().append(address, allocation);
      }
      loop.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
      while (loop.retirementQueueDepth() != 0L && System.nanoTime() < deadline) {
        Thread.sleep(1L);
      }
      assertEquals(
          loop.retirementQueueDepth(),
          0L,
          "the worker must continue bounded seal/reclaim passes instead of parking with records"
              + " pending: "
              + loop.retiredEntries()
              + "/"
              + loop.retirementQueueDepth()
              + ", parked="
              + loop.isParked());
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void retirementPublicationSignalsParkedWorker() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, Ticker.DEFAULT, 1 << 20, Eviction.LRU, newReaderRegistry(memory), Long.MAX_VALUE);
    try {
      loop.start();
      waitUntilParked(loop);

      long allocation = ValueBlock.allocationLength(1);
      long value = memory.newWriterArena().allocate(allocation);
      ValueBlock.initialize(value, 0L, 1, 0L);
      loop.retirementJournal().append(value, allocation);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
      while (loop.retirementQueueDepth() != 0L && System.nanoTime() < deadline) {
        Thread.sleep(1L);
      }
      assertEquals(
          loop.retirementQueueDepth(),
          0L,
          "an actor retirement append must wake maintenance even when the actor is idle");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void firstPartialWriterReservationWakesTheIdleGateWithoutAReadyHint()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            newReaderRegistry(memory),
            journal,
            Long.MAX_VALUE);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      loop.start();
      waitUntilParked(loop);

      long allocation = ValueBlock.allocationLength(1);
      long value = memory.newWriterArena().allocate(allocation);
      ValueBlock.initialize(value, 0L, 1, 0L);
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, value, allocation);
      lane.commit(reservation);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
      while (journal.completedRecordsTotal() == 0L && System.nanoTime() < deadline) {
        Thread.sleep(1L);
      }
      assertEquals(
          journal.completedRecordsTotal(),
          1L,
          "the gate-only open signal must drive the final idle cut and reclaim");
    } finally {
      if (reservation.segment() != null) {
        journal.lane(1).cancel(reservation);
      }
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  private static ThreadContext writerContext(
      NativeMemory.Memory memory, RetirementJournal journal) {
    WriterResourceRegistry resources =
        new WriterResourceRegistry(memory, new WriterLifecycleJournal(), journal);
    ThreadContext context = new ThreadContext(null);
    context.bindWriterResource(resources.acquire());
    return context;
  }

  private static void appendRetirements(RetirementJournal.Lane lane, int count) {
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    for (int index = 0; index < count; index++) {
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
    }
  }

  private static void retireOne(MaintenanceEventLoop loop, NativeMemory.Memory memory) {
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, 0L, 1, 0L);
    loop.retirementJournal().append(value, allocation);
  }

  private static void waitForEpoch(MaintenanceEventLoop loop, long expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (loop.epoch() < expected && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(loop.epoch() >= expected, "maintenance actor did not seal retirement");
  }

  private static void waitForRetiredEntries(MaintenanceEventLoop loop, int expected) {
    RetirementJournal journal = loop.retirementJournal();
    long deadline = System.nanoTime() + 1_000_000_000L;
    // These callers hold a reader epoch. Wait for the actor's sealed accounting instead of
    // sampling retiredEntries while records move from the producer batch to global counters.
    while (journal.unsafeRecords() < expected && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(journal.unsafeRecords() >= expected, "maintenance actor did not seal expected records");
    assertTrue(
        loop.retiredEntries() >= expected, "maintenance actor did not retain expected records");
  }

  private static void waitForNoRetiredEntries(MaintenanceEventLoop loop) {
    RetirementJournal journal = loop.retirementJournal();
    long[] watermark = journal.captureWatermark();
    long deadline = System.nanoTime() + 1_000_000_000L;
    // Capturing this fence does not cut producers or request work. The actor must still reclaim
    // through the ordinary reader notification / epoch deadline, without help from flush.
    while (!journal.watermarkComplete(watermark) && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(journal.watermarkComplete(watermark), "retirement watermark did not complete");
    assertEquals(
        loop.retiredEntries(),
        0,
        "the bounded epoch deadline did not reclaim after reader quiescence");
  }

  private static void waitUntilParked(MaintenanceEventLoop loop) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4L);
    while (!loop.isParked() && System.nanoTime() < deadline) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
    }
    assertTrue(loop.isParked(), "maintenance actor did not park within 4 seconds");
  }

  private static void waitForMonotonicCalls(CountingTicker ticker, int expected) {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (ticker.monotonicCalls.get() < expected && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(
        ticker.monotonicCalls.get() >= expected,
        "maintenance actor did not enter the expected number of passes");
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(2, TimeUnit.SECONDS)) {
        throw new AssertionError("test latch was not released");
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new AssertionError(error);
    }
  }

  private static Object getField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void assertMissingField(Class<?> type, String name) {
    try {
      type.getDeclaredField(name);
      throw new AssertionError("obsolete field must be removed: " + name);
    } catch (NoSuchFieldException expected) {
      // Expected after the work-conserving refactor.
    }
  }

  private static void assertMissingNestedClass(Class<?> type, String name) {
    for (Class<?> nested : type.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(name)) {
        throw new AssertionError("obsolete nested class must be removed: " + name);
      }
    }
  }

  private static boolean hasField(Class<?> type, String name) {
    try {
      type.getDeclaredField(name);
      return true;
    } catch (NoSuchFieldException missing) {
      return false;
    }
  }

  private static long getLongField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.getLong(target);
  }

  private static int intField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.getInt(target);
  }

  private static boolean getBooleanField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.getBoolean(target);
  }

  private static int intField(Class<?> type, String name) throws Exception {
    Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return field.getInt(null);
  }

  private static void setLongField(Object target, String name, long value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.setLong(target, value);
  }

  private static void setBooleanField(Object target, String name, boolean value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.setBoolean(target, value);
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

  private static void invokeMaintenancePass(MaintenanceEventLoop loop) throws Exception {
    invokeMaintenancePassWork(loop);
  }

  private static void invokeParkUntilWork(MaintenanceEventLoop loop) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("parkUntilWork");
    method.setAccessible(true);
    method.invoke(loop);
  }

  private static void invokeCaptureWorkDecision(MaintenanceEventLoop loop, boolean consume)
      throws Exception {
    Method method =
        MaintenanceEventLoop.class.getDeclaredMethod("captureWorkDecision", boolean.class);
    method.setAccessible(true);
    method.invoke(loop, consume);
  }

  private static void invokeCompleteFlushIfIdle(MaintenanceEventLoop loop) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("completeFlushIfIdle");
    method.setAccessible(true);
    method.invoke(loop);
  }

  private static int invokeMaintenancePassWork(MaintenanceEventLoop loop) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("maintenancePass");
    method.setAccessible(true);
    return (Integer) method.invoke(loop);
  }

  private static boolean awaitCompletedRecords(
      RetirementJournal journal, long target, long timeoutMillis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (journal.completedRecordsTotal() < target && System.nanoTime() < deadline) {
      Thread.yield();
    }
    return journal.completedRecordsTotal() >= target;
  }

  private static boolean awaitCompletedExceptionally(
      CompletableFuture<?> future, long timeoutMillis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (!future.isCompletedExceptionally() && System.nanoTime() < deadline) {
      Thread.yield();
    }
    return future.isCompletedExceptionally();
  }

  private static int invokeDrainAsyncMutations(MaintenanceEventLoop loop, int limit)
      throws Exception {
    Method method =
        MaintenanceEventLoop.class.getDeclaredMethod("drainMailbox", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(loop, limit);
  }

  private static int invokeDrainMutations(MaintenanceEventLoop loop, int limit) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("drainMailbox", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(loop, limit);
  }

  private static boolean invokeBooleanMethod(Object target, String name) throws Exception {
    Method method = target.getClass().getDeclaredMethod(name);
    method.setAccessible(true);
    return (Boolean) method.invoke(target);
  }

  private static int readerCount(ReaderRegistry readers) throws Exception {
    Method method = ReaderRegistry.class.getDeclaredMethod("registeredCount");
    method.setAccessible(true);
    return (Integer) method.invoke(readers);
  }

  private static final class FrozenTicker implements Ticker {
    @Override
    public long nanos() {
      return 0L;
    }

    @Override
    public long currentTimeMillis() {
      return 0L;
    }
  }

  private static final class BlockingPassTicker implements Ticker {
    final CountDownLatch passStarted = new CountDownLatch(1);
    final CountDownLatch releasePass = new CountDownLatch(1);
    volatile boolean blockPass;

    @Override
    public long nanos() {
      if (blockPass) {
        passStarted.countDown();
        await(releasePass);
      }
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  }

  private static final class CountingTicker implements Ticker {
    final AtomicInteger monotonicCalls = new AtomicInteger();
    final AtomicInteger wallCalls = new AtomicInteger();

    @Override
    public long nanos() {
      monotonicCalls.incrementAndGet();
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      wallCalls.incrementAndGet();
      return System.currentTimeMillis();
    }

    int calls() {
      return monotonicCalls.get() + wallCalls.get();
    }

    void reset() {
      monotonicCalls.set(0);
      wallCalls.set(0);
    }
  }

  private static final class CountingIndex extends ConcurrentHashMap<Entry, Entry> {
    final AtomicInteger keySetCalls = new AtomicInteger();
    final AtomicInteger getCalls = new AtomicInteger();
    final AtomicInteger valuesCalls = new AtomicInteger();

    @Override
    public com.red.ohc.index.Entry get(Object key) {
      getCalls.incrementAndGet();
      return super.get(key);
    }

    @Override
    public KeySetView<com.red.ohc.index.Entry, com.red.ohc.index.Entry> keySet() {
      keySetCalls.incrementAndGet();
      return super.keySet();
    }

    @Override
    public java.util.Collection<com.red.ohc.index.Entry> values() {
      valuesCalls.incrementAndGet();
      return super.values();
    }
  }

  private static ConcurrentHashMap<Entry, Entry> index() {
    return new ConcurrentHashMap<>();
  }
}
