package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jctools.queues.MpscArrayQueue;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.AccessRing;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class MaintenanceEventLoopTest {
  @Test
  public void cpuScaledBatchesFollowTheBoundedCaffeineStyleFormula() {
    assertBatchLimits(1, 16, 32, 4, 4);
    assertBatchLimits(2, 32, 64, 8, 8);
    assertBatchLimits(3, 64, 128, 16, 16);
    assertBatchLimits(4, 64, 128, 16, 16);
    assertBatchLimits(8, 128, 256, 32, 32);
    assertBatchLimits(16, 256, 256, 64, 64);
    assertBatchLimits(1 << 20, 256, 256, 64, 64);
  }

  @Test
  public void windowSchedulerNeverRunsAFirstOrFollowupPassBeforeOneMillisecond() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(100L);
    assertEquals(scheduler.nextPassNanos(), 1_000_100L);
    assertFalse(scheduler.isDue(1_000_099L));
    assertTrue(scheduler.isDue(1_000_100L));

    scheduler.complete(1_000_100L, true, Long.MIN_VALUE);
    assertEquals(scheduler.nextPassNanos(), 2_000_100L);
    assertFalse(scheduler.isDue(2_000_099L));
    assertTrue(scheduler.isDue(2_000_100L));
  }

  @Test
  public void windowSchedulerDoesNotLetSignalsBypassAnActiveCooldown() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(0L);
    scheduler.complete(1_000_000L, true, Long.MIN_VALUE);
    assertEquals(scheduler.nextPassNanos(), 2_000_000L);

    scheduler.request(1_500_000L);
    assertEquals(scheduler.nextPassNanos(), 2_000_000L);
    assertFalse(scheduler.isDue(1_999_999L));
    assertTrue(scheduler.isDue(2_000_000L));
  }

  @Test
  public void windowSchedulerUsesTheLaterOfCooldownAndRetryDeadline() {
    MaintenanceEventLoop.WindowScheduler cooldownWins =
        new MaintenanceEventLoop.WindowScheduler();
    cooldownWins.request(0L);
    cooldownWins.complete(1_000_000L, true, 1_500_000L);
    assertEquals(cooldownWins.nextPassNanos(), 2_000_000L);

    MaintenanceEventLoop.WindowScheduler retryWins = new MaintenanceEventLoop.WindowScheduler();
    retryWins.request(0L);
    retryWins.complete(1_000_000L, true, 3_000_000L);
    assertEquals(retryWins.nextPassNanos(), 3_000_000L);
  }

  @Test
  public void windowSchedulerReturnsToIdleWithoutContinuation() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(0L);
    scheduler.complete(1_000_000L, false, Long.MIN_VALUE);
    assertEquals(scheduler.nextPassNanos(), Long.MIN_VALUE);
    assertFalse(scheduler.isDue(Long.MAX_VALUE));

    scheduler.request(5_000_000L);
    assertEquals(scheduler.nextPassNanos(), 6_000_000L);
  }

  @Test
  public void windowSchedulerSaturatesCooldownDeadlineAtLongMaxValue() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(0L);
    scheduler.complete(Long.MAX_VALUE - 500_000L, true, Long.MIN_VALUE);

    assertEquals(scheduler.nextPassNanos(), Long.MAX_VALUE);
    assertFalse(scheduler.isDue(Long.MAX_VALUE - 1L));
    assertTrue(scheduler.isDue(Long.MAX_VALUE));
  }

  @Test(timeOut = 5_000L)
  public void removalBacklogCannotStarveAsyncMutationsWithinOnePass() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry(),
            8_192);
    try {
      ReliableRemovalQueue removals =
          (ReliableRemovalQueue) getField(loop, "reliableRemovals");
      int general = (Integer) getField(getField(loop, "batchLimits"), "general");
      int removalCount = 4_096 + 1;
      ReliableRemovalQueue.Reservation reservation = new ReliableRemovalQueue.Reservation();
      for (int index = 0; index < removalCount; index++) {
        assertTrue(removals.tryReserve(reservation));
        removals.commit(reservation, null);
      }

      CountDownLatch asyncExecuted = new CountDownLatch(1);
      assertTrue(loop.submitAsyncMutation(asyncExecuted::countDown, Throwable::printStackTrace));

      invokeMaintenancePassWork(loop);
      assertEquals(asyncExecuted.getCount(), 0L);
      assertEquals(removals.size(), (long) removalCount - general);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushCompletesAfterEveryCoveredAsyncBatchRunsInOrder() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            ticker,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    int asyncBatch = (Integer) getField(getField(loop, "batchLimits"), "async");
    int taskCount = asyncBatch * 2 + 1;
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
      assertEquals(ticker.monotonicCalls.get(), 3);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void closingAStartedWorkerRejectsRemainingAsyncBatchesAndClearsDepth()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    int asyncBatch = (Integer) getField(getField(loop, "batchLimits"), "async");
    int pendingCount = asyncBatch * 2 + 1;
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

  @Test(timeOut = 5_000L)
  public void shutdownRejectsEveryQueuedAsyncBatchAndClearsDepth() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            ticker,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    int asyncBatch = (Integer) getField(getField(loop, "batchLimits"), "async");
    int taskCount = asyncBatch * 2 + 1;
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
      assertEquals(ticker.monotonicCalls.get(), 3);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void idleWorkerStopsWithoutWaitingForAWorkWindow() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            new NativeMemory.Memory(AllocatorType.JNA),
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    Field field = MaintenanceEventLoop.class.getDeclaredField("asyncMutations");
    field.setAccessible(true);
    field.set(loop, new SizeForbiddenQueue<>());
    try {
      assertEquals(
          loop.asyncMutationQueueDepth(),
          0L,
          "an eventually consistent statistic must not traverse an unbounded concurrent queue");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void closingWaitsForAnAdmittedAsyncTaskToPublish() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    BlockingOfferQueue<Object> queue = new BlockingOfferQueue<>();
    Field field = MaintenanceEventLoop.class.getDeclaredField("asyncMutations");
    field.setAccessible(true);
    field.set(loop, queue);
    AtomicBoolean submitted = new AtomicBoolean();
    CompletableFuture<Void> terminal = new CompletableFuture<>();
    CountDownLatch closeReturned = new CountDownLatch(1);
    Thread submitter =
        new Thread(
            () ->
                submitted.set(
                    loop.submitAsyncMutation(
                        () -> terminal.complete(null), terminal::completeExceptionally)));
    Thread closer =
        new Thread(
            () -> {
              loop.beginClosing();
              loop.stop();
              closeReturned.countDown();
            });
    loop.start();
    try {
      submitter.start();
      assertTrue(queue.offerEntered.await(1L, TimeUnit.SECONDS));
      closer.start();
      assertFalse(
          closeReturned.await(100L, TimeUnit.MILLISECONDS),
          "close must linearize after a submission that already passed admission");

      queue.releaseOffer.countDown();
      submitter.join(1_000L);
      closer.join(1_000L);
      loop.join(1_000L);

      assertTrue(submitted.get());
      assertTrue(terminal.isDone(), "the admitted task must execute or be rejected before shutdown");
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      queue.releaseOffer.countDown();
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void failedAsyncOfferDoesNotPublishAPhantomSequence() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    Field field = MaintenanceEventLoop.class.getDeclaredField("asyncMutations");
    field.setAccessible(true);
    field.set(loop, new ThrowingOfferQueue<>());
    CompletableFuture<Void> rejected = new CompletableFuture<>();
    try {
      assertFalse(loop.submitAsyncMutation(() -> {}, rejected::completeExceptionally));
      assertTrue(rejected.isCompletedExceptionally());
      assertEquals(((AtomicLong) getField(loop, "asyncSubmitted")).get(), 0L);
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
      loop.start();
    } finally {
      loop.beginClosing();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void terminalDrainCanRaceAnActorPeekWithoutRegressingQueueDepth() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    PeekBlockingQueue<Object> queue = new PeekBlockingQueue<>();
    Field field = MaintenanceEventLoop.class.getDeclaredField("asyncMutations");
    field.setAccessible(true);
    field.set(loop, queue);
    CompletableFuture<Void> first = new CompletableFuture<>();
    CompletableFuture<Void> second = new CompletableFuture<>();
    AtomicReference<Throwable> drainFailure = new AtomicReference<>();
    Thread draining =
        new Thread(
            () -> {
              try {
                invokeDrainAsyncMutations(loop, 2);
              } catch (Throwable failure) {
                drainFailure.set(failure);
              }
            });
    try {
      assertTrue(loop.submitAsyncMutation(() -> {}, first::completeExceptionally));
      assertTrue(loop.submitAsyncMutation(() -> {}, second::completeExceptionally));
      draining.start();
      assertTrue(queue.peekEntered.await(1L, TimeUnit.SECONDS));

      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      queue.releasePeek.countDown();
      draining.join(1_000L);

      assertEquals(drainFailure.get(), null);
      assertTrue(first.isCompletedExceptionally());
      assertTrue(second.isCompletedExceptionally());
      assertEquals(loop.asyncMutationQueueDepth(), 0L);
    } finally {
      queue.releasePeek.countDown();
      draining.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void oneBrokenRejectCallbackCannotStrandLaterAsyncTasks() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
  public void windowSchedulerHonorsTheLaterRetryDeadlineWithoutLettingSignalsBypassIt() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(0L);
    scheduler.complete(1_000_000L, true, 11_000_000L);
    assertEquals(scheduler.nextPassNanos(), 11_000_000L);

    scheduler.request(2_000_000L);
    assertEquals(
        scheduler.nextPassNanos(),
        11_000_000L,
        "a producer signal must not bypass a blocked maintenance retry");
  }

  @Test
  public void emptyWindowRetainsAnOutstandingRetryDeadline() {
    MaintenanceEventLoop.WindowScheduler scheduler = new MaintenanceEventLoop.WindowScheduler();

    scheduler.request(0L);
    scheduler.complete(1_000_000L, true, 3_000_000L);
    assertFalse(scheduler.isDue(2_999_999L));
    assertTrue(scheduler.isDue(3_000_000L));
  }

  @Test
  public void workPlanKeepsPerSubsystemDrainsWithinCpuScaledBatches() throws Exception {
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            new NativeMemory.Memory(AllocatorType.JNA),
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      Object limits = getField(loop, "batchLimits");
      int general = (Integer) getField(limits, "general");
      int access = (Integer) getField(limits, "access");
      int eviction = (Integer) getField(limits, "eviction");
      int async = (Integer) getField(limits, "async");

      assertTrue(general >= 16 && general <= 256);
      assertEquals(access, Math.min(general * 2, 256));
      assertEquals(eviction, Math.min(general / 4, 64));
      assertEquals(async, Math.min(general / 4, 64));
    } finally {
      ((NativeMemory.Memory) getField(loop, "memory")).closeArenas();
    }
  }

  private static void assertBatchLimits(
      int cpuCount, int general, int access, int eviction, int async) {
    MaintenanceEventLoop.BatchLimits limits = MaintenanceEventLoop.BatchLimits.forCpu(cpuCount);
    assertEquals(limits.general, general);
    assertEquals(limits.access, access);
    assertEquals(limits.eviction, eviction);
    assertEquals(limits.async, async);
  }

  @Test
  public void removalNotificationObservesValueBeforeNativeRetirement() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE, 64L << 20);
    Budget budget = new Budget(1L << 20);
    WriterArena arena = memory.newWriterArena();
    ThreadContext context = new ThreadContext(arena);
    ConcurrentHashMap<Entry, Entry> data = index();
    AtomicInteger observedPayload = new AtomicInteger(-1);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            budget,
            Ticker.DEFAULT,
            1L << 20,
            Eviction.LRU,
            (entry, address, cause) ->
                observedPayload.set(
                    NativeMemory.getByte(ValueBlock.payloadAddress(address)) & 0xff),
            new ReaderRegistry(),
            1_024);
    long keyAllocation = 16L;
    long valueAllocation = ValueBlock.allocationLength(8);
    long weight =
        WriterArena.allocationWeight(keyAllocation)
            + WriterArena.allocationWeight(valueAllocation);
    try {
      assertTrue(budget.tryReserve(weight, 0));
      long keyAddress = arena.allocate(keyAllocation);
      long valueAddress = arena.allocate(valueAllocation);
      ValueBlock.initialize(valueAddress, 1L, 8);
      NativeMemory.putByte(ValueBlock.payloadAddress(valueAddress), (byte) 0x11);
      Entry entry =
          new Entry(
              keyAddress,
              8,
              7,
              0x1234L,
              Entry.tagValueAddress(valueAddress, true));
      data.put(entry, entry);

      assertTrue(entry.claimWriter());
      assertTrue(loop.prepareReliableRemoval(context, entry));
      entry.markRetired();
      assertTrue(data.remove(entry, entry));
      entry.clearValue();
      entry.finishWriter();

      loop.publishRemovalAndRetire(
          context,
          entry,
          false,
          valueAddress,
          valueAllocation,
          RemovalCause.EXPIRED);
      invokeMaintenancePass(loop);

      assertEquals(observedPayload.get(), 0x11);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void mutationTransportUsesABoundedQueueAndRepairSideChannel() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      Field queue = MaintenanceEventLoop.class.getDeclaredField("queue");
      queue.setAccessible(true);
      assertTrue(
          queue.get(loop) instanceof MpscArrayQueue,
          "mutation hints must use a bounded non-blocking queue");
      boolean hasRepair = false;
      for (Field field : MaintenanceEventLoop.class.getDeclaredFields()) {
        hasRepair |= field.getName().contains("repair");
      }
      assertTrue(hasRepair, "a full advisory queue must leave a repair signal");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void repairDebtUsesOneDirtyShardTokenAndAnO1Aggregate() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
            new ReaderRegistry(), 4_096);
    Entry entry = new Entry(0L, 0, 31, 0L);
    try {
      Field queueField = MaintenanceEventLoop.class.getDeclaredField("dirtyRepairShardQueue");
      queueField.setAccessible(true);
      ConcurrentLinkedQueue<?> dirtyShards =
          (ConcurrentLinkedQueue<?>) queueField.get(loop);
      Field debtField = MaintenanceEventLoop.class.getDeclaredField("repairDebtTotal");
      debtField.setAccessible(true);
      AtomicLong debt = (AtomicLong) debtField.get(loop);
      Method mark =
          MaintenanceEventLoop.class.getDeclaredMethod("markMutationForRepair", Entry.class);
      mark.setAccessible(true);

      assertTrue(entry.publishMutation(Entry.PENDING_UPDATE));
      mark.invoke(loop, entry);
      assertEquals(dirtyShards.size(), 1);
      assertEquals(debt.get(), 1L);

      Method repair = MaintenanceEventLoop.class.getDeclaredMethod("repairMutations", int.class);
      repair.setAccessible(true);
      assertEquals(repair.invoke(loop, 4_096), 1);
      assertEquals(debt.get(), 0L);
      assertTrue(dirtyShards.isEmpty());
    } finally {
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
  public void dirtyReaderSignalUsesOneTokenAndRequeuesAfterTheDrainLimit() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ReaderRegistry readers = new ReaderRegistry();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
    ReaderSlot slot = new ReaderSlot();
    slot.access = new AccessRing();
    slot.access.offer(new Entry(0L, 0, 1, 0L), 1L);
    slot.access.offer(new Entry(0L, 0, 2, 0L), 1L);
    try {
      loop.signalAccess(slot);
      loop.signalAccess(slot);

      Field field = MaintenanceEventLoop.class.getDeclaredField("dirtyReaderQueue");
      field.setAccessible(true);
      Queue<?> dirtyReaders = (Queue<?>) field.get(loop);
      assertEquals(dirtyReaders.size(), 1, "a dirty reader must publish one coalesced token");

      Method drain = MaintenanceEventLoop.class.getDeclaredMethod("drainAccesses", int.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, 1), 1);
      assertEquals(dirtyReaders.size(), 1, "remaining ring data must requeue the same reader");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void dirtyReaderOverflowRetainsTokensWhenTheReusableQueueIsFull() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU,
            new ReaderRegistry(), 1_024);
    List<ReaderSlot> slots = new ArrayList<>(1_025);
    try {
      for (int i = 0; i < 1_025; i++) {
        ReaderSlot slot = new ReaderSlot();
        slot.access = new AccessRing();
        slot.access.offer(new Entry(0L, 0, i + 1, 0L), 1L);
        slots.add(slot);
        loop.signalAccess(slot);
      }

      Field overflowField =
          MaintenanceEventLoop.class.getDeclaredField("dirtyReaderOverflowQueue");
      overflowField.setAccessible(true);
      Queue<?> overflow = (Queue<?>) overflowField.get(loop);
      assertEquals(overflow.size(), 1, "the full fixed queue must spill its token");

      Method drain = MaintenanceEventLoop.class.getDeclaredMethod("drainAccesses", int.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, 1_024), 1_024);
      assertEquals(drain.invoke(loop, 1_024), 1);
      assertTrue(overflow.isEmpty(), "overflow tokens must be drained");
      for (ReaderSlot slot : slots) {
        assertFalse(slot.hasAccessPending(), "draining a token must clear its pending state");
        assertTrue(slot.access.isEmpty(), "draining a token must consume its access ring");
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readerRegistrationDoesNotWakeAnIdleWorker() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      loop.start();
      waitUntilParked(loop);
      loop.registerReader(new ReaderSlot());
      Thread.sleep(10L);
      assertTrue(loop.isParked(), "registering a reader is not maintenance work");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void idleWorkerDoesNotReadAClockWithoutTimerOrRetirementWork() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            ticker,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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

  @Test(timeOut = 2_000L)
  public void idleStartupProbesTheAsyncSourceOnceBeforeParking() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    CountingQueue<Object> asyncMutations = new CountingQueue<>();
    Field field = MaintenanceEventLoop.class.getDeclaredField("asyncMutations");
    field.setAccessible(true);
    field.set(loop, asyncMutations);
    try {
      loop.start();
      waitUntilParked(loop);

      assertEquals(
          asyncMutations.emptyChecks.get(),
          1,
          "initial idle arming must not repeat the complete source probe");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void forcedPhysicalClockSampleIsReusedForEvictionDue() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena arena = memory.newWriterArena();
    long key = arena.allocate(8L);
    long allocation = ValueBlock.allocationLength(1);
    long value = arena.allocate(allocation);
    NativeMemory.putLong(key, 129L);
    ValueBlock.initialize(value, 0L, 1);
    Entry entry = new Entry(key, 0, 129, value);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new Budget(1 << 20), ticker, 1L, Eviction.LRU, new ReaderRegistry());
    try {
      invokeApplyEntry(loop, entry);
      assertTrue(entry.claimWriter());
      Field retry = MaintenanceEventLoop.class.getDeclaredField("evictionRetryNanos");
      retry.setAccessible(true);
      retry.setLong(loop, 0L);
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), ticker, 1 << 20, Eviction.LRU, new ReaderRegistry());
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

  @Test(timeOut = 2_000L)
  public void futureTtlSleepsByWheelTickWithoutEarlyExpiry() throws Exception {
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    long value = memory.newWriterArena().allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(value, 64L, 1);
    Entry entry = new Entry(0L, 0, 93, 0L, Entry.tagValueAddress(value, true));
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new Budget(1 << 20), ticker, 1 << 20, Eviction.LRU, new ReaderRegistry());
    try {
      invokeApplyEntry(loop, entry);
      loop.start();
      waitUntilParked(loop);
      Thread.sleep(130L);

      assertTrue(
          wallCalls.get() > 1,
          "a scheduled TTL must wake the actor at least once for its wheel tick");
      assertTrue(
          wallCalls.get() <= 12,
          "a future TTL must not drive one-millisecond maintenance passes: " + wallCalls.get());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot reader = new ReaderSlot();
    readers.register(reader);
    WeakReference<ReaderSlot> readerReference = readers.references().iterator().next();
    readerReference.clear();
    assertTrue(readerReference.enqueue());

    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
    try {
      CompletableFuture<Void> flush = loop.flush();
      assertEquals(invokeMaintenancePassWork(loop), 0);
      assertTrue(flush.isDone(), "the flush pass must consume the queued reader cleanup");
      assertEquals(readerCount(readers), 0);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void boundedTransportAcceptsVisibleHintsWithoutMakingTheCacheUnhealthy() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    Entry[] entries = new Entry[4];
    try {
      for (int i = 0; i < entries.length; i++) {
        entries[i] = new Entry(0L, 0, i + 1, 0L);
        loop.publishMutation(entries[i], Entry.PENDING_ADD);
        loop.afterWrite();
      }
      assertEquals(loop.queueDepth(), 4L);
      assertEquals(
          loop.snapshot().unhealthy,
          false,
          "a visible mutation must not make maintenance unhealthy");

      loop.start();
      loop.flush().join();
      for (Entry entry : entries) {
        assertEquals(entry.pendingFlags, 0);
      }
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
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
    ThreadContext context = new ThreadContext(null);
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
  public void boundedHintQueueFallsBackToRepairInsteadOfRejectingPublication() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      int limit = (int) loop.queueCapacity();
      for (int i = 0; i < limit; i++) {
        Entry entry = new Entry(0L, 0, i + 1, 0L);
        data.put(entry, entry);
        loop.publishMutation(entry, Entry.PENDING_ADD);
        loop.afterWrite();
      }
      Entry rejected = new Entry(0L, 0, limit + 1, 0L);
      data.put(rejected, rejected);
      loop.publishMutation(rejected, Entry.PENDING_ADD);
      assertTrue(
          rejected.pendingFlags != 0, "the full queue must leave a repairable pending marker");

      loop.start();
      loop.flush().join();
      assertEquals(rejected.pendingFlags, 0, "repair must eventually apply the dropped hint");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void repairDebtConvergesWhenReliableRemovalConsumesTheDroppedHint() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    try {
      int limit = (int) loop.queueCapacity();
      for (int i = 0; i < limit; i++) {
        Entry entry = new Entry(0L, 0, i + 1, 0L);
        data.put(entry, entry);
        loop.publishMutation(entry, Entry.PENDING_ADD);
      }
      Entry removed = new Entry(0L, 0, limit + 1, 0L);
      data.put(removed, removed);
      loop.publishMutation(removed, Entry.PENDING_ADD);
      assertTrue(removed.isRepairMarked());

      assertTrue(removed.claimWriter());
      loop.prepareReliableRemoval(context, removed);
      removed.markRetired();
      assertTrue(data.remove(removed, removed));
      removed.finishWriter();
      loop.publishRemovalAndRetire(context, removed, true, 0L, 0L, null);

      loop.start();
      loop.flush().join();
      assertEquals(
          loop.queueDepth(),
          0L,
          "repair debt must be released when reliable removal consumes the marker");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void deferredAdvisoryRepairClearsMarkerAfterAConcurrentRemovalClaimIsCancelled()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      int limit = (int) loop.queueCapacity();
      for (int i = 0; i < limit; i++) {
        Entry entry = new Entry(0L, 0, i + 1, 0L);
        data.put(entry, entry);
        loop.publishMutation(entry, Entry.PENDING_ADD);
      }
      Entry raced = new Entry(0L, 0, limit + 1, 0L);
      data.put(raced, raced);
      loop.publishMutation(raced, Entry.PENDING_ADD);
      assertTrue(raced.isRepairMarked());
      assertTrue(raced.tryBeginPending(Entry.PENDING_REMOVE));

      Method repair = MaintenanceEventLoop.class.getDeclaredMethod("repairMutations", int.class);
      repair.setAccessible(true);
      repair.invoke(loop, 1);
      raced.cancelPendingClaim();

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod("drainDeferredMutations", int.class);
      drain.setAccessible(true);
      drain.invoke(loop, 1);

      assertFalse(
          raced.isRepairMarked(),
          "a deferred advisory mutation must release its repair marker after the remove claim"
              + " cancels");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void failedReliableRemovalPreparationReleasesAdmissionAndReservation() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    try {
      try {
        loop.prepareReliableRemoval(context, null);
        throw new AssertionError("invalid removal preparation must fail");
      } catch (NullPointerException expected) {
        // The failed attempt must not strand the admission lock or its native reservation.
      }

      Entry entry = new Entry(0L, 0, 123, 0L);
      loop.prepareReliableRemoval(context, entry);
      loop.cancelReliableRemoval(context, entry);
      assertEquals(
          entry.pendingFlags,
          0,
          "canceling a pre-publication removal must release its pending claim");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void uncommittedReliableRemovalDoesNotKeepWorkerSpinning() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    Entry entry = new Entry(0L, 0, 124, 0L);
    try {
      loop.prepareReliableRemoval(context, entry);
      loop.start();
      waitUntilParked(loop);
    } finally {
      loop.cancelReliableRemoval(context, entry);
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void flushRemainsPendingUntilAnUncommittedReliableRemovalIsResolved() throws Exception {
    CountingTicker ticker = new CountingTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            ticker,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    Entry entry = new Entry(0L, 0, 127, 0L);
    try {
      assertTrue(loop.prepareReliableRemoval(context, entry));
      loop.start();
      waitUntilParked(loop);

      CompletableFuture<Void> flush = loop.flush();
      assertFalse(flush.isDone(), "flush must not complete before the reservation is resolved");

      waitForMonotonicCalls(ticker, 2);
      assertFalse(flush.isDone(), "flush must remain pending while the reservation is unresolved");

      loop.cancelReliableRemoval(context, entry);
      flush.get(1L, TimeUnit.SECONDS);
    } finally {
      if (context.reliableRemoval().active()) {
        loop.cancelReliableRemoval(context, entry);
      }
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void stoppingWorkerDoesNotSpinOnAnUncommittedReliableRemoval() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    Entry entry = new Entry(0L, 0, 126, 0L);
    try {
      assertTrue(loop.prepareReliableRemoval(context, entry));
      loop.start();
      waitUntilParked(loop);

      loop.stop();
      loop.join(1_000L);
      assertFalse(loop.isAlive(), "stop must reach teardown without waiting for an uncommitted slot");
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void cancelingARemovalReservationSignalsAparkedWorker() throws Exception {
    BlockingPassTicker ticker = new BlockingPassTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            ticker,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    Entry entry = new Entry(0L, 0, 125, 0L);
    try {
      loop.start();
      waitUntilParked(loop);
      loop.prepareReliableRemoval(context, entry);
      ticker.blockPass = true;
      loop.cancelReliableRemoval(context, entry);

      assertTrue(
          ticker.passStarted.await(1L, TimeUnit.SECONDS),
          "canceling a parked removal reservation must enter the next maintenance pass");
    } finally {
      ticker.releasePass.countDown();
      if (context.reliableRemoval().active()) {
        loop.cancelReliableRemoval(context, entry);
      }
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void maintenanceHintCapacityIsBoundedAndPowerOfTwo() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      assertTrue(loop.queueCapacity() >= 1_024L);
      assertEquals(
          Integer.bitCount((int) loop.queueCapacity()),
          1,
          "the bounded hint queue capacity must be a power of two");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void actorDefersQueuedMutationUntilTheEntryWriterPublishes() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Entry entry = new Entry(0L, 0, 77, 0L);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      // This is the deterministic version of the interleaving: the actor has taken the
      // old transport item while a later writer has claimed the Entry but not yet made
      // its new pointer visible.
      assertTrue(entry.claimWriter());
      invokeProcessEntry(loop, entry);

      assertTrue(
          (entry.pendingFlags & Entry.PENDING_UPDATE) != 0,
          "the actor must retain the pending event while a writer owns the Entry");
    } finally {
      if ((entry.lifecycle & Entry.WRITER_LOCK) != 0L) {
        entry.finishWriter();
      }
      memory.closeArenas();
    }
  }

  @Test
  public void deferredMutationBackoffDoublesWithoutProgress() throws Exception {
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), ticker, 1 << 20, Eviction.LRU, new ReaderRegistry());
    Entry entry = new Entry(0L, 0, 79, 0L);
    try {
      assertTrue(entry.tryBeginPending(Entry.PENDING_REMOVE));
      @SuppressWarnings("unchecked")
      ArrayDeque<Entry> deferred = (ArrayDeque<Entry>) getField(loop, "deferredMutations");
      deferred.add(entry);

      invokeMaintenancePass(loop);
      assertEquals(
          getLongField(loop, "deferredMutationRetryNanos"),
          1_000_000L,
          "the first blocked deferred retry must wait one maintenance window");

      invokeMaintenancePass(loop);
      assertEquals(
          getLongField(loop, "deferredMutationRetryNanos"),
          1_000_000L,
          "a deferred mutation must not be retried before its backoff deadline");

      nowNanos.set(1_000_000L);
      invokeMaintenancePass(loop);
      assertEquals(
          getLongField(loop, "deferredMutationRetryNanos"),
          3_000_000L,
          "a second blocked deferred retry must double its backoff");
    } finally {
      entry.cancelPendingClaimIfPresent();
      memory.closeArenas();
    }
  }

  @Test
  public void deferredMutationAttemptsAreCappedEvenWhenEveryEntryIsWriterOwned() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());
    Entry first = new Entry(0L, 0, 171, 0L);
    Entry second = new Entry(0L, 0, 172, 0L);
    Entry third = new Entry(0L, 0, 173, 0L);
    try {
      assertTrue(first.tryBeginPending(Entry.PENDING_REMOVE));
      assertTrue(second.tryBeginPending(Entry.PENDING_REMOVE));
      assertTrue(third.tryBeginPending(Entry.PENDING_REMOVE));
      @SuppressWarnings("unchecked")
      ArrayDeque<Entry> deferred = (ArrayDeque<Entry>) getField(loop, "deferredMutations");
      deferred.addLast(first);
      deferred.addLast(second);
      deferred.addLast(third);

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod("drainDeferredMutations", int.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, 2), 0);
      assertEquals(deferred.size(), 3);
      assertEquals(
          deferred.removeFirst(),
          third,
          "two writer-owned attempts must leave the third entry for the next maintenance window");
    } finally {
      first.cancelPendingClaimIfPresent();
      second.cancelPendingClaimIfPresent();
      third.cancelPendingClaimIfPresent();
      memory.closeArenas();
    }
  }

  @Test
  public void deferredMutationRetainsAnAdvisoryUpdateWhileTheWriterMutexIsHeld() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());
    Entry entry = new Entry(0L, 0, 174, 0L);
    try {
      assertTrue(entry.publishMutation(Entry.PENDING_UPDATE));
      assertTrue(entry.claimWriter());
      @SuppressWarnings("unchecked")
      ArrayDeque<Entry> deferred = (ArrayDeque<Entry>) getField(loop, "deferredMutations");
      deferred.addLast(entry);

      Method drain =
          MaintenanceEventLoop.class.getDeclaredMethod("drainDeferredMutations", int.class);
      drain.setAccessible(true);
      assertEquals(drain.invoke(loop, 1), 0);
      assertEquals(deferred.size(), 1);
      assertTrue(
          (entry.pendingFlags & Entry.PENDING_UPDATE) != 0,
          "the actor must not consume an advisory update while its writer has not published");
    } finally {
      if (entry.isWriterLocked()) {
        entry.finishWriter();
      }
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
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    try {
      loop.publishMutation(entry, Entry.PENDING_UPDATE);
      assertTrue(entry.claimWriter());
      invokeProcessEntry(loop, entry);

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
  public void actorCannotClearACoalescedMutationBeforeTheConcurrentWriterPublishes()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, System.currentTimeMillis() + 60_000L, 1);
    Entry entry = new Entry(0L, 0, 79, 0L);
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
          (entry.pendingFlags & Entry.PENDING_UPDATE) != 0,
          "the old queue item must defer instead of clearing the later writer's update");

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

  @Test(timeOut = 2_000L)
  public void evictionRetriesAfterAWriterLockIsReleasedWithoutAnotherExternalWrite()
      throws Exception {
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
    long weight =
        com.red.ohc.storage.WriterArena.allocationWeight(allocation)
            + com.red.ohc.storage.WriterArena.allocationWeight(one.keyAllocationLength());
    Budget budget = new Budget(1 << 20);
    assertTrue(budget.tryReserve(weight * 2L, 0));
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            budget,
            Ticker.DEFAULT,
            weight + 1L,
            Eviction.S3_FIFO,
            new ReaderRegistry());
    try {
      invokeApplyEntry(loop, one);
      invokeApplyEntry(loop, two);
      assertTrue(one.claimWriter());
      assertTrue(two.claimWriter());
      loop.start();
      java.util.concurrent.CompletableFuture<Void> flush = loop.flush();
      Field retry = MaintenanceEventLoop.class.getDeclaredField("evictionRetryNanos");
      retry.setAccessible(true);
      long blockedDeadline = System.nanoTime() + 1_000_000_000L;
      while (retry.getLong(loop) == Long.MAX_VALUE
          && System.nanoTime() < blockedDeadline) {
        Thread.yield();
      }
      assertTrue(
          retry.getLong(loop) != Long.MAX_VALUE,
          "maintenance did not observe the locked eviction victim");

      one.finishWriter();
      two.finishWriter();
      flush.join();
      long deadline = System.nanoTime() + 1_000_000_000L;
      while (data.size() > 1 && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(
          data.size() <= 1,
          "a deferred eviction must re-arm itself after the writer mutex becomes available");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void evictionRetryDeadlineSuppressesARepeatedScanOnAnUnrelatedMaintenanceWake()
      throws Exception {
    FrozenTicker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    com.red.ohc.storage.WriterArena arena = memory.newWriterArena();
    long key = arena.allocate(8L);
    long value = arena.allocate(ValueBlock.allocationLength(1));
    NativeMemory.putLong(key, 121L);
    ValueBlock.initialize(value, 0L, 1);
    Entry entry = new Entry(key, 0, 121, value);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new Budget(1 << 20), ticker, 1L, Eviction.LRU, new ReaderRegistry());
    try {
      invokeApplyEntry(loop, entry);
      Field retry = MaintenanceEventLoop.class.getDeclaredField("evictionRetryNanos");
      retry.setAccessible(true);
      retry.setLong(loop, 1_000L);

      invokeMaintenancePass(loop);
      assertEquals(
          data.size(),
          1,
          "an unrelated wake must not bypass the eviction retry deadline");

      retry.setLong(loop, 0L);
      invokeMaintenancePass(loop);
      assertEquals(data.size(), 0, "eviction must resume once its retry deadline is due");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void successfulEvictionResetsBackoffEvenWhenMoreVictimsRemain() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena arena = memory.newWriterArena();
    long allocation = ValueBlock.allocationLength(1);
    long keyOne = arena.allocate(8L);
    long keyTwo = arena.allocate(8L);
    long valueOne = arena.allocate(allocation);
    long valueTwo = arena.allocate(allocation);
    NativeMemory.putLong(keyOne, 131L);
    NativeMemory.putLong(keyTwo, 132L);
    ValueBlock.initialize(valueOne, 0L, 1);
    ValueBlock.initialize(valueTwo, 0L, 1);
    Entry one = new Entry(keyOne, 0, 131, valueOne);
    Entry two = new Entry(keyTwo, 0, 132, valueTwo);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(one, one);
    data.put(two, two);
    long weight =
        com.red.ohc.storage.WriterArena.allocationWeight(allocation)
            + com.red.ohc.storage.WriterArena.allocationWeight(one.keyAllocationLength());
    Budget budget = new Budget(1 << 20);
    assertTrue(budget.tryReserve(weight * 2L, 0));
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, budget, new FrozenTicker(), 0L, Eviction.LRU, new ReaderRegistry());
    try {
      invokeApplyEntry(loop, one);
      invokeApplyEntry(loop, two);
      assertEquals(((MaintenancePolicy) getField(loop, "policy")).usedWeight(), weight * 2L);
      setLongField(loop, "evictionRetryNanos", 0L);
      setLongField(loop, "evictionRetryBackoffNanos", 10_000_000L);

      invokeEvictIfNeeded(loop, 1);
      assertEquals(data.size(), 1, "one bounded pass must leave another victim under pressure");
      assertEquals(getLongField(loop, "evictionRetryNanos"), Long.MAX_VALUE);
      assertEquals(getLongField(loop, "evictionRetryBackoffNanos"), 1_000_000L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void evictionRetryBackoffStartsAtOneMillisecondAndCapsAtTenMillis() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, new Budget(1 << 20), new FrozenTicker(), 1L, Eviction.LRU, new ReaderRegistry());
    try {
      invokeScheduleEvictionRetry(loop);
      assertEquals(
          getLongField(loop, "evictionRetryNanos"),
          1_000_000L,
          "the first blocked eviction retry must wait one maintenance window");

      for (int retry = 1; retry < 9; retry++) {
        invokeScheduleEvictionRetry(loop);
      }
      assertEquals(
          getLongField(loop, "evictionRetryNanos"),
          10_000_000L,
          "blocked eviction retries must cap at 10 milliseconds");
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void idleLoopParksInsteadOfBusySpinning() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
  public void terminalFailureWithPendingReservationStopsAndFreesTheActor() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    Entry entry = new Entry(0L, 0, 126, 0L);
    try {
      loop.prepareReliableRemoval(context, entry);
      loop.start();
      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      loop.stop();
      loop.join(1_000L);

      assertTrue(!loop.isAlive(), "terminal failure during close must not retry pending work");
      assertEquals(memory.allocated(), 0L);
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void failedActorRemovalCancelsTheUnusedActorRetirementBatch() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    ConcurrentHashMap<Entry, Entry> data = index();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data,
            memory,
            new Budget(16L << 20),
            Ticker.DEFAULT,
            16L << 20,
            Eviction.LRU,
            readers);
    try {
      Entry entry = new Entry(0L, 0, 127, 8L);
      data.put(entry, new Entry(0L, 0, 128, 0L));

      assertFalse(loop.removeFromMap(entry, false, entry.generation(), 8L));
      assertTrue(data.containsKey(entry), "a failed actor removal must preserve the mapping");
      assertFalse(
          ((RetirementQueue.Reservation) getField(loop, "actorRetirement")).active(),
          "a failed actor removal must not leave a partial actor reservation");
    } finally {
      activeReader.epoch = 0L;
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushBarrierDoesNotWaitForAsyncMutationsSubmittedAfterIt() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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
  public void flushWaitsForRetirementReclaimWhileAReaderIsActive() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
    try {
      loop.start();
      retireOne(loop, memory, budget, new ThreadContext(null));
      waitForRetiredEntries(loop, 1);

      CompletableFuture<Void> flush = loop.flush();
      Thread.sleep(100L);
      assertFalse(flush.isDone(), "flush must include pending QSBR retirement reclaim");

      activeReader.epoch = 0L;
      loop.readerQuiescent();
      flush.get(3, TimeUnit.SECONDS);
    } finally {
      activeReader.epoch = 0L;
      loop.stop();
      loop.join(1_000L);
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, budget, ticker, 1 << 20, Eviction.LRU, readers);
    try {
      retireOne(loop, memory, budget, new ThreadContext(null));
      invokeMaintenancePass(loop);

      nowNanos.set(1_000_000L);
      invokeMaintenancePass(loop);

      assertEquals(
          getLongField(loop, "reclaimRetryNanos"),
          2_000_000L,
          "the first blocked QSBR reclaim must wait one maintenance window before rescanning readers");
    } finally {
      activeReader.epoch = 0L;
      memory.closeArenas();
    }
  }

  @Test
  public void fullActorRetirementRingDoesNotBypassBlockedQsbrBackoff() throws Exception {
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, budget, ticker, 1 << 20, Eviction.LRU, readers);
    try {
      retireOne(loop, memory, budget, new ThreadContext(null));
      invokeMaintenancePass(loop);

      RetirementQueue queue = retirementQueue(loop);
      while (true) {
        RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
        if (!queue.reserve(reservation, 1)) {
          break;
        }
        queue.cancel(reservation);
      }
      assertFalse(
          queue.reserve(new RetirementQueue.Reservation(), 2),
          "the test must exhaust actor retirement capacity before checking recovery");

      assertFalse(
          invokePrepareActorRetirement(loop, 2),
          "capacity recovery must not rescan a QSBR-blocked registry before its deadline");
    } finally {
      activeReader.epoch = 0L;
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void terminalFailureRejectsQueuedAsyncMutationsBeforeClose() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
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

      assertTrue(
          pending.isCompletedExceptionally(),
          "terminal failure must complete queued async mutations exceptionally");
    } finally {
      releaseFirst.countDown();
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void terminalFailureCloseClearsPendingMutationQueues() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(),
            memory,
            new Budget(1 << 20),
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry());
    Entry entry = new Entry(0L, 0, 128, 0L);
    try {
      loop.publishMutation(entry, Entry.PENDING_ADD);
      @SuppressWarnings("unchecked")
      ConcurrentLinkedQueue<Entry>[] repairQueues =
          (ConcurrentLinkedQueue<Entry>[]) getField(loop, "repairQueues");
      for (ConcurrentLinkedQueue<Entry> repairQueue : repairQueues) {
        repairQueue.offer(entry);
      }
      @SuppressWarnings("unchecked")
      ArrayDeque<Entry> deferred = (ArrayDeque<Entry>) getField(loop, "deferredMutations");
      deferred.add(entry);

      loop.start();
      loop.recordTerminalFailure(new IllegalStateException("maintenance boom"));
      loop.stop();
      loop.join(1_000L);

      assertTrue(((MpscArrayQueue<?>) getField(loop, "queue")).isEmpty());
      for (ConcurrentLinkedQueue<Entry> repairQueue : repairQueues) {
        assertTrue(repairQueue.isEmpty());
      }
      assertTrue(deferred.isEmpty());
    } finally {
      if (loop.isAlive()) {
        loop.stop();
        loop.join(1_000L);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void retirementBurstsDoNotAdvanceTheReaderEpochForEverySingleRecord() throws Exception {
    Ticker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, budget, ticker, 1 << 20, Eviction.LRU, readers);
    ThreadContext context = new ThreadContext(null);
    loop.start();
    try {
      retireOne(loop, memory, budget, context);
      waitForEpoch(loop, 2L);
      long firstEpoch = loop.epoch();
      waitForRetiredEntries(loop, 1);

      retireOne(loop, memory, budget, context);
      waitForRetiredEntries(loop, 2);

      assertEquals(
          loop.epoch(),
          firstEpoch,
          "a second record in the same actor time window must not force every reader to retry its"
              + " epoch enter");
    } finally {
      activeReader.epoch = 0L;
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 2_000L)
  public void ordinaryReaderQuiescenceUsesTheEpochDeadlineInsteadOfAnImmediateWake()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot reader = new ReaderSlot();
    reader.epoch = 1L;
    readers.register(reader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, readers);
    ThreadContext context = new ThreadContext(null);
    loop.start();
    try {
      retireOne(loop, memory, budget, context);
      waitForRetiredEntries(loop, 1);

      reader.epoch = 0L;
      loop.readerQuiescent();

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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(16L << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 16L << 20, Eviction.LRU, readers);
    RetirementQueue retirements = retirementQueue(loop);
    RetirementQueue.Reservation[] reservations =
        new RetirementQueue.Reservation[(int) retirements.capacityRecords()];
    try {
      for (int index = 0; index < reservations.length; index++) {
        RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
        assertTrue(retirements.reserve(reservation, 1), "retirement ring must be fillable");
        retirements.append(reservation, 0L, 0L);
        reservations[index] = reservation;
      }
      ThreadContext context = new ThreadContext(null);
      assertFalse(loop.prepareRetirement(context, 1), "a full retirement ring must fail fast");

      loop.start();
      waitUntilParked(loop);
      activeReader.epoch = 0L;

      loop.requestMaintenance();
      loop.flush().join();
      assertTrue(loop.prepareRetirement(context, 1));
      loop.retireValue(context, 0L, 0L);
      assertFalse(context.retirement().active(), "the one-record reservation must be published");
    } finally {
      activeReader.epoch = 0L;
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void workerDoesNotParkWithMoreThanOneReclaimBatchStillPending() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(16L << 20);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 16L << 20, Eviction.LRU, new ReaderRegistry());
    RetirementQueue retirements = retirementQueue(loop);
    long allocation = ValueBlock.allocationLength(1);
    int recordCount = 2_048;
    try {
      for (int index = 0; index < recordCount; index++) {
        RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
        assertTrue(retirements.reserve(reservation, 1));
        assertTrue(
            budget.tryReserve(
                com.red.ohc.storage.WriterArena.allocationWeight(allocation), 0));
        long address = memory.newWriterArena().allocate(allocation);
        ValueBlock.initialize(address, 0L, 1);
        retirements.append(reservation, address, allocation);
      }
      loop.afterWrite();
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
              + loop.snapshot().retiredEntries
              + "/"
              + loop.snapshot().retirementQueueDepth
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());
    ThreadContext context = new ThreadContext(null);
    try {
      loop.start();
      waitUntilParked(loop);
      loop.afterWrite(context);
      waitUntilParked(loop);

      long allocation = ValueBlock.allocationLength(1);
      assertTrue(
          budget.tryReserve(
              com.red.ohc.storage.WriterArena.allocationWeight(allocation), 0));
      long value = memory.newWriterArena().allocate(allocation);
      ValueBlock.initialize(value, 0L, 1);
      assertTrue(loop.prepareRetirement(context, 1));
      loop.retireValue(context, value, allocation);
      loop.afterWrite(context);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
      while (loop.retirementQueueDepth() != 0L && System.nanoTime() < deadline) {
        Thread.sleep(1L);
      }
      assertEquals(
          loop.retirementQueueDepth(),
          0L,
          "a retirement append must publish a maintenance work mask even when the actor is idle");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  private static RetirementQueue retirementQueue(MaintenanceEventLoop loop) throws Exception {
    Field field = MaintenanceEventLoop.class.getDeclaredField("retirements");
    field.setAccessible(true);
    return (RetirementQueue) field.get(loop);
  }

  private static void retireOne(
      MaintenanceEventLoop loop, NativeMemory.Memory memory, Budget budget, ThreadContext context) {
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, 0L, 1);
    assertTrue(
        budget.tryReserve(
            com.red.ohc.storage.WriterArena.allocationWeight(allocation), 0));
    assertTrue(loop.prepareRetirement(context, 1));
    loop.retireValue(context, value, allocation);
    loop.afterWrite();
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
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (loop.retiredEntries() < expected && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertTrue(
        loop.retiredEntries() >= expected, "maintenance actor did not retain expected records");
  }

  private static void waitForNoRetiredEntries(MaintenanceEventLoop loop) {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (loop.retiredEntries() != 0 && System.nanoTime() < deadline) {
      Thread.yield();
    }
    assertEquals(
        loop.retiredEntries(),
        0,
        "the bounded epoch deadline did not reclaim after reader quiescence");
  }

  private static void waitUntilParked(MaintenanceEventLoop loop) throws InterruptedException {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (!loop.isParked() && System.nanoTime() < deadline) {
      Thread.sleep(1L);
    }
    assertTrue(loop.isParked(), "maintenance actor did not park");
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

  private static long getLongField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.getLong(target);
  }

  private static void setLongField(Object target, String name, long value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.setLong(target, value);
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

  private static int invokeMaintenancePassWork(MaintenanceEventLoop loop) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("maintenancePass");
    method.setAccessible(true);
    return (Integer) method.invoke(loop);
  }

  private static int invokeDrainAsyncMutations(MaintenanceEventLoop loop, int limit)
      throws Exception {
    Method method =
        MaintenanceEventLoop.class.getDeclaredMethod("drainAsyncMutations", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(loop, limit);
  }

  private static void invokeScheduleEvictionRetry(MaintenanceEventLoop loop) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("scheduleEvictionRetry");
    method.setAccessible(true);
    method.invoke(loop);
  }

  private static boolean invokePrepareActorRetirement(MaintenanceEventLoop loop, int records)
      throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("prepareActorRetirement", int.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(loop, records);
  }

  private static int invokeEvictIfNeeded(MaintenanceEventLoop loop, int limit) throws Exception {
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("evictIfNeeded", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(loop, limit);
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

  private static final class CountingQueue<E> extends ConcurrentLinkedQueue<E> {
    final AtomicInteger emptyChecks = new AtomicInteger();

    @Override
    public boolean isEmpty() {
      emptyChecks.incrementAndGet();
      return super.isEmpty();
    }
  }

  private static final class SizeForbiddenQueue<E> extends ConcurrentLinkedQueue<E> {
    @Override
    public int size() {
      throw new AssertionError("queue traversal is forbidden");
    }
  }

  private static final class BlockingOfferQueue<E> extends ConcurrentLinkedQueue<E> {
    final CountDownLatch offerEntered = new CountDownLatch(1);
    final CountDownLatch releaseOffer = new CountDownLatch(1);

    @Override
    public boolean offer(E element) {
      offerEntered.countDown();
      await(releaseOffer);
      return super.offer(element);
    }
  }

  private static final class ThrowingOfferQueue<E> extends ConcurrentLinkedQueue<E> {
    @Override
    public boolean offer(E element) {
      throw new OutOfMemoryError("injected async queue offer failure");
    }
  }

  private static final class PeekBlockingQueue<E> extends ConcurrentLinkedQueue<E> {
    final CountDownLatch peekEntered = new CountDownLatch(1);
    final CountDownLatch releasePeek = new CountDownLatch(1);
    final AtomicBoolean blockFirstPeek = new AtomicBoolean(true);

    @Override
    public E peek() {
      E element = super.peek();
      if (element != null && blockFirstPeek.compareAndSet(true, false)) {
        peekEntered.countDown();
        await(releasePeek);
      }
      return element;
    }
  }

  private static ConcurrentHashMap<Entry, Entry> index() {
    return new ConcurrentHashMap<>();
  }
}
