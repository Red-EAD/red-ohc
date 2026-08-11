package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jctools.queues.MpscArrayQueue;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.AccessConsumer;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class MaintenanceEventLoopTest {
  @Test
  public void removalNotificationObservesValueBeforeNativeRetirement() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE, 64L << 20);
    Budget budget = new Budget(1L << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();
    WriterArena arena = memory.newWriterArena();
    ThreadContext context = new ThreadContext(arena, lease);
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
      assertTrue(budget.tryReserve(lease, weight));
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
    int constructorCalls = ticker.calls.get();
    loop.start();
    try {
      waitUntilParked(loop);
      Thread.sleep(20L);
      assertEquals(
          ticker.calls.get(),
          constructorCalls,
          "an idle worker must not sample a clock just to decide to park");
    } finally {
      loop.stop();
      loop.join(1_000L);
      memory.closeArenas();
    }
  }

  @Test
  public void batchGraceBeforeTheWheelDeadlineDoesNotRequestAClockRefresh() throws Exception {
    FrozenTicker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> data = index();
    long allocation = ValueBlock.allocationLength(1);
    long value = memory.newWriterArena().allocate(allocation);
    ValueBlock.initialize(value, 10_000L, 1);
    Entry entry =
        new Entry(0L, 0, 92, 0L, Entry.tagValueAddress(value, true));
    data.put(entry, entry);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            data, memory, new Budget(1 << 20), ticker, 1 << 20, Eviction.LRU, new ReaderRegistry());
    try {
      invokeApplyEntry(loop, entry);
      Method park =
          MaintenanceEventLoop.class.getDeclaredMethod("parkUntilWorkOrTimer", boolean.class);
      park.setAccessible(true);
      park.invoke(loop, true);

      Field refresh = MaintenanceEventLoop.class.getDeclaredField("clockRefreshRequested");
      refresh.setAccessible(true);
      assertFalse(
          refresh.getBoolean(loop),
          "the 1ms batching grace ended before the real TTL deadline");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void batchGraceEndingAtADeferredMutationDeadlineRequestsAClockRefresh() throws Exception {
    FrozenTicker ticker = new FrozenTicker();
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
    try {
      Field retry = MaintenanceEventLoop.class.getDeclaredField("deferredMutationRetryNanos");
      retry.setAccessible(true);
      retry.setLong(loop, 1L);

      Method park =
          MaintenanceEventLoop.class.getDeclaredMethod("parkUntilWorkOrTimer", boolean.class);
      park.setAccessible(true);
      park.invoke(loop, true);

      Field refresh = MaintenanceEventLoop.class.getDeclaredField("clockRefreshRequested");
      refresh.setAccessible(true);
      assertTrue(
          refresh.getBoolean(loop),
          "a real non-wheel deadline must refresh the actor clock after waking");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void idleBudgetLeasesAreReclaimedOnlyAfterBudgetPressure() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(128L);
    Budget.Lease first = budget.leaseForCurrentThread();
    Budget.Lease second = budget.leaseForCurrentThread();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 128L, Eviction.LRU, new ReaderRegistry());
    try {
      assertTrue(budget.tryReserve(first, 1L));
      assertFalse(
          budget.tryReserve(second, 64L),
          "the first lease must retain the refill credit before maintenance runs");

      invokeMaintenancePass(loop);
      assertFalse(
          budget.tryReserve(second, 64L),
          "an ordinary maintenance pass must not scan and reclaim idle writer leases");

      Method request =
          MaintenanceEventLoop.class.getDeclaredMethod("requestBudgetPressure");
      request.invoke(loop);
      invokeMaintenancePass(loop);
      assertTrue(
          budget.tryReserve(second, 64L),
          "budget pressure must reclaim idle writer credit for a later reservation");
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
    ThreadContext context = new ThreadContext(null, null);
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
    ThreadContext context = new ThreadContext(null, null);
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
    ThreadContext context = new ThreadContext(null, null);
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
  public void cancelingARemovalReservationSignalsAparkedWorker() throws Exception {
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
    ThreadContext context = new ThreadContext(null, null);
    Entry entry = new Entry(0L, 0, 125, 0L);
    try {
      loop.start();
      waitUntilParked(loop);
      long idleGeneration = idleGeneration(loop);
      loop.prepareReliableRemoval(context, entry);
      loop.cancelReliableRemoval(context, entry);

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
      while (idleGeneration(loop) == idleGeneration && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(
          idleGeneration(loop) > idleGeneration,
          "canceling a parked removal reservation must signal the maintenance worker");
    } finally {
      if (context.reliableRemoval.active()) {
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
    assertTrue(budget.tryReserve(budget.leaseForCurrentThread(), weight * 2L));
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
    ThreadContext context = new ThreadContext(null, null);
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
      retireOne(loop, memory, budget, new ThreadContext(null, null));
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
    FrozenTicker ticker = new FrozenTicker();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot activeReader = new ReaderSlot();
    activeReader.epoch = 1L;
    readers.register(activeReader);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(index(), memory, budget, ticker, 1 << 20, Eviction.LRU, readers);
    ThreadContext context = new ThreadContext(null, null);
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
    ThreadContext context = new ThreadContext(null, null);
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
    Budget.Lease lease = budget.leaseForCurrentThread();
    RetirementQueue.Reservation[] reservations =
        new RetirementQueue.Reservation[(int) retirements.capacityRecords()];
    try {
      for (int index = 0; index < reservations.length; index++) {
        RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
        assertTrue(retirements.reserve(reservation, 1), "retirement ring must be fillable");
        retirements.append(reservation, 0L, 0L);
        reservations[index] = reservation;
      }
      ThreadContext context = new ThreadContext(null, null);
      assertFalse(loop.prepareRetirement(context, 1), "a full retirement ring must fail fast");

      loop.start();
      waitUntilParked(loop);
      activeReader.epoch = 0L;

      loop.requestMaintenance();
      loop.flush().join();
      assertTrue(loop.prepareRetirement(context, 1));
      loop.retireValue(context, 0L, 0L);
      assertFalse(context.retirement.active(), "the one-record reservation must be published");
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
    Budget.Lease lease = budget.leaseForCurrentThread();
    long allocation = ValueBlock.allocationLength(1);
    int recordCount = 2_048;
    try {
      for (int index = 0; index < recordCount; index++) {
        RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
        assertTrue(retirements.reserve(reservation, 1));
        assertTrue(
            budget.tryReserve(
                lease, com.red.ohc.storage.WriterArena.allocationWeight(allocation)));
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
  public void retirementPublicationSignalsParkedWorkerEvenAfterThreadGenerationWasSignaled()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Budget budget = new Budget(1 << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            index(), memory, budget, Ticker.DEFAULT, 1 << 20, Eviction.LRU, new ReaderRegistry());
    ThreadContext context = new ThreadContext(null, null);
    try {
      loop.start();
      waitUntilParked(loop);
      Field generation = MaintenanceEventLoop.class.getDeclaredField("idleGeneration");
      generation.setAccessible(true);
      long idleGeneration = generation.getLong(loop);
      loop.afterWrite(context);
      waitUntilParked(loop);
      idleGeneration = generation.getLong(loop);
      assertFalse(context.needsMaintenanceWake(idleGeneration));

      long allocation = ValueBlock.allocationLength(1);
      assertTrue(
          budget.tryReserve(
              lease, com.red.ohc.storage.WriterArena.allocationWeight(allocation)));
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
          "a retirement append must not rely on a second producer signal in the same idle"
              + " generation");
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
            budget.leaseForCurrentThread(),
            com.red.ohc.storage.WriterArena.allocationWeight(allocation)));
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

  private static long idleGeneration(MaintenanceEventLoop loop) throws Exception {
    Field field = MaintenanceEventLoop.class.getDeclaredField("idleGeneration");
    field.setAccessible(true);
    return field.getLong(loop);
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
    Method method = MaintenanceEventLoop.class.getDeclaredMethod("maintenancePass");
    method.setAccessible(true);
    method.invoke(loop);
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

  private static final class CountingTicker implements Ticker {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public long nanos() {
      calls.incrementAndGet();
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      calls.incrementAndGet();
      return System.currentTimeMillis();
    }
  }

  private static ConcurrentHashMap<Entry, Entry> index() {
    return new ConcurrentHashMap<>();
  }
}
