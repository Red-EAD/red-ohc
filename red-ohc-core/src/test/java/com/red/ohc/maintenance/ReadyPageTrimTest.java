package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

public final class ReadyPageTrimTest {
  @Test
  public void idleTrimRunsWithoutRegisteredReadersOrOtherWork() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    AtomicLong now = new AtomicLong(1L);
    try (Fixture fixture = new Fixture(memory, 1 << 26, ticker(now))) {
      WriterArena[] arenas = new WriterArena[10];
      long[] values = new long[arenas.length];
      for (int index = 0; index < arenas.length; index++) {
        arenas[index] = memory.newWriterArena();
        values[index] = arenas[index].allocate(32_700L);
      }
      for (int index = 0; index < arenas.length; index++) {
        arenas[index].detach();
        memory.releaseEntry(values[index], 32_700L);
      }
      assertEquals(memory.pageReadyCount(), 10L);
      fixture.trim();
      assertFalse(hasRunnableWork(fixture.loop));

      now.set(30_000_000_001L);
      assertTrue(hasRunnableWork(fixture.loop), "idle trim needs its own runnable source");
      Method capture =
          MaintenanceEventLoop.class.getDeclaredMethod("captureWorkDecision", boolean.class);
      capture.setAccessible(true);
      Object decision = capture.invoke(fixture.loop, false);
      Field planField = decision.getClass().getDeclaredField("plan");
      planField.setAccessible(true);
      Object plan = planField.get(decision);
      Method actions = plan.getClass().getDeclaredMethod("hasActions");
      actions.setAccessible(true);
      assertTrue((Boolean) actions.invoke(plan), "the actor must execute the due trim pass");
      Method pass = MaintenanceEventLoop.class.getDeclaredMethod("maintenancePass");
      pass.setAccessible(true);
      pass.invoke(fixture.loop);
      assertEquals(memory.pageTrimmedCount(), 4L);
      assertEquals(memory.pageReadyCount(), 6L);
      assertFalse(hasRunnableWork(fixture.loop), "the scan must rearm its interval");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void idleTrimWakePrecedesAFarFutureTtl() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    AtomicLong now = new AtomicLong(1L);
    try (Fixture fixture = new Fixture(memory, 1 << 26, ticker(now))) {
      Entry entry = EntryTestSupport.entry(memory, 0, 81, 0L);
      Field wheelField = MaintenanceEventLoop.class.getDeclaredField("wheel");
      wheelField.setAccessible(true);
      ((TimerWheel) wheelField.get(fixture.loop)).add(entry, 600_000_000_000L);
      fixture.trim();
      Field deadlineField = MaintenanceEventLoop.class.getDeclaredField("parkDeadlineNanos");
      deadlineField.setAccessible(true);
      long start = System.nanoTime();
      MaintenanceTestSupport.start(fixture.loop);
      long deadline = 0L;
      while (deadline == 0L && System.nanoTime() - start < 2_000_000_000L) {
        deadline = deadlineField.getLong(fixture.loop);
        LockSupport.parkNanos(1_000_000L);
      }
      assertTrue(deadline > 0L, "actor must park with a deadline");
      assertTrue(
          deadline - start < 2_000_000_000L,
          "ready-page scan must not wait for the far-future TTL");
    } finally {
      memory.closeArenas();
    }
  }

  private static boolean hasRunnableWork(MaintenanceEventLoop loop) throws Exception {
    Field sampled = MaintenanceEventLoop.class.getDeclaredField("monotonicSampledThisPass");
    sampled.setAccessible(true);
    sampled.setBoolean(loop, false);
    Method runnable = MaintenanceEventLoop.class.getDeclaredMethod("hasRunnableWork");
    runnable.setAccessible(true);
    return (Boolean) runnable.invoke(loop);
  }

  private static Ticker ticker(AtomicLong now) {
    return new Ticker() {
      @Override
      public long nanos() {
        return now.get();
      }

      @Override
      public long currentTimeMillis() {
        return now.get() / 1_000_000L;
      }
    };
  }

  @Test
  public void overCapacityKeepsWarmEmptyReadyPages() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try (Fixture fixture = new Fixture(memory, 1 << 10)) {
      assertEquals(memory.pageReadyCount(), 1L);
      assertTrue(memory.allocated() > (1 << 10), "the fixture must be over capacity");

      fixture.trim();

      assertEquals(memory.pageTrimmedCount(), 0L);
      assertEquals(memory.pageReadyCount(), 1L);
      assertEquals(memory.trimmedBytesTotal(), 0L);

      long after = fixture.arena.allocate(112L);
      assertTrue(after != 0L, "allocation must work after the trim");
      memory.releaseEntry(after, 112L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void underCapacityKeepsReadyPagesAsAllocationHeadroom() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try (Fixture fixture = new Fixture(memory, 1 << 22)) {
      assertEquals(memory.pageReadyCount(), 1L);

      fixture.trim();

      assertEquals(memory.pageTrimmedCount(), 0L);
      assertEquals(memory.pageReadyCount(), 1L);
      assertEquals(memory.trimmedBytesTotal(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readyPagesWithLiveSlotsSurviveTheTrim() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      ReaderRegistry readers = new ReaderRegistry(memory);
      MaintenanceEventLoop loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(),
              memory,
              Ticker.DEFAULT,
              1 << 10,
              Eviction.LRU,
              readers,
              Long.MAX_VALUE);
      loop.bindLogicalAdmission(new LogicalAdmission(1 << 10, false));
      WriterArena arena = memory.newWriterArena();
      long first = arena.allocate(32_700L);
      arena.allocate(32_700L);
      arena.detach();
      memory.releaseEntry(first, 32_700L);
      assertEquals(memory.pageReadyCount(), 1L);

      Method trim = MaintenanceEventLoop.class.getDeclaredMethod("trimIdleReadyPages");
      trim.setAccessible(true);
      trim.invoke(loop);

      assertEquals(memory.pageTrimmedCount(), 0L);
      assertEquals(memory.pageReadyCount(), 1L);

      loop.stop();
      if (loop.thread().getState() == Thread.State.NEW) {
        MaintenanceTestSupport.start(loop);
      }
      loop.join(2_000L);
    } finally {
      memory.closeArenas();
    }
  }

  private static final class Fixture implements AutoCloseable {
    final WriterArena arena;
    final MaintenanceEventLoop loop;
    private final Method trim;

    Fixture(NativeMemory.Memory memory, long capacity) throws Exception {
      this(memory, capacity, Ticker.DEFAULT);
    }

    Fixture(NativeMemory.Memory memory, long capacity, Ticker ticker) throws Exception {
      ReaderRegistry readers = new ReaderRegistry(memory);
      loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(),
              memory,
              ticker,
              capacity,
              Eviction.LRU,
              readers,
              Long.MAX_VALUE);
      loop.bindLogicalAdmission(new LogicalAdmission(capacity, false));
      arena = memory.newWriterArena();
      long first = arena.allocate(32_700L);
      long second = arena.allocate(32_700L);
      arena.detach();
      memory.releaseEntry(first, 32_700L);
      memory.releaseEntry(second, 32_700L);
      trim = MaintenanceEventLoop.class.getDeclaredMethod("trimIdleReadyPages");
      trim.setAccessible(true);
    }

    void trim() throws Exception {
      trim.invoke(loop);
    }

    @Override
    public void close() throws Exception {
      loop.stop();
      if (loop.thread().getState() == Thread.State.NEW) {
        MaintenanceTestSupport.start(loop);
      }
      loop.join(2_000L);
    }
  }
}
