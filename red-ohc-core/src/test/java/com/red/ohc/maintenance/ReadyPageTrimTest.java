package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

public final class ReadyPageTrimTest {
  @Test
  public void overCapacityReturnsEmptyReadyPagesToTheAllocator() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try (Fixture fixture = new Fixture(memory, 1 << 10)) {
      assertEquals(memory.pageReadyCount(), 1L);
      assertTrue(memory.allocated() > (1 << 10), "the fixture must be over capacity");

      fixture.trim();

      assertEquals(memory.pageTrimmedCount(), 1L);
      assertEquals(memory.pageReadyCount(), 0L);
      assertTrue(memory.trimmedBytesTotal() > 0L);

      long after = fixture.arena.allocate(112L);
      assertTrue(after != 0L, "allocation must work after the trim");
      memory.releaseEntry(after, 112L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void underCapacityKeepsReadyPagesAsAllocationHeadroom() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
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
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      ReaderRegistry readers = new ReaderRegistry(memory);
      MaintenanceEventLoop loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(), memory, Ticker.DEFAULT, 1 << 10,
              Eviction.LRU, readers, Long.MAX_VALUE);
      loop.bindLogicalAdmission(new LogicalAdmission(1 << 10, false));
      WriterArena arena = memory.newWriterArena();
      long first = arena.allocate(32_700L);
      arena.allocate(32_700L);
      arena.detach();
      memory.releaseEntry(first, 32_700L);
      assertEquals(memory.pageReadyCount(), 1L);

      Method trim =
          MaintenanceEventLoop.class.getDeclaredMethod("trimReadyPagesWhenOverCapacity");
      trim.setAccessible(true);
      trim.invoke(loop);

      assertEquals(memory.pageTrimmedCount(), 0L);
      assertEquals(memory.pageReadyCount(), 1L);

      loop.stop();
      if (loop.thread().getState() == Thread.State.NEW) {
        loop.start();
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
      ReaderRegistry readers = new ReaderRegistry(memory);
      loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(), memory, Ticker.DEFAULT, capacity,
              Eviction.LRU, readers, Long.MAX_VALUE);
      loop.bindLogicalAdmission(new LogicalAdmission(capacity, false));
      arena = memory.newWriterArena();
      long first = arena.allocate(32_700L);
      long second = arena.allocate(32_700L);
      arena.detach();
      memory.releaseEntry(first, 32_700L);
      memory.releaseEntry(second, 32_700L);
      trim = MaintenanceEventLoop.class.getDeclaredMethod("trimReadyPagesWhenOverCapacity");
      trim.setAccessible(true);
    }

    void trim() throws Exception {
      trim.invoke(loop);
    }

    @Override
    public void close() throws Exception {
      loop.stop();
      if (loop.thread().getState() == Thread.State.NEW) {
        loop.start();
      }
      loop.join(2_000L);
    }
  }
}
