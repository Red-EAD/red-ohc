package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.WriterResource;
import com.red.ohc.runtime.WriterResourceRegistry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.SizeClasses;

public final class IdleArenaSealTest {
  @Test
  public void silentLifecycleLaneIsSealedAfterTheIdleWindow() throws Exception {
    MutableTicker ticker = new MutableTicker(0L);
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try (Fixture fixture = new Fixture(memory, ticker)) {
      publishOneRecord(fixture.resource);

      fixture.patrol();
      assertEquals(memory.retainedPagesInUse(), 1L, "the first sweep only records a baseline");

      ticker.millis = 20_000L;
      fixture.patrol();
      assertEquals(memory.retainedPagesInUse(), 1L, "20s of silence is inside the idle window");

      ticker.millis = 61_000L;
      fixture.patrol();
      assertEquals(memory.retainedPagesInUse(), 0L, "60s of silence must spill the retention");
      assertEquals(memory.pageReadyCount(), 1L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void activeLifecycleLaneIsNeverSealed() throws Exception {
    MutableTicker ticker = new MutableTicker(0L);
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try (Fixture fixture = new Fixture(memory, ticker)) {
      publishOneRecord(fixture.resource);

      for (long millis = 0L; millis <= 130_000L; millis += 30_000L) {
        ticker.millis = millis;
        publishOneRecord(fixture.resource);
        fixture.patrol();
      }
      assertEquals(memory.retainedPagesInUse(), 1L, "an active lane must keep its retention");
      assertEquals(memory.pageReadyCount(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  /** Publishes one cancelled lane record so the patrol sees the counter move without an entry. */
  private static void publishOneRecord(WriterResource resource) {
    WriterLifecycleLane lane = resource.lifecycleLane();
    lane.cancel(lane.reserve());
  }

  private static final class Fixture implements AutoCloseable {
    final WriterResource resource;
    final MaintenanceEventLoop loop;
    private final Method patrol;

    Fixture(NativeMemory.Memory memory, Ticker ticker) throws Exception {
      RetirementJournal retirementJournal = new RetirementJournal(memory);
      WriterResourceRegistry registry =
          new WriterResourceRegistry(memory, new WriterLifecycleJournal(), retirementJournal);
      resource = registry.acquire();
      long bytes = 112L;
      int slotsPerPage =
          SizeClasses.pageBytes(SizeClasses.indexForEntry(bytes))
              / SizeClasses.slotBytes(SizeClasses.indexForEntry(bytes));
      long[] firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        firstPageEntries[index] = resource.arena().allocate(bytes);
      }
      resource.arena().allocate(bytes);
      for (int index = 0; index < slotsPerPage / 2; index++) {
        memory.releaseEntry(firstPageEntries[index], bytes);
      }
      ReaderRegistry readers = new ReaderRegistry(memory);
      loop =
          new MaintenanceEventLoop(
              new ConcurrentHashMap<>(),
              memory,
              ticker,
              1 << 22,
              Eviction.LRU,
              readers,
              retirementJournal,
              Long.MAX_VALUE);
      loop.bindWriterResourceRegistry(registry);
      patrol = MaintenanceEventLoop.class.getDeclaredMethod("maintenancePass");
      patrol.setAccessible(true);
    }

    void patrol() throws Exception {
      patrol.invoke(loop);
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

  private static final class MutableTicker implements Ticker {
    volatile long millis;

    MutableTicker(long millis) {
      this.millis = millis;
    }

    @Override
    public long nanos() {
      return millis * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return millis;
    }
  }
}
