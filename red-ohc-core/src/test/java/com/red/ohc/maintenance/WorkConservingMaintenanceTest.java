package com.red.ohc.maintenance;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.NativeMemory;

public final class WorkConservingMaintenanceTest {
  @Test
  public void sourceCutsReplaceFixedBudgetsAndDynamicBatchState() throws Exception {
    assertMissingField(MaintenanceEventLoop.class, "LIFECYCLE_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "MUTATION_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "ACCESS_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "TTL_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "RETIREMENT_BUDGET");
    assertMissingField(MaintenanceEventLoop.class, "ASYNC_BUDGET");
    assertMissingNestedClass(MaintenanceEventLoop.class, "TurnSnapshot");
    assertTrue(MaintenanceEventLoop.class.getDeclaredField("turnCuts") != null);
    assertMissingField(MaintenanceEventLoop.class, "PASS_COOLDOWN_NANOS");
    assertMissingField(MaintenanceEventLoop.class, "CONTINUATION_PAUSE_NANOS");
    assertMissingField(MaintenanceEventLoop.class, "maintenanceBatchFactor");
    assertMissingField(MaintenanceEventLoop.class, "windowScheduler");
  }

  @Test
  public void lifecycleWriteHasExplicitRemovalOperation() throws Exception {
    Method removal =
        WriterLifecycleLane.class.getDeclaredMethod(
            "writeRemoval", long.class, com.red.ohc.index.Entry.class, long.class,
            long.class, long.class, com.red.ohc.api.RemovalCause.class);
    assertTrue(Modifier.isPublic(removal.getModifiers()));
    assertMissingMethod(WriterLifecycleLane.class, "writeReplacement", long.class, long.class, long.class);
  }

  @Test
  public void lifecycleReadySignalUsesExclusiveResourceLanesWithoutAnUnconsumedQueue()
      throws Exception {
    assertMissingField(WriterLifecycleJournal.class, "readyLanes");
    assertMissingField(WriterLifecycleJournal.class, "currentLane");
    assertMissingField(WriterLifecycleJournal.class, "laneMask");
  }

  @Test
  public void statsExposeActorActivitySnapshotsInsteadOfPressureControls() throws Exception {
    Class<?> stats = com.red.ohc.api.OHCacheStats.class;
    assertTrue(Modifier.isPrivate(stats.getDeclaredField("maintenanceActiveNanosTotal").getModifiers()));
    assertTrue(Modifier.isPrivate(stats.getDeclaredField("maintenanceParkNanosTotal").getModifiers()));
    assertTrue(
        Modifier.isPrivate(
            stats.getDeclaredField("maintenanceImmediateContinuationCount").getModifiers()));
    assertMissingField(stats, "maintenanceBatchFactor");
    assertMissingField(stats, "maintenancePressurePermille");
  }

  @Test
  public void reservationHoleIsNotReportedAsRunnableActorWork() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<Entry, Entry>(),
            memory,
            Ticker.DEFAULT,
            1L << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    try {
      loop.bindWriterLifecycleJournal(journal);
      WriterLifecycleLane lane = journal.lane(0);
      lane.reserve();
      long committedBehindHole = lane.reserve();
      lane.writeRemoval(committedBehindHole, null, 0L, 0L, 0L, null);
      lane.commit(committedBehindHole);

      Method method = MaintenanceEventLoop.class.getDeclaredMethod("hasRunnableWork");
      method.setAccessible(true);
      assertTrue(!(Boolean) method.invoke(loop), "a reservation hole must not cause actor spin");
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  private static void assertMissingField(Class<?> type, String name) {
    try {
      type.getDeclaredField(name);
      fail("obsolete field must be removed: " + type.getName() + "." + name);
    } catch (NoSuchFieldException expected) {
      // Expected after the work-conserving refactor.
    }
  }

  private static void assertMissingNestedClass(Class<?> type, String name) {
    for (Class<?> nested : type.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(name)) {
        fail("obsolete nested class must be removed: " + type.getName() + "." + name);
      }
    }
  }

  private static void assertMissingMethod(Class<?> type, String name, Class<?>... parameterTypes) {
    try {
      type.getDeclaredMethod(name, parameterTypes);
      fail("obsolete method must be removed: " + type.getName() + "." + name);
    } catch (NoSuchMethodException expected) {
      // Expected after the explicit lifecycle operation split.
    }
  }
}
