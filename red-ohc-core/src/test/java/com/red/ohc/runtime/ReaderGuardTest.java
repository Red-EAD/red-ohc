package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.storage.NativeMemory;

public class ReaderGuardTest {
  private static MaintenanceEventLoop loop(
      NativeMemory.Memory memory, ReaderRegistry readers) {
    return new MaintenanceEventLoop(
        new ConcurrentHashMap<>(),
        memory,
        Ticker.DEFAULT,
        1 << 20,
        Eviction.LRU,
        readers,
        Long.MAX_VALUE);
  }

  @Test
  public void enterPublishesAnOddSequenceAndExitQuiescesTheReader() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderGuard guard = new ReaderGuard(loop(memory, readers));
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      long seq = readers.readerSequence(context.slot);
      assertTrue((seq & 1L) != 0L);
      assertTrue((seq & ReaderRegistry.VALUE_PROTECTION_BIT) != 0L);
      guard.exit(context);
      long exited = readers.readerSequence(context.slot);
      assertEquals(exited, (seq & Long.MAX_VALUE) + 1L);
      assertEquals(exited & 1L, 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void lookupEntryPublishesOddWithoutTheValueBit() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderGuard guard = new ReaderGuard(loop(memory, readers));
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enterLookupAfterAdmission(context));
      long seq = readers.readerSequence(context.slot);
      assertTrue((seq & 1L) != 0L);
      assertEquals(seq & ReaderRegistry.VALUE_PROTECTION_BIT, 0L);
      guard.exit(context);
      assertEquals(readers.readerSequence(context.slot) & 1L, 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closeRacingAfterRegistrationRejectsTheReaderBeforeNativeDereference() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop = loop(memory, readers);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      loop.beginClosing();
      readers.clear();
      assertFalse(guard.enter(context));
      assertEquals(context.readerDepth(), 0);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void firstReadRacingRegistryCloseReturnsMissWithoutLeakingAnInternalException() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop = loop(memory, readers);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      loop.beginClosing();
      readers.clear();
      assertFalse(guard.enter(context));
      assertFalse(context.isRegistered());
      assertEquals(context.readerDepth(), 0);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void nestedReadersReuseOneOddSequenceAndExitOnlyAtTheOuterBoundary() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderGuard guard = new ReaderGuard(loop(memory, readers));
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      long seq = readers.readerSequence(context.slot);
      assertTrue(guard.enter(context));
      assertEquals(readers.readerSequence(context.slot), seq);
      guard.exit(context);
      assertEquals(readers.readerSequence(context.slot), seq);
      guard.exit(context);
      assertEquals(readers.readerSequence(context.slot) & 1L, 0L);
      assertEquals(context.readerDepth(), 0);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void nestedValueGuardUpgradesTheBitAndKeepsItUntilTheOuterExit() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderGuard guard = new ReaderGuard(loop(memory, readers));
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enterLookupAfterAdmission(context));
      long seq = readers.readerSequence(context.slot);
      assertTrue((seq & 1L) != 0L);
      assertEquals(seq & ReaderRegistry.VALUE_PROTECTION_BIT, 0L);

      assertTrue(guard.enter(context));
      assertEquals(
          readers.readerSequence(context.slot),
          seq | ReaderRegistry.VALUE_PROTECTION_BIT);
      guard.exit(context);
      // The value bit deliberately persists until the depth-0 exit: a mid-stack downgrade is a
      // no-op so the actor never observes an unprotecting store inside one op.
      assertEquals(
          readers.readerSequence(context.slot),
          seq | ReaderRegistry.VALUE_PROTECTION_BIT);

      guard.exit(context);
      assertEquals(readers.readerSequence(context.slot) & 1L, 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void readerExitSignalsAReclaimBlockedActor() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop = loop(memory, readers);
    Field requestedWorkField = MaintenanceEventLoop.class.getDeclaredField("requestedWork");
    requestedWorkField.setAccessible(true);
    AtomicInteger requestedWork = (AtomicInteger) requestedWorkField.get(loop);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      requestedWork.set(0);
      readers.armReaderQuiescence(new long[readers.slotCapacity()]);
      guard.exit(context);
      assertTrue(
          requestedWork.get() != 0,
          "marked reader exit must publish one reclaim wake without reclaiming on the reader thread");
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }
}
