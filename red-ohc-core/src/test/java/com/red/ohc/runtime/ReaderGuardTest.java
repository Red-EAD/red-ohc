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
  @Test
  public void enterPublishesAnEpochAndExitQuiescesTheReader() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      long epoch = context.readerPublishedEpoch();
      assertTrue(epoch != 0L);
      assertEquals(
          readers.readerState(context.readerSlotIndex()),
          ReaderRegistry.VALUE_PROTECTION_BIT | epoch);
      guard.exit(context);
      assertEquals(context.readerPublishedEpoch(), 0L);
      assertEquals(readers.readerState(context.readerSlotIndex()), 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void admittedReaderUsesTheSamePublishedEpochAndQuiescesOnExit() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers,
            Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enterAfterAdmission(context));
      long epoch = context.readerPublishedEpoch();
      assertTrue(epoch != 0L);
      assertEquals(
          readers.readerState(context.readerSlotIndex()),
          ReaderRegistry.VALUE_PROTECTION_BIT | epoch);
      guard.exit(context);
      assertEquals(readers.readerState(context.readerSlotIndex()), 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closeRacingAfterEpochPublicationRejectsTheReaderBeforeItCanDereferenceNativeMemory() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      // Close is quiesced-only now: a reader rejected by an unbinding registry must fail
      // cleanly instead of dereferencing freed native memory.
      loop.beginClosing();
      readers.clear();
      assertFalse(guard.enter(context));
      assertEquals(context.readerPublishedEpoch(), 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void firstReadRacingRegistryCloseReturnsMissInsteadOfLeakingAnInternalException() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers,
            Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      loop.beginClosing();
      readers.clear();
      assertFalse(guard.enter(context));
      assertFalse(context.isRegistered());
      assertEquals(context.readerPublishedEpoch(), 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void nestedReadersReuseOneEpochAndExitOnlyAtTheOuterBoundary() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      long epoch = context.readerPublishedEpoch();
      assertTrue(guard.enter(context));
      assertEquals(context.readerPublishedEpoch(), epoch);
      guard.exit(context);
      assertEquals(context.readerPublishedEpoch(), epoch);
      guard.exit(context);
      assertEquals(context.readerPublishedEpoch(), 0L);
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void nestedValueGuardUpgradesAndThenRestoresLookupOnlyProtection() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enterLookupAfterAdmission(context));
      long epoch = context.readerPublishedEpoch();
      assertTrue(epoch != 0L);
      assertEquals(readers.readerState(context.readerSlotIndex()), epoch);

      assertTrue(guard.enter(context));
      assertEquals(
          readers.readerState(context.readerSlotIndex()),
          ReaderRegistry.VALUE_PROTECTION_BIT | epoch);
      guard.exit(context);
      assertEquals(context.readerPublishedEpoch(), epoch);
      assertEquals(readers.readerState(context.readerSlotIndex()), epoch);

      guard.exit(context);
      assertEquals(context.readerPublishedEpoch(), 0L);
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
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    Field requestedWorkField = MaintenanceEventLoop.class.getDeclaredField("requestedWork");
    requestedWorkField.setAccessible(true);
    AtomicInteger requestedWork = (AtomicInteger) requestedWorkField.get(loop);
    ReaderGuard guard = new ReaderGuard(loop);
    ThreadContext context = new ThreadContext(null);
    try {
      assertTrue(guard.enter(context));
      requestedWork.set(0);
      readers.markActiveReaderNotifications();
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
