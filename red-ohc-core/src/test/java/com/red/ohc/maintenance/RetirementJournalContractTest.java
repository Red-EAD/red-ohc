package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/** Contract tests for the single retirement transport planned for Red OHC. */
public final class RetirementJournalContractTest {
  private static final String JOURNAL =
      "com.red.ohc.maintenance.RetirementJournal";
  private static final String WRITER_JOURNAL =
      "com.red.ohc.maintenance.WriterRetirementJournal";
  private static final String NATIVE_LOG =
      "com.red.ohc.maintenance.NativeRetirementLog";

  @Test
  public void oneRetirementJournalReplacesTheSplitTransports() throws Exception {
    Class<?> journal = load(JOURNAL);
    assertNotNull(journal, "the unified retirement journal must exist");
    assertFalse(load(WRITER_JOURNAL) != null, "the writer-only compatibility transport is obsolete");
    assertFalse(load(NATIVE_LOG) != null, "the actor-only compatibility transport is obsolete");

    assertNotNull(
        method(journal, "sealReadySegments", long.class),
        "the journal must expose fixed-watermark epoch sealing");
    assertNull(
        method(journal, "seal", long.class), "the obsolete compatibility sealing API must be gone");
    assertNotNull(
        method(journal, "publishSafe", long.class),
        "the journal must expose segment-level SAFE publication");
    assertNotNull(
        method(journal, "queuedRecords"), "the journal must expose an O(1) backlog counter");
    assertNull(
        field(journal, "retirementDebtLock"),
        "writer retirement admission must not serialize all producers on a monitor");
  }

  @Test
  public void segmentCapacityRemainsADataLayoutUnit() throws Exception {
    Class<?> segment = load("com.red.ohc.maintenance.RetirementSegment");
    assertNotNull(segment, "the retirement segment is the journal's storage unit");
    assertNotNull(segment.getField("CAPACITY"));
  }

  @Test
  public void queueDepthNeverReportsNegativeDuringAWeakSnapshot() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    try {
      Field completedRecords = RetirementJournal.class.getDeclaredField("completedRecords");
      completedRecords.setAccessible(true);
      ((AtomicLong) completedRecords.get(journal)).set(1L);

      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void committedRetirementCountsBeforeItsProducerSegmentIsSealed() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    long allocation = ValueBlock.allocationLength(64);
    long weight = WriterArena.allocationWeight(allocation);
    try {
      RetirementJournal.Lane lane = journal.actorLane();
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, allocation);
      lane.commit(reservation);

      assertEquals(journal.generatedBytesTotal(), weight);
      assertEquals(journal.retiredBytes(), weight);
      assertEquals(journal.retiredEntries(), 1);
      assertEquals(journal.sealedRecordsTotal(), 0L);

      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      assertEquals(journal.generatedBytesTotal(), weight);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      RetirementJournal.ReclaimResult result =
          journal.reclaimActorResult(memory, Integer.MAX_VALUE);
      assertEquals(result.segments, 1);
      assertEquals(result.records, 1);
      assertEquals(result.physicalRecords, 1);
      assertEquals(result.bytes, weight);
      assertEquals(journal.retiredBytes(), 0L);
      assertEquals(journal.completedBytesTotal(), weight);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void completionOwnerHandsOffAConcurrentSegmentFinish() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    long allocation = ValueBlock.allocationLength(64);
    try {
      RetirementJournal.Lane lane = journal.actorLane();
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, allocation);
      lane.commit(reservation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      Field workField = lane.getClass().getDeclaredField("completionAdvanceWork");
      workField.setAccessible(true);
      AtomicInteger completionWork = (AtomicInteger) workField.get(lane);
      Field cursorField = lane.getClass().getDeclaredField("completionCursor");
      cursorField.setAccessible(true);
      @SuppressWarnings("unchecked")
      AtomicReference<RetirementSegment> completionCursor =
          (AtomicReference<RetirementSegment>) cursorField.get(lane);
      RetirementSegment completedSegment = completionCursor.get();
      completionWork.set(1);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 1);
      assertEquals(completionWork.get(), 2, "the contending finish must publish WIP");
      Method drainOwner = lane.getClass().getDeclaredMethod("drainCompletionAdvances", int.class);
      drainOwner.setAccessible(true);
      drainOwner.invoke(lane, 1);

      assertTrue(
          completionCursor.get() != completedSegment,
          "the current completion owner must consume a finish published by a contending writer");
      assertEquals(completionWork.get(), 0, "the iterative owner must drain all published WIP");
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorReclaimConsumesOneSafeSegmentPerBatch() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    try {
      appendSegment(journal.actorLane());
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), RetirementSegment.CAPACITY);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      appendSegment(journal.actorLane());
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(2L), RetirementSegment.CAPACITY);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      RetirementJournal.ReclaimResult result =
          journal.reclaimActorSafeBatchResult(memory, 1);
      assertEquals(result.segments, 1);
      assertEquals(result.records, RetirementSegment.CAPACITY);
      assertEquals(journal.safeSegmentDebt(), 1L);
      assertEquals(journal.completedRecordsTotal(), RetirementSegment.CAPACITY);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorReclaimBatchAggregatesMultipleSafeSegments() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    try {
      appendSegment(journal.actorLane());
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), RetirementSegment.CAPACITY);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      appendSegment(journal.actorLane());
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(2L), RetirementSegment.CAPACITY);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      Method method =
          RetirementJournal.class.getDeclaredMethod(
              "reclaimActorSafeBatchResult", NativeMemory.Memory.class, int.class);
      method.setAccessible(true);
      RetirementJournal.ReclaimResult result =
          (RetirementJournal.ReclaimResult)
              method.invoke(journal, memory, 2);

      assertEquals(result.segments, 2);
      assertEquals(result.records, 2 * RetirementSegment.CAPACITY);
      assertEquals(result.physicalRecords, 0);
      assertEquals(result.bytes, 0L);
      assertEquals(journal.safeSegmentDebt(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorPageMemoSpansSegmentsAndEndsBeforeReclaimReturns() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long[] entries = new long[RetirementSegment.CAPACITY + 1];
    long allocation = 112L;
    try {
      WriterArena arena = memory.newWriterArena();
      for (int index = 0; index < entries.length; index++) {
        entries[index] = arena.allocate(allocation);
      }
      long pageKey =
          NativeMemory.Memory.entryAllocatorHandle(entries[0], allocation) >>> 14;
      for (long entry : entries) {
        assertEquals(
            NativeMemory.Memory.entryAllocatorHandle(entry, allocation) >>> 14,
            pageKey,
            "the fixture must span segments while retaining one allocator page");
      }

      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int index = 0; index < RetirementSegment.CAPACITY; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, entries[index], allocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), RetirementSegment.CAPACITY);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      assertTrue(lane.reserve(reservation));
      lane.write(reservation, entries[entries.length - 1], allocation);
      lane.commit(reservation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(2L), 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      Field actorContextField = RetirementJournal.class.getDeclaredField("actorContext");
      actorContextField.setAccessible(true);
      ThreadContext actorContext = (ThreadContext) actorContextField.get(journal);
      AtomicBoolean invalidatedBeforeCallback = new AtomicBoolean();
      Method hook = WriterArena.class.getDeclaredMethod("setRetirementHookForTest", Runnable.class);
      hook.setAccessible(true);
      hook.invoke(
          arena,
          (Runnable)
              () -> {
                assertNull(actorContext.releasePageMemoLookup(pageKey));
                invalidatedBeforeCallback.set(true);
              });

      RetirementJournal.ReclaimResult result =
          journal.reclaimActorSafeBatchResult(memory, 2);
      assertEquals(result.segments, 2);
      assertEquals(result.records, entries.length);
      assertEquals(result.physicalRecords, entries.length);
      assertTrue(invalidatedBeforeCallback.get());
      assertNull(actorContext.releasePageMemoLookup(pageKey));
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void lookupFailureEndsMemoScopeAndRequeuesTheUnreleasedSegment() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long allocation = 112L;
    long entry = 0L;
    try {
      WriterArena arena = memory.newWriterArena();
      entry = arena.allocate(allocation);
      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      assertTrue(lane.reserve(reservation));
      RetirementSegment segment = reservation.segment();
      int recordIndex = reservation.index();
      lane.write(reservation, entry, allocation);
      lane.commit(reservation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      Field handlesAddressField = RetirementSegment.class.getDeclaredField("handlesAddress");
      handlesAddressField.setAccessible(true);
      long handleAddress =
          handlesAddressField.getLong(segment) + (long) recordIndex * Long.BYTES;
      long validHandle = NativeMemory.getLong(handleAddress);
      // Slot bits occupy the low 14 bits and page-id bits the next 24. Toggle the first
      // generation bit so lookup cannot resolve this record to the registered descriptor.
      NativeMemory.putLong(handleAddress, validHandle ^ (1L << 38));
      try {
        journal.reclaimActorSafeBatchResult(memory, 1);
        throw new AssertionError("the corrupted page generation must fail lookup");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage().contains("unknown allocator slot handle"));
      }

      Field actorContextField = RetirementJournal.class.getDeclaredField("actorContext");
      actorContextField.setAccessible(true);
      ThreadContext actorContext = (ThreadContext) actorContextField.get(journal);
      actorContext.beginReleasePageMemo();
      actorContext.endReleasePageMemo();
      assertEquals(journal.safeSegmentDebt(), 1L);
      assertEquals(journal.safeRecords(), 1L);
      assertEquals(journal.claimedRecords(), 0L);
      assertEquals(journal.completedRecordsTotal(), 0L);

      NativeMemory.putLong(handleAddress, validHandle);
      RetirementJournal.ReclaimResult retry =
          journal.reclaimActorSafeBatchResult(memory, 1);
      assertEquals(retry.segments, 1);
      assertEquals(retry.records, 1);
      assertEquals(retry.physicalRecords, 1);
      assertEquals(journal.safeSegmentDebt(), 0L);
      assertEquals(journal.completedRecordsTotal(), 1L);
      entry = 0L;
    } finally {
      if (entry != 0L) {
        memory.releaseEntry(entry, allocation);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 20_000L)
  public void multipleSafePublishersAndOneActorConsumerPreserveAllSegments() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory, 4);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch publishersFinished = new CountDownLatch(4);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean publishing = new AtomicBoolean(true);
    Method publishSafe =
        RetirementJournal.Lane.class.getDeclaredMethod("publishSafe", long.class, long.class);
    publishSafe.setAccessible(true);
    Thread actor = null;
    try {
      for (int laneIndex = 1; laneIndex <= 4; laneIndex++) {
        appendSegment(journal.lane(laneIndex));
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 4 * RetirementSegment.CAPACITY);

      actor =
          new Thread(
              () -> {
                try {
                  start.await();
                  while (publishing.get() || journal.safeSegmentDebt() != 0L) {
                    RetirementJournal.ReclaimResult result =
                        journal.reclaimActorSafeBatchResult(memory, 1);
                    if (result.segments == 0) {
                      Thread.yield();
                    }
                  }
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                  publishing.set(false);
                }
              },
              "retirement-single-actor");
      actor.start();
      for (int laneIndex = 1; laneIndex <= 4; laneIndex++) {
        final RetirementJournal.Lane lane = journal.lane(laneIndex);
        Thread publisher =
            new Thread(
                () -> {
                  try {
                    start.await();
                    publishSafe.invoke(lane, Long.MAX_VALUE, Long.MAX_VALUE);
                  } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                  } finally {
                    publishersFinished.countDown();
                  }
                },
                "retirement-safe-publisher-" + laneIndex);
        publisher.start();
      }
      start.countDown();
      assertTrue(publishersFinished.await(10, TimeUnit.SECONDS), "SAFE publishers did not finish");
      publishing.set(false);
      actor.join(10_000L);
      assertTrue(!actor.isAlive(), "the single actor did not drain SAFE segments");
      assertNull(failure.get());
      assertEquals(
          journal.completedRecordsTotal(), (long) 4 * RetirementSegment.CAPACITY);
      assertEquals(journal.safeSegmentDebt(), 0L);
      assertEquals(journal.lagRecords(), 0L);
    } finally {
      publishing.set(false);
      if (actor != null) {
        actor.interrupt();
        actor.join(1_000L);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  /**
   * One reclaim wave must publish a page shared by multiple segments exactly once: the merged
   * decode groups the wave's records per page before any publication, so the availability
   * callback (and its freeBits/summary/freedSlots rounds) fires once per page per wave instead
   * of once per segment.
   */
  @Test
  public void mergedWavePublishesASharedPageOnce() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long[] entries = new long[RetirementSegment.CAPACITY + 1];
    long allocation = 112L;
    try {
      WriterArena arena = memory.newWriterArena();
      for (int index = 0; index < entries.length; index++) {
        entries[index] = arena.allocate(allocation);
      }
      long pageKey =
          NativeMemory.Memory.entryAllocatorHandle(entries[0], allocation) >>> 14;
      for (long entry : entries) {
        assertEquals(
            NativeMemory.Memory.entryAllocatorHandle(entry, allocation) >>> 14,
            pageKey,
            "the fixture must span two segments while retaining one allocator page");
      }

      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int index = 0; index < entries.length; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, entries[index], allocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), RetirementSegment.CAPACITY + 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 2);

      Method hook = WriterArena.class.getDeclaredMethod("setRetirementHookForTest", Runnable.class);
      hook.setAccessible(true);
      AtomicInteger availabilityCallbacks = new AtomicInteger();
      hook.invoke(
          arena,
          (Runnable) () -> availabilityCallbacks.incrementAndGet());

      RetirementJournal.ReclaimResult result = journal.reclaimActorSafeBatchResult(memory, 2);
      assertEquals(result.segments, 2);
      assertEquals(result.records, entries.length);
      assertEquals(result.physicalRecords, entries.length);
      assertEquals(
          availabilityCallbacks.get(),
          1,
          "one shared page must be published exactly once per merged wave");
      assertEquals(journal.safeSegmentDebt(), 0L);
      assertEquals(journal.completedRecordsTotal(), entries.length);
      // Every slot of the shared page must be reusable at its original address.
      for (long entry : entries) {
        assertEquals(arena.allocate(allocation), entry, "freed slots must recycle in place");
      }
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  /**
   * A decode-phase validation failure must leave the whole wave unreleased: no address column
   * cleared, no ledger move, both segments re-queued, and a retry after the fix completes
   * everything exactly once.
   */
  @Test
  public void mergedWaveDecodeFailureReleasesNothing() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long[] entries = new long[RetirementSegment.CAPACITY + 1];
    long allocation = 112L;
    long handleAddress = 0L;
    long validHandle = 0L;
    try {
      WriterArena arena = memory.newWriterArena();
      for (int index = 0; index < entries.length; index++) {
        entries[index] = arena.allocate(allocation);
      }
      long readyPagesBefore = memory.pageReadyCount();
      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      RetirementSegment corruptedSegment = null;
      int corruptedIndex = -1;
      for (int index = 0; index < entries.length; index++) {
        assertTrue(lane.reserve(reservation));
        if (index == RetirementSegment.CAPACITY) {
          corruptedSegment = reservation.segment();
          corruptedIndex = reservation.index();
        }
        lane.write(reservation, entries[index], allocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), RetirementSegment.CAPACITY + 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 2);

      Field handlesAddressField = RetirementSegment.class.getDeclaredField("handlesAddress");
      handlesAddressField.setAccessible(true);
      handleAddress =
          handlesAddressField.getLong(corruptedSegment) + (long) corruptedIndex * Long.BYTES;
      validHandle = NativeMemory.getLong(handleAddress);
      NativeMemory.putLong(handleAddress, validHandle ^ (1L << 38));

      try {
        journal.reclaimActorSafeBatchResult(memory, 2);
        throw new AssertionError("the corrupted handle must fail the merged wave decode");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage().contains("unknown allocator slot handle"));
      }
      assertEquals(journal.safeSegmentDebt(), 2L, "both segments must return to the safe queue");
      assertEquals(journal.safeRecords(), (long) entries.length);
      assertEquals(journal.claimedRecords(), 0L);
      assertEquals(journal.completedRecordsTotal(), 0L);
      assertEquals(
          memory.pageReadyCount(),
          readyPagesBefore,
          "no page may be published to the ready stack before a successful release");

      NativeMemory.putLong(handleAddress, validHandle);
      RetirementJournal.ReclaimResult retry = journal.reclaimActorSafeBatchResult(memory, 2);
      assertEquals(retry.segments, 2);
      assertEquals(retry.records, entries.length);
      assertEquals(journal.completedRecordsTotal(), entries.length);
      assertEquals(journal.safeSegmentDebt(), 0L);
      assertEquals(
          memory.pageReadyCount(),
          readyPagesBefore,
          "the arena's still-active current page is retained by its owner, not pushed");
      for (long entry : entries) {
        assertEquals(arena.allocate(allocation), entry, "freed slots must recycle in place");
      }
    } finally {
      if (handleAddress != 0L) {
        NativeMemory.putLong(handleAddress, validHandle);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  /**
   * A publication-phase failure must settle per segment: a segment whose every record was
   * already cleared by a published group finishes exactly like the success path, while untouched
   * segments abort, reverse their ledger move, and re-queue; the retry frees the remainder
   * exactly once.
   */
  @Test
  public void mergedWaveCallbackFailureSettlesCompleteSegments() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    int firstSegmentRecords = 3;
    int secondSegmentRecords = 2;
    long allocation = 112L;
    try {
      // Two arenas so each segment's records land on a different allocator page, i.e. in
      // exactly one publication group each.
      WriterArena firstArena = memory.newWriterArena();
      WriterArena secondArena = memory.newWriterArena();
      long[] firstPageEntries = new long[firstSegmentRecords];
      for (int index = 0; index < firstPageEntries.length; index++) {
        firstPageEntries[index] = firstArena.allocate(allocation);
      }
      long[] secondPageEntries = new long[secondSegmentRecords];
      for (int index = 0; index < secondPageEntries.length; index++) {
        secondPageEntries[index] = secondArena.allocate(allocation);
      }

      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (long entry : firstPageEntries) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, entry, allocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), firstSegmentRecords);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      for (long entry : secondPageEntries) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, entry, allocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(2L), secondSegmentRecords);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      Method hook = WriterArena.class.getDeclaredMethod("setRetirementHookForTest", Runnable.class);
      hook.setAccessible(true);
      // Fail the availability callback of the first published group (the first segment's page).
      AtomicReference<Runnable> hookSlot = new AtomicReference<>();
      hookSlot.set(
          () -> {
            throw new IllegalStateException("injected availability failure");
          });
      hook.invoke(
          firstArena,
          (Runnable)
              () -> {
                Runnable pending = hookSlot.getAndSet(null);
                if (pending != null) {
                  pending.run();
                }
              });

      try {
        journal.reclaimActorSafeBatchResult(memory, 2);
        throw new AssertionError("the injected availability failure must fail the wave");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage().contains("injected availability failure"));
      }
      // The first segment's records were cleared by the published group: completed exactly
      // once, segment finished. The second segment was never published: re-queued untouched.
      assertEquals(journal.completedRecordsTotal(), firstSegmentRecords);
      assertEquals(journal.safeSegmentDebt(), 1L);
      assertEquals(journal.safeRecords(), secondSegmentRecords);
      assertEquals(journal.claimedRecords(), 0L);
      assertEquals(
          firstArena.allocate(allocation),
          firstPageEntries[0],
          "the published page's first slot must recycle after the failed wave");

      RetirementJournal.ReclaimResult retry = journal.reclaimActorSafeBatchResult(memory, 2);
      assertEquals(retry.segments, 1);
      assertEquals(retry.records, secondSegmentRecords);
      assertEquals(journal.completedRecordsTotal(), firstSegmentRecords + secondSegmentRecords);
      assertEquals(journal.safeSegmentDebt(), 0L);
      for (long entry : secondPageEntries) {
        assertEquals(secondArena.allocate(allocation), entry, "freed slots must recycle in place");
      }
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  /**
   * Whole-page fast path: a detached (FULL) page whose last live slots are freed by one merged
   * wave must recycle in fresh bump mode at full capacity — no bitmap publication, page pushed
   * to the shared ready stack, and every slot served again in slot order by the next owner.
   */
  @Test
  public void wholePageDeathRecyclesAFreshBumpModePageAtFullCapacity() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long allocation = 112L;
    try {
      WriterArena source = memory.newWriterArena();
      WriterArena target = memory.newWriterArena();
      int sizeClass = com.red.ohc.storage.SizeClasses.indexForEntry(allocation);
      int slotsPerPage =
          com.red.ohc.storage.SizeClasses.pageBytes(sizeClass)
              / com.red.ohc.storage.SizeClasses.slotBytes(sizeClass);
      long[] entries = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        entries[index] = source.allocate(allocation);
      }
      source.detach();
      long readyPages = memory.pageReadyCount();

      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (long entry : entries) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, entry, allocation);
        lane.commit(reservation);
      }
      int segments =
          (slotsPerPage + RetirementSegment.CAPACITY - 1) / RetirementSegment.CAPACITY;
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), slotsPerPage);
      assertEquals(
          journal.publishSafe(Long.MAX_VALUE), segments, "the fixture must publish every segment");

      RetirementJournal.ReclaimResult result = journal.reclaimActorSafeBatchResult(memory, segments);
      assertEquals(result.records, slotsPerPage);
      assertEquals(
          memory.pageReadyCount(),
          readyPages + 1,
          "the fully-freed detached page must be published to the shared ready stack");
      for (int index = 0; index < slotsPerPage; index++) {
        assertEquals(
            target.allocate(allocation),
            entries[index],
            "the recycled page must serve its full capacity in fresh bump order");
      }
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  /**
   * Production retirement streams alternate size classes (value block, then key block, per
   * eviction). The grouped decode must keep one last-page memo slot per class so the alternation
   * still groups each class's records onto its page; a class-confused memo would fail ownership
   * validation or mis-account the release.
   */
  @Test
  public void mergedWaveGroupsAlternatingSizeClassesPerClassPage() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    RetirementJournal journal = new RetirementJournal(memory);
    long valueAllocation = ValueBlock.allocationLength(5_120);
    long keyAllocation = 112L;
    int pairs = 4;
    try {
      WriterArena arena = memory.newWriterArena();
      long[] values = new long[pairs];
      long[] keys = new long[pairs];
      for (int index = 0; index < pairs; index++) {
        values[index] = arena.allocate(valueAllocation);
        keys[index] = arena.allocate(keyAllocation);
      }

      RetirementJournal.Lane lane = journal.actorLane();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int index = 0; index < pairs; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, values[index], valueAllocation);
        lane.commit(reservation);
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, keys[index], keyAllocation);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), pairs * 2);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      RetirementJournal.ReclaimResult result = journal.reclaimActorSafeBatchResult(memory, 1);
      assertEquals(result.segments, 1);
      assertEquals(result.records, pairs * 2);
      assertEquals(journal.completedRecordsTotal(), pairs * 2L);
      assertEquals(journal.safeSegmentDebt(), 0L);
      for (int index = 0; index < pairs; index++) {
        assertEquals(
            arena.allocate(valueAllocation), values[index], "value slots must recycle in place");
        assertEquals(arena.allocate(keyAllocation), keys[index], "key slots must recycle in place");
      }
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  private static void appendSegment(RetirementJournal.Lane lane) {
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    for (int index = 0; index < RetirementSegment.CAPACITY; index++) {
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
    }
  }

  private static Class<?> load(String name) {
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException absent) {
      return null;
    }
  }

  private static Method method(Class<?> type, String name, Class<?>... parameterTypes) {
    try {
      return type.getDeclaredMethod(name, parameterTypes);
    } catch (NoSuchMethodException absent) {
      return null;
    }
  }

  private static Field field(Class<?> type, String name) {
    try {
      return type.getDeclaredField(name);
    } catch (NoSuchFieldException absent) {
      return null;
    }
  }
}
