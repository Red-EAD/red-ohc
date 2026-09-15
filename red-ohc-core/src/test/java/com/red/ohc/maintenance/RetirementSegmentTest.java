package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public final class RetirementSegmentTest {
  @Test
  public void closedJournalRejectsProducersInsteadOfWritingFreedSegments() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementJournal journal = new RetirementJournal(memory);
    RetirementJournal.Lane lane = journal.createLane();
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      assertTrue(lane.reserve(reservation), "an open journal must hand out slots");
      lane.cancel(reservation);

      journal.close();

      assertFalse(
          lane.reserve(reservation),
          "close frees every segment payload, so a later reserve would write freed memory");
      assertFalse(journal.actorLane().reserve(reservation));
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void segmentUsesAStableFixedRecordCapacity() {
    assertEquals(RetirementSegment.CAPACITY, 256);
  }

  @Test
  public void staleProducerCannotReserveAcrossRecycledOwnerGeneration() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 2, 0L, 3);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(segment);
    try {
      long staleGeneration = segment.generation();
      finishEmptySegment(segment);
      assertTrue(segment.tryBeginRecycle());

      segment.reset(100L, 7);

      assertEquals(segment.tryReserve(staleGeneration, 3, producer), -1);

      int currentSlot = segment.tryReserve(segment.generation(), 7, producer);
      assertEquals(currentSlot, 0);
      segment.cancel(currentSlot);
    } finally {
      segment.freePayload();
      memory.closeArenas();
    }
  }

  @Test
  public void anUnpublishedReservationPreventsSegmentRecycle() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 2, 0L, 0);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(segment);
    try {
      int slot = segment.tryReserve(segment.generation(), 0, producer);
      assertEquals(slot, 0);
      segment.closeForSnapshot();
      assertFalse(
          segment.tryBeginRecycle(),
          "an unpublished reservation must keep recycle from resetting the native columns");

      segment.cancelReserved(slot);
      assertTrue(segment.seal(1L));
      assertTrue(segment.tryClaimSegment());
      segment.finishSegmentClaim();
      assertTrue(segment.tryBeginRecycle());
    } finally {
      segment.freePayload();
      memory.closeArenas();
    }
  }

  @Test
  public void fastReservationRejectsAProducerThatNoLongerOwnsTheSegment() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 2, 0L, 0);
    RetirementSegment replacement = new RetirementSegment(memory, 2, 2L, 1);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(replacement);
    try {
      assertEquals(segment.tryReserve(segment.generation(), 0, producer), -1);
      assertEquals(segment.reservationCount(), 0);
    } finally {
      segment.freePayload();
      replacement.freePayload();
      memory.closeArenas();
    }
  }

  @Test
  public void resetDoesNotAcceptPreviousGenerationPublicationWords() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 2, 0L, 0);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(segment);
    try {
      int firstSlot = segment.tryReserve(segment.generation(), 0, producer);
      assertEquals(firstSlot, 0);
      segment.writeReserved(firstSlot, 0L, 123L);
      segment.commitReserved(firstSlot);
      segment.closeForSnapshot();
      assertTrue(segment.isComplete());
      assertTrue(segment.seal(1L));
      assertTrue(segment.tryClaimSegment());
      segment.finishSegmentClaim();

      assertTrue(segment.tryBeginRecycle());
      segment.reset(0L, 1);

      int secondSlot = segment.tryReserve(segment.generation(), 1, producer);
      assertEquals(secondSlot, 0);
      segment.closeForSnapshot();
      assertEquals(segment.addressAt(0), 0L);
      assertEquals(segment.allocationAt(0), 0L);
      assertEquals(segment.handleAt(0), 0L);
      assertFalse(
          segment.isComplete(),
          "a reused segment must not treat a previous generation publication as committed");
      segment.cancelReserved(secondSlot);
      assertTrue(segment.isComplete());
    } finally {
      segment.freePayload();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentExclusiveLanesKeepOneSequencePerPublishedRecord() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    int writers = 4;
    RetirementJournal journal = new RetirementJournal(memory, writers);
    int recordsPerWriter = 2_048;
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread[] threads = new Thread[writers];
    try {
      for (int writer = 0; writer < writers; writer++) {
        RetirementJournal.Lane lane = journal.lane(writer + 1);
        threads[writer] =
            new Thread(
                () -> {
                  try {
                    RetirementSegment.Reservation reservation =
                        new RetirementSegment.Reservation();
                    start.await();
                    for (int index = 0; index < recordsPerWriter; index++) {
                      assertTrue(lane.reserve(reservation));
                      lane.write(reservation, 0L, 0L);
                      lane.commit(reservation);
                    }
                  } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                  }
                },
                "retirement-fast-writer-" + writer);
        threads[writer].start();
      }
      start.countDown();
      for (Thread thread : threads) {
        thread.join();
      }
      if (failure.get() != null) {
        throw new AssertionError("concurrent fast reservation failed", failure.get());
      }

      int records = writers * recordsPerWriter;
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), records);
      assertEquals(
          journal.publishSafe(Long.MAX_VALUE),
          (records + RetirementSegment.CAPACITY - 1) / RetirementSegment.CAPACITY);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, records);
      assertEquals(journal.publishedRecordsTotal(), records);
      assertEquals(journal.completedRecordsTotal(), records);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void staleProducerCannotReserveWhileCrossLaneReuseIsBeingPublished() throws Exception {
    assertStaleProducerCannotReserveDuringReuse(0, 1);
  }

  @Test(timeOut = 10_000L)
  public void staleProducerCannotReserveWhileSameLaneReuseIsNotCurrent() throws Exception {
    assertStaleProducerCannotReserveDuringReuse(0, 0);
  }

  private static void assertStaleProducerCannotReserveDuringReuse(
      int oldLaneIndex, int newLaneIndex) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 2, 0L, oldLaneIndex);
    RetirementSegment replacement = new RetirementSegment(memory, 2, 2L, newLaneIndex);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(segment);
    CountDownLatch producerRead = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    CountDownLatch reservationAttempted = new CountDownLatch(1);
    AtomicReference<Integer> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try {
      Thread staleProducer =
          new Thread(
              () -> {
                try {
                  RetirementSegment observed = producer.get();
                  producerRead.countDown();
                  assertTrue(resume.await(5, TimeUnit.SECONDS));
                  long generation = observed.generation();
                  int slot = invokeReserve(observed, generation, oldLaneIndex, producer);
                  if (slot >= 0) {
                    observed.cancel(slot);
                  }
                  result.set(slot);
                } catch (Throwable error) {
                  failure.set(error);
                } finally {
                  reservationAttempted.countDown();
                }
              },
              "stale-retirement-producer");
      staleProducer.start();

      assertTrue(producerRead.await(5, TimeUnit.SECONDS));
      producer.set(replacement);
      finishEmptySegment(segment);
      assertTrue(segment.tryBeginRecycle());
      segment.reset(100L, newLaneIndex);
      resume.countDown();
      assertTrue(reservationAttempted.await(5, TimeUnit.SECONDS));
      staleProducer.join();
      if (failure.get() != null) {
        throw new AssertionError("stale producer race failed", failure.get());
      }
      assertEquals(result.get().intValue(), -1);
    } finally {
      segment.freePayload();
      replacement.freePayload();
      memory.closeArenas();
    }
  }

  private static int invokeReserve(
      RetirementSegment segment,
      long generation,
      int laneIndex,
      AtomicReference<RetirementSegment> producer) {
    return segment.tryReserve(generation, laneIndex, producer);
  }

  private static void finishEmptySegment(RetirementSegment segment) {
    assertEquals(segment.closeForSnapshot(), 0);
    assertTrue(segment.isComplete());
    assertTrue(segment.seal(1L));
    assertTrue(segment.tryClaimSegment());
    segment.finishSegmentClaim();
  }

  @Test
  public void trimmedDescriptorsLeaveCloseTrackingOwnership() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    try {
      int records = RetirementSegment.CAPACITY * 8 + 1;
      for (int index = 0; index < records; index++) {
        journal.append(0L, 0L);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), records);
      journal.publishSafe(Long.MAX_VALUE);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, records);

      Field trackingField = RetirementJournal.class.getDeclaredField("allocatedSegmentDescriptors");
      trackingField.setAccessible(true);
      Object tracking = trackingField.get(journal);
      assertTrue(tracking instanceof Set, "close tracking must release trimmed descriptors");
      assertEquals(
          ((Set<?>) tracking).size(),
          journal.allocatedSegments() - journal.trimmedSegments());
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void writerAndActorRecordsShareOneSafeReclaimQueue() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    try {
      RetirementJournal.Lane writer = journal.lane(1);
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      assertTrue(writer.reserve(reservation));
      writer.write(reservation, 0L, 0L);
      writer.commit(reservation);
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();

      assertEquals(journal.sealReadySegments(7L), 2);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 2);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 2);
      assertEquals(journal.queuedRecords(), 0L);
      assertEquals(journal.actorReclaimedRecordsTotal(), 2L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void retirementCountersTrackUnsafeSafeClaimedAndCompletedStates() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    long allocation = 112L;
    long entry = memory.newWriterArena().allocate(allocation);
    try {
      journal.append(entry, allocation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(3L), 1);
      assertEquals(journal.unsafeRecords(), 1L);
      assertEquals(journal.unsafeBytes(), WriterArena.allocationWeight(allocation));
      assertEquals(journal.safeRecords(), 0L);

      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      assertEquals(journal.unsafeRecords(), 0L);
      assertEquals(journal.safeRecords(), 1L);
      assertEquals(journal.claimedRecords(), 0L);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 1);
      assertEquals(journal.safeRecords(), 0L);
      assertEquals(journal.claimedRecords(), 0L);
      assertEquals(journal.completedRecordsTotal(), 1L);
      assertEquals(journal.actorReclaimedRecordsTotal(), 1L);
      assertEquals(journal.retiredEntries(), 0);
      assertEquals(journal.retiredBytes(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorReclaimsNativeColumnsGroupedByOneAllocatorPage() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    WriterArena arena = memory.newWriterArena();
    int count = 12;
    long allocation = ValueBlock.allocationLength(5_120);
    long[] entries = new long[count];
    try {
      for (int index = 0; index < count; index++) {
        entries[index] = arena.allocate(allocation);
        assertTrue(entries[index] != 0L);
        journal.append(entries[index], allocation);
      }

      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), count);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, count);
      assertEquals(journal.queuedRecords(), 0L);

      for (int index = 0; index < count; index++) {
        entries[index] = arena.allocate(allocation);
        assertTrue(entries[index] != 0L);
      }
    } finally {
      for (long entry : entries) {
        if (entry != 0L) {
          memory.releaseEntry(entry, allocation);
        }
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorBatchReclaimsMixedPooledAndDirectEntriesWithoutReadingFreedMetadata() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    WriterArena arena = memory.newWriterArena();
    long pooledAllocation = ValueBlock.allocationLength(5_120);
    long directAllocation = ValueBlock.allocationLength(32_768);
    long pooledEntry = arena.allocate(pooledAllocation);
    long directEntry = arena.allocate(directAllocation);
    try {
      assertTrue(pooledEntry != 0L);
      assertTrue(directEntry != 0L);
      journal.append(pooledEntry, pooledAllocation);
      journal.append(directEntry, directAllocation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 2);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 2);
      pooledEntry = 0L;
      directEntry = 0L;
    } finally {
      if (pooledEntry != 0L) {
        memory.releaseEntry(pooledEntry, pooledAllocation);
      }
      if (directEntry != 0L) {
        memory.releaseEntry(directEntry, directAllocation);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void groupedReleaseMakesCompletedRecordsIdempotentForRetry() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    WriterArena arena = memory.newWriterArena();
    RetirementSegment segment = new RetirementSegment(memory, 4, 0L, 0);
    AtomicReference<RetirementSegment> producer = new AtomicReference<>(segment);
    long allocation = ValueBlock.allocationLength(64);
    long first = arena.allocate(allocation);
    long second = arena.allocate(allocation);
    try {
      assertTrue(first != 0L);
      assertTrue(second != 0L);
      int firstSlot = segment.tryReserve(segment.generation(), 0, producer);
      segment.writeReserved(firstSlot, first, allocation);
      segment.commitReserved(firstSlot, allocation, false);
      int secondSlot = segment.tryReserve(segment.generation(), 0, producer);
      segment.writeReserved(secondSlot, second, allocation);
      segment.commitReserved(secondSlot, allocation, false);
      assertEquals(segment.closeForSnapshot(), 2);
      assertTrue(segment.seal(1L));
      assertTrue(segment.tryClaimSegment());

      ThreadContext context = new ThreadContext(null);
      assertEquals(segment.releaseRecords(memory, context), 2);
      assertEquals(segment.addressAt(0), 0L);
      assertEquals(segment.addressAt(1), 0L);

      // A grouped retry must observe only cleared slots and must not free either allocator slot a
      // second time.
      assertEquals(segment.releaseRecords(memory, context), 0);
      segment.finishSegmentClaim();
      first = 0L;
      second = 0L;
    } finally {
      if (first != 0L) {
        memory.releaseEntry(first, allocation);
      }
      if (second != 0L) {
        memory.releaseEntry(second, allocation);
      }
      segment.freePayload();
      memory.closeArenas();
    }
  }

  @Test
  public void journalGroupedRetryCountsAcceptedNativeGroupsOnlyOnce() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementJournal journal = new RetirementJournal(memory);
    WriterArena arena = memory.newWriterArena();
    long allocation = ValueBlock.allocationLength(64);
    long first = arena.allocate(allocation);
    long second = arena.allocate(allocation);
    AtomicBoolean failOnce = new AtomicBoolean(true);
    Method hook = WriterArena.class.getDeclaredMethod("setRetirementHookForTest", Runnable.class);
    hook.setAccessible(true);
    hook.invoke(
        arena,
        (Runnable)
            () -> {
              if (failOnce.getAndSet(false)) {
                throw new IllegalStateException("group availability callback failure");
              }
            });
    long weight = WriterArena.allocationWeight(allocation);
    try {
      journal.append(first, allocation);
      journal.append(second, allocation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 2);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);

      try {
        journal.reclaimActorSafeBatchResult(memory, 1);
        throw new AssertionError("the injected grouped callback failure must be reported");
      } catch (IllegalStateException expected) {
        // The allocator accepted the group before the callback failed. Both native slots are
        // therefore already cleared and must be accounted for exactly once.
        first = 0L;
        second = 0L;
      }
      Field actorContextField = RetirementJournal.class.getDeclaredField("actorContext");
      actorContextField.setAccessible(true);
      ThreadContext actorContext = (ThreadContext) actorContextField.get(journal);
      actorContext.beginReleasePageMemo();
      actorContext.endReleasePageMemo();
      assertEquals(journal.completedRecordsTotal(), 2L);
      assertEquals(journal.completedBytesTotal(), weight * 2L);
      assertEquals(journal.retiredBytes(), 0L);
      assertEquals(journal.retiredEntries(), 0);
      assertEquals(journal.queuedRecords(), 0L);

      // The retry only completes the segment claim; it must not free or count either slot again.
      assertEquals(journal.reclaimActorSafeBatchResult(memory, 1).records, 0);
      assertEquals(journal.completedRecordsTotal(), 2L);
      assertEquals(journal.completedBytesTotal(), weight * 2L);
      assertEquals(journal.safeSegmentDebt(), 0L);
    } finally {
      if (first != 0L) {
        memory.releaseEntry(first, allocation);
      }
      if (second != 0L) {
        memory.releaseEntry(second, allocation);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void activeReaderPinsSealedSegmentUntilEpochIsQuiescent() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot active = new ReaderSlot();
    readers.register(active);
    readers.setValueEpoch(active, 7L);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    try {
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(7L), 1);
      assertEquals(journal.reclaimActorResult(readers, Integer.MAX_VALUE).records, 0);
      assertEquals(journal.queuedRecords(), 1L);
      assertFalse(journal.hasSafeSegments());

      readers.setEpoch(active, 0L);
      assertEquals(journal.reclaimActorResult(readers, Integer.MAX_VALUE).records, 1);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      readers.setEpoch(active, 0L);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void pinnedSealedSegmentIsNotActorRunnableUntilReaderQuiesces() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    ReaderRegistry readers = new ReaderRegistry();
    ReaderSlot active = new ReaderSlot();
    readers.register(active);
    readers.setEpoch(active, 7L);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    try {
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(7L), 1);
      journal.finishReadyDrains();

      assertTrue(journal.hasSealedSegments());
      assertFalse(
          journal.hasActorWork(),
          "a sealed segment pinned by a reader must wait for reclaim retry or quiescence");
    } finally {
      readers.setEpoch(active, 0L);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void segmentClaimInProgressIsNotActorRunnable() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    try {
      RetirementJournal.Lane lane = journal.lane(0);
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      journal.finishReadyDrains();

      Field completionCursorField = lane.getClass().getDeclaredField("completionCursor");
      completionCursorField.setAccessible(true);
      @SuppressWarnings("unchecked")
      AtomicReference<RetirementSegment> completionCursor =
          (AtomicReference<RetirementSegment>) completionCursorField.get(lane);
      RetirementSegment segment = completionCursor.get();
      assertTrue(segment.tryClaimSegment());
      try {
        assertFalse(
            journal.hasActorWork(),
            "a segment claimed by a writer must not make the actor spin on completion");
      } finally {
        segment.abortSegmentClaim();
      }
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closeReclaimsRecordsSealedDuringClose() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    long allocation = 112L;
    long address = memory.newWriterArena().allocate(allocation);
    try {
      journal.append(address, allocation);

      journal.close();

      assertEquals(journal.queuedRecords(), 0L);
      assertEquals(journal.retiredEntries(), 0);
      assertEquals(journal.retiredBytes(), 0L);
      assertEquals(journal.allocatedBytes(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void producerGrowsWithoutAnAdmissionDebtOrSegmentLimit() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    int records = RetirementSegment.CAPACITY * 16 + 3;
    try {
      RetirementJournal.Lane lane = journal.lane(0);
      for (int index = 0; index < records; index++) {
        RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), records);
      journal.publishSafe(Long.MAX_VALUE);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, records);
      assertEquals(journal.queuedRecords(), 0L);
      assertTrue(journal.allocatedSegments() >= 17L);
      assertTrue(
          journal.allocatedSegments() - journal.trimmedSegments() <= 256L,
          "reusable retirement segments must remain bounded");
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void singleLaneLargeTailKeepsSequenceAndCompletionCountsAligned() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    int records = 1_000_000;
    try {
      RetirementJournal.Lane lane = journal.lane(0);
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int index = 0; index < records; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), records);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), records / RetirementSegment.CAPACITY + 1);
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, records);
      assertEquals(journal.publishedRecordsTotal(), records);
      assertEquals(journal.completedRecordsTotal(), records);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 45_000L)
  public void concurrentWriterAndActorReclaimKeepCompletionCountsAligned() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    int records = 1_000_000;
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean writing = new AtomicBoolean(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                RetirementJournal.Lane lane = journal.lane(0);
                RetirementSegment.Reservation reservation =
                    new RetirementSegment.Reservation();
                start.await();
                for (int index = 0; index < records; index++) {
                  assertTrue(lane.reserve(reservation));
                  lane.write(reservation, 0L, 0L);
                  lane.commit(reservation);
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              } finally {
                writing.set(false);
              }
            });
    Thread actor =
        new Thread(
            () -> {
              try {
                start.await();
                long epoch = 1L;
                while (writing.get() || journal.queuedRecords() != 0L) {
                  journal.cutAllProducersAtWatermark();
                  journal.sealReadySegments(epoch++);
                  journal.publishSafe(Long.MAX_VALUE);
                  journal.reclaimActorResult(memory, Integer.MAX_VALUE);
                  journal.finishReadyDrains();
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              }
            });
    try {
      writer.start();
      actor.start();
      start.countDown();
      writer.join(30_000L);
      actor.join(30_000L);
      if (writer.isAlive()) {
        fail(stallReport("writer", writer, actor, journal));
      }
      if (actor.isAlive()) {
        fail(stallReport("actor", writer, actor, journal));
      }
      Throwable error = failure.get();
      if (error != null) {
        throw new AssertionError(error);
      }
      assertEquals(journal.publishedRecordsTotal(), records);
      assertEquals(journal.completedRecordsTotal(), records);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      writer.interrupt();
      actor.interrupt();
      writer.join(1_000L);
      actor.join(1_000L);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 45_000L)
  public void concurrentWriterLaneAndActorReclaimKeepReservationProgress() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    int records = 250_000;
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean writing = new AtomicBoolean(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                RetirementJournal.Lane lane = journal.lane(1);
                RetirementSegment.Reservation reservation =
                    new RetirementSegment.Reservation();
                start.await();
                for (int index = 0; index < records; index++) {
                  assertTrue(lane.reserve(reservation));
                  lane.write(reservation, 0L, 0L);
                  lane.commit(reservation);
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              } finally {
                writing.set(false);
              }
            },
            "retirement-writer-lane");
    Thread actor =
        new Thread(
            () -> {
              try {
                start.await();
                long epoch = 1L;
                while (writing.get() || journal.queuedRecords() != 0L) {
                  journal.cutAllProducersAtWatermark();
                  journal.sealReadySegments(epoch++);
                  journal.publishSafe(Long.MAX_VALUE);
                  journal.reclaimActorResult(memory, Integer.MAX_VALUE);
                  journal.finishReadyDrains();
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              }
            },
            "retirement-actor");
    try {
      writer.start();
      actor.start();
      start.countDown();
      writer.join(20_000L);
      actor.join(20_000L);
      if (writer.isAlive()) {
        fail(stallReport("writer", writer, actor, journal));
      }
      if (actor.isAlive()) {
        fail(stallReport("actor", writer, actor, journal));
      }
      Throwable error = failure.get();
      if (error != null) {
        throw new AssertionError(error);
      }
      assertEquals(journal.publishedRecordsTotal(), records);
      assertEquals(journal.completedRecordsTotal(), records);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      writer.interrupt();
      actor.interrupt();
      writer.join(1_000L);
      actor.join(1_000L);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 75_000L)
  public void concurrentWriterLaneAndBoundedActorReclaimKeepReservationProgress() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    int records = 1_000_000;
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean writing = new AtomicBoolean(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                RetirementJournal.Lane lane = journal.lane(1);
                RetirementSegment.Reservation reservation =
                    new RetirementSegment.Reservation();
                start.await();
                for (int index = 0; index < records; index++) {
                  assertTrue(lane.reserve(reservation));
                  lane.write(reservation, 0L, 0L);
                  lane.commit(reservation);
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              } finally {
                writing.set(false);
              }
            },
            "retirement-writer-actor-bounded");
    Thread actor =
        new Thread(
            () -> {
              try {
                start.await();
                long epoch = 1L;
                while (writing.get() || journal.queuedRecords() != 0L) {
                  journal.cutAllProducersAtWatermark();
                  journal.sealReadySegments(epoch++);
                  journal.publishSafe(Long.MAX_VALUE);
                  while (journal.reclaimActorSafeBatchResult(memory, 1).records
                      != 0) {
                    // Drain all segments that became SAFE in this turn.
                  }
                  journal.finishReadyDrains();
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              }
            },
            "retirement-actor-bounded-reclaimer");
    try {
      writer.start();
      actor.start();
      start.countDown();
      writer.join(60_000L);
      actor.join(60_000L);
      if (writer.isAlive()) {
        fail(stallReport("writer", writer, actor, journal));
      }
      if (actor.isAlive()) {
        fail(stallReport("actor", writer, actor, journal));
      }
      Throwable error = failure.get();
      if (error != null) {
        throw new AssertionError(error);
      }
      assertEquals(journal.publishedRecordsTotal(), records);
      assertEquals(journal.completedRecordsTotal(), records);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      writer.interrupt();
      actor.interrupt();
      writer.join(1_000L);
      actor.join(1_000L);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorOwnsSafeSegmentReclaim() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    try {
      for (int index = 0; index < RetirementSegment.CAPACITY * 4; index++) {
        journal.append(0L, 0L);
      }
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 4 * RetirementSegment.CAPACITY);
      journal.publishSafe(Long.MAX_VALUE);

      RetirementJournal.ReclaimResult result =
          journal.reclaimActorResult(memory, Integer.MAX_VALUE);
      assertEquals(result.segments, 4);
      assertEquals(result.records, RetirementSegment.CAPACITY * 4);
      assertEquals(journal.queuedRecords(), 0L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  private static String stallReport(
      String who, Thread writer, Thread actor, RetirementJournal journal) {
    StringBuilder report = new StringBuilder(who).append(" did not finish; ");
    report
        .append("queued=").append(journal.queuedRecords())
        .append(" published=").append(journal.publishedRecordsTotal())
        .append(" completed=").append(journal.completedRecordsTotal())
        .append(" openProducer=").append(journal.hasOpenProducerRecords())
        .append(" sealed=").append(journal.hasSealedSegments())
        .append(" safe=").append(journal.hasSafeSegments())
        .append(" readyHint=").append(journal.hasReadyHint());
    appendStack(report, writer);
    appendStack(report, actor);
    return report.toString();
  }

  private static void appendStack(StringBuilder report, Thread thread) {
    report.append('\n').append(thread.getName()).append(" alive=").append(thread.isAlive());
    for (StackTraceElement frame : thread.getStackTrace()) {
      report.append("\n    ").append(frame);
    }
  }

}
