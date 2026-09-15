package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;

public final class RetirementJournalWakeTest {
  @Test
  public void journalUsesExactAggregateWorkStateAndADirectOpenSegmentReference()
      throws Exception {
    assertEquals(
        RetirementJournal.class.getDeclaredField("openProducerCount").getType(),
        AtomicInteger.class);
    assertEquals(
        RetirementJournal.class.getDeclaredField("sealedSegmentCount").getType(),
        AtomicInteger.class);
    assertEquals(
        RetirementJournal.class.getDeclaredField("readyLaneCount").getType(),
        AtomicInteger.class);
    assertEquals(
        RetirementJournal.class.getDeclaredField("readyLanes").getType(),
        MpscUnboundedArrayQueue.class);
    assertEquals(
        RetirementJournal.Lane.class.getDeclaredField("countedOpenProducer").getType(),
        RetirementSegment.class);
    assertEquals(RetirementJournal.class.getDeclaredMethod("workState").getReturnType(), int.class);
  }

  @Test
  public void firstOpenProducerOnlySignalsTheIdleGate() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    AtomicInteger readySignals = new AtomicInteger();
    AtomicInteger openSignals = new AtomicInteger();
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      journal.bindReadySignal(readySignals::incrementAndGet);
      Method bindOpenProducerSignal =
          RetirementJournal.class.getDeclaredMethod("bindOpenProducerSignal", Runnable.class);
      bindOpenProducerSignal.setAccessible(true);
      bindOpenProducerSignal.invoke(journal, (Runnable) openSignals::incrementAndGet);

      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));

      assertEquals(openSignals.get(), 1, "the global open count changed from zero to one");
      assertEquals(readySignals.get(), 0, "a partial producer is not runnable retirement work");
      assertTrue(journal.hasOpenProducerRecords());
      assertTrue(
          (journal.workState() & RetirementJournal.WORK_STATE_OPEN) != 0,
          "the O(1) aggregate state exposes the open producer before the actor drains it");
      assertFalse(journal.hasActorWork());

      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      assertEquals(openSignals.get(), 1, "additional records in the same segment are coalesced");
      assertEquals(readySignals.get(), 0);
    } finally {
      if (reservation.segment() != null) {
        journal.lane(1).cancel(reservation);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void writerReservationTrustEndsWhenTheTicketIsCommitted() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      assertTrue(reservation.trusted());

      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);

      assertFalse(reservation.trusted());
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void readySignalFailureDoesNotCancelACommittedRetirement() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      journal.bindReadySignal(
          () -> {
            throw new IllegalStateException("injected ready signal failure");
          });
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      assertEquals(journal.queuedRecords(), 1L);
      assertTrue(journal.hasWork());
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void aggregateWorkStateTracksOpenSealedAndSafeTransitions() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    Method workState = RetirementJournal.class.getDeclaredMethod("workState");
    workState.setAccessible(true);
    int ready = intConstant("WORK_STATE_READY");
    int open = intConstant("WORK_STATE_OPEN");
    int sealed = intConstant("WORK_STATE_SEALED");
    int safe = intConstant("WORK_STATE_SAFE");
    try {
      assertEquals(workState.invoke(journal), 0);

      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      assertEquals(workState.invoke(journal), open);

      long[] watermark = journal.captureAndCutWatermark();
      assertEquals(((Integer) workState.invoke(journal)).intValue() & open, 0);
      assertTrue((((Integer) workState.invoke(journal)).intValue() & ready) != 0);

      assertEquals(journal.sealSnapshotSegments(1L, watermark), 1);
      journal.finishReadyDrains();
      assertEquals(workState.invoke(journal), sealed);

      assertEquals(journal.publishSafe(Long.MAX_VALUE), 1);
      assertEquals(workState.invoke(journal), safe);

      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 1);
      assertEquals(workState.invoke(journal), 0);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void readyLaneCountTracksOneQueuedMarkerPerReadyCycle() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    Field readyLaneCountField = RetirementJournal.class.getDeclaredField("readyLaneCount");
    readyLaneCountField.setAccessible(true);
    AtomicInteger readyLaneCount = (AtomicInteger) readyLaneCountField.get(journal);
    try {
      for (int laneIndex = 1; laneIndex <= 2; laneIndex++) {
        RetirementJournal.Lane lane = journal.lane(laneIndex);
        for (int index = 0; index < RetirementJournal.WRITER_WAKE_RECORDS; index++) {
          assertTrue(lane.reserve(reservation));
          lane.write(reservation, 0L, 0L);
          lane.commit(reservation);
        }
      }

      assertEquals(readyLaneCount.get(), 2);
      journal.finishReadyDrains();
      assertEquals(
          readyLaneCount.get(),
          2,
          "a wakeable producer must rearm its existing marker without adding duplicates");

      long[] watermark = journal.captureAndCutWatermark();
      assertEquals(
          journal.sealSnapshotSegments(1L, watermark),
          RetirementJournal.WRITER_WAKE_RECORDS * 2);
      assertEquals(journal.publishSafe(Long.MAX_VALUE), 2);
      assertEquals(
          journal.reclaimActorResult(memory, Integer.MAX_VALUE).records,
          RetirementJournal.WRITER_WAKE_RECORDS * 2);
      journal.finishReadyDrains();
      assertEquals(readyLaneCount.get(), 0);
      assertEquals(journal.workState(), 0);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closedCompleteProducerRemainsRunnableAfterReadyHintIsConsumed() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementJournal.Lane lane = journal.lane(1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      for (int index = 0; index < RetirementJournal.SEGMENT_CAPACITY; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }

      assertTrue(journal.consumeReadyHint());
      assertFalse(journal.hasReadyHint());
      assertTrue(
          journal.hasRunnableWork(),
          "a closed and complete producer must remain discoverable after the wake hint is consumed");
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void latePublicationFromAClosedSegmentCannotClearItsOpenSuccessor() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation predecessor = new RetirementSegment.Reservation();
    RetirementSegment.Reservation successor = new RetirementSegment.Reservation();
    Field openProducerCount = RetirementJournal.class.getDeclaredField("openProducerCount");
    openProducerCount.setAccessible(true);
    AtomicInteger openCount = (AtomicInteger) openProducerCount.get(journal);
    try {
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(predecessor));
      lane.write(predecessor, 0L, 0L);
      assertEquals(openCount.get(), 1);

      journal.captureAndCutWatermark();
      assertEquals(openCount.get(), 0);

      assertTrue(lane.reserve(successor));
      lane.write(successor, 0L, 0L);
      assertEquals(openCount.get(), 1);

      lane.commit(predecessor);
      assertTrue(journal.hasOpenProducerRecords());
      assertEquals(openCount.get(), 1, "the predecessor may only unregister its own segment");

      lane.commit(successor);
      lane.cutAndCaptureWatermark();
      assertFalse(journal.hasOpenProducerRecords());
      assertEquals(openCount.get(), 0);
    } finally {
      if (predecessor.segment() != null) {
        journal.lane(1).cancel(predecessor);
      }
      if (successor.segment() != null) {
        journal.lane(1).cancel(successor);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void busyProgressOwnerKeepsTheJournalReadyHintSticky() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    RetirementJournal.Lane lane = journal.lane(1);
    Field progressOwnerField = RetirementJournal.Lane.class.getDeclaredField("progressOwner");
    progressOwnerField.setAccessible(true);
    AtomicBoolean progressOwner = (AtomicBoolean) progressOwnerField.get(lane);
    try {
      for (int index = 0; index < RetirementJournal.WRITER_WAKE_RECORDS; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }
      assertTrue(progressOwner.compareAndSet(false, true));

      journal.finishReadyDrains();

      assertTrue(journal.hasReadyHint(), "a skipped lane still requires a later ready drain");
    } finally {
      progressOwner.set(false);
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void stalledLaneDoesNotBlockActorProgressOnAnotherWriterLane() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    RetirementJournal.Lane stalled = journal.lane(1);
    RetirementJournal.Lane independent = journal.lane(2);
    Field progressOwnerField = RetirementJournal.Lane.class.getDeclaredField("progressOwner");
    progressOwnerField.setAccessible(true);
    AtomicBoolean stalledOwner = (AtomicBoolean) progressOwnerField.get(stalled);
    try {
      for (int index = 0; index < RetirementJournal.WRITER_WAKE_RECORDS; index++) {
        assertTrue(independent.reserve(reservation));
        independent.write(reservation, 0L, 0L);
        independent.commit(reservation);
      }
      assertTrue(stalledOwner.compareAndSet(false, true));
      long[] watermark = new long[journal.laneCount() + 1];
      journal.captureAndCutReadyWatermark(watermark);

      assertEquals(
          journal.sealSnapshotSegments(1L, watermark),
          RetirementJournal.WRITER_WAKE_RECORDS);
      assertEquals(journal.publishSafe(Long.MAX_VALUE, Long.MAX_VALUE), 1);
      assertEquals(journal.safeSegmentDebt(), 1L);
    } finally {
      stalledOwner.set(false);
      journal.close();
      memory.closeArenas();
    }
  }

  private static int intConstant(String name) throws Exception {
    Field field = RetirementJournal.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.getInt(null);
  }

  @Test
  public void lookupOnlyWriterDoesNotPinValueRetirement() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory);
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderSlot lookupOnly = new ReaderSlot();
    int lookupIndex = readers.register(lookupOnly);
    readers.setEpoch(lookupIndex, 1L);
    try {
      journal.append(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);

      assertEquals(
          journal.publishSafe(readers.minActiveEpoch(), readers.minActiveValueEpoch()),
          1,
          "a key lookup cannot pin unrelated replaced values");
      assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 1);
    } finally {
      readers.setEpoch(lookupIndex, 0L);
      readers.clear();
      readers.close();
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void lookupOnlyWriterStillPinsStructuralRetirement() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory);
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderSlot lookupOnly = new ReaderSlot();
    int lookupIndex = readers.register(lookupOnly);
    readers.setEpoch(lookupIndex, 1L);
    try {
      journal.appendStructural(0L, 0L);
      journal.cutAllProducersAtWatermark();
      assertEquals(journal.sealReadySegments(1L), 1);

      assertEquals(
          journal.publishSafe(readers.minActiveEpoch(), readers.minActiveValueEpoch()),
          0,
          "native key retirement must still wait for an in-flight lookup");
      readers.setEpoch(lookupIndex, 0L);
      assertEquals(
          journal.publishSafe(readers.minActiveEpoch(), readers.minActiveValueEpoch()), 1);
    } finally {
      readers.setEpoch(lookupIndex, 0L);
      readers.clear();
      readers.close();
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void writerLaneCoalescesWakeupsUntilItsFixedRecordBoundary() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    AtomicInteger signalCount = new AtomicInteger();
    try {
      journal.bindReadySignal(signalCount::incrementAndGet);
      RetirementJournal.Lane lane = journal.lane(1);
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int index = 0; index < RetirementJournal.WRITER_WAKE_RECORDS - 1; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }

      assertEquals(signalCount.get(), 0);
      assertFalse(journal.hasReadyHint());
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      assertEquals(signalCount.get(), 1);
      assertTrue(journal.hasReadyHint());

      long[] watermark = journal.captureAndCutWatermark();
      journal.sealSnapshotSegments(1L, watermark);
      journal.publishSafe(Long.MAX_VALUE);
      journal.reclaimActorResult(memory, Integer.MAX_VALUE);
      journal.finishReadyDrains();
      signalCount.set(0);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      assertEquals(signalCount.get(), 0);
      lane.cutAndCaptureWatermark();
      assertEquals(signalCount.get(), 1);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void readyTurnCutsOnlyTheWriterLaneThatReachedItsBoundary() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane ready = journal.lane(1);
      RetirementJournal.Lane partial = journal.lane(2);
      for (int index = 0; index < RetirementJournal.WRITER_WAKE_RECORDS; index++) {
        assertTrue(ready.reserve(reservation));
        ready.write(reservation, 0L, 0L);
        ready.commit(reservation);
      }
      assertTrue(partial.reserve(reservation));
      partial.write(reservation, 0L, 0L);
      partial.commit(reservation);

      long[] watermark = new long[3];
      journal.captureAndCutReadyWatermark(watermark);

      assertEquals(watermark[1], RetirementJournal.WRITER_WAKE_RECORDS);
      assertEquals(watermark[2], 0L, "an unrelated partial writer lane must stay open");
      assertEquals(
          journal.sealSnapshotSegments(1L, watermark),
          RetirementJournal.WRITER_WAKE_RECORDS);
      assertTrue(journal.hasOpenProducerRecords());
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closedSegmentWakeDoesNotCutTheNextPartialProducer() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane lane = journal.lane(1);
      for (int index = 0; index < RetirementSegment.CAPACITY + 1; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }

      long[] watermark = new long[2];
      journal.captureAndCutReadyWatermark(watermark);

      assertEquals(
          watermark[1],
          RetirementSegment.CAPACITY,
          "the full predecessor is ready without cutting its one-record successor");
      assertEquals(
          journal.sealSnapshotSegments(1L, watermark), RetirementSegment.CAPACITY);
      assertTrue(journal.hasOpenProducerRecords());
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void publicationAfterAnIncompleteCutRearmsTheLane() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    AtomicInteger signalCount = new AtomicInteger();
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      journal.bindReadySignal(signalCount::incrementAndGet);
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);

      assertEquals(lane.cutAndCaptureWatermark(), 1L);
      journal.finishReadyDrains();
      signalCount.set(0);
      assertFalse(journal.hasReadyHint());

      lane.commit(reservation);

      assertEquals(signalCount.get(), 1);
      assertTrue(journal.hasReadyHint());
    } finally {
      if (reservation.segment() != null) {
        journal.lane(1).cancel(reservation);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void sealStatisticsCountVisitedAndProductiveLanesRatherThanRecords() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane lane = journal.lane(1);
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      lane.commit(reservation);
      long[] watermark = journal.captureAndCutWatermark();

      assertEquals(journal.sealSnapshotSegments(1L, watermark), 1);
      assertEquals(journal.lastSealScannedLanes(), 3);
      assertEquals(journal.lastSealSealedLanes(), 1);
      assertEquals(journal.sealScannedLanesTotal(), 3L);

      assertEquals(journal.sealSnapshotSegments(2L, watermark), 0);
      assertEquals(journal.lastSealScannedLanes(), 3);
      assertEquals(journal.lastSealSealedLanes(), 0);
      assertEquals(journal.sealScannedLanesTotal(), 6L);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void laneRejectsAReservationOwnedByAnotherLane() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 2);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    try {
      RetirementJournal.Lane owner = journal.lane(0);
      RetirementJournal.Lane other = journal.lane(1);
      assertTrue(owner.reserve(reservation));
      try {
        other.write(reservation, 0L, 0L);
        throw new AssertionError("a reservation must not be written through another lane");
      } catch (IllegalStateException expected) {
        // The stamped reservation cursor still validates lane ownership at every use.
      }
      owner.cancel(reservation);
    } finally {
      journal.close();
      memory.closeArenas();
    }
  }

  @Test
  public void retirementTransportUsesMpscSafeQueueAndUnboundedReusableQueue()
      throws Exception {
    Field safeSegments = RetirementJournal.class.getDeclaredField("safeSegments");
    Field reusableSegments = RetirementJournal.class.getDeclaredField("reusableSegments");
    Field sealedSegments = RetirementJournal.Lane.class.getDeclaredField("sealedSegments");
    assertEquals(reusableSegments.getType(), MpmcUnboundedXaddArrayQueue.class);
    assertEquals(sealedSegments.getType(), ArrayDeque.class);
    assertEquals(safeSegments.getType(), MpscUnboundedArrayQueue.class);
    assertFalse(
        hasField(RetirementJournal.class, "safeReclaimLock"),
        "single actor reclaim must not coordinate SAFE consumption with a monitor");
    assertFalse(
        hasMethod(RetirementSegment.class, "copyRecords"),
        "actor reclaim must consume native retirement columns directly");
  }

  @Test
  public void retirementSegmentCountersDoNotWrapEverySlotInAtomicObjects() throws Exception {
    assertEquals(RetirementSegment.class.getDeclaredField("reservationState").getType(), long.class);
    assertFalse(
        hasField(RetirementSegment.class, "activeReservations"),
        "the generation-stamped reservation cursor must be the only writer reservation RMW");
    assertEquals(RetirementSegment.class.getDeclaredField("segmentClaimState").getType(), int.class);
    assertEquals(RetirementSegment.class.getDeclaredField("payloadFreed").getType(), int.class);
  }

  @Test
  public void singleProducerCompletionUsesOneCommittedWatermark() throws Exception {
    assertFalse(
        hasField(RetirementSegment.class, "publishedAddress"),
        "single-producer lanes must not scan a native publication word for every record");
    assertEquals(RetirementSegment.class.getDeclaredField("committedTail").getType(), int.class);
    assertFalse(
        hasMethod(RetirementSegment.class, "retirementTotals"),
        "seal must consume producer-maintained segment totals without rescanning native columns");
    assertEquals(
        RetirementSegment.Reservation.class.getDeclaredField("allocation").getType(), long.class);

    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementSegment segment = new RetirementSegment(memory, 8, 0L, 1);
    try {
      assertEquals(segment.payloadBytes(), 8L * 3L * Long.BYTES);
    } finally {
      segment.freePayload();
      memory.closeArenas();
    }
  }

  private static boolean hasField(Class<?> type, String name) {
    try {
      type.getDeclaredField(name);
      return true;
    } catch (NoSuchFieldException expected) {
      return false;
    }
  }

  private static boolean hasMethod(Class<?> type, String name) {
    for (Method method : type.getDeclaredMethods()) {
      if (method.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }
}
