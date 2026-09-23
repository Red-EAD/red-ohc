package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public final class LifecycleJournalTest {
  @Test
  public void recordsBecomeVisibleOnlyAfterCommitAndCancelPublishesATombstone() {
    WriterLifecycleLane journal = new WriterLifecycleLane(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    Entry entry = new Entry(0L, 0, 1L);

    long sequence = journal.reserve();
    assertFalse(journal.hasCommittedRecords());
    assertFalse(journal.poll(record));

    journal.writeRemoval(sequence, entry, 17L, 24L, 0L, null);
    journal.commit(sequence);
    assertTrue(journal.poll(record));
    assertEquals(record.entry, entry);
    assertEquals(record.valueAddress, 17L);
    journal.release(record);
    assertEquals(journal.queuedRecords(), 0L);

    sequence = journal.reserve();
    journal.cancel(sequence);
    assertTrue(journal.poll(record));
    assertEquals(record.entry, null);
    journal.release(record);
    assertEquals(journal.queuedRecords(), 0L);
  }

  @Test
  public void rolloverPreservesOrderAndRecyclesConsumedSegments() {
    WriterLifecycleLane journal = new WriterLifecycleLane(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();

    for (int index = 0; index < 4; index++) {
      journal.reserve();
      long sequence = index;
      journal.writeRemoval(sequence, null, index, 1L, index, null);
      journal.commit(sequence);
    }
    assertEquals(journal.queuedRecords(), 4L);
    for (int index = 0; index < 4; index++) {
      assertTrue(journal.poll(record));
      assertEquals(record.valueAddress, (long) index);
      journal.release(record);
    }
    assertFalse(journal.poll(record));
    assertEquals(journal.queuedRecords(), 0L);
    assertEquals(journal.allocatedSegments(), 2L);

    for (int index = 0; index < 4; index++) {
      long sequence = journal.reserve();
      journal.writeRemoval(sequence, null, index, 1L, index, null);
      journal.commit(sequence);
    }
    while (journal.poll(record)) {
      journal.release(record);
    }
    assertEquals(
        journal.allocatedSegments(),
        3L,
        "the linked SPSC lane retains one consumed cursor plus the producer high-water segments");
  }

  @Test
  public void consumedSegmentsAreReusedAcrossWriterLanes() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(2);
    WriterLifecycleLane first = journal.lane(0);
    WriterLifecycleLane second = journal.lane(1);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();

    appendCommittedRemovals(first, WriterLifecycleJournal.SEGMENT_CAPACITY + 1);
    while (first.poll(record)) {
      first.release(record);
    }
    assertEquals(journal.allocatedSegments(), 3L);

    appendCommittedRemovals(second, WriterLifecycleJournal.SEGMENT_CAPACITY + 1);
    assertEquals(
        journal.allocatedSegments(),
        3L,
        "a cache-level pool must reuse one lane's consumed segment in another lane");
  }

  @Test
  public void reservationHoleIsOutstandingButNotReadyWork() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    WriterLifecycleLane lane = journal.lane(0);
    long first = lane.reserve();
    long second = lane.reserve();
    lane.writeRemoval(second, null, 2L, 1L, 0L, null);
    lane.commit(second);

    assertTrue(journal.hasCommittedRecords());
    assertFalse(journal.hasReadyRecords());

    lane.writeRemoval(first, null, 1L, 1L, 0L, null);
    lane.commit(first);
    assertTrue(journal.hasReadyRecords());
  }

  @Test
  public void reservationHoleRearmsTheReadyMarkerBeforeTheHeadCommits() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    WriterLifecycleLane lane = journal.lane(0);
    AtomicInteger signals = new AtomicInteger();
    journal.bindReadySignal(signals::incrementAndGet);

    long first = lane.reserve();
    long second = lane.reserve();
    lane.writeRemoval(second, null, 2L, 1L, 0L, null);
    lane.commit(second);
    assertEquals(signals.get(), 1);

    assertFalse(journal.probeUnmanagedReadyRecords());
    assertEquals(signals.get(), 1, "probing a hole clears the stale marker without a wake");

    lane.writeRemoval(first, null, 1L, 1L, 0L, null);
    lane.commit(first);
    assertTrue(journal.probeUnmanagedReadyRecords());
    assertEquals(signals.get(), 2, "the later head commit must not be swallowed by coalescing");
  }

  @Test
  public void mailboxOwnedHeadRemainsExcludedUntilReleaseExposesTheUnmanagedHead() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    WriterLifecycleLane lane = journal.lane(0);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    AtomicInteger signals = new AtomicInteger();
    journal.bindReadySignal(signals::incrementAndGet);

    long mailboxSequence = lane.reserve();
    lane.writeMutation(
        mailboxSequence, null, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
    lane.commitForMailbox(mailboxSequence);
    long unmanagedSequence = lane.reserve();
    lane.writeRemoval(unmanagedSequence, null, 2L, 1L, 0L, null);
    lane.commit(unmanagedSequence);

    assertFalse(journal.probeUnmanagedReadyRecords());
    assertEquals(signals.get(), 1);

    assertTrue(lane.poll(record));
    lane.release(record);
    lane.finishReadyDrain();
    assertTrue(journal.probeUnmanagedReadyRecords());
    assertTrue(lane.pollUnmanaged(record));
    lane.release(record);
  }

  @Test
  public void probeVisitsAllLanesAndSupportsRepeatedReadyCycles() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    AtomicInteger signals = new AtomicInteger();
    journal.bindReadySignal(signals::incrementAndGet);

    for (int index = 0; index < journal.laneCount(); index++) {
      WriterLifecycleLane lane = journal.lane(index);
      long sequence = lane.reserve();
      lane.writeRemoval(sequence, null, index + 1L, 1L, 0L, null);
      lane.commit(sequence);
    }
    assertTrue(journal.probeUnmanagedReadyRecords());
    assertEquals(signals.get(), 2);

    for (int index = 0; index < journal.laneCount(); index++) {
      WriterLifecycleLane lane = journal.lane(index);
      assertTrue(lane.pollUnmanaged(record));
      lane.release(record);
      lane.finishReadyDrain();
    }
    assertFalse(journal.probeUnmanagedReadyRecords());

    WriterLifecycleLane lane = journal.lane(1);
    long sequence = lane.reserve();
    lane.writeRemoval(sequence, null, 3L, 1L, 0L, null);
    lane.commit(sequence);
    assertTrue(journal.probeUnmanagedReadyRecords());
    assertEquals(signals.get(), 3);
    assertTrue(lane.pollUnmanaged(record));
    lane.release(record);
    lane.finishReadyDrain();
  }

  @Test
  public void lifecycleWatermarkExcludesReservationsMadeAfterTheCapture() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    WriterLifecycleLane lane = journal.lane(0);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();

    long beforeFlush = lane.reserve();
    long[] watermark = journal.captureWatermark();
    long afterFlush = lane.reserve();

    assertFalse(journal.watermarkComplete(watermark));
    lane.cancel(beforeFlush);
    assertTrue(lane.poll(record));
    lane.release(record);
    assertTrue(journal.watermarkComplete(watermark));
    assertEquals(lane.reservedRecords(), 1L);

    lane.cancel(afterFlush);
    assertTrue(lane.poll(record));
    lane.release(record);
    assertEquals(lane.reservedRecords(), 0L);
  }


  @Test
  public void readyLaneCountTracksOneQueuedMarkerPerReadyCycle() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    assertFalse(journal.hasPendingReadyLanes());

    WriterLifecycleLane first = journal.lane(0);
    long sequence = first.reserve();
    first.writeRemoval(sequence, null, 1L, 1L, 0L, null);
    first.commit(sequence);
    assertTrue(journal.hasPendingReadyLanes());
    assertEquals(journal.readyLaneCount(), 1);

    WriterLifecycleLane second = journal.lane(1);
    sequence = second.reserve();
    second.writeRemoval(sequence, null, 2L, 1L, 0L, null);
    second.commit(sequence);
    assertEquals(journal.readyLaneCount(), 2);

    assertTrue(first.pollUnmanaged(record));
    first.release(record);
    first.finishReadyDrain();
    assertEquals(journal.readyLaneCount(), 1);

    assertTrue(second.pollUnmanaged(record));
    second.release(record);
    second.finishReadyDrain();
    assertFalse(journal.hasPendingReadyLanes());
  }

  @Test
  public void dirtyDrainVisitsOnlySignalledLanesInThePassPath() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();

    WriterLifecycleLane idle = journal.lane(0);
    WriterLifecycleLane busy = journal.lane(1);
    long sequence = busy.reserve();
    busy.writeRemoval(sequence, null, 5L, 1L, 0L, null);
    busy.commit(sequence);

    assertEquals(journal.readyLaneCount(), 1);
    WriterLifecycleLane polled = journal.pollReadyLane();
    assertTrue(polled == busy, "only the signalled lane joins the dirty set");
    assertTrue(polled.laneIndex() == 1);
    assertTrue(polled.pollUnmanaged(record));
    polled.release(record);
    polled.finishReadyDrain();
    assertFalse(journal.hasPendingReadyLanes());
    assertTrue(idle.pollUnmanaged(record) == false);
    assertEquals(idle.completedRecordsTotal(), 0L);
  }

  @Test
  public void reservationHoleLeavesTheDirtySetWithoutSpinningTheActor() {
    WriterLifecycleJournal journal = new WriterLifecycleJournal(1);
    WriterLifecycleLane lane = journal.lane(0);

    long first = lane.reserve();
    long second = lane.reserve();
    lane.writeRemoval(second, null, 2L, 1L, 0L, null);
    lane.commit(second);

    assertTrue(journal.hasPendingReadyLanes());
    WriterLifecycleLane polled = journal.pollReadyLane();
    assertTrue(polled == lane);
    polled.finishReadyDrain();
    assertFalse(
        journal.hasPendingReadyLanes(), "a reservation hole must not keep the lane dirty");

    lane.writeRemoval(first, null, 1L, 1L, 0L, null);
    lane.commit(first);
    assertTrue(journal.hasPendingReadyLanes(), "the hole filling must re-dirty the lane");
  }

  private static void appendCommittedRemovals(WriterLifecycleLane lane, int count) {
    for (int index = 0; index < count; index++) {
      long sequence = lane.reserve();
      lane.writeRemoval(sequence, null, index + 1L, 1L, index, null);
      lane.commit(sequence);
    }
  }
}
