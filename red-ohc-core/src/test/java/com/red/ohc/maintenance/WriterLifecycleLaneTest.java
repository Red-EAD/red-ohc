package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public final class WriterLifecycleLaneTest {
  @Test
  public void commitCancelPollAndRecycleSegments() {
    WriterLifecycleLane lane = new WriterLifecycleLane(4);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    Entry entry = new Entry(0L, 0, 1L);

    long first = lane.reserve();
    lane.writeRemoval(first, entry, 20L, 30L, 4L, null);
    lane.commit(first);
    long second = lane.reserve();
    lane.cancel(second);
    assertTrue(lane.poll(record));
    assertEquals(record.entry, entry);
    assertEquals(record.valueAddress, 20L);
    lane.release(record);
    assertTrue(lane.poll(record));
    assertNull(record.entry);
    lane.release(record);
    assertFalse(lane.poll(record));
    assertEquals(lane.queuedRecords(), 0L);
    assertEquals(lane.reservedRecords(), 0L);

    long recycled = lane.reserve();
    lane.commit(recycled);
    assertTrue(lane.poll(record));
    lane.release(record);
    assertEquals(lane.allocatedSegments(), 1L);
  }

  @Test
  public void journalCreatesOneExclusiveLanePerWriterResource() throws Exception {
    WriterLifecycleJournal journal = new WriterLifecycleJournal();
    Method createLane = WriterLifecycleJournal.class.getDeclaredMethod("createLane");
    createLane.setAccessible(true);
    WriterLifecycleLane first = (WriterLifecycleLane) createLane.invoke(journal);
    WriterLifecycleLane second = (WriterLifecycleLane) createLane.invoke(journal);
    assertTrue(first != second);
    assertEquals(journal.laneCount(), 2);
  }

  @Test
  public void lifecycleSegmentsUseTheRetirementBatchSize() {
    assertEquals(WriterLifecycleJournal.SEGMENT_CAPACITY, 256);
  }

  @Test
  public void mutationHintUsesTheWriterExclusiveLaneAndCanDeferItsWake() {
    WriterLifecycleLane lane = new WriterLifecycleLane(4);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    AtomicInteger signals = new AtomicInteger();
    Entry entry = new Entry(0L, 0, 1L);
    lane.bindReadySignal(signals::incrementAndGet);

    long sequence = lane.reserve();
    lane.writeMutation(
        sequence, entry, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
    lane.commit(sequence, false);

    assertEquals(signals.get(), 0);
    assertTrue(lane.poll(record));
    assertEquals(record.operation, WriterLifecycleLane.MUTATION);
    assertEquals(record.entry, entry);
    lane.release(record);
    assertEquals(lane.queuedRecords(), 0L);
  }

  @Test
  public void mutationRecordCarriesPrimitiveSeedsWithoutChangingItsPhysicalShape() {
    WriterLifecycleLane lane = new WriterLifecycleLane(4);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    Entry entry = new Entry(0L, 0, 1L);
    int hash = 0x5566_7788;
    long allocation = 2_048L;
    long version = 17L;

    long sequence = lane.reserve();
    lane.writeMutation(sequence, entry, hash, allocation, version);
    lane.commit(sequence, false);

    assertTrue(lane.poll(record));
    assertEquals(record.valueAddress, hash);
    assertEquals(record.allocation, allocation);
    assertEquals(record.generation, version);
    lane.release(record);
  }

  @Test
  public void readySignalFailureDoesNotMakeACommittedRecordRetryable() {
    WriterLifecycleLane lane = new WriterLifecycleLane(4);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    lane.bindReadySignal(
        () -> {
          throw new IllegalStateException("injected ready signal failure");
        });

    long sequence = lane.reserve();
    lane.writeMutation(
        sequence,
        new Entry(0L, 0, 1L),
        0,
        0L,
        WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
    lane.commit(sequence);
    assertEquals(lane.publishedRecordsTotal(), 1L);
    assertTrue(lane.poll(record));
    lane.release(record);
    assertEquals(lane.queuedRecords(), 0L);
  }

  @Test
  public void rolloverCommitBetweenPollAndReadyDrainRearmsTheReadySignal() {
    WriterLifecycleLane lane = new WriterLifecycleLane(4);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    AtomicInteger signals = new AtomicInteger();
    lane.bindReadySignal(signals::incrementAndGet);

    for (int index = 0; index < 4; index++) {
      long sequence = lane.reserve();
      lane.writeRemoval(sequence, null, sequence, 8L, 0L, null);
      lane.commit(sequence);
    }
    assertEquals(signals.get(), 1);
    for (int index = 0; index < 4; index++) {
      assertTrue(lane.poll(record));
      lane.release(record);
    }
    assertFalse(lane.poll(record));

    long next = lane.reserve();
    lane.writeRemoval(next, null, next, 8L, 0L, null);
    lane.commit(next);
    lane.finishReadyDrain();

    assertEquals(signals.get(), 2);
    assertTrue(lane.poll(record));
    lane.release(record);
  }

  @Test
  public void laneRecordsFlowInOrderAcrossSegmentRecycling() {
    WriterLifecycleLane lane = new WriterLifecycleLane(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    Entry entry = new Entry(0L, 0, 1L);

    for (int index = 0; index < 5; index++) {
      long sequence = lane.reserve();
      lane.writeMutation(
          sequence, entry, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
      lane.commit(sequence, false);
      assertTrue(lane.poll(record));
      assertEquals(record.sequence, sequence);
      assertEquals(record.operation, WriterLifecycleLane.MUTATION);
      lane.release(record);
    }
    assertEquals(lane.allocatedSegments(), 2L);
  }

  @Test
  public void writeMutationAndCommitPublishTheSlotForPolling() {
    WriterLifecycleLane lane = new WriterLifecycleLane(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    Entry entry = new Entry(0L, 0, 1L);

    long sequence = lane.reserve();
    lane.writeMutation(sequence, entry, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
    lane.commit(sequence, false);

    assertTrue(lane.poll(record));
    assertEquals(record.sequence, sequence);
    assertEquals(record.operation, WriterLifecycleLane.MUTATION);
    assertSame(record.entry, entry);
    lane.release(record);
  }

  @Test
  public void cancelPublishesATombstoneSlotTheActorConsumesAsNoOp() {
    WriterLifecycleLane lane = new WriterLifecycleLane(2);
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();

    long sequence = lane.reserve();
    lane.cancel(sequence, false);

    assertTrue(lane.poll(record));
    assertNull(record.entry);
    assertEquals(record.operation, 0);
    lane.release(record);
    assertEquals(lane.publishedRecordsTotal(), 1L);
    assertEquals(lane.completedRecordsTotal(), 1L);
  }
}
