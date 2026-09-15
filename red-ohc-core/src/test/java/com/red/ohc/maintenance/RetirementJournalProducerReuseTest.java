package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public final class RetirementJournalProducerReuseTest {
  private enum Operation {
    COMMIT,
    CANCEL,
    RESERVE,
    CAPTURE_WATERMARK
  }

  @Test(timeOut = 20_000L)
  public void committedBoundaryCannotCloseARecycledProducer() throws Exception {
    assertDelayedProducerKeepsRecycledLaneOpen(Operation.COMMIT);
  }

  @Test(timeOut = 20_000L)
  public void cancelledBoundaryCannotCloseARecycledProducer() throws Exception {
    assertDelayedProducerKeepsRecycledLaneOpen(Operation.CANCEL);
  }

  @Test(timeOut = 20_000L)
  public void delayedReservationCannotCloseARecycledProducer() throws Exception {
    assertDelayedProducerKeepsRecycledLaneOpen(Operation.RESERVE);
  }

  @Test(timeOut = 20_000L)
  public void delayedWatermarkCannotCloseARecycledProducer() throws Exception {
    assertDelayedProducerKeepsRecycledLaneOpen(Operation.CAPTURE_WATERMARK);
  }

  @Test(timeOut = 45_000L)
  public void concurrentWritersKeepReservationsInTheirOwnRecycledLane() throws Exception {
    int writers = 10;
    int recordsPerWriter = 1_000_000;
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, writers);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(writers);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread[] threads = new Thread[writers + 1];
    for (int index = 0; index < writers; index++) {
      RetirementJournal.Lane lane = journal.lane(index + 1);
      threads[index] =
          new Thread(
              () -> {
                RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
                try {
                  start.await();
                  for (int record = 0; record < recordsPerWriter && !stop.get(); record++) {
                    assertTrue(lane.reserve(reservation));
                    assertEquals(
                        reservation.segment().laneIndex(),
                        reservation.laneIndex(),
                        "a successful reservation must pin its own lane and generation");
                    lane.write(reservation, 0L, 0L);
                    lane.commit(reservation);
                  }
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                  stop.set(true);
                } finally {
                  finished.countDown();
                }
              },
              "retirement-reuse-writer-" + index);
    }
    threads[writers] =
        new Thread(
            () -> {
              try {
                start.await();
                long epoch = 1L;
                while (!stop.get() && (finished.getCount() != 0L || journal.hasWork())) {
                  journal.cutAllProducersAtWatermark();
                  journal.sealReadySegments(epoch++);
                  journal.publishSafe(Long.MAX_VALUE);
                  journal.reclaimActorResult(memory, Integer.MAX_VALUE);
                  journal.finishReadyDrains();
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
                stop.set(true);
              }
            },
            "retirement-reuse-actor");
    try {
      for (Thread thread : threads) {
        thread.start();
      }
      start.countDown();
      assertTrue(finished.await(30L, TimeUnit.SECONDS), "writers did not finish");
      threads[writers].join(5_000L);
      assertFalse(threads[writers].isAlive(), "actor did not finish");
      assertNull(failure.get());
      assertTrue(journal.reusedSegments() > 0L, "the run must exercise segment recycling");
      assertEquals(journal.publishedRecordsTotal(), (long) writers * recordsPerWriter);
      assertEquals(journal.completedRecordsTotal(), journal.publishedRecordsTotal());
      assertEquals(journal.workState(), 0);
    } finally {
      stop.set(true);
      start.countDown();
      for (Thread thread : threads) {
        thread.join(5_000L);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  @Test(timeOut = 30_000L)
  public void completedWatermarkNeverOvertakesReclaimAccounting() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementJournal.Lane lane = journal.lane(1);
    int rounds = 100_000;
    long allocation = ValueBlock.allocationLength(64);
    long weight = WriterArena.allocationWeight(allocation);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong observed = new AtomicLong();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread observer =
        new Thread(
            () -> {
              try {
                for (long watermark = 1L; watermark <= rounds && !stop.get(); watermark++) {
                  while (!stop.get() && !lane.watermarkComplete(watermark)) {
                    assertTrue(System.nanoTime() < deadline, "reclaim watermark did not advance");
                    Thread.onSpinWait();
                  }
                  if (stop.get()) {
                    return;
                  }
                  assertTrue(
                      journal.completedRecordsTotal() >= watermark,
                      "a completed watermark must include its reclaim accounting");
                  assertEquals(journal.completedBytesTotal(), watermark * weight);
                  assertEquals(journal.retiredEntries(), 0);
                  assertEquals(journal.retiredBytes(), 0L);
                  observed.set(watermark);
                }
              } catch (Throwable error) {
                failure.set(error);
                stop.set(true);
              }
            },
            "retirement-watermark-observer");
    try {
      observer.start();
      WriterArena arena = memory.newWriterArena();
      RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
      for (int round = 1; round <= rounds && !stop.get(); round++) {
        assertTrue(lane.reserve(reservation));
        long value = arena.allocate(allocation);
        ValueBlock.initialize(value, 0L, 64, 0L);
        lane.write(reservation, value, allocation);
        lane.commit(reservation);
        journal.cutAllProducersAtWatermark();
        assertEquals(journal.sealReadySegments(round), 1);
        journal.publishSafe(Long.MAX_VALUE);
        assertEquals(journal.reclaimActorResult(memory, Integer.MAX_VALUE).records, 1);
        journal.finishReadyDrains();
        while (!stop.get() && observed.get() < round) {
          assertTrue(System.nanoTime() < deadline, "watermark observer did not advance");
          Thread.onSpinWait();
        }
      }
      observer.join(5_000L);
      assertFalse(observer.isAlive(), "watermark observer did not finish");
      assertNull(failure.get());
      assertEquals(journal.completedRecordsTotal(), rounds);
      assertTrue(journal.reusedSegments() > 0L);
    } finally {
      stop.set(true);
      observer.join(5_000L);
      journal.close();
      memory.closeArenas();
    }
  }

  private static void assertDelayedProducerKeepsRecycledLaneOpen(
      Operation operation) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    RetirementJournal journal = new RetirementJournal(memory, 1);
    RetirementJournal.Lane lane = journal.lane(1);
    RetirementSegment.Reservation reservation = new RetirementSegment.Reservation();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicLong captured = new AtomicLong(-1L);
    Thread writer = null;
    try {
      for (int index = 0; index < RetirementJournal.SEGMENT_CAPACITY - 1; index++) {
        assertTrue(lane.reserve(reservation));
        lane.write(reservation, 0L, 0L);
        lane.commit(reservation);
      }
      assertTrue(lane.reserve(reservation));
      lane.write(reservation, 0L, 0L);
      RetirementSegment original = reservation.segment();
      if (operation == Operation.RESERVE || operation == Operation.CAPTURE_WATERMARK) {
        lane.commit(reservation);
      }

      writer =
          new Thread(
              () -> {
                try {
                  if (operation == Operation.RESERVE) {
                    assertTrue(lane.reserve(reservation));
                    lane.write(reservation, 0L, 0L);
                    lane.commit(reservation);
                  } else if (operation == Operation.CAPTURE_WATERMARK) {
                    captured.set(lane.cutAndCaptureWatermark());
                  } else if (operation == Operation.CANCEL) {
                    lane.cancel(reservation);
                  } else {
                    lane.commit(reservation);
                  }
                } catch (Throwable error) {
                  failure.set(error);
                }
              },
              "delayed-retirement-producer");
      RetirementJournal.Lane receiver;
      RetirementSegment received;
      RetirementSegment.Reservation next = new RetirementSegment.Reservation();
      synchronized (lane) {
        writer.start();
        awaitBlockedOnCurrentThread(writer);

        // The actor can finish a published segment while its writer is blocked on the lane's
        // cold-path monitor. If completion is not published yet, reclamation must wait instead.
        journal.cutAllProducersAtWatermark();
        int sealed = journal.sealReadySegments(1L);
        journal.publishSafe(Long.MAX_VALUE);
        journal.reclaimActorResult(memory, Integer.MAX_VALUE);
        receiver = journal.createLane();
        assertTrue(receiver.reserve(next));
        received = next.segment();
        if (sealed != 0) {
          assertTrue(received == original, "the actor must actually exercise cross-lane reuse");
        }
        receiver.write(next, 0L, 0L);
        receiver.commit(next);
      }

      writer.join(5_000L);
      assertFalse(writer.isAlive(), "the delayed producer did not finish");
      assertNull(failure.get());
      assertFalse(
          received.isClosed(),
          "the old producer must not close a segment that now belongs to another lane");
      if (operation == Operation.CAPTURE_WATERMARK) {
        assertEquals(
            captured.get(),
            (long) RetirementJournal.SEGMENT_CAPACITY,
            "the watermark must stay in its original lane's sequence domain");
      }
      assertTrue(receiver.reserve(next), "a reused lane must accept its next record");
      receiver.write(next, 0L, 0L);
      receiver.commit(next);

      long[] watermark = journal.captureAndCutWatermark();
      journal.sealSnapshotSegments(2L, watermark);
      journal.publishSafe(Long.MAX_VALUE);
      journal.reclaimActorResult(memory, Integer.MAX_VALUE);
      journal.finishReadyDrains();
      assertTrue(journal.watermarkComplete(watermark));
      assertEquals(journal.reservedRecords(), 0L);
      assertEquals(journal.completedRecordsTotal(), journal.publishedRecordsTotal());
      assertEquals(journal.workState(), 0);
    } finally {
      if (writer != null) {
        writer.join(5_000L);
      }
      journal.close();
      memory.closeArenas();
    }
  }

  private static void awaitBlockedOnCurrentThread(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
    while (System.nanoTime() < deadline) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockOwnerId() == Thread.currentThread().getId()) {
        return;
      }
      Thread.sleep(1L);
    }
    throw new AssertionError("producer did not reach the lane monitor: " + thread.getState());
  }
}
