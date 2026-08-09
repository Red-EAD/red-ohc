package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class RetirementQueueTest {
  @Test
  public void queuedDepthDoesNotUseAGlobalAtomicReservationLedger() {
    for (Field field : RetirementQueue.class.getDeclaredFields()) {
      assertFalse(
          field.getType() == AtomicLong.class && field.getName().equals("queuedRecords"),
          "every replacement must not contend on a cache-global retirement depth counter");
    }
  }

  @Test
  public void constructorRollsBackPreviouslyAllocatedStripesWhenLaterStripeFails() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA, 100L);
    try {
      try {
        new RetirementQueue(memory, 2, 2);
        throw new AssertionError("the second stripe must exceed the native hard limit");
      } catch (NativeMemory.AllocationLimitException expected) {
        assertEquals(
            memory.allocated(),
            0L,
            "a failed queue construction must release every stripe allocated before the failure");
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void sealedEntryWaitsForItsReaderEpochBeforeReturningToTheArena() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 1, 2);
    WriterArena arena = memory.newWriterArena();
    long allocation = ValueBlock.allocationLength(8);
    long address = arena.allocate(allocation);
    RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
    ReaderSlot reader = new ReaderSlot();
    ReaderRegistry readers = new ReaderRegistry();
    readers.register(reader);
    try {
      assertTrue(queue.reserve(reservation, 1));
      queue.append(reservation, address, allocation);
      assertEquals(queue.seal(1, 7L), 1);
      assertEquals(queue.retiredEntries(), 1);
      assertTrue(
          queue.hasPendingReclaim(),
          "the actor must publish that a sealed record awaits a reader epoch");

      reader.epoch = 7L;
      assertEquals(queue.reclaim(readers, 1), 0);
      assertEquals(queue.retiredEntries(), 1);

      reader.epoch = 0L;
      assertEquals(queue.reclaim(readers, 1), 1);
      assertEquals(queue.retiredEntries(), 0);
      assertTrue(
          !queue.hasPendingReclaim(),
          "the reader-side wake hint must clear after the final sealed record is reclaimed");
      assertEquals(arena.allocate(allocation), address);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void canceledReservationPublishesAnEmptyRecordAndReturnsItsRingSlot() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 1, 2);
    RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
    try {
      assertTrue(queue.reserve(reservation, 1));
      queue.cancel(reservation);
      assertEquals(queue.seal(1, 1L), 1);
      assertEquals(queue.reclaim(new ReaderRegistry(), 1), 1);
      assertTrue(
          queue.reserve(reservation, 2),
          "a canceled pre-publication mutation must not strand native ring capacity");
      queue.cancel(reservation);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void cancelPrefixLeavesTheRemainingBatchReservationUsable() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 1, 8);
    RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
    try {
      assertTrue(queue.reserve(reservation, 4));
      queue.append(reservation, 0L, 0L);
      queue.cancelPrefix(reservation, 1);
      assertTrue(reservation.active(), "a prefix cancel must not discard the rest of the batch");
      queue.cancel(reservation);
      assertEquals(queue.seal(4, 1L), 4);
      assertEquals(queue.reclaim(new ReaderRegistry(), 4), 4);
      assertTrue(queue.reserve(reservation, 8), "all tombstones must return the ring capacity");
      queue.cancel(reservation);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void oneBusyProducerStripeCanUseAnotherFixedStripeBeforeRejecting() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 2, 2);
    RetirementQueue.Reservation first = new RetirementQueue.Reservation();
    RetirementQueue.Reservation second = new RetirementQueue.Reservation();
    try {
      assertTrue(queue.reserve(first, 2));
      queue.cancel(first);

      assertTrue(
          queue.reserve(second, 2),
          "a transiently full stripe must not reject while another native stripe has capacity");
      queue.cancel(second);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorSealsPublishedRecordsAndTracksOnlyActiveStripes() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 8, 2);
    RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
    try {
      assertEquals(queue.activeStripeCount(), 0);
      assertTrue(queue.reserve(reservation, 1));
      queue.append(reservation, 0L, 0L);

      assertTrue(queue.consumeReadyHint());
      assertEquals(queue.seal(1, 1L), 1);
      assertFalse(queue.consumeReadyHint());
      assertEquals(
          queue.activeStripeCount(),
          1,
          "a sealed stripe remains actor-owned until its FIFO record is reclaimed");

      assertEquals(queue.reclaim(new ReaderRegistry(), 1), 1);
      assertEquals(queue.activeStripeCount(), 0);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void publicationAfterAConsumedHintRemainsVisibleToTheNextActorPass() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 2, 4);
    try {
      RetirementQueue.Reservation first = new RetirementQueue.Reservation();
      assertTrue(queue.reserve(first, 1));
      queue.append(first, 0L, 0L);

      assertTrue(queue.consumeReadyHint(), "the first published record must wake a sealing pass");
      assertFalse(queue.consumeReadyHint(), "one actor pass consumes the currently visible hint");

      RetirementQueue.Reservation second = new RetirementQueue.Reservation();
      assertTrue(queue.reserve(second, 1));
      queue.append(second, 0L, 0L);

      assertTrue(
          queue.consumeReadyHint(),
          "a producer racing after the prior pass must request another seal, not strand a record");
      assertEquals(queue.seal(2, 1L), 2);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void reclaimVisitsRemainingActiveStripesWhenTheFirstStripeDrains() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 2, 2);
    RetirementQueue.Reservation first = new RetirementQueue.Reservation();
    RetirementQueue.Reservation second = new RetirementQueue.Reservation();
    try {
      assertTrue(queue.reserve(first, 2));
      queue.append(first, 0L, 0L);
      queue.append(first, 0L, 0L);
      assertTrue(queue.reserve(second, 1));
      queue.append(second, 0L, 0L);
      assertEquals(queue.seal(3, 1L), 3);
      assertEquals(queue.activeStripeCount(), 2);

      assertEquals(
          queue.reclaim(new ReaderRegistry(), 3),
          3,
          "draining the first active stripe must not skip the next active stripe");
      assertEquals(queue.activeStripeCount(), 0);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }

  @Test
  public void queuedReservationsExposeCapacityAndTheSevenEighthsBackpressureWatermark() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue queue = new RetirementQueue(memory, 1, 8);
    RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
    try {
      assertEquals(queue.capacityRecords(), 8L);
      assertTrue(queue.reserve(reservation, 7));
      assertEquals(queue.queuedRecords(), 7L);
      assertTrue(queue.exceedsHighWatermark());
      assertFalse(
          queue.reserve(new RetirementQueue.Reservation(), 2),
          "a producer must reject before it can overwrite an unretired FIFO record");
      queue.cancel(reservation);
    } finally {
      queue.freeAll();
      queue.close();
      memory.closeArenas();
    }
  }
}
