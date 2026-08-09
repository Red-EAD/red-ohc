package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public final class ReliableRemovalQueueTest {
  @Test
  public void reservationIsInvisibleUntilCommitAndTombstoneStillAdvancesTheRing() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();
    Entry entry = new Entry(0L, 0, 1, 0L);

    assertTrue(queue.reserve(first));
    assertFalse(queue.poll() != null, "an uncommitted ticket must block consumption");
    queue.commit(first, entry);
    assertEquals(queue.poll(), entry);

    assertTrue(queue.reserve(second));
    queue.cancel(second);
    assertTrue(queue.pollTombstone());
    assertEquals(queue.size(), 0L);
  }

  @Test
  public void anUncommittedHeadPreventsLaterTicketsFromBeingConsumedOutOfOrder() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();

    assertTrue(queue.reserve(first));
    assertTrue(queue.reserve(second));
    queue.commit(second, new Entry(0L, 0, 2, 0L));
    assertFalse(queue.pollTombstone());
    queue.cancel(first);
    assertTrue(queue.pollTombstone());
    assertTrue(queue.poll() != null);
    assertEquals(queue.size(), 0L);
  }

  @Test
  public void cancelAfterCommitIsIdempotent() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation reservation = new ReliableRemovalQueue.Reservation();
    Entry entry = new Entry(0L, 0, 3, 0L);

    assertTrue(queue.reserve(reservation));
    queue.commit(reservation, entry);
    queue.cancel(reservation);

    assertEquals(
        queue.size(), 1L, "exception cleanup must not cancel an already committed removal ticket");
    assertEquals(queue.poll(), entry);
  }

  @Test
  public void drainConsumesCommittedEntriesAndTombstones() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(4);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();

    assertTrue(queue.reserve(first));
    queue.commit(first, new Entry(0L, 0, 4, 0L));
    assertTrue(queue.reserve(second));
    queue.cancel(second);

    assertEquals(queue.drain(), 2);
    assertEquals(queue.size(), 0L);
  }
}
