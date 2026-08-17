package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.api.RemovalCause;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;

public final class ReliableRemovalQueueTest {
  @Test
  public void reservationIsInvisibleUntilCommitAndTombstoneStillAdvancesTheRing() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Record record = new ReliableRemovalQueue.Record();
    Entry entry = EntryTestSupport.entry(0, 1, 0L);

    assertTrue(queue.tryReserve(first));
    assertFalse(queue.pollNext(record), "an uncommitted ticket must block consumption");
    queue.commit(first, entry, 17L, 24L, RemovalCause.SIZE);
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry, entry);
    assertEquals(record.valueAddress, 17L);
    assertEquals(record.valueAllocation, 24L);
    assertEquals(record.cause, RemovalCause.SIZE);

    assertTrue(queue.tryReserve(second));
    queue.cancel(second);
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry, null);
    assertEquals(queue.size(), 0L);
  }

  @Test
  public void uncommittedHeadDoesNotPublishACommittedHint() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation reservation = new ReliableRemovalQueue.Reservation();

    assertTrue(queue.tryReserve(reservation));
    assertEquals(queue.size(), 1L);
    assertFalse(queue.hasCommittedHint());

    queue.commit(reservation, EntryTestSupport.entry(0, 5, 0L));
    assertTrue(queue.hasCommittedHint());
  }

  @Test
  public void anUncommittedHeadPreventsLaterTicketsFromBeingConsumedOutOfOrder() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Record record = new ReliableRemovalQueue.Record();

    assertTrue(queue.tryReserve(first));
    assertTrue(queue.tryReserve(second));
    queue.commit(second, EntryTestSupport.entry(0, 2, 0L));
    assertFalse(queue.pollNext(record));
    queue.cancel(first);
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry, null);
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry.keyHash(), 2);
    assertEquals(queue.size(), 0L);
  }

  @Test
  public void committedHintClearsWhenACommittedPrefixMeetsAnUncommittedHead() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(4);
    ReliableRemovalQueue.Reservation committed = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation pending = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Record record = new ReliableRemovalQueue.Record();

    assertTrue(queue.tryReserve(committed));
    queue.commit(committed, EntryTestSupport.entry(0, 6, 0L));
    assertTrue(queue.tryReserve(pending));

    assertTrue(queue.pollNext(record));
    assertFalse(queue.pollNext(record));
    assertFalse(
        queue.hasCommittedHint(),
        "an uncommitted head must not keep the actor in an immediate-work loop");

    queue.commit(pending, EntryTestSupport.entry(0, 7, 0L));
    assertTrue(queue.hasCommittedHint(), "a later commit must re-arm the actor hint");
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry.keyHash(), 7);
  }

  @Test
  public void cancelAfterCommitIsIdempotent() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(2);
    ReliableRemovalQueue.Reservation reservation = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Record record = new ReliableRemovalQueue.Record();
    Entry entry = EntryTestSupport.entry(0, 3, 0L);

    assertTrue(queue.tryReserve(reservation));
    queue.commit(reservation, entry, 0L, 0L, null);
    queue.cancel(reservation);

    assertEquals(
        queue.size(), 1L, "exception cleanup must not cancel an already committed removal ticket");
    assertTrue(queue.pollNext(record));
    assertEquals(record.entry, entry);
  }

  @Test
  public void drainConsumesCommittedEntriesAndTombstones() {
    ReliableRemovalQueue queue = new ReliableRemovalQueue(4);
    ReliableRemovalQueue.Reservation first = new ReliableRemovalQueue.Reservation();
    ReliableRemovalQueue.Reservation second = new ReliableRemovalQueue.Reservation();

    assertTrue(queue.tryReserve(first));
    queue.commit(first, EntryTestSupport.entry(0, 4, 0L));
    assertTrue(queue.tryReserve(second));
    queue.cancel(second);

    assertEquals(queue.drain(), 2);
    assertEquals(queue.size(), 0L);
  }
}
