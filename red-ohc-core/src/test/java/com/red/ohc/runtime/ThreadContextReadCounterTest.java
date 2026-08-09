package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public final class ThreadContextReadCounterTest {
  @Test
  public void oneReadSequenceDrivesStatsAndAccessSampling() {
    ThreadContext context = new ThreadContext(null, null);
    long first = context.hit();
    context.finishRead(first);
    assertEquals(first, 1L);
    assertEquals(context.slot.publishedHits, 0L);
    for (int index = 0; index < 1_023; index++) {
      long sequence = context.miss();
      context.finishRead(sequence);
    }
    assertEquals(context.slot.publishedHits, 1L);
    assertEquals(context.slot.publishedMisses, 1_023L);
  }

  @Test
  public void accessRingRetainsEachOfIts256DeliveredHitsBeforeDropping() {
    AccessRing ring = new AccessRing();
    Entry entry = new Entry(0L, 0, 1, 0L);

    for (int index = 0; index < 256; index++) {
      assertTrue(ring.offer(entry, index + 1L));
    }
    assertFalse(ring.offer(entry, 257L));

    int drained = 0;
    while (ring.poll((ignored, generation) -> {})) {
      drained++;
    }
    assertEquals(drained, 256);
    assertTrue(ring.isEmpty());
  }

  @Test
  public void businessAccessSamplingPublishesOneEventPerSixteenHits() {
    ThreadContext context = new ThreadContext(null, null);
    Entry entry = new Entry(0L, 0, 2, 0L);

    for (int index = 0; index < 32; index++) {
      context.access(entry);
    }

    int delivered = 0;
    while (context.slot.access.poll((ignored, generation) -> {})) {
      delivered++;
    }
    assertEquals(delivered, 2);
  }

  @Test
  public void bulkAccessSamplingPublishesOneEventPerSixteenHits() {
    ThreadContext context = new ThreadContext(null, null);
    Entry entry = new Entry(0L, 0, 3, 0L);

    context.beginBulkRead();
    for (int index = 0; index < 32; index++) {
      context.bulkHit(entry);
    }
    context.finishBulkRead();

    int delivered = 0;
    while (context.slot.access.poll((ignored, generation) -> {})) {
      delivered++;
    }
    assertEquals(delivered, 2);
  }
}
