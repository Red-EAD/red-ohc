package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;

public class TimerWheelTest {
  @Test
  public void maximumExpiryDoesNotOverflowIntoTheCurrentWheelRound() {
    TimerWheel wheel = new TimerWheel(0L);
    Entry entry = EntryTestSupport.entry(0, 1, 0L);
    wheel.add(entry, Long.MAX_VALUE);

    AtomicInteger expiries = new AtomicInteger();
    wheel.advance(TimerWheel.TICK_NANOS, (ignored, generation, address) -> expiries.incrementAndGet());

    assertEquals(expiries.get(), 0);
    assertTrue(wheel.hasPending());
  }

  @Test
  public void nextWakeTickPointsAtTheNextSixtyFourMillisecondBoundary() {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(EntryTestSupport.entry(0, 2, 0L), 10_000_000_000L);

    assertEquals(wheel.nextWakeTick(), 157L);
  }

  @Test
  public void idleCatchUpSkipsEmptyTicksToReachALongOverdueDeadline() {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(EntryTestSupport.entry(0, 23, 0L), 10_000L * TimerWheel.TICK_NANOS);

    assertEquals(
        wheel.advance(10_000L * TimerWheel.TICK_NANOS, (ignored, generation, address) -> {}),
        1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void overflowHeapIndexZeroRemainsRemovable() {
    TimerWheel wheel = new TimerWheel(0L);
    Entry entry = EntryTestSupport.entry(0, 22, 0L);
    wheel.add(entry, 10_000_000_000_000_000L);
    assertTrue(entry.timerInOverflowHeap());

    wheel.remove(entry);
    assertEquals(wheel.scheduled(), 0L);
    assertTrue(!entry.timerScheduled());
  }

  @Test
  public void ceilPlacementHandlesTickBoundariesWithoutWheelRoundDelay() {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(EntryTestSupport.entry(0, 3, 0L), 63_000_000L);
    wheel.add(EntryTestSupport.entry(0, 4, 0L), TimerWheel.TICK_NANOS);
    wheel.add(EntryTestSupport.entry(0, 5, 0L), 65_000_000L);

    wheel.advance(TimerWheel.TICK_NANOS, (ignored, generation, address) -> {});
    assertEquals(wheel.scheduled(), 1L);
    wheel.advance(2L * TimerWheel.TICK_NANOS, (ignored, generation, address) -> {});
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void sameScheduledSlotOnlyAcceptsADeadlineInTheInstalledBucket() {
    TimerWheel wheel = new TimerWheel(0L);
    Entry entry = EntryTestSupport.entry(0, 7, 0L);
    long deadline = 10_000_000_000L;
    wheel.add(entry, deadline);

    assertTrue(wheel.hasSameScheduledSlot(entry, deadline + 1_000_000L));
    assertFalse(wheel.hasSameScheduledSlot(entry, deadline + TimerWheel.TICK_NANOS));

    wheel.remove(entry);
    assertFalse(wheel.hasSameScheduledSlot(entry, deadline));
  }

  @Test
  public void expiryBudgetContinuesTheCurrentBucketBeforeTheNextWheelRound() {
    TimerWheel wheel = new TimerWheel(0L);
    for (int index = 0; index < 2_049; index++) {
      wheel.add(EntryTestSupport.entry(0, index + 10, 0L), TimerWheel.TICK_NANOS);
    }

    assertEquals(
        wheel.advance(TimerWheel.TICK_NANOS, 1_024, (ignored, generation, address) -> {}),
        1_024);
    assertEquals(
        wheel.scheduled(), 1_025L, "the over-budget entries must remain in the current bucket");

    assertEquals(
        wheel.advance(TimerWheel.TICK_NANOS, 1_024, (ignored, generation, address) -> {}),
        1_024);
    assertEquals(
        wheel.scheduled(), 1L, "the deferred entries must not wait for a full wheel round");
    assertEquals(
        wheel.advance(TimerWheel.TICK_NANOS, 1_024, (ignored, generation, address) -> {}), 1);
    assertEquals(
        wheel.scheduled(), 0L, "the final deferred entry must not wait for a full wheel round");
  }

  @Test
  public void expiryBudgetUsesTheBinaryMaintenanceBatchSize() {
    TimerWheel wheel = new TimerWheel(0L);
    for (int index = 0; index < 1_025; index++) {
      wheel.add(EntryTestSupport.entry(0, index + 3_000, 0L), TimerWheel.TICK_NANOS);
    }

    assertEquals(
        wheel.advance(TimerWheel.TICK_NANOS, 1_024, (ignored, generation, address) -> {}),
        1_024);
    assertEquals(wheel.scheduled(), 1L);
    assertEquals(
        wheel.advance(TimerWheel.TICK_NANOS, 1_024, (ignored, generation, address) -> {}), 1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void idleCatchUpFindsALevelZeroSlotAfterWheelWrap() {
    long nowNanos = 100L * TimerWheel.TICK_NANOS;
    TimerWheel wheel = new TimerWheel(nowNanos);
    // tick=100 starts the search at slot 101. target=1114 maps back to slot 90, which is
    // in the same bitmap word but below the starting bit after one L0 rotation.
    wheel.add(EntryTestSupport.entry(0, 6, 0L), 1_114L * TimerWheel.TICK_NANOS);

    assertEquals(
        wheel.advance(1_114L * TimerWheel.TICK_NANOS, (ignored, generation, address) -> {}), 1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void cascadesEveryHierarchicalLevelBeforeTheDeadlineBucketIsConsumed() throws Exception {
    assertCascadeConsumesDeadline(1_024L);
    assertCascadeConsumesDeadline(65_536L);
    assertCascadeConsumesDeadline(4_194_304L);
    assertCascadeConsumesDeadline(134_217_728L);
  }

  @Test
  public void cascadeBitmapRotationMatchesTheLinearReferenceForEveryCycle() {
    assertCascadeRotation(64, 10);
    assertCascadeRotation(32, 22);
  }

  private static void assertCascadeRotation(int size, int shift) {
    long allBits = size == 64 ? -1L : 0xffff_ffffL;
    for (int cycle = 0; cycle < size; cycle++) {
      assertEquals(TimerWheel.nextCascade(0L, cycle, shift, size), Long.MAX_VALUE);
      for (int slot = 0; slot < size; slot++) {
        long occupied = 1L << slot;
        assertEquals(
            TimerWheel.nextCascade(occupied, cycle, shift, size),
            referenceNextCascade(occupied, cycle, shift, size),
            "cycle=" + cycle + ", slot=" + slot + ", size=" + size);
      }
      long currentAndLater =
          (1L << (cycle & (size - 1))) | (1L << ((cycle + 7) & (size - 1)));
      assertEquals(
          TimerWheel.nextCascade(currentAndLater, cycle, shift, size),
          referenceNextCascade(currentAndLater, cycle, shift, size));
      assertEquals(
          TimerWheel.nextCascade(allBits, cycle, shift, size),
          referenceNextCascade(allBits, cycle, shift, size));
    }
  }

  private static long referenceNextCascade(long occupied, long cycle, int shift, int size) {
    for (int offset = 1; offset <= size; offset++) {
      int slot = (int) (cycle + offset) & (size - 1);
      if ((occupied & (1L << slot)) != 0L) {
        return (cycle + offset) << shift;
      }
    }
    return Long.MAX_VALUE;
  }

  private static void assertCascadeConsumesDeadline(long targetTick) throws Exception {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(
        EntryTestSupport.entry(0, (int) targetTick, 0L), targetTick * TimerWheel.TICK_NANOS);
    Field tick = TimerWheel.class.getDeclaredField("tick");
    tick.setAccessible(true);
    tick.setLong(wheel, targetTick - 1L);

    wheel.advance(targetTick * TimerWheel.TICK_NANOS, (ignored, generation, address) -> {});
    assertEquals(
        wheel.scheduled(), 0L, "cascade did not return level deadline " + targetTick + " to L0");
  }
}
