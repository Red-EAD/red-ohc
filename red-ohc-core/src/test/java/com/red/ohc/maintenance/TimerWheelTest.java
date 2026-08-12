package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class TimerWheelTest {
  @Test
  public void maximumExpiryDoesNotOverflowIntoTheCurrentWheelRound() {
    TimerWheel wheel = new TimerWheel(0L);
    Entry entry = new Entry(0L, 0, 1, 0L);
    wheel.add(entry, Long.MAX_VALUE);

    AtomicInteger expiries = new AtomicInteger();
    wheel.advance(64L, (ignored, generation, address) -> expiries.incrementAndGet());

    assertEquals(expiries.get(), 0);
    assertTrue(wheel.hasPending());
  }

  @Test
  public void idleCatchUpSkipsEmptyTicksToReachALongOverdueDeadline() {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(new Entry(0L, 0, 23, 0L), 10_000L * 64L);

    assertEquals(
        wheel.advance(10_000L * 64L, (ignored, generation, address) -> {}),
        1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void overflowHeapIndexZeroRemainsRemovable() {
    TimerWheel wheel = new TimerWheel(0L);
    Entry entry = new Entry(0L, 0, 22, 0L);
    wheel.add(entry, 10_000_000_000L);
    assertTrue(entry.timerInOverflowHeap());

    wheel.remove(entry);
    assertEquals(wheel.scheduled(), 0L);
    assertTrue(!entry.timerScheduled());
  }

  @Test
  public void ceilPlacementHandlesTickBoundariesWithoutWheelRoundDelay() {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(new Entry(0L, 0, 3, 0L), 63L);
    wheel.add(new Entry(0L, 0, 4, 0L), 64L);
    wheel.add(new Entry(0L, 0, 5, 0L), 65L);

    wheel.advance(64L, (ignored, generation, address) -> {});
    assertEquals(wheel.scheduled(), 1L);
    wheel.advance(128L, (ignored, generation, address) -> {});
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void expiryBudgetContinuesTheCurrentBucketBeforeTheNextWheelRound() {
    TimerWheel wheel = new TimerWheel(0L);
    for (int index = 0; index < 2_049; index++) {
      wheel.add(new Entry(0L, 0, index + 10, 0L), 64L);
    }

    assertEquals(wheel.advance(64L, 1_024, (ignored, generation, address) -> {}), 1_024);
    assertEquals(
        wheel.scheduled(), 1_025L, "the over-budget entries must remain in the current bucket");

    assertEquals(wheel.advance(64L, 1_024, (ignored, generation, address) -> {}), 1_024);
    assertEquals(
        wheel.scheduled(), 1L, "the deferred entries must not wait for a full wheel round");
    assertEquals(wheel.advance(64L, 1_024, (ignored, generation, address) -> {}), 1);
    assertEquals(
        wheel.scheduled(), 0L, "the final deferred entry must not wait for a full wheel round");
  }

  @Test
  public void expiryBudgetUsesTheBinaryMaintenanceBatchSize() {
    TimerWheel wheel = new TimerWheel(0L);
    for (int index = 0; index < 1_025; index++) {
      wheel.add(new Entry(0L, 0, index + 3_000, 0L), 64L);
    }

    assertEquals(wheel.advance(64L, 1_024, (ignored, generation, address) -> {}), 1_024);
    assertEquals(wheel.scheduled(), 1L);
    assertEquals(wheel.advance(64L, 1_024, (ignored, generation, address) -> {}), 1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void idleCatchUpFindsALevelZeroSlotAfterWheelWrap() {
    long nowMillis = 100L * 64L;
    TimerWheel wheel = new TimerWheel(nowMillis);
    // tick=100 starts the search at slot 101. target=1114 maps back to slot 90, which is
    // in the same bitmap word but below the starting bit after one L0 rotation.
    wheel.add(new Entry(0L, 0, 6, 0L), 1_114L * 64L);

    assertEquals(wheel.advance(1_114L * 64L, (ignored, generation, address) -> {}), 1);
    assertEquals(wheel.scheduled(), 0L);
  }

  @Test
  public void cascadesEveryHierarchicalLevelBeforeTheDeadlineBucketIsConsumed() throws Exception {
    assertCascadeConsumesDeadline(1_024L);
    assertCascadeConsumesDeadline(65_536L);
    assertCascadeConsumesDeadline(4_194_304L);
    assertCascadeConsumesDeadline(134_217_728L);
  }

  private static void assertCascadeConsumesDeadline(long targetTick) throws Exception {
    TimerWheel wheel = new TimerWheel(0L);
    wheel.add(new Entry(0L, 0, (int) targetTick, 0L), targetTick * 64L);
    Field tick = TimerWheel.class.getDeclaredField("tick");
    tick.setAccessible(true);
    tick.setLong(wheel, targetTick - 1L);

    wheel.advance(targetTick * 64L, (ignored, generation, address) -> {});
    assertEquals(
        wheel.scheduled(), 0L, "cascade did not return level deadline " + targetTick + " to L0");
  }
}
