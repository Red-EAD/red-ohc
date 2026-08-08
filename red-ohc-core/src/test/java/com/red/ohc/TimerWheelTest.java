package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
    public void longTtlParksUntilTheNextCascadeInsteadOfWakingEveryBaseTick() {
        TimerWheel wheel = new TimerWheel(0L);
        wheel.add(new Entry(0L, 0, 2, 0L), 130_000L);

        assertTrue(wheel.nextDelayNanos(0L) > 64_000_000L,
                "a TTL outside L0 must schedule the L1 cascade rather than a 64ms poll");
    }

    @Test
    public void ceilPlacementHandlesTickBoundariesWithoutWheelRoundDelay() {
        TimerWheel wheel = new TimerWheel(0L);
        wheel.add(new Entry(0L, 0, 3, 0L), 63L);
        wheel.add(new Entry(0L, 0, 4, 0L), 64L);
        wheel.add(new Entry(0L, 0, 5, 0L), 65L);

        wheel.advance(64L, (ignored, generation, address) -> { });
        assertEquals(wheel.scheduled(), 1L);
        wheel.advance(128L, (ignored, generation, address) -> { });
        assertEquals(wheel.scheduled(), 0L);
    }

    @Test
    public void expiryBudgetContinuesTheCurrentBucketBeforeTheNextWheelRound() {
        TimerWheel wheel = new TimerWheel(0L);
        for (int index = 0; index < 1_001; index++) {
            wheel.add(new Entry(0L, 0, index + 10, 0L), 64L);
        }

        assertEquals(wheel.advance(64L, 1_000, (ignored, generation, address) -> { }), 1_000);
        assertEquals(wheel.scheduled(), 1L, "the over-budget entry must remain in the current bucket");

        assertEquals(wheel.advance(64L, 1_000, (ignored, generation, address) -> { }), 1);
        assertEquals(wheel.scheduled(), 0L, "the deferred entry must not wait for a full wheel round");
    }
}
