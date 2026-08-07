package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.TimerWheel;

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
}
