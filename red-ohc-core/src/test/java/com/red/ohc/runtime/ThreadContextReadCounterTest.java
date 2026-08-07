package com.red.ohc.runtime;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public final class ThreadContextReadCounterTest {
    @Test
    public void oneReadSequenceDrivesStatsAndAccessSampling() {
        ThreadContext context = new ThreadContext();
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
}
