package com.red.ohc.index;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public final class EntryMappingStateTest {
    @Test
    public void retiredEntryIsImmediatelyInvisibleAndCannotBeClaimedAgain() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertTrue(entry.isAlive());
        assertTrue(entry.claimWriter());
        entry.markRetired();
        entry.finishWriter();
        assertFalse(entry.isAlive());
        assertFalse(entry.claimWriter());
    }

    @Test(timeOut = 2_000L)
    public void delayedPendingClaimCoalescesTheSecondSameEntryMutation() throws Exception {
        Entry entry = new Entry(0L, 1, 7, 0L);
        assertTrue(entry.beginPending(Entry.PENDING_ADD));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> second = executor.submit(() -> entry.beginPending(Entry.PENDING_UPDATE));
            Thread.sleep(5L);
            entry.commitPendingClaim(Entry.PENDING_ADD);
            assertFalse(second.get(1L, TimeUnit.SECONDS));
            assertEquals(entry.takePending(), Entry.PENDING_ADD | Entry.PENDING_UPDATE);
        } finally {
            executor.shutdownNow();
        }
    }
}
