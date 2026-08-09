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
            entry.completePendingClaim();
            assertFalse(second.get(1L, TimeUnit.SECONDS));
            entry.completePendingClaim();
            assertEquals(entry.takePending(), Entry.PENDING_ADD | Entry.PENDING_UPDATE);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void coalescedMutationCannotBeConsumedBeforeItsWriterPublishes() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertTrue(entry.beginPending(Entry.PENDING_ADD));
        entry.completePendingClaim();
        assertTrue(entry.publishPending());

        // A later writer coalesces with the queued ADD. The actor may already have observed
        // the transport item, but it must not clear the coalesced UPDATE until the writer has
        // published the replacement pointer and released this claim.
        assertFalse(entry.beginPending(Entry.PENDING_UPDATE));
        assertEquals(entry.takePending(), Entry.PENDING_BUSY);

        entry.completePendingClaim();
        assertEquals(entry.takePending(), Entry.PENDING_ADD | Entry.PENDING_UPDATE);
    }

    @Test
    public void completedPendingPublicationAdvancesTheMutationVersion() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertEquals(entry.mutationVersion(), 0L);
        assertTrue(entry.beginPending(Entry.PENDING_ADD));
        entry.completePendingClaim();
        assertEquals(entry.mutationVersion(), 1L);
        assertEquals(entry.appliedVersion(), 0L,
                "the maintenance worker must publish the applied version");
        assertTrue(entry.markAppliedVersion(1L));
        assertEquals(entry.appliedVersion(), 1L);
    }

    @Test
    public void appliedVersionDoesNotAdvanceAfterAConcurrentMutation() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertTrue(entry.beginPending(Entry.PENDING_ADD));
        entry.completePendingClaim();
        assertTrue(entry.markAppliedVersion(1L));

        assertFalse(entry.beginPending(Entry.PENDING_UPDATE));
        entry.completePendingClaim();
        assertFalse(entry.markAppliedVersion(1L));
        assertEquals(entry.appliedVersion(), 1L);
    }

    @Test
    public void maintenanceVersionSurvivesPolicyMetadataUpdates() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertTrue(entry.beginPending(Entry.PENDING_ADD));
        entry.completePendingClaim();
        assertTrue(entry.markAppliedVersion(1L));

        entry.policyState(Entry.POLICY_TINY_PROTECTED);
        entry.policyAccessCount(3);

        assertEquals(entry.mutationVersion(), 1L);
        assertEquals(entry.appliedVersion(), 1L);
        assertEquals(entry.policyState(), Entry.POLICY_TINY_PROTECTED);
        assertEquals(entry.policyAccessCount(), 3);
    }
}
