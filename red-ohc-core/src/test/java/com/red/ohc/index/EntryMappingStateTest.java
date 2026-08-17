package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public final class EntryMappingStateTest {
  @Test
  public void retiredEntryIsImmediatelyInvisibleAndCannotBeClaimedAgain() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    assertTrue(entry.isAlive());
    assertTrue(entry.claimWriter());
    entry.markRetired();
    entry.finishWriter();
    assertFalse(entry.isAlive());
    assertFalse(entry.claimWriter());
  }

  @Test(timeOut = 2_000L)
  public void reliableRemovalClaimFailsImmediatelyWhenAnotherRemovalOwnsTheEntry() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    assertTrue(entry.tryBeginPending(Entry.PENDING_REMOVE));
    assertFalse(entry.tryBeginPending(Entry.PENDING_REMOVE));
    entry.completePendingClaim();
  }

  @Test
  public void completedPendingPublicationAdvancesTheMutationVersion() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    assertEquals(entry.mutationVersion(), 0L);
    assertTrue(entry.tryBeginPending(Entry.PENDING_REMOVE));
    entry.completePendingClaim();
    assertEquals(entry.mutationVersion(), 1L);
    assertEquals(
        entry.appliedVersion(), 0L, "the maintenance worker must publish the applied version");
    assertTrue(entry.markAppliedVersion(1L));
    assertEquals(entry.appliedVersion(), 1L);
  }

  @Test
  public void appliedVersionDoesNotAdvanceAfterAConcurrentMutation() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    assertTrue(entry.publishMutation(Entry.PENDING_ADD));
    assertTrue(entry.markAppliedVersion(1L));

    entry.publishMutation(Entry.PENDING_UPDATE);
    assertFalse(entry.markAppliedVersion(1L));
    assertEquals(entry.appliedVersion(), 1L);
  }

  @Test
  public void maintenanceVersionSurvivesPolicyMetadataUpdates() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    entry.publishMutation(Entry.PENDING_ADD);
    assertTrue(entry.markAppliedVersion(1L));

    entry.policyState(Entry.POLICY_TINY_PROTECTED);
    entry.policyAccessCount(3);

    assertEquals(entry.mutationVersion(), 1L);
    assertEquals(entry.appliedVersion(), 1L);
    assertEquals(entry.policyState(), Entry.POLICY_TINY_PROTECTED);
    assertEquals(entry.policyAccessCount(), 3);
  }

  @Test
  public void policyPresenceIsPublishedInTheHeapPendingWord() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    assertEquals(entry.pendingFlags, 0);
    entry.policyState(Entry.POLICY_LRU);
    assertTrue(entry.policyPresent());
    entry.policyState(Entry.POLICY_NONE);
    assertFalse(entry.policyPresent());
    assertEquals(entry.pendingFlags, 0);
  }

  @Test
  public void advisoryMutationUsesPackedVersionsWithoutAWriterClaim() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);

    assertTrue(entry.publishMutation(Entry.PENDING_ADD));
    assertEquals(entry.mutationVersion(), 1L);
    assertEquals(entry.appliedVersion(), 0L);
    assertFalse(entry.isWriterLocked());

    assertFalse(entry.publishMutation(Entry.PENDING_UPDATE));
    assertEquals(entry.mutationVersion(), 2L);
    assertEquals(entry.takePending(), Entry.PENDING_ADD | Entry.PENDING_UPDATE);
    assertTrue(entry.markAppliedVersion(2L));
    assertEquals(entry.appliedVersion(), 2L);
  }

  @Test
  public void mutationVersionDoesNotWrapWhileMaintenanceIsBehind() {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    EntryTestSupport.maintenanceMeta(entry, (0xffffffffL << 32) | 0xfffffffeL);

    assertTrue(entry.publishMutation(Entry.PENDING_UPDATE));
    assertEquals(
        entry.mutationVersion(),
        0xffffffffL,
        "the producer must fence the version lane instead of wrapping over an unapplied mutation");
  }

  @Test(timeOut = 2_000L)
  public void laterMutationDoesNotWaitForTheRolloverFenceToDrain() throws Exception {
    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    EntryTestSupport.maintenanceMeta(entry, (0xffffffffL << 32) | 0xffffffffL);
    assertTrue(entry.publishMutation(Entry.PENDING_ADD));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> later = executor.submit(() -> entry.publishMutation(Entry.PENDING_UPDATE));
      assertFalse(later.get(1L, TimeUnit.SECONDS), "the coalesced mutation is not a new queue item");

      assertEquals(entry.takePending(), Entry.PENDING_ADD | Entry.PENDING_UPDATE);
      assertTrue(entry.tryRolloverMaintenanceVersion());
      assertEquals(entry.mutationVersion(), 0L);
    } finally {
      executor.shutdownNow();
    }
  }
}
