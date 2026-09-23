package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.storage.NativeMemory;

public final class EntryLinksTest {
  @Test
  public void nativeRecordsAreDenseAndRegistryIdsAreReused() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry first = EntryTestSupport.entry(memory, 1, 1, 11L, 0L);
    Entry second = EntryTestSupport.entry(memory, 1, 2, 22L, 0L);
    long beforeLinks = memory.allocated();
    try {
      int firstId = links.ensure(first);
      assertEquals(memory.allocated() - beforeLinks, 64L * 1024L + 4L * 1024L - 1L);
      int secondId = links.ensure(second);
      assertTrue(secondId != firstId);

      links.policyNext(first, second);
      assertEquals(links.policyNextEntry(first), second);
      assertNotNull(links.entry(firstId));

      links.policyNext(first, null);
      links.maybeRelease(first);
      assertNull(links.entry(firstId));
      Entry replacement = EntryTestSupport.entry(memory, 1, 3, 33L, 0L);
      try {
        assertEquals(links.ensure(replacement), firstId);
      } finally {
        memory.releaseEntry(replacement.nativeKeyAddress, replacement.keyAllocationLength());
      }
    } finally {
      links.close();
      memory.releaseEntry(first.nativeKeyAddress, first.keyAllocationLength());
      memory.releaseEntry(second.nativeKeyAddress, second.keyAllocationLength());
      memory.closeArenas();
    }
  }

  @Test
  public void retiredEntryReleasesItsUnlinkedNativeRecord() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(memory, 1, 4, 44L, 0L);
    try {
      int id = links.ensure(entry);
      entry.valueAddress = 2L;

      links.maybeRelease(entry);

      assertNull(links.entry(id));
      assertEquals(links.activeLinkCount(), 0);
      assertEquals(entry.policyLinkId(), 0);
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void terminalEntryClearsStaleMutationFlagsBeforeReleasingRecord() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(1, 7, 77L, 0L);
    try {
      int id = links.ensure(entry);
      assertTrue(entry.publishMutation(Entry.PENDING_UPDATE));
      assertTrue(entry.pendingFlags() != 0);

      entry.markDead();
      links.maybeRelease(entry);

      assertEquals(entry.pendingFlags(), 0);
      assertEquals(entry.policyLinkId(), 0);
      assertNull(links.entry(id));
    } finally {
      links.close();
    }
  }

  @Test
  public void stalePolicyPresentHintDoesNotKeepATerminalLinkAlive() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(1, 9, 99L, 0L);
    Method setPolicyPresent = Entry.class.getDeclaredMethod("setPolicyPresent", boolean.class);
    setPolicyPresent.setAccessible(true);
    try {
      int id = links.ensure(entry);
      entry.markDead();
      setPolicyPresent.invoke(entry, true);

      assertTrue(entry.policyPresent());
      entry.policyState(Entry.POLICY_NONE);
      setPolicyPresent.invoke(entry, true);

      links.maybeRelease(entry);

      assertFalse(entry.policyPresent());
      assertEquals(entry.policyLinkId(), 0);
      assertEquals(links.activeLinkCount(), 0);
      assertNull(links.entry(id));
    } finally {
      links.close();
    }
  }

  @Test
  public void liveEntryPendingMutationIsNotClearedByLinkReleaseAttempt() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(1, 8, 88L, 0L);
    try {
      links.ensure(entry);
      assertTrue(entry.publishMutation(Entry.PENDING_UPDATE));

      links.maybeRelease(entry);

      assertTrue(entry.pendingFlags() != 0);
      assertTrue(entry.policyLinkId() != 0);
    } finally {
      links.close();
    }
  }

  @Test
  public void logicalPageBoundaryKeepsAdjacentRecordsPacked() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry[] entries = new Entry[EntryLinks.RECORDS_PER_LOGICAL_PAGE + 1];
    try {
      for (int index = 0; index < entries.length; index++) {
        entries[index] = EntryTestSupport.entry(0, index + 1, index + 1L, 0L);
        assertEquals(links.ensure(entries[index]), index + 1);
      }

      links.policyNext(
          entries[EntryLinks.RECORDS_PER_LOGICAL_PAGE - 1],
          entries[EntryLinks.RECORDS_PER_LOGICAL_PAGE]);
      assertEquals(
          links.policyNextEntry(entries[EntryLinks.RECORDS_PER_LOGICAL_PAGE - 1]),
          entries[EntryLinks.RECORDS_PER_LOGICAL_PAGE]);
      assertEquals(links.nativeBytes(), 64L * 1024L + 4L * 1024L - 1L);
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void policyMetadataMirrorSurvivesPackedLinkUpdates() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry first = EntryTestSupport.entry(memory, 1, 21, 0L, 0L);
    Entry second = EntryTestSupport.entry(memory, 1, 22, 0L, 0L);
    try {
      int firstId = links.ensure(first);
      int secondId = links.ensure(second);
      links.policyState(firstId, Entry.POLICY_S4_SKIP);
      links.policyAccessCount(firstId, 3);
      links.keyHash(firstId, 0);
      links.policyByteWeight(firstId, 777);
      links.policyState(secondId, Entry.POLICY_TINY_PROTECTED);
      links.policyAccessCount(secondId, 2);

      links.linkHead(
          firstId,
          second,
          EntryLinks.POLICY_PREVIOUS_OFFSET,
          EntryLinks.POLICY_NEXT_OFFSET);
      links.linkHead(
          firstId,
          second,
          EntryLinks.TIMER_PREVIOUS_OFFSET,
          EntryLinks.TIMER_NEXT_OFFSET);

      assertEquals(links.policyPrev(firstId), 0);
      assertEquals(links.policyNext(firstId), secondId);
      assertEquals(links.timerNext(firstId), secondId);
      assertEquals(links.policyState(firstId), Entry.POLICY_S4_SKIP);
      assertEquals(links.policyAccessCount(firstId), 3);
      assertEquals(links.keyHash(firstId), 0, "zero is a valid mirrored hash");
      assertEquals(links.policyByteWeight(firstId), 777L);
      assertEquals(links.policyState(secondId), Entry.POLICY_TINY_PROTECTED);
      assertEquals(links.policyAccessCount(secondId), 2);

      links.unlink(
          firstId, EntryLinks.POLICY_PREVIOUS_OFFSET, EntryLinks.POLICY_NEXT_OFFSET);
      assertEquals(links.policyNext(firstId), 0);
      assertEquals(links.policyPrev(secondId), 0);
      assertEquals(links.policyState(firstId), Entry.POLICY_S4_SKIP);
      assertEquals(links.policyAccessCount(firstId), 3);
      assertEquals(links.policyState(secondId), Entry.POLICY_TINY_PROTECTED);

      links.clear(firstId, EntryLinks.TIMER_PREVIOUS_OFFSET, EntryLinks.TIMER_NEXT_OFFSET);
      assertEquals(links.timerNext(firstId), 0);
      assertEquals(links.policyState(firstId), Entry.POLICY_S4_SKIP);
      assertEquals(links.policyByteWeight(firstId), 777L);
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void releasedRecordIsFullyClearedBeforeFreeListReuse() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry first = EntryTestSupport.entry(memory, 1, 31, 0x1234L, 0L);
    Entry replacement = EntryTestSupport.entry(memory, 1, 32, 0x5678L, 0L);
    try {
      int firstId = links.ensure(first);
      links.keyHash(firstId, -1);
      links.policyByteWeight(firstId, 909);
      links.policyState(firstId, Entry.POLICY_TINY_PROTECTED);
      links.policyAccessCount(firstId, 3);
      entryPolicyNone(first, links, firstId);
      links.maybeRelease(first);
      assertEquals(first.policyLinkId(), 0);

      int replacementId = links.ensure(replacement);
      assertEquals(replacementId, firstId);
      assertEquals(links.policyState(replacementId), Entry.POLICY_NONE);
      assertEquals(links.policyAccessCount(replacementId), 0);
      assertEquals(links.keyHash(replacementId), 0);
      assertEquals(links.policyByteWeight(replacementId), 0L);
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void policyMetadataUsesDedicatedPackedWordAndFixedOffsets() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(memory, 1, 41, 0L, 0L);
    Method recordAddress = EntryLinks.class.getDeclaredMethod("recordAddress", int.class);
    recordAddress.setAccessible(true);
    try {
      int id = links.ensure(entry);
      links.policyState(id, Entry.POLICY_S4_SKIP);
      links.policyAccessCount(id, 3);
      links.policyByteWeight(id, 777);
      links.keyHash(id, 0x5566_7788);

      long address = (Long) recordAddress.invoke(links, id);
      assertEquals(NativeMemory.getInt(address), 0, "link word must not contain policy metadata");
      assertEquals(
          NativeMemory.getInt(address + 16L),
          Entry.POLICY_S4_SKIP | (3 << 3),
          "state/access must use the dedicated metadata word");
      assertEquals(NativeMemory.getInt(address + 20L), 777);
      assertEquals(NativeMemory.getInt(address + 24L), 0x5566_7788);
      assertEquals(links.policyState(id), Entry.POLICY_S4_SKIP);
      assertEquals(links.policyAccessCount(id), 3);
      assertEquals(links.policyByteWeight(id), 777);
      assertEquals(links.keyHash(id), 0x5566_7788);

      links.policyState(id, Entry.POLICY_LRU);
      assertEquals(links.policyAccessCount(id), 3);
      links.policyAccessCount(id, 1);
      assertEquals(links.policyState(id), Entry.POLICY_LRU);
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  @Test
  public void policyWeightMirrorRejectsValuesOutsideItsIntRepresentation() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    Entry entry = EntryTestSupport.entry(memory, 1, 42, 0L, 0L);
    try {
      int id = links.ensure(entry);
      boolean rejected = false;
      try {
        links.policyByteWeight(id, -1);
      } catch (IllegalArgumentException expected) {
        rejected = true;
      }
      assertTrue(rejected, "the mirror must reject negative weights");
    } finally {
      links.close();
      memory.closeArenas();
    }
  }

  private static void entryPolicyNone(Entry entry, EntryLinks links, int linkId) {
    entry.policyState(Entry.POLICY_NONE);
    links.policyState(linkId, Entry.POLICY_NONE);
    links.policyAccessCount(linkId, 0);
    links.policyByteWeight(linkId, 0);
  }

}
