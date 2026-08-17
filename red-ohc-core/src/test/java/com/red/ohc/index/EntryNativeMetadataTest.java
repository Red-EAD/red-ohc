package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;

public final class EntryNativeMetadataTest {
  @AfterSuite(alwaysRun = true)
  public void releaseEntryFixtures() {
    EntryTestSupport.close();
  }

  @Test
  public void allocatorReuseResetsEveryNativeMetadataWord() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long allocation = Entry.keyAllocationLengthForKeyLength(0);
    long firstAddress = memory.newWriterArena().allocate(allocation);
    try {
      Entry first = new Entry(firstAddress, 0, 1, 1L, 0L);
      first.initializeNativeMetadata();
      first.policyState(Entry.POLICY_TINY_PROTECTED);
      first.policyAccessCount(3);
      first.timerHeapIndex(0);
      first.timerDeadlineTick(123_456L);
      first.policyByteWeight(777L);
      assertTrue(first.publishMutation(Entry.PENDING_UPDATE));
      assertTrue(first.markAppliedVersion(1L));

      memory.releaseEntry(firstAddress, allocation);
      long reusedAddress = memory.newWriterArena().allocate(allocation);
      try {
        assertEquals(reusedAddress, firstAddress, "the test must exercise allocator slot reuse");
        Entry reused = new Entry(reusedAddress, 0, 2, 2L, 0L);
        reused.initializeNativeMetadata();

        assertEquals(reused.policyState(), Entry.POLICY_NONE);
        assertEquals(reused.policyAccessCount(), 0);
        assertEquals(reused.timerInOverflowHeap(), false);
        assertEquals(reused.timerScheduled(), false);
        assertEquals(reused.timerDeadlineTick(), 0L);
        assertEquals(reused.policyByteWeight(), 0L);
        assertEquals(reused.mutationVersion(), 0L);
        assertEquals(reused.appliedVersion(), 0L);
      } finally {
        memory.releaseEntry(reusedAddress, allocation);
      }
    } finally {
      memory.closeArenas();
    }
  }
}
