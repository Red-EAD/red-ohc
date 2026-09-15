package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

public final class EntryNativeMetadataTest {
  @Test
  public void allocatorReuseResetsEveryNativeMetadataWord() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena arena = memory.newWriterArena();
    long allocation = Entry.keyAllocationLengthForKeyLength(0);
    long firstAddress = arena.allocate(allocation);
    try {
      Entry first = new Entry(firstAddress, 0, 0L);
      first.initializeNativeMetadata();
      first.policyState(Entry.POLICY_TINY_PROTECTED);
      first.policyAccessCount(3);
      first.timerHeapIndex(0);
      first.timerDeadlineTick(123_456L);
      first.policyByteWeight(777L);
      assertTrue(first.publishMutation(Entry.PENDING_UPDATE));
      assertTrue(first.markAppliedVersion(1L));

      memory.releaseEntry(firstAddress, allocation);
      long reusedAddress = arena.allocate(allocation);
      try {
        assertEquals(reusedAddress, firstAddress, "the test must exercise allocator slot reuse");
        Entry reused = new Entry(reusedAddress, 0, 0L);
        reused.initializeNativeMetadata();

        assertEquals(reused.policyState(), Entry.POLICY_NONE);
        assertEquals(reused.policyAccessCount(), 0);
        assertEquals(reused.timerInOverflowHeap(), false);
        assertEquals(reused.timerScheduled(), false);
        assertEquals(reused.timerDeadlineTick(), 0L);
        assertEquals(reused.policyByteWeight(), 0L);
        assertEquals(reused.currentValueAllocation(), 0L);
        assertEquals(reused.mutationVersion(), 0L);
        assertEquals(reused.appliedVersion(), 0L);
      } finally {
        memory.releaseEntry(reusedAddress, allocation);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void currentValueAllocationIsPublishedInNativeMetadata() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    long allocation = Entry.keyAllocationLengthForKeyLength(0);
    long keyAddress = memory.newWriterArena().allocate(allocation);
    try {
      Entry entry = new Entry(keyAddress, 0, 0L);
      entry.initializeNativeMetadata();
      entry.currentValueAllocation(72L);
      assertEquals(entry.currentValueAllocation(), 72L);
    } finally {
      memory.releaseEntry(keyAddress, allocation);
      memory.closeArenas();
    }
  }
}
