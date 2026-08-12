package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;

public class PageIdReuseTest {
  @Test
  public void freePageIdHeadUsesAStampForEveryStackMutation() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      Field headField = NativeMemory.Memory.class.getDeclaredField("freePageIdHead");
      headField.setAccessible(true);
      Object headValue = headField.get(memory);
      assertTrue(headValue instanceof AtomicLong, "free page ids require a stamped head");
      AtomicLong head = (AtomicLong) headValue;

      WriterArena.Page first = memory.acquireEntryPage(0);
      int initialStamp = (int) (head.get() >>> 32);
      memory.returnUnusedPage(first);
      int pushedStamp = (int) (head.get() >>> 32);
      assertEquals(pushedStamp, initialStamp + 1);

      WriterArena.Page second = memory.acquireEntryPage(0);
      int poppedStamp = (int) (head.get() >>> 32);
      assertEquals(poppedStamp, pushedStamp + 1);
      memory.returnUnusedPage(second);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void physicallyFreedPageReusesItsSparseIdWithANewVersionStamp() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena.Page first = memory.acquireEntryPage(0);
    try {
      int id = first.id;
      int pageKey = first.pageKey;
      memory.returnUnusedPage(first);

      WriterArena.Page second = memory.acquireEntryPage(0);
      try {
        assertEquals(second.id, id);
        assertNotEquals(
            second.pageKey,
            pageKey,
            "a reused sparse id must reject stale allocator handles from the old page");
      } finally {
        memory.returnUnusedPage(second);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void exhaustedPageIdsDoNotStrandThePooledPageAdmissionCount() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      Field nextPageIdField = NativeMemory.Memory.class.getDeclaredField("nextPageId");
      nextPageIdField.setAccessible(true);
      AtomicInteger nextPageId = (AtomicInteger) nextPageIdField.get(memory);
      nextPageId.set(WriterArena.PAGE_ID_MASK + 1);

      try {
        memory.tryAcquireEntryPage(0);
        throw new AssertionError("page-id exhaustion must reject a new pooled page");
      } catch (NativeMemory.AllocationLimitException expected) {
        // The failed admission must not retain a phantom pooled page.
      }

      Field pooledPageCountField = NativeMemory.Memory.class.getDeclaredField("pooledPageCount");
      pooledPageCountField.setAccessible(true);
      AtomicInteger pooledPageCount = (AtomicInteger) pooledPageCountField.get(memory);
      assertEquals(pooledPageCount.get(), 0);
      assertEquals(memory.allocated(), 0L);
    } finally {
      memory.closeArenas();
    }
  }
}
