package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;

public class PageIdReuseTest {
  private static final int CACHE_LINE_LONGS = 8;

  @Test
  public void allocatorUses64BitGenerationSafeHandlesAndPerClassLifoStacks() throws Exception {
    Field pageBits = WriterArena.class.getDeclaredField("HANDLE_PAGE_BITS");
    Field generationBits = WriterArena.class.getDeclaredField("HANDLE_GENERATION_BITS");
    assertEquals(pageBits.getInt(null), 24);
    assertEquals(generationBits.getInt(null), 26);

    Method handleAt = WriterArena.Page.class.getDeclaredMethod("handleAt", int.class);
    assertEquals(handleAt.getReturnType(), long.class);

    Field nextPageIdField = NativeMemory.Memory.class.getDeclaredField("nextPageId");
    assertEquals(nextPageIdField.getType(), AtomicLong.class);

    Field sizeClasses = WriterArena.class.getDeclaredField("sizeClasses");
    assertEquals(sizeClasses.getType(), WriterArena.SizeClassState[].class);

    Field readyStacks = NativeMemory.Memory.class.getDeclaredField("readyPageStacks");
    assertEquals(readyStacks.getType(), AtomicLongArray.class);

    Field readyNext = NativeMemory.Memory.class.getDeclaredField("readyPageNext");
    assertEquals(readyNext.getType().getSimpleName(), "IntChunkTable");

    for (Field field : WriterArena.Page.class.getDeclaredFields()) {
      assertFalse(
          !Modifier.isStatic(field.getModifiers()) && field.getType().isArray(),
          "per-page heap metadata must stay fixed-size instead of scaling with native slots: "
              + field.getName());
    }

    for (Field field : WriterArena.class.getDeclaredFields()) {
      assertNotEquals(
          field.getName(),
          "partialDirectory",
          "sticky pages must not retain the old shared partial-page directory");
    }

    for (Class<?> nested : NativeMemory.Memory.class.getDeclaredClasses()) {
      assertNotEquals(
          nested.getSimpleName(),
          "PageIdExhaustedException",
          "page-id exhaustion must not be a normal allocator result");
    }
  }

  @Test
  public void pageIdsContinuePastTheLegacy18BitBoundary() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      Field nextPageIdField = NativeMemory.Memory.class.getDeclaredField("nextPageId");
      nextPageIdField.setAccessible(true);
      AtomicLong nextPageId = (AtomicLong) nextPageIdField.get(memory);
      long pageId = (1L << 18) + 1L;
      nextPageId.set(pageId);

      WriterArena.Page page = memory.tryAcquireEntryPage(0);
      assertNotNull(page);
      assertEquals(page.id & 0xffff_ffffL, pageId);
      memory.returnUnusedPage(page);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readyPageStackHeadsDoNotShareCacheLinesAcrossSizeClasses() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      Field readyStacksField = NativeMemory.Memory.class.getDeclaredField("readyPageStacks");
      readyStacksField.setAccessible(true);
      AtomicLongArray readyStacks = (AtomicLongArray) readyStacksField.get(memory);
      assertEquals(
          readyStacks.length(),
          SizeClasses.count() * CACHE_LINE_LONGS,
          "each independently mutated size-class head must reserve one 64-byte cache line");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void freeBitmapPublicationReportsOnlyEmptyToNonEmptyTransitions() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena.Page page = memory.tryAcquireEntryPage(0);
    assertNotNull(page);
    try {
      Method orFreeBits =
          WriterArena.Page.class.getDeclaredMethod("orFreeBits", int.class, long.class);
      orFreeBits.setAccessible(true);
      Object firstPublication = orFreeBits.invoke(page, 0, 1L);
      assertNotNull(
          firstPublication,
          "bitmap publication must expose whether the word was previously empty");
      assertEquals(
          ((Number) firstPublication).longValue(),
          0L,
          "the first free in a word must report an empty-to-non-empty transition");
      Object secondPublication = orFreeBits.invoke(page, 0, 2L);
      assertNotNull(
          secondPublication,
          "bitmap publication must expose the prior word for summary suppression");
      assertEquals(
          ((Number) secondPublication).longValue(),
          1L,
          "later frees in the same word must not request another summary publication");
    } finally {
      memory.returnUnusedPage(page);
    }
  }

  @Test
  public void pageAcquisitionPreparesReadyLinkStorageBeforePublication() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    WriterArena.Page page = memory.tryAcquireEntryPage(0);
    assertNotNull(page);
    try {
      Field readyNextField = NativeMemory.Memory.class.getDeclaredField("readyPageNext");
      readyNextField.setAccessible(true);
      Object readyNext = readyNextField.get(memory);
      Field chunksField = readyNext.getClass().getDeclaredField("chunks");
      chunksField.setAccessible(true);
      AtomicReferenceArray<?> chunks = (AtomicReferenceArray<?>) chunksField.get(readyNext);
      assertTrue(
          chunks.length() <= 1 << 12,
          "ready-link outer storage must be bounded to the 24-bit page-id namespace");
      AtomicIntegerArray prepared = (AtomicIntegerArray) chunks.get(0);
      assertNotNull(
          prepared,
          "a live page must never reach the allocation-free ready publication path unprepared");
      assertTrue(
          prepared.length() <= 1 << 12,
          "the first pooled page must not allocate a 256 KiB ready-link chunk");
      assertEquals(
          (long) chunks.length() * prepared.length(),
          1L << WriterArena.HANDLE_PAGE_BITS,
          "ready-link storage must cover exactly the valid page-id namespace");
    } finally {
      memory.returnUnusedPage(page);
    }
  }

  @Test
  public void freePageIdHeadUsesAStampForEveryStackMutation() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      Field headField = NativeMemory.Memory.class.getDeclaredField("freePageIdHead");
      headField.setAccessible(true);
      Object headValue = headField.get(memory);
      assertTrue(headValue instanceof AtomicLong, "free page ids require a stamped head");
      AtomicLong head = (AtomicLong) headValue;

      WriterArena.Page first = memory.tryAcquireEntryPage(0);
      assertNotNull(first);
      int initialStamp = (int) (head.get() >>> 32);
      memory.returnUnusedPage(first);
      int pushedStamp = (int) (head.get() >>> 32);
      assertEquals(pushedStamp, initialStamp + 1);

      WriterArena.Page second = memory.tryAcquireEntryPage(0);
      assertNotNull(second);
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
    WriterArena.Page first = memory.tryAcquireEntryPage(0);
    assertNotNull(first);
    try {
      int id = first.id;
      long pageKey = first.pageKey;
      memory.returnUnusedPage(first);

      WriterArena.Page second = memory.tryAcquireEntryPage(0);
      assertNotNull(second);
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
  public void physicallyFreedPageAllocatesANewDescriptorForTheNewGeneration() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    WriterArena.Page first = memory.tryAcquireEntryPage(0);
    assertNotNull(first);
    try {
      long oldHandle = first.handleAt(0);
      long oldPageKey = first.pageKey;
      long oldAddress = first.address;
      memory.returnUnusedPage(first);

      WriterArena.Page second = memory.tryAcquireEntryPage(0);
      assertNotNull(second);
      try {
        assertNotSame(
            second,
            first,
            "a new native page generation must not reuse the old Java descriptor");
        assertEquals(first.pageKey, oldPageKey, "the old descriptor must remain generation-stable");
        assertEquals(first.address, oldAddress, "the old descriptor must remain address-stable");
        assertNull(
            memory.pageForHandle(oldHandle),
            "a stale handle must not resolve through the newly registered page generation");
      } finally {
        memory.returnUnusedPage(second);
      }
    } finally {
      memory.closeArenas();
    }
  }

}
