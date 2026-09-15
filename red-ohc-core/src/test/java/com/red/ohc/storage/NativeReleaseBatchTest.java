package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ThreadContext;

public final class NativeReleaseBatchTest {
  @Test
  public void mixedNativeColumnsReusePagesAndSkipClearedRecords() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(15L * Long.BYTES);
    long allocations = columns + 5L * Long.BYTES;
    long handles = allocations + 5L * Long.BYTES;
    try {
      WriterArena first = memory.newWriterArena();
      WriterArena second = memory.newWriterArena();
      long[] entries = {first.allocate(112L), second.allocate(112L),
          first.allocate(5_152L), first.allocate(32_769L), 0L};
      long[] bytes = {112L, 112L, 5_152L, 32_769L, 0L};
      writeColumns(columns, allocations, handles, entries, bytes);
      long[] expectedHandles = readColumn(handles, entries.length);

      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, entries.length), 4);
      assertScratchReleased(context);
      assertEquals(context.releaseBatchRecords(), 4);
      assertEquals(context.releaseBatchBytes(),
          2L * WriterArena.allocationWeight(112L)
              + WriterArena.allocationWeight(5_152L)
              + WriterArena.allocationWeight(32_769L));
      assertAddressesCleared(columns, entries.length);
      assertColumnEquals(allocations, bytes);
      assertColumnEquals(handles, expectedHandles);
      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, entries.length), 0);

      Set<Long> smallEntries = new HashSet<>();
      smallEntries.add(first.allocate(112L));
      smallEntries.add(second.allocate(112L));
      assertEquals(smallEntries.size(), 2);
      assertTrue(smallEntries.contains(entries[0]));
      assertTrue(smallEntries.contains(entries[1]));
      for (long entry : smallEntries) {
        memory.releaseEntry(entry, 112L);
      }
      long large = first.allocate(5_152L);
      assertEquals(large, entries[2]);
      memory.releaseEntry(large, 5_152L);
    } finally {
      memory.free(columns, 15L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void aBadSlotInAnAlreadySeenPageFailsBeforeAnyPooledRelease() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(9L * Long.BYTES);
    long allocations = columns + 3L * Long.BYTES;
    long handles = allocations + 3L * Long.BYTES;
    try {
      WriterArena arena = memory.newWriterArena();
      long[] entries = {arena.allocate(112L), arena.allocate(5_152L), arena.allocate(112L)};
      long[] bytes = {112L, 5_152L, 112L};
      writeColumns(columns, allocations, handles, entries, bytes);
      long firstHandle = NativeMemory.getLong(entries[0] - Long.BYTES);
      long lastHandle = NativeMemory.getLong(entries[2] - Long.BYTES);
      NativeMemory.putLong(handles + 2L * Long.BYTES, firstHandle);
      try {
        memory.releaseEntryBatch(context, columns, allocations, handles, entries.length);
        fail("a matching page key must not bypass per-record slot/address validation");
      } catch (IllegalStateException expected) {
        assertEquals(context.releaseBatchRecords(), 0);
        assertScratchReleased(context);
        for (int index = 0; index < entries.length; index++) {
          assertEquals(NativeMemory.getLong(columns + (long) index * Long.BYTES), entries[index]);
        }
        assertEquals(NativeMemory.getLong(handles + 2L * Long.BYTES), firstHandle);
      } finally {
        NativeMemory.putLong(handles + 2L * Long.BYTES, lastHandle);
      }
      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, entries.length), 3);
      assertAddressesCleared(columns, entries.length);
    } finally {
      memory.free(columns, 9L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void stalePageGenerationIsRejectedBeforeRelease() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(3L * Long.BYTES);
    long allocations = columns + Long.BYTES;
    long handles = allocations + Long.BYTES;
    try {
      WriterArena arena = memory.newWriterArena();
      long entry = arena.allocate(112L);
      long handle = NativeMemory.getLong(entry - Long.BYTES);
      NativeMemory.putLong(columns, entry);
      NativeMemory.putLong(allocations, 112L);
      NativeMemory.putLong(
          handles, handle ^ (1L << (WriterArena.HANDLE_SLOT_BITS + 32)));
      try {
        memory.releaseEntryBatch(context, columns, allocations, handles, 1);
        fail("the page generation must match the retirement handle");
      } catch (IllegalStateException expected) {
        assertEquals(context.releaseBatchRecords(), 0);
        assertScratchReleased(context);
        assertEquals(NativeMemory.getLong(columns), entry);
      } finally {
        NativeMemory.putLong(handles, handle);
      }
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
    } finally {
      memory.free(columns, 3L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void zeroPageKeyHandleUsesTheUnknownHandleFailurePath() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(3L * Long.BYTES);
    long allocations = columns + Long.BYTES;
    long handles = allocations + Long.BYTES;
    long entry = 0L;
    try {
      WriterArena arena = memory.newWriterArena();
      entry = arena.allocate(112L);
      long validHandle = NativeMemory.Memory.entryAllocatorHandle(entry);
      long slotOnlyHandle = WriterArena.Page.slotOf(validHandle);
      if (slotOnlyHandle == 0L) {
        slotOnlyHandle = 1L;
      }
      NativeMemory.putLong(columns, entry);
      NativeMemory.putLong(allocations, 112L);
      NativeMemory.putLong(handles, slotOnlyHandle);

      try {
        memory.releaseEntryBatch(context, columns, allocations, handles, 1);
        fail("a zero page key must not resolve an allocator descriptor");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage().contains("unknown allocator slot handle"));
        assertEquals(NativeMemory.getLong(columns), entry);
      }

      NativeMemory.putLong(handles, validHandle);
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      entry = 0L;
    } finally {
      if (entry != 0L) {
        memory.releaseEntry(entry, 112L);
      }
      memory.free(columns, 3L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void callbackFailureKeepsProgressAndRetryReleasesOnlyTheRemainingGroup() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(12L * Long.BYTES);
    long allocations = columns + 4L * Long.BYTES;
    long handles = allocations + 4L * Long.BYTES;
    try {
      WriterArena arena = memory.newWriterArena();
      long[] entries = {arena.allocate(112L), arena.allocate(5_152L),
          arena.allocate(112L), arena.allocate(5_152L)};
      writeColumns(
          columns,
          allocations,
          handles,
          entries,
          new long[] {112L, 5_152L, 112L, 5_152L});
      AtomicBoolean firstCallback = new AtomicBoolean(true);
      arena.setRetirementHookForTest(() -> {
        if (firstCallback.getAndSet(false)) {
          throw new IllegalStateException("availability failure after group publication");
        }
      });
      try {
        memory.releaseEntryBatch(context, columns, allocations, handles, entries.length);
        fail("the injected callback failure must propagate");
      } catch (IllegalStateException expected) {
        assertEquals(context.releaseBatchRecords(), 2);
        assertScratchReleased(context);
        assertEquals(context.releaseBatchBytes(), 2L * WriterArena.allocationWeight(112L));
        assertEquals(NativeMemory.getLong(columns), 0L);
        assertEquals(NativeMemory.getLong(columns + 2L * Long.BYTES), 0L);
        assertEquals(NativeMemory.getLong(columns + Long.BYTES), entries[1]);
        assertEquals(NativeMemory.getLong(columns + 3L * Long.BYTES), entries[3]);
      }
      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, entries.length), 2);
      assertScratchReleased(context);
      assertEquals(context.releaseBatchBytes(), 2L * WriterArena.allocationWeight(5_152L));
      assertAddressesCleared(columns, entries.length);
      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, entries.length), 0);
    } finally {
      memory.free(columns, 12L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void lastLiveSlotInvalidatesTheScopedMemoBeforeAvailabilityCallback() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(3L * Long.BYTES);
    long allocations = columns + Long.BYTES;
    long handles = allocations + Long.BYTES;
    try {
      WriterArena arena = memory.newWriterArena();
      long first = arena.allocate(112L);
      long last = arena.allocate(112L);
      long firstHandle = NativeMemory.Memory.entryAllocatorHandle(first);
      long lastHandle = NativeMemory.Memory.entryAllocatorHandle(last);
      WriterArena.Page page = memory.pageForHandle(firstHandle);
      assertNotNull(page);
      assertSame(memory.pageForHandle(lastHandle), page);

      context.beginReleasePageMemo();
      writeColumns(
          columns, allocations, handles, new long[] {first}, new long[] {112L});
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      assertSame(context.releasePageMemoLookup(page.pageKey), page);

      AtomicBoolean invalidatedBeforeCallback = new AtomicBoolean();
      arena.setRetirementHookForTest(
          () -> {
            assertNull(context.releasePageMemoLookup(page.pageKey));
            invalidatedBeforeCallback.set(true);
          });
      writeColumns(
          columns, allocations, handles, new long[] {last}, new long[] {112L});
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      assertTrue(invalidatedBeforeCallback.get());
      assertNull(context.releasePageMemoLookup(page.pageKey));
    } finally {
      context.endReleasePageMemo();
      memory.free(columns, 3L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void invalidMemoDescriptorFallsBackToTheCurrentPageTableEntry() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long columns = memory.allocateRaw(3L * Long.BYTES);
    long allocations = columns + Long.BYTES;
    long handles = allocations + Long.BYTES;
    try {
      WriterArena targetArena = memory.newWriterArena();
      WriterArena decoyArena = memory.newWriterArena();
      long first = targetArena.allocate(112L);
      long last = targetArena.allocate(112L);
      long decoy = decoyArena.allocate(112L);
      long firstHandle = NativeMemory.Memory.entryAllocatorHandle(first);
      WriterArena.Page targetPage = memory.pageForHandle(firstHandle);
      WriterArena.Page decoyPage =
          memory.pageForHandle(NativeMemory.Memory.entryAllocatorHandle(decoy));
      assertNotNull(targetPage);
      assertNotNull(decoyPage);

      context.beginReleasePageMemo();
      context.releasePageMemoRemember(targetPage.pageKey, decoyPage);
      writeColumns(
          columns, allocations, handles, new long[] {first}, new long[] {112L});
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      assertSame(
          context.releasePageMemoLookup(targetPage.pageKey),
          targetPage,
          "a mismatched cached descriptor must be invalidated and resolved through the page table");

      writeColumns(
          columns, allocations, handles, new long[] {last}, new long[] {112L});
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      memory.releaseEntry(decoy, 112L);
    } finally {
      context.endReleasePageMemo();
      memory.free(columns, 3L * Long.BYTES);
      memory.closeArenas();
    }
  }

  @Test
  public void releaseMatcherChecksEveryDecodedDescriptorDimension() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long bytes = 112L;
    long entry = 0L;
    try {
      WriterArena arena = memory.newWriterArena();
      entry = arena.allocate(bytes);
      long handle = NativeMemory.Memory.entryAllocatorHandle(entry);
      long pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
      int slot = WriterArena.Page.slotOf(handle);
      int sizeClass = SizeClasses.indexForEntry(bytes);
      long block = entry - WriterArena.PREFIX_BYTES;
      WriterArena.Page page = memory.pageForHandle(handle);
      assertNotNull(page);

      assertTrue(page.matchesRelease(pageKey, sizeClass, block, slot));
      assertTrue(!page.matchesRelease(pageKey + 1L, sizeClass, block, slot));
      assertTrue(!page.matchesRelease(pageKey, sizeClass + 1, block, slot));
      assertTrue(
          !page.matchesRelease(
              pageKey, sizeClass, block, (slot + 1) % page.slotCount));
      assertTrue(!page.matchesRelease(pageKey, sizeClass, block + page.slotBytes, slot));
    } finally {
      if (entry != 0L) {
        memory.releaseEntry(entry, bytes);
      }
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void memoHitOnTheLastLiveSlotCanRacePhysicalTrimWithoutRepublishingThePage()
      throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long bytes = 112L;
    int sizeClass = SizeClasses.indexForEntry(bytes);
    int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
    int firstBatchCount = slotsPerPage - 1;
    long columnBytes = 3L * firstBatchCount * Long.BYTES;
    long columns = memory.allocateRaw(columnBytes);
    long allocations = columns + (long) firstBatchCount * Long.BYTES;
    long handles = allocations + (long) firstBatchCount * Long.BYTES;
    Thread trimmer = null;
    try {
      WriterArena arena = memory.newWriterArena();
      long[] entries = new long[slotsPerPage];
      long[] entryBytes = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        entries[index] = arena.allocate(bytes);
        entryBytes[index] = bytes;
      }
      long extra = arena.allocate(bytes);
      long lastHandle = NativeMemory.Memory.entryAllocatorHandle(entries[slotsPerPage - 1]);
      WriterArena.Page page = memory.pageForHandle(lastHandle);
      assertNotNull(page);
      for (long entry : entries) {
        assertSame(memory.pageForHandle(NativeMemory.Memory.entryAllocatorHandle(entry)), page);
      }

      context.beginReleasePageMemo();
      writeColumns(
          columns,
          allocations,
          handles,
          java.util.Arrays.copyOf(entries, firstBatchCount),
          java.util.Arrays.copyOf(entryBytes, firstBatchCount));
      assertEquals(
          memory.releaseEntryBatch(
              context, columns, allocations, handles, firstBatchCount),
          firstBatchCount);
      assertSame(context.releasePageMemoLookup(page.pageKey), page);

      CountDownLatch trimStart = new CountDownLatch(1);
      CountDownLatch trimDone = new CountDownLatch(1);
      AtomicLong trimmedBytes = new AtomicLong();
      AtomicReference<Throwable> trimFailure = new AtomicReference<>();
      trimmer =
          new Thread(
              () -> {
                try {
                  trimStart.await();
                  trimmedBytes.set(memory.trimAvailablePages());
                } catch (Throwable failure) {
                  trimFailure.set(failure);
                } finally {
                  trimDone.countDown();
                }
              },
              "release-page-memo-trimmer");
      trimmer.start();
      arena.setRetirementHookForTest(
          () -> {
            assertNull(
                context.releasePageMemoLookup(page.pageKey),
                "the last decrement must invalidate the descriptor before trim can win");
            trimStart.countDown();
            try {
              assertTrue(trimDone.await(2L, TimeUnit.SECONDS), "trimmer did not finish");
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new AssertionError(interrupted);
            }
          });

      writeColumns(
          columns,
          allocations,
          handles,
          new long[] {entries[slotsPerPage - 1]},
          new long[] {bytes});
      assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      trimmer.join(2_000L);
      assertTrue(!trimmer.isAlive());
      assertNull(trimFailure.get());
      assertTrue(trimmedBytes.get() >= SizeClasses.pageBytes(sizeClass));
      assertNull(memory.pageForHandle(lastHandle), "a physically freed descriptor must stay gone");

      WriterArena replacementArena = memory.newWriterArena();
      long replacement = replacementArena.allocate(bytes);
      long replacementHandle = NativeMemory.Memory.entryAllocatorHandle(replacement);
      assertTrue(
          replacementHandle >>> WriterArena.HANDLE_SLOT_BITS != page.pageKey,
          "page-id reuse must carry a different full generation key");
      memory.releaseEntry(extra, bytes);
      memory.releaseEntry(replacement, bytes);
    } finally {
      context.endReleasePageMemo();
      if (trimmer != null) {
        trimmer.interrupt();
        trimmer.join(1_000L);
      }
      memory.free(columns, columnBytes);
      memory.closeArenas();
    }
  }

  @Test(timeOut = 15_000L)
  public void warmMemoLastSlotReleaseSurvivesConcurrentTrimStress() throws Exception {
    for (int round = 0; round < 32; round++) {
      assertWarmMemoLastSlotTrimRace(round);
    }
  }

  private static void assertWarmMemoLastSlotTrimRace(int round) throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ThreadContext context = new ThreadContext(null);
    long bytes = 112L;
    int sizeClass = SizeClasses.indexForEntry(bytes);
    int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
    int warmCount = slotsPerPage - 1;
    long columnBytes = 3L * warmCount * Long.BYTES;
    long columns = memory.allocateRaw(columnBytes);
    long allocations = columns + (long) warmCount * Long.BYTES;
    long handles = allocations + (long) warmCount * Long.BYTES;
    AtomicBoolean releaseFinished = new AtomicBoolean();
    AtomicReference<Throwable> trimFailure = new AtomicReference<>();
    CountDownLatch trimmerReady = new CountDownLatch(1);
    CountDownLatch startRace = new CountDownLatch(1);
    Thread trimmer = null;
    long extra = 0L;
    try {
      WriterArena arena = memory.newWriterArena();
      long[] entries = new long[slotsPerPage];
      long[] entryBytes = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        entries[index] = arena.allocate(bytes);
        entryBytes[index] = bytes;
      }
      extra = arena.allocate(bytes);
      long last = entries[slotsPerPage - 1];
      long lastHandle = NativeMemory.Memory.entryAllocatorHandle(last);
      WriterArena.Page page = memory.pageForHandle(lastHandle);
      assertNotNull(page);

      context.beginReleasePageMemo();
      writeColumns(
          columns,
          allocations,
          handles,
          java.util.Arrays.copyOf(entries, warmCount),
          java.util.Arrays.copyOf(entryBytes, warmCount));
      assertEquals(
          memory.releaseEntryBatch(context, columns, allocations, handles, warmCount),
          warmCount);
      assertSame(context.releasePageMemoLookup(page.pageKey), page);

      trimmer =
          new Thread(
              () -> {
                trimmerReady.countDown();
                try {
                  startRace.await();
                  int attemptsAfterRelease = 0;
                  while (memory.pageForHandle(lastHandle) != null) {
                    memory.trimAvailablePages();
                    if (releaseFinished.get() && ++attemptsAfterRelease > 100_000) {
                      throw new AssertionError("round " + round + " did not trim the released page");
                    }
                    Thread.yield();
                  }
                } catch (Throwable failure) {
                  trimFailure.set(failure);
                }
              },
              "release-page-memo-trim-stress-" + round);
      trimmer.start();
      assertTrue(trimmerReady.await(2L, TimeUnit.SECONDS));
      startRace.countDown();
      try {
        writeColumns(
            columns, allocations, handles, new long[] {last}, new long[] {bytes});
        assertEquals(memory.releaseEntryBatch(context, columns, allocations, handles, 1), 1);
      } finally {
        releaseFinished.set(true);
      }

      trimmer.join(2_000L);
      assertTrue(!trimmer.isAlive(), "round " + round + " trimmer did not finish");
      assertNull(trimFailure.get(), "round " + round + " trimmer failed");
      assertNull(context.releasePageMemoLookup(page.pageKey));
      assertNull(memory.pageForHandle(lastHandle));
      memory.releaseEntry(extra, bytes);
      extra = 0L;
    } finally {
      releaseFinished.set(true);
      startRace.countDown();
      context.endReleasePageMemo();
      if (trimmer != null && trimmer.isAlive()) {
        trimmer.interrupt();
        trimmer.join(1_000L);
      }
      if (extra != 0L) {
        memory.releaseEntry(extra, bytes);
      }
      memory.free(columns, columnBytes);
      memory.closeArenas();
    }
  }

  private static void writeColumns(
      long addresses, long allocations, long handles, long[] entries, long[] bytes) {
    for (int index = 0; index < entries.length; index++) {
      NativeMemory.putLong(addresses + (long) index * Long.BYTES, entries[index]);
      NativeMemory.putLong(allocations + (long) index * Long.BYTES, bytes[index]);
      NativeMemory.putLong(
          handles + (long) index * Long.BYTES,
          NativeMemory.Memory.entryAllocatorHandle(entries[index]));
    }
  }

  private static long[] readColumn(long address, int count) {
    long[] values = new long[count];
    for (int index = 0; index < count; index++) {
      values[index] = NativeMemory.getLong(address + (long) index * Long.BYTES);
    }
    return values;
  }

  private static void assertColumnEquals(long address, long[] expected) {
    for (int index = 0; index < expected.length; index++) {
      assertEquals(NativeMemory.getLong(address + (long) index * Long.BYTES), expected[index]);
    }
  }

  private static void assertAddressesCleared(long addresses, int count) {
    for (int index = 0; index < count; index++) {
      assertEquals(NativeMemory.getLong(addresses + (long) index * Long.BYTES), 0L);
    }
  }

  private static void assertScratchReleased(ThreadContext context) {
    for (long pageKey : context.releaseGroupKeys()) {
      assertEquals(pageKey, 0L, "a completed or failed batch must clear touched group keys");
    }
    for (Object page : context.releaseGroupPages()) {
      assertNull(page, "a completed or failed batch must not retain allocator pages");
    }
  }
}
