package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.runtime.ReaderRegistry;

public class WriterArenaTest {
  @Test
  public void smallEntryReuseDoesNotAllocateANativeBlockPerEntry() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;

      long first = arena.allocate(bytes);
      Assert.assertEquals(memory.rawAllocationCount(), 1L);
      memory.releaseEntry(first, bytes);

      long second = arena.allocate(bytes);
      Assert.assertEquals(second, first);
      Assert.assertEquals(memory.rawAllocationCount(), 1L);
      memory.releaseEntry(second, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void largeEntryUsesOneDirectNativeBlock() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 32_769L;

      long entry = arena.allocate(bytes);
      Assert.assertEquals(memory.rawAllocationCount(), 1L);
      memory.releaseEntry(entry, bytes);
      Assert.assertEquals(memory.allocated(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void smallEntriesAllocateOnlyThePagesRequiredByTheirSizeClass() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int slotBytes = 128;
      int count = 1_025;
      List<Long> entries = new ArrayList<>(count);

      for (int i = 0; i < count; i++) {
        entries.add(arena.allocate(bytes));
      }

      long requiredPages =
          (count * (long) slotBytes + SizeClasses.PAGE_BYTES - 1L) / SizeClasses.PAGE_BYTES;
      Assert.assertTrue(
          memory.rawAllocationCount() <= requiredPages,
          "small writes must consume pooled pages rather than one native allocation per entry");
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void aCompletelyFreePageIsReusedByAnotherAllocatorStripe() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 112L;
      int slotsPerPage =
          SizeClasses.PAGE_BYTES / SizeClasses.slotBytes(SizeClasses.indexForEntry(bytes));
      WriterArena first = arena(memory, 0);
      WriterArena second = arena(memory, 1);
      List<Long> entries = new ArrayList<>(slotsPerPage);

      for (int i = 0; i < slotsPerPage; i++) {
        entries.add(first.allocate(bytes));
      }
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }

      Assert.assertEquals(memory.rawAllocationCount(), 1L);
      long reused = second.allocate(bytes);
      Assert.assertEquals(
          memory.rawAllocationCount(),
          1L,
          "a full idle page must leave its stripe and be reused before allocating another native"
              + " page");
      memory.releaseEntry(reused, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void idlePageDepotIsBoundedAndReturnsExcessPagesToTheNativeAllocator() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = arena(memory, 0);
      long bytes = 32_752L;
      int pages = memory.pooledPageLimit() + 1;
      List<Long> entries = new ArrayList<>(pages * 2);
      for (int index = 0; index < pages * 2; index++) {
        entries.add(arena.allocate(bytes));
      }
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }

      Assert.assertEquals(
          memory.allocated(),
          (long) memory.pooledPageLimit() * SizeClasses.PAGE_BYTES,
          "only the bounded shared idle-page reserve may remain physically allocated");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void sizeClassLookupCoversEachSmallAllocationBoundary() {
    Assert.assertEquals(SizeClasses.indexForEntry(112L), 0);
    Assert.assertEquals(SizeClasses.indexForEntry(113L), 1);
    Assert.assertEquals(SizeClasses.indexForEntry(1_008L), 28);
    Assert.assertEquals(SizeClasses.indexForEntry(32_752L), 85);
    Assert.assertEquals(SizeClasses.indexForEntry(32_753L), -1);
  }

  @Test
  public void reusingFreeSlotsDoesNotAllocateAnotherNativePage() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int count = (256 << 10) / SizeClasses.slotBytes(sizeClass);
      List<Long> entries = new ArrayList<>(count);

      for (int i = 0; i < count; i++) {
        entries.add(arena.allocate(bytes));
      }
      long pagesBeforeReuse = memory.rawAllocationCount();
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }

      long reclaimed = arena.allocate(bytes);
      Assert.assertEquals(
          memory.rawAllocationCount(),
          pagesBeforeReuse,
          "a reclaimed small slot must be served from the cache-owned page pool");
      memory.releaseEntry(reclaimed, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void cacheOwnsAFixedPowerOfTwoSetOfAllocatorStripes() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      int expected = 1;
      int target = NativeMemory.LOGICAL_CPU_COUNT * 4;
      while (expected < target) {
        expected <<= 1;
      }
      Assert.assertEquals(memory.writerStripeCount(), expected);
      Assert.assertSame(
          memory.newWriterArena(),
          memory.newWriterArena(),
          "a calling thread must select a cache-owned stripe rather than create a permanent arena");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void emptyPartialPageCanBeReclaimedAndReallocatedSafely() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      long first = arena.allocate(bytes);
      memory.releaseEntry(first, bytes);
      long reused = arena.allocate(bytes);
      memory.releaseEntry(reused, bytes);
      Assert.assertTrue(memory.allocated() >= 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void retirementKeepsTheCurrentPageWhenAllocationRacesAfterEmptyCheck() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      long first = arena.allocate(bytes);
      CountDownLatch hookEntered = new CountDownLatch(1);
      CountDownLatch releaseHook = new CountDownLatch(1);
      arena.setRetirementHookForTest(
          new Runnable() {
            private boolean firstCall = true;

            @Override
            public synchronized void run() {
              if (!firstCall) {
                return;
              }
              firstCall = false;
              hookEntered.countDown();
              try {
                releaseHook.await(2L, TimeUnit.SECONDS);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
              }
            }
          });

      Thread freeing = new Thread(() -> memory.releaseEntry(first, bytes));
      freeing.start();
      Assert.assertTrue(hookEntered.await(2L, TimeUnit.SECONDS));
      long second = arena.allocate(bytes);
      Assert.assertNotEquals(second, 0L);
      releaseHook.countDown();
      freeing.join(2_000L);
      Assert.assertFalse(freeing.isAlive());

      Field pagesField = WriterArena.class.getDeclaredField("currentPages");
      pagesField.setAccessible(true);
      Assert.assertNotNull(((AtomicReferenceArray<?>) pagesField.get(arena)).get(sizeClass));

      memory.releaseEntry(second, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void pooledPageLimitFallsBackToDirectAllocationForSmallEntries() {
    long hardLimit = 512L * SizeClasses.PAGE_BYTES + WriterArena.directAllocationBytes(32_752L);
    NativeMemory.Memory memory =
        new NativeMemory.Memory(AllocatorType.JNA, hardLimit);
    int pooledPageLimit = memory.pooledPageLimit();
    Assert.assertTrue(pooledPageLimit > 128, "the page pool must exceed the legacy global cap");
    List<Long> entries = new ArrayList<>(pooledPageLimit + 1);
    long bytes = 32_752L;
    try {
      int entriesPerPage =
          SizeClasses.PAGE_BYTES / SizeClasses.slotBytes(SizeClasses.indexForEntry(bytes));
      int total = pooledPageLimit * entriesPerPage + 1;
      for (int i = 0; i < total; i++) {
        entries.add(memory.newWriterArena().allocate(bytes));
      }
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }
      Assert.assertEquals(
          memory.allocated(),
          (long) pooledPageLimit * SizeClasses.PAGE_BYTES,
          "small allocations must use direct fallback once pooled pages hit their hard cap");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void subPageHardLimitUsesDirectFallbackWithoutAllocatingAPage() {
    long bytes = 112L;
    long hardLimit = WriterArena.directAllocationBytes(bytes);
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA, hardLimit);
    try {
      Assert.assertEquals(memory.pooledPageLimit(), 0);
      long entry = memory.newWriterArena().allocate(bytes);
      Assert.assertEquals(memory.allocated(), hardLimit);
      memory.releaseEntry(entry, bytes);
      Assert.assertEquals(memory.allocated(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void retirementConsumesARecordWhenDepotOfferFallsBackToFree() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    RetirementQueue retirements = new RetirementQueue(memory, 1, 2);
    long allocation = ValueBlock.allocationLength(8);
    int sizeClass = SizeClasses.indexForEntry(allocation);
    Field depotField = NativeMemory.Memory.class.getDeclaredField("pageDepot");
    depotField.setAccessible(true);
    PageDepot depot = (PageDepot) depotField.get(memory);
    Field pagesField = PageDepot.class.getDeclaredField("pages");
    pagesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<WriterArena.Page>[] pages =
        (ConcurrentLinkedQueue<WriterArena.Page>[]) pagesField.get(depot);
    ConcurrentLinkedQueue<WriterArena.Page> original = pages[sizeClass];
    pages[sizeClass] = new ThrowingOfferQueue<>();
    try {
      long address = memory.newWriterArena().allocate(allocation);
      RetirementQueue.Reservation reservation = new RetirementQueue.Reservation();
      Assert.assertTrue(retirements.reserve(reservation, 1));
      retirements.append(reservation, address, allocation);
      Assert.assertEquals(retirements.seal(1, 1L), 1);

      Assert.assertEquals(retirements.reclaim(new ReaderRegistry(), 1), 1);
      Assert.assertEquals(retirements.queuedRecords(), 0L);
      Assert.assertEquals(retirements.retiredEntries(), 0);
      Assert.assertEquals(
          memory.allocated(),
          retirements.allocatedBytes(),
          "a failed depot enqueue must consume the empty page without stranding its retirement"
              + " record");
      Field idleField = PageDepot.class.getDeclaredField("idlePages");
      idleField.setAccessible(true);
      Assert.assertEquals(((java.util.concurrent.atomic.AtomicInteger) idleField.get(depot)).get(), 0);
      Assert.assertNull(depot.acquire(sizeClass));
    } finally {
      pages[sizeClass] = original;
      retirements.close();
      memory.closeArenas();
    }
  }

  private static long headState(WriterArena arena, int sizeClass) throws Exception {
    Field pagesField = WriterArena.class.getDeclaredField("currentPages");
    pagesField.setAccessible(true);
    Object page = ((AtomicReferenceArray<?>) pagesField.get(arena)).get(sizeClass);
    Field headField = page.getClass().getDeclaredField("freeHead");
    headField.setAccessible(true);
    return ((AtomicLong) headField.get(page)).get();
  }

  private static WriterArena arena(NativeMemory.Memory memory, int index) throws Exception {
    Field field = NativeMemory.Memory.class.getDeclaredField("arenas");
    field.setAccessible(true);
    return ((WriterArena[]) field.get(memory))[index];
  }

  private static final class ThrowingOfferQueue<E> extends ConcurrentLinkedQueue<E> {
    @Override
    public boolean offer(E element) {
      throw new OutOfMemoryError("injected page depot offer failure");
    }
  }
}
