package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.cache.CacheTestSupport;

public final class PageMappingBudgetTest {
  @Test
  public void mappingSlotsAreHeldUntilPhysicalPageRelease() throws Exception {
    MappingAllocator allocator = new MappingAllocator();
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    AtomicLong count = mappingCount(memory);
    try {
      WriterArena.Page page = memory.tryAcquireEntryPage(SizeClasses.indexForEntry(112L));
      assertEquals(count.get(), 1L);
      memory.returnUnusedPage(page);
      assertEquals(count.get(), 0L);
      assertEquals(allocator.maps.get(), 1);
      assertEquals(allocator.unmaps.get(), 1);
    } finally {
      memory.closeArenas();
    }
    assertEquals(count.get(), 0L);
  }

  @Test
  public void failedMappingReturnsItsReservation() throws Exception {
    MappingAllocator allocator = new MappingAllocator();
    allocator.fail = true;
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    AtomicLong count = mappingCount(memory);
    try {
      expectThrows(
          OutOfMemoryError.class,
          () -> memory.tryAcquireEntryPage(SizeClasses.indexForEntry(112L)));
      assertEquals(count.get(), 0L);
      assertEquals(memory.allocated(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentAcquisitionsShareOneRemainingMappingSlot() throws Exception {
    MappingAllocator allocator = new MappingAllocator();
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    AtomicLong count = mappingCount(memory);
    Field budgetField = NativeMemory.Memory.class.getDeclaredField("PAGE_MAPPING_BUDGET");
    budgetField.setAccessible(true);
    long budget = budgetField.getInt(null);
    count.set(budget - 1L);
    ExecutorService callers = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    Throwable primary = null;
    try {
      Future<WriterArena.Page> first = callers.submit(() -> acquire(memory, start));
      Future<WriterArena.Page> second = callers.submit(() -> acquire(memory, start));
      start.countDown();
      first.get(2L, TimeUnit.SECONDS);
      second.get(2L, TimeUnit.SECONDS);
      assertEquals(allocator.maps.get(), 1, "only one acquisition may use the last mapped slot");
      assertEquals(count.get(), budget);
    } catch (Throwable failure) {
      primary = failure;
      throw failure;
    } finally {
      start.countDown();
      try {
        CacheTestSupport.awaitCallers(callers, primary);
      } finally {
        memory.closeArenas();
      }
    }
    assertEquals(count.get(), budget - 1L);
  }

  private static WriterArena.Page acquire(NativeMemory.Memory memory, CountDownLatch start)
      throws InterruptedException {
    start.await();
    return memory.tryAcquireEntryPage(SizeClasses.indexForEntry(112L));
  }

  private static AtomicLong mappingCount(NativeMemory.Memory memory) throws Exception {
    Field field = NativeMemory.Memory.class.getDeclaredField("directMappedPageCount");
    field.setAccessible(true);
    return (AtomicLong) field.get(memory);
  }

  /** Uses real owned memory while isolating the mapping budget from the host ABI. */
  private static final class MappingAllocator extends NativeAllocator {
    private final AtomicInteger maps = new AtomicInteger();
    private final AtomicInteger unmaps = new AtomicInteger();
    private boolean fail;

    @Override
    public boolean pageMappingsAvailable() {
      return true;
    }

    @Override
    public long mapPage(long bytes) {
      if (fail) {
        throw new OutOfMemoryError("injected mapping failure");
      }
      maps.incrementAndGet();
      return allocate(bytes);
    }

    @Override
    public void unmapPage(long address, long bytes) {
      unmaps.incrementAndGet();
      free(address);
    }
  }
}
