package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ThreadContext;

public class WriterArenaTest {
  @Test
  public void availablePagesUseOneDirectLifoStackPerSizeClass() {
    Field[] memoryFields = NativeMemory.Memory.class.getDeclaredFields();
    boolean hasDirectReadyStacks = false;
    boolean hasOwnerQueues = false;
    for (Field field : memoryFields) {
      if (field.getName().equals("readyPageStacks")) {
        hasDirectReadyStacks = true;
      }
      if (field.getName().equals("availableOwnerQueues")) {
        hasOwnerQueues = true;
      }
    }
    Assert.assertTrue(hasDirectReadyStacks, "memory must index ready pages directly by size class");
    Assert.assertFalse(hasOwnerQueues, "owner indirection must not remain on the allocation path");
    for (Method method : NativeMemory.Memory.class.getDeclaredMethods()) {
      Assert.assertNotEquals(
          method.getName(), "scanAvailablePage", "the page table scan fallback must be removed");
    }
  }

  @Test
  public void slotClassesAre64ByteAlignedAndUseBoundedPageSizing() {
    for (int sizeClass = 0; sizeClass < SizeClasses.count(); sizeClass++) {
      int slotBytes = SizeClasses.slotBytes(sizeClass);
      Assert.assertEquals(
          slotBytes & 63,
          0,
          "slot bytes must be rounded up to the native header alignment");

      int expectedPageBytes;
      if (slotBytes < 4 * 1024) {
        long required = Math.max(64L * 1024L, 256L * slotBytes);
        expectedPageBytes = 64 * 1024;
        while (expectedPageBytes < required) {
          expectedPageBytes <<= 1;
        }
      } else {
        expectedPageBytes = 2 * 1024 * 1024;
      }
      Assert.assertEquals(SizeClasses.pageBytes(sizeClass), expectedPageBytes);
    }
  }

  @Test
  public void pooledSmallAllocationDoesNotUseDirectFallback() throws Exception {
    Method counter = null;
    for (Method method : NativeMemory.Memory.class.getDeclaredMethods()) {
      if (method.getName().equals("smallAllocationFallbackCount")
          && method.getParameterCount() == 0) {
        counter = method;
        break;
      }
    }
    Assert.assertNotNull(counter, "small allocation fallback must be observable");

    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long entry = arena.allocate(112L);
      Assert.assertEquals(((Number) counter.invoke(memory)).longValue(), 0L);
      memory.releaseEntry(entry, 112L);
    } finally {
      memory.closeArenas();
    }
  }

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
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotBytes = SizeClasses.slotBytes(sizeClass);
      int count = 1_025;
      List<Long> entries = new ArrayList<>(count);

      for (int i = 0; i < count; i++) {
        entries.add(arena.allocate(bytes));
      }

      long requiredPages =
          (count + (long) SizeClasses.pageBytes(sizeClass) / slotBytes - 1L)
              / (SizeClasses.pageBytes(sizeClass) / slotBytes);
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
  public void boundedFiveKEntriesStayOnPooledPagesWhenTheBudgetFits() {
    long valueBytes = ValueBlock.allocationLength(5_120);
    int slotBytes = SizeClasses.slotBytes(SizeClasses.indexForEntry(valueBytes));
    int entries = 6_000;
    int slotsPerPage = SizeClasses.pageBytes(SizeClasses.indexForEntry(valueBytes)) / slotBytes;
    long requiredPages = (entries + (long) slotsPerPage - 1L) / slotsPerPage;
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    List<Long> allocations = new ArrayList<>(entries);
    try {
      WriterArena arena = memory.newWriterArena();
      for (int index = 0; index < entries; index++) {
        allocations.add(arena.allocate(valueBytes));
      }
      Assert.assertEquals(memory.smallAllocationFallbackCount(), 0L);
    } finally {
      for (long allocation : allocations) {
        memory.releaseEntry(allocation, valueBytes);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void aCompletelyFreePageCanRebalanceAcrossWriterResources() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena first = memory.newWriterArena();
      WriterArena second = memory.newWriterArena();
      List<Long> entries = new ArrayList<>(slotsPerPage);

      for (int i = 0; i < slotsPerPage; i++) {
        entries.add(first.allocate(bytes));
      }
      long firstCurrentPageEntry = first.allocate(bytes);
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }

      Assert.assertEquals(memory.rawAllocationCount(), 2L);
      long reusedBefore = memory.pageReusedCount();
      long secondEntry = second.allocate(bytes);
      Assert.assertEquals(
          memory.rawAllocationCount(),
          2L,
          "a writer with no local page must reuse a fully empty page before allocating native");
      Assert.assertEquals(memory.pageReusedCount(), reusedBefore + 1L);
      memory.releaseEntry(firstCurrentPageEntry, bytes);
      memory.releaseEntry(secondEntry, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void anAvailablePartialPageCanRebalanceWhileItsOldSlotIsStillLive() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 32_700L;
      WriterArena first = memory.newWriterArena();
      WriterArena second = memory.newWriterArena();
      long oldLiveEntry = first.allocate(bytes);
      long releasedEntry = first.allocate(bytes);
      long firstCurrentPageEntry = first.allocate(bytes);
      long availablePageKey =
          NativeMemory.getLong(oldLiveEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      first.detach();
      memory.releaseEntry(releasedEntry, bytes);

      long allocationsBefore = memory.rawAllocationCount();
      long rebalancedEntry = second.allocate(bytes);
      Assert.assertEquals(
          NativeMemory.getLong(rebalancedEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS,
          availablePageKey,
          "a writer must claim an available page even while an old slot remains live");
      Assert.assertEquals(memory.rawAllocationCount(), allocationsBefore);

      memory.releaseEntry(oldLiveEntry, bytes);
      memory.releaseEntry(rebalancedEntry, bytes);
      memory.releaseEntry(firstCurrentPageEntry, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void staleLocalReadyEntryCannotReactivateAPageAfterOwnerTransfer() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 32_700L;
      WriterArena first = memory.newWriterArena();
      WriterArena second = memory.newWriterArena();

      long firstPageEntry = first.allocate(bytes);
      long firstPageEntry2 = first.allocate(bytes);
      long firstCurrentPageEntry = first.allocate(bytes);
      long firstPageKey =
          NativeMemory.getLong(firstPageEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      long firstCurrentPageKey =
          NativeMemory.getLong(firstCurrentPageEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      first.detach();
      memory.releaseEntry(firstPageEntry, bytes);
      memory.releaseEntry(firstPageEntry2, bytes);

      long secondEntry = second.allocate(bytes);
      Assert.assertEquals(
          NativeMemory.getLong(secondEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS,
          firstPageKey,
          "the second writer must claim the globally empty page");
      Assert.assertEquals(
          NativeMemory.getLong(firstCurrentPageEntry - 8L) >>> WriterArena.HANDLE_SLOT_BITS,
          firstCurrentPageKey);
      memory.releaseEntry(firstCurrentPageEntry, bytes);
      memory.releaseEntry(secondEntry, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void readyPagesArePoppedInLifoOrderWithinOneSizeClass() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      WriterArena firstTarget = memory.newWriterArena();
      WriterArena secondTarget = memory.newWriterArena();
      long[] entries = new long[slotsPerPage * 2];
      for (int index = 0; index < entries.length; index++) {
        entries[index] = source.allocate(bytes);
      }
      long firstPageKey =
          NativeMemory.getLong(entries[0] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      long secondPageKey =
          NativeMemory.getLong(entries[slotsPerPage] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      Assert.assertNotEquals(firstPageKey, secondPageKey);
      source.detach();

      // Publish the second page first, then the first page. The first page must be the stack top.
      for (int index = slotsPerPage; index < entries.length; index++) {
        memory.releaseEntry(entries[index], bytes);
      }
      for (int index = 0; index < slotsPerPage; index++) {
        memory.releaseEntry(entries[index], bytes);
      }

      WriterArena.Page first =
          memory.tryStealAvailablePage(sizeClass, firstTarget.sizeClassState(sizeClass));
      WriterArena.Page second =
          memory.tryStealAvailablePage(sizeClass, secondTarget.sizeClassState(sizeClass));
      Assert.assertNotNull(first);
      Assert.assertNotNull(second);
      Assert.assertEquals(first.pageKey, firstPageKey);
      Assert.assertEquals(second.pageKey, secondPageKey);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void trimSkipsPartialStackTopAndReachesEmptyPageBelow() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      long[] firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < firstPageEntries.length; index++) {
        firstPageEntries[index] = source.allocate(bytes);
      }
      long partialPageEntry = source.allocate(bytes);
      for (long entry : firstPageEntries) {
        memory.releaseEntry(entry, bytes);
      }
      source.detach();

      Assert.assertEquals(memory.pageReadyCount(), 2L);
      long trimmed = memory.trimAvailablePages();

      Assert.assertEquals(trimmed, (long) SizeClasses.pageBytes(sizeClass));
      Assert.assertEquals(memory.pageTrimmedCount(), 1L);
      Assert.assertEquals(
          memory.pageReadyCount(),
          1L,
          "the partial stack-top page must be restored after the lower empty page is trimmed");
      memory.releaseEntry(partialPageEntry, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void trimmedReadyTokenCannotActivateAReusedPageId() {
    FailingAllocator allocator = new FailingAllocator();
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    try {
      long bytes = 32_700L;
      WriterArena source = memory.newWriterArena();
      WriterArena target = memory.newWriterArena();
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      long[] firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < firstPageEntries.length; index++) {
        firstPageEntries[index] = source.allocate(bytes);
      }
      long sourceCurrent = source.allocate(bytes);
      long trimmedPageKey =
          NativeMemory.getLong(firstPageEntries[0] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      source.detach();
      for (long entry : firstPageEntries) {
        memory.releaseEntry(entry, bytes);
      }

      allocator.failNextAllocations(1);
      long pressureAllocation = target.allocate(112L);
      Assert.assertNotEquals(pressureAllocation, 0L);
      Assert.assertEquals(memory.pageTrimmedCount(), 1L);

      long replacement = target.allocate(bytes);
      long replacementPageKey =
          NativeMemory.getLong(replacement - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      Assert.assertNotEquals(
          replacementPageKey,
          trimmedPageKey,
          "a stale token must not activate the page after it was trimmed");
      memory.releaseEntry(sourceCurrent, bytes);
      memory.releaseEntry(pressureAllocation, 112L);
      memory.releaseEntry(replacement, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void oneAvailablePageCannotBeActivatedByTwoWriters() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      long bytes = 32_700L;
      WriterArena producer = memory.newWriterArena();
      WriterArena firstConsumer = memory.newWriterArena();
      WriterArena secondConsumer = memory.newWriterArena();
      long released = producer.allocate(bytes);
      long stillLive = producer.allocate(bytes);
      long producerCurrent = producer.allocate(bytes);
      long availablePageKey =
          NativeMemory.getLong(released - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      producer.detach();
      memory.releaseEntry(released, bytes);

      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Long> firstResult = new AtomicReference<>();
      AtomicReference<Long> secondResult = new AtomicReference<>();
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread first =
          new Thread(
              () -> {
                await(start);
                try {
                  firstResult.set(firstConsumer.allocate(bytes));
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                }
              });
      Thread second =
          new Thread(
              () -> {
                await(start);
                try {
                  secondResult.set(secondConsumer.allocate(bytes));
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                }
              });
      first.start();
      second.start();
      start.countDown();
      first.join(2_000L);
      second.join(2_000L);

      Assert.assertFalse(first.isAlive());
      Assert.assertFalse(second.isAlive());
      Assert.assertNull(failure.get());
      Assert.assertNotEquals(firstResult.get().longValue(), secondResult.get().longValue());
      int consumersOfAvailablePage = 0;
      if ((NativeMemory.getLong(firstResult.get() - 8L) >>> WriterArena.HANDLE_SLOT_BITS)
          == availablePageKey) {
        consumersOfAvailablePage++;
      }
      if ((NativeMemory.getLong(secondResult.get() - 8L) >>> WriterArena.HANDLE_SLOT_BITS)
          == availablePageKey) {
        consumersOfAvailablePage++;
      }
      Assert.assertEquals(
          consumersOfAvailablePage,
          1,
          "AVAILABLE -> REBALANCING must have exactly one CAS winner");

      memory.releaseEntry(stillLive, bytes);
      memory.releaseEntry(producerCurrent, bytes);
      memory.releaseEntry(firstResult.get(), bytes);
      memory.releaseEntry(secondResult.get(), bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void idlePagesRemainAllocatedAtTheArenaHighWaterMark() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      int pages = 3;
      List<Long> entries = new ArrayList<>(pages * slotsPerPage);
      for (int index = 0; index < pages * slotsPerPage; index++) {
        entries.add(arena.allocate(bytes));
      }
      for (long entry : entries) {
        memory.releaseEntry(entry, bytes);
      }

      Assert.assertEquals(
          memory.allocated(),
          (long) pages * (SizeClasses.pageBytes(sizeClass) + 128L),
          "normal frees must retain pages for high-water reuse instead of physically trimming");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void sizeClassLookupCoversEachSmallAllocationBoundary() {
    for (int sizeClass = 0; sizeClass < SizeClasses.count(); sizeClass++) {
      long largestEntry = SizeClasses.slotBytes(sizeClass) - WriterArena.PREFIX_BYTES;
      Assert.assertEquals(SizeClasses.indexForEntry(largestEntry), sizeClass);
      if (sizeClass + 1 < SizeClasses.count()) {
        Assert.assertEquals(SizeClasses.indexForEntry(largestEntry + 1L), sizeClass + 1);
      } else {
        Assert.assertEquals(SizeClasses.indexForEntry(largestEntry + 1L), -1);
      }
    }
  }

  @Test
  public void fiveKiBValuesUseAnExactPooledSizeClass() {
    long valueAllocation = ValueBlock.allocationLength(5_120);
    int sizeClass = SizeClasses.indexForEntry(valueAllocation);
    Assert.assertEquals(SizeClasses.slotBytes(sizeClass), 5_632);
    Assert.assertEquals(SizeClasses.pageBytes(sizeClass), 2 * 1024 * 1024);
    Assert.assertEquals(
        SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass), 372);
  }

  @Test
  public void pageCountersArePrimitiveFieldsRatherThanAtomicWrapperObjects() {
    Field allocatedSlots = requirePageField("allocatedSlots");
    Field freedSlots = requirePageField("freedSlots");
    Assert.assertEquals(allocatedSlots.getType(), long.class);
    Assert.assertEquals(freedSlots.getType(), long.class);
    Assert.assertEquals(allocatedSlots.getDeclaringClass(), WriterArena.PageOwnerLine.class);
    Assert.assertEquals(freedSlots.getDeclaringClass(), WriterArena.PageSharedLine.class);
    Assert.assertEquals(
        requirePageField("state").getDeclaringClass(), WriterArena.PageSharedLine.class);
    Assert.assertEquals(
        requirePageField("ownerClass").getDeclaringClass(), WriterArena.PageSharedLine.class);
    for (Class<?> type = WriterArena.Page.class;
        type != null;
        type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        if (field.getName().equals("nextSlot")
            || field.getName().equals("allocatedSlots")
            || field.getName().equals("freedSlots")
            || field.getName().equals("inFlight")
            || field.getName().equals("freeHead")
            || field.getName().equals("state")) {
          Assert.assertTrue(
              field.getType().isPrimitive(), field.getName() + " must be stored as a primitive");
        }
        Assert.assertNotEquals(
            field.getName(), "LIVE_SLOTS", "the legacy shared live counter must be removed");
        Assert.assertNotEquals(
            field.getName(), "liveSlots", "the allocation path must not update a shared live RMW");
      }
    }
  }

  @Test
  public void pageCountersUseSeparateCacheLinesForOwnerAndReclaimerState() {
    ClassLayout layout = ClassLayout.parseClass(WriterArena.Page.class);
    Map<String, FieldLayout> fields = new HashMap<>();
    for (FieldLayout field : layout.fields()) {
      fields.put(field.name(), field);
    }
    FieldLayout nextSlot = requirePageLayoutField(fields, "nextSlot");
    FieldLayout allocatedSlots = requirePageLayoutField(fields, "allocatedSlots");
    long ownerLine = allocatedSlots.offset() / 64L;
    Assert.assertEquals(
        nextSlot.offset() / 64L,
        ownerLine,
        "owner allocation fields must share the owner cache line");

    String[] sharedFields = {
      "freedSlots",
      "freeSummary",
      "freeBits0",
      "freeBits1",
      "freeBits2",
      "freeBits3",
      "freeBits4",
      "freeBits5",
      "freeBits6",
      "freeBits7",
      "state",
      "ownerClass"
    };
    for (String name : sharedFields) {
      Assert.assertNotEquals(
          requirePageLayoutField(fields, name).offset() / 64L,
          ownerLine,
          name + " must not share the owner cache line");
    }

    // The actor-only cumulative freed counter must stay off the bitmap lines the writers read
    // on every allocation: its remote-free RMWs would otherwise invalidate the writer-side line.
    long freedLine = requirePageLayoutField(fields, "freedSlots").offset() / 64L;
    for (String name :
        new String[] {
          "nextSlot",
          "allocatedSlots",
          "freeSummary",
          "freeBits0",
          "freeBits1",
          "freeBits2",
          "freeBits3",
          "freeBits4",
          "freeBits5",
          "freeBits6",
          "freeBits7",
          "state",
          "ownerClass"
        }) {
      Assert.assertNotEquals(
          requirePageLayoutField(fields, name).offset() / 64L,
          freedLine,
          name + " must not share the freed-counter cache line");
    }
  }

  @Test
  public void splitPageCountersPreserveLiveCountAcrossLongWraparound() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      long entry = arena.allocate(bytes);
      long block = entry - WriterArena.PREFIX_BYTES;
      long handle = NativeMemory.getLong(block + 56L);
      WriterArena.Page page = arena.sizeClassState(sizeClass).currentPage;
      Field allocatedSlots = requirePageField("allocatedSlots");
      Field freedSlots = requirePageField("freedSlots");
      allocatedSlots.setAccessible(true);
      freedSlots.setAccessible(true);
      allocatedSlots.setLong(page, Long.MIN_VALUE);
      freedSlots.setLong(page, Long.MAX_VALUE);

      Assert.assertEquals(page.freeSlot(block, handle), 0);
      arena.detach();
      Assert.assertTrue(
          page.beginTrimming(), "a wrapped zero live-count page must remain trimmable");
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void splitPageCountersStillDetectUnderflowAfterLongWraparound() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      long entry = arena.allocate(bytes);
      long block = entry - WriterArena.PREFIX_BYTES;
      long handle = NativeMemory.getLong(block + 56L);
      WriterArena.Page page = arena.sizeClassState(sizeClass).currentPage;
      Field allocatedSlots = requirePageField("allocatedSlots");
      Field freedSlots = requirePageField("freedSlots");
      allocatedSlots.setAccessible(true);
      freedSlots.setAccessible(true);
      allocatedSlots.setLong(page, Long.MIN_VALUE);
      freedSlots.setLong(page, Long.MAX_VALUE);

      Assert.assertEquals(page.freeSlot(block, handle), 0);
      try {
        page.freeSlot(block, handle);
        Assert.fail("a duplicate free must underflow after the cumulative counters wrap");
      } catch (IllegalStateException expected) {
        Assert.assertTrue(expected.getMessage().contains("live-slot underflow"));
      }
    } finally {
      memory.closeArenas();
    }
  }

  private static Field requirePageField(String name) {
    for (Class<?> type = WriterArena.Page.class;
        type != null;
        type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        if (field.getName().equals(name)) {
          return field;
        }
      }
    }
    Assert.fail("allocator page must expose the split primitive counter " + name);
    throw new AssertionError(name);
  }

  private static FieldLayout requirePageLayoutField(
      Map<String, FieldLayout> fields, String name) {
    FieldLayout field = fields.get(name);
    Assert.assertNotNull(field, "allocator page layout must expose " + name);
    return field;
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
  public void actorReleaseBatchReusesSlotsFromOnePage() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      ThreadContext context = new ThreadContext(null);
      long bytes = 112L;
      int count = 8;
      long[] entries = new long[count];
      long[] allocations = new long[count];
      long[] handles = new long[count];
      for (int index = 0; index < count; index++) {
        entries[index] = arena.allocate(bytes);
        allocations[index] = bytes;
      }
      long rawAllocations = memory.rawAllocationCount();

      memory.releaseEntryBatch(context, entries, allocations, handles, count);
      assertReleaseGroupKeysCleared(context);

      Assert.assertEquals(
          memory.rawAllocationCount(),
          rawAllocations,
          "a page batch release must return the page to the cache-owned reuse path");
      for (int index = 0; index < count; index++) {
        entries[index] = arena.allocate(bytes);
      }
      memory.releaseEntryBatch(context, entries, allocations, handles, count);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void actorReleaseBatchRetrySkipsRecordsFreedBeforeAvailabilityFailure() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      ThreadContext context = new ThreadContext(null);
      long bytes = 112L;
      long[] entries = {arena.allocate(bytes), arena.allocate(bytes)};
      long[] allocations = {bytes, bytes};
      long[] handles = new long[entries.length];
      AtomicBoolean failOnce = new AtomicBoolean(true);
      arena.setRetirementHookForTest(
          () -> {
            if (failOnce.getAndSet(false)) {
              throw new IllegalStateException("availability callback failure");
            }
          });

      try {
        memory.releaseEntryBatch(context, entries, allocations, handles, entries.length);
        Assert.fail("the injected availability failure must reach the grouped caller");
      } catch (IllegalStateException expected) {
        // The allocator group was accepted before the optional availability callback failed.
      }
      assertReleaseGroupKeysCleared(context);
      Assert.assertEquals(entries[0], 0L);
      Assert.assertEquals(entries[1], 0L);
      Assert.assertEquals(handles[0], 0L);
      Assert.assertEquals(handles[1], 0L);

      // The retry must not free either slot a second time, and the page remains reusable.
      memory.releaseEntryBatch(context, entries, allocations, handles, entries.length);
      entries[0] = arena.allocate(bytes);
      entries[1] = arena.allocate(bytes);
      memory.releaseEntryBatch(context, entries, allocations, handles, entries.length);
    } finally {
      memory.closeArenas();
    }
  }

  private static void assertReleaseGroupKeysCleared(ThreadContext context) {
    for (long pageKey : context.releaseGroupKeys()) {
      Assert.assertEquals(
          pageKey, 0L, "a completed or failed batch must clear touched group keys");
    }
  }

  @Test
  public void actorReleaseBatchHandlesPooledAndDirectEntries() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      ThreadContext context = new ThreadContext(null);
      long pooledBytes = 112L;
      long directBytes = 32_769L;
      long[] entries = {arena.allocate(pooledBytes), arena.allocate(directBytes)};
      long[] allocations = {pooledBytes, directBytes};
      long[] handles = new long[entries.length];

      memory.releaseEntryBatch(context, entries, allocations, handles, entries.length);

      Assert.assertTrue(memory.allocated() >= SizeClasses.MIN_PAGE_BYTES);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void actorReleaseBatchGroupsMixedPagesArenasAndDirectEntries() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena first = memory.newWriterArena();
      WriterArena second = memory.newWriterArena();
      ThreadContext context = new ThreadContext(null);
      long pooledBytes = 112L;
      long directBytes = 32_769L;
      int pooledClass = SizeClasses.indexForEntry(pooledBytes);
      int slotsPerPage = SizeClasses.pageBytes(pooledClass) / SizeClasses.slotBytes(pooledClass);
      int firstCount = slotsPerPage + 1;
      int secondCount = 2;
      int count = firstCount + secondCount + 1;
      long[] entries = new long[count];
      long[] allocations = new long[count];
      long[] handles = new long[count];

      int index = 0;
      for (int entry = 0; entry < firstCount; entry++) {
        entries[index] = first.allocate(pooledBytes);
        allocations[index++] = pooledBytes;
      }
      for (int entry = 0; entry < secondCount; entry++) {
        entries[index] = second.allocate(pooledBytes);
        allocations[index++] = pooledBytes;
      }
      entries[index] = first.allocate(directBytes);
      allocations[index] = directBytes;

      long firstPageKey = NativeMemory.getLong(entries[0] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      long secondPageKey =
          NativeMemory.getLong(entries[slotsPerPage] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      long otherArenaPageKey =
          NativeMemory.getLong(entries[firstCount] - 8L) >>> WriterArena.HANDLE_SLOT_BITS;
      Assert.assertTrue(
          firstPageKey != secondPageKey,
          "first page=" + firstPageKey + ", second page=" + secondPageKey);
      Assert.assertTrue(
          firstPageKey != otherArenaPageKey,
          "first page=" + firstPageKey + ", other arena page=" + otherArenaPageKey);
      Assert.assertNotEquals(
          NativeMemory.getLong(entries[0] - WriterArena.PREFIX_BYTES + 48L) & 0xffff_ffffL,
          NativeMemory.getLong(entries[firstCount] - WriterArena.PREFIX_BYTES + 48L)
              & 0xffff_ffffL,
          "records from different writer arenas must retain their owner metadata");

      long rawAllocations = memory.rawAllocationCount();
      memory.releaseEntryBatch(context, entries, allocations, handles, count);

      long[] reused = new long[firstCount];
      for (int entry = 0; entry < reused.length; entry++) {
        reused[entry] = first.allocate(pooledBytes);
      }
      Assert.assertEquals(
          memory.rawAllocationCount(),
          rawAllocations,
          "a mixed batch must return every pooled page to the shared reuse path");
      for (long entry : reused) {
        memory.releaseEntry(entry, pooledBytes);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void concurrentPageBatchesPublishFreeSlotsWithoutCorruptingHandles() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      WriterArena arena = memory.newWriterArena();
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int count = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      count -= count & 1;
      int half = count / 2;
      long[] firstEntries = new long[half];
      long[] secondEntries = new long[half];
      long[] firstBytes = new long[half];
      long[] secondBytes = new long[half];
      long[] firstHandles = new long[half];
      long[] secondHandles = new long[half];
      ThreadContext firstContext = new ThreadContext(null);
      ThreadContext secondContext = new ThreadContext(null);
      for (int index = 0; index < count; index++) {
        if (index < half) {
          firstEntries[index] = arena.allocate(bytes);
          firstBytes[index] = bytes;
        } else {
          secondEntries[index - half] = arena.allocate(bytes);
          secondBytes[index - half] = bytes;
        }
      }
      long rawAllocations = memory.rawAllocationCount();
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread first =
          new Thread(
              () -> {
                await(start);
                try {
                  memory.releaseEntryBatch(
                      firstContext, firstEntries, firstBytes, firstHandles, half);
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                }
              });
      Thread second =
          new Thread(
              () -> {
                await(start);
                try {
                  memory.releaseEntryBatch(
                      secondContext, secondEntries, secondBytes, secondHandles, half);
                } catch (Throwable error) {
                  failure.compareAndSet(null, error);
                }
              });
      first.start();
      second.start();
      start.countDown();
      first.join(2_000L);
      second.join(2_000L);
      Assert.assertFalse(first.isAlive());
      Assert.assertFalse(second.isAlive());
      Assert.assertNull(failure.get());

      long[] reused = new long[count];
      long[] reusedBytes = new long[count];
      long[] reusedHandles = new long[count];
      for (int index = 0; index < count; index++) {
        reused[index] = arena.allocate(bytes);
        reusedBytes[index] = bytes;
      }
      Assert.assertEquals(memory.rawAllocationCount(), rawAllocations);
      memory.releaseEntryBatch(firstContext, reused, reusedBytes, reusedHandles, count);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void eachWriterResourceCanOwnADistinctAllocator() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      List<WriterArena> arenas = new ArrayList<>();
      for (int index = 0; index < NativeMemory.LOGICAL_CPU_COUNT * 2; index++) {
        WriterArena arena = memory.newWriterArena();
        Assert.assertFalse(arenas.contains(arena), "writer allocators must not be round-robin shared");
        arenas.add(arena);
      }
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void allocationFailureTrimsOnlyAnEmptyAvailablePageAndRetriesOnce() {
    FailingAllocator allocator = new FailingAllocator();
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    try {
      long idleBytes = 32_700L;
      long pressureBytes = 112L;
      WriterArena idleOwner = memory.newWriterArena();
      long first = idleOwner.allocate(idleBytes);
      long second = idleOwner.allocate(idleBytes);
      idleOwner.detach();
      memory.releaseEntry(first, idleBytes);
      memory.releaseEntry(second, idleBytes);
      Assert.assertEquals(memory.pageReadyCount(), 1L);

      allocator.failNextAllocations(1);
      WriterArena pressureWriter = memory.newWriterArena();
      long retried = pressureWriter.allocate(pressureBytes);

      Assert.assertNotEquals(retried, 0L);
      Assert.assertEquals(memory.pageTrimmedCount(), 1L);
      Assert.assertEquals(memory.pageReadyCount(), 0L);
      Assert.assertEquals(memory.smallAllocationFallbackCount(), 0L);
      memory.releaseEntry(retried, pressureBytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void repeatedNativeFailurePropagatesAfterTheSingleColdTrimRetry() {
    FailingAllocator allocator = new FailingAllocator();
    NativeMemory.Memory memory = new NativeMemory.Memory(allocator);
    try {
      allocator.failNextAllocations(2);
      try {
        memory.newWriterArena().allocate(112L);
        Assert.fail("the second native allocation failure must propagate");
      } catch (OutOfMemoryError expected) {
        Assert.assertEquals(expected.getMessage(), "injected native allocation failure");
      }
      Assert.assertEquals(memory.pageTrimmedCount(), 0L);
      Assert.assertEquals(memory.smallAllocationFallbackCount(), 0L);
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

      Field pagesField = WriterArena.class.getDeclaredField("sizeClasses");
      pagesField.setAccessible(true);
      Object[] classes = (Object[]) pagesField.get(arena);
      Field currentPage = classes[sizeClass].getClass().getDeclaredField("currentPage");
      currentPage.setAccessible(true);
      Assert.assertNotNull(currentPage.get(classes[sizeClass]));

      memory.releaseEntry(second, bytes);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void partiallyFreedPageIsRetainedByItsLiveOwnerWithoutTheSharedStack() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    List<Long> entries = new ArrayList<>();
    long[] firstPageEntries = new long[0];
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        firstPageEntries[index] = source.allocate(bytes);
      }
      // The next allocation exhausts the first page and starts a second one.
      long secondPageEntry = source.allocate(bytes);
      entries.add(secondPageEntry);
      long pagesBefore = memory.pageAllocatedCount();
      long readyBefore = memory.pageReadyCount();
      long retainedBefore = memory.retainedPageCount();
      // Free half the slots: the page still owns live slots, so its owner retains it instead of
      // publishing to the shared ready stack.
      for (int index = 0; index < slotsPerPage / 2; index++) {
        memory.releaseEntry(firstPageEntries[index], bytes);
      }
      for (int index = slotsPerPage / 2; index < slotsPerPage; index++) {
        entries.add(firstPageEntries[index]);
      }
      Assert.assertEquals(memory.pageReadyCount(), readyBefore);
      Assert.assertEquals(memory.retainedPageCount(), retainedBefore + 1L);
      // Fill the second page so the next allocation must look for another page.
      for (int index = 1; index < slotsPerPage; index++) {
        entries.add(source.allocate(bytes));
      }
      long reusedBefore = memory.pageReusedCount();
      long consumedBefore = memory.retainedPageConsumedCount();
      long retainedReuse = source.allocate(bytes);
      entries.add(retainedReuse);
      Assert.assertNotEquals(retainedReuse, 0L);
      // The owner consumed its retained page without a fresh page or shared-stack traffic.
      Assert.assertEquals(memory.pageAllocatedCount(), pagesBefore);
      Assert.assertEquals(memory.pageReusedCount(), reusedBefore + 1L);
      Assert.assertEquals(memory.retainedPageConsumedCount(), consumedBefore + 1L);
    } finally {
      for (long entry : entries) {
        memory.releaseEntry(entry, 112L);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void retainedPageOverflowSpillsToTheSharedReadyStack() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    List<Long> entries = new ArrayList<>();
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      long[][] pageEntries = new long[WriterArena.RETAINED_PAGE_LIMIT + 1][slotsPerPage];
      for (int page = 0; page < pageEntries.length; page++) {
        for (int slot = 0; slot < slotsPerPage; slot++) {
          pageEntries[page][slot] = source.allocate(bytes);
        }
      }
      // One more allocation exhausts the last filled page, so every freed page is non-current.
      entries.add(source.allocate(bytes));
      long pagesBefore = memory.pageAllocatedCount();
      long readyBefore = memory.pageReadyCount();
      long retainedBefore = memory.retainedPageCount();
      // Free half the slots of every filled page: each stays partially live, so each is retained
      // until the owner's slots are full; the page beyond the limit spills to the shared stack.
      for (int page = 0; page < pageEntries.length; page++) {
        for (int slot = 0; slot < slotsPerPage / 2; slot++) {
          memory.releaseEntry(pageEntries[page][slot], bytes);
        }
        for (int slot = slotsPerPage / 2; slot < slotsPerPage; slot++) {
          entries.add(pageEntries[page][slot]);
        }
      }
      Assert.assertEquals(
          memory.retainedPageCount(), retainedBefore + WriterArena.RETAINED_PAGE_LIMIT);
      Assert.assertEquals(memory.pageReadyCount(), readyBefore + 1L);
      // Another arena steals exactly the spilled page without a fresh allocation.
      long reusedBefore = memory.pageReusedCount();
      WriterArena target = memory.newWriterArena();
      long stolen = target.allocate(bytes);
      entries.add(stolen);
      Assert.assertNotEquals(stolen, 0L);
      Assert.assertEquals(memory.pageAllocatedCount(), pagesBefore);
      Assert.assertEquals(memory.pageReusedCount(), reusedBefore + 1L);
    } finally {
      for (long entry : entries) {
        memory.releaseEntry(entry, 112L);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void completelyFreedPageLeavesRetentionAndStaysTrimmable() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    List<Long> entries = new ArrayList<>();
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      long[] firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        firstPageEntries[index] = source.allocate(bytes);
      }
      long secondPageEntry = source.allocate(bytes);
      entries.add(secondPageEntry);
      long readyBefore = memory.pageReadyCount();
      long trimmedBefore = memory.pageTrimmedCount();
      // Free every slot of the first page: with no live slots left the page must leave owner
      // retention and reach the shared ready stack so allocation pressure can trim it.
      for (long entry : firstPageEntries) {
        memory.releaseEntry(entry, bytes);
      }
      Assert.assertEquals(memory.pageReadyCount(), readyBefore + 1L);
      memory.trimAvailablePages();
      Assert.assertEquals(memory.pageTrimmedCount(), trimmedBefore + 1L);
      Assert.assertEquals(memory.pageReadyCount(), readyBefore);
    } finally {
      for (long entry : entries) {
        memory.releaseEntry(entry, 112L);
      }
      memory.closeArenas();
    }
  }

  @Test
  public void detachPublishesRetainedAndCurrentPagesToTheSharedStack() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    List<Long> entries = new ArrayList<>();
    try {
      long bytes = 112L;
      int sizeClass = SizeClasses.indexForEntry(bytes);
      int slotsPerPage = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
      WriterArena source = memory.newWriterArena();
      long[] firstPageEntries = new long[slotsPerPage];
      for (int index = 0; index < slotsPerPage; index++) {
        firstPageEntries[index] = source.allocate(bytes);
      }
      long secondPageEntry = source.allocate(bytes);
      entries.add(secondPageEntry);
      long readyBefore = memory.pageReadyCount();
      // Free half the first page so the owner retains it.
      for (int index = 0; index < slotsPerPage / 2; index++) {
        memory.releaseEntry(firstPageEntries[index], bytes);
      }
      for (int index = slotsPerPage / 2; index < slotsPerPage; index++) {
        entries.add(firstPageEntries[index]);
      }
      Assert.assertEquals(memory.pageReadyCount(), readyBefore);
      source.detach();
      // The retained page and the partially filled current page both reach the shared stack.
      Assert.assertEquals(memory.pageReadyCount(), readyBefore + 2L);
      long pagesBefore = memory.pageAllocatedCount();
      long reusedBefore = memory.pageReusedCount();
      WriterArena target = memory.newWriterArena();
      long stolen = target.allocate(bytes);
      entries.add(stolen);
      Assert.assertNotEquals(stolen, 0L);
      Assert.assertEquals(memory.pageAllocatedCount(), pagesBefore);
      Assert.assertEquals(memory.pageReusedCount(), reusedBefore + 1L);
      Assert.assertEquals(memory.pageReadyCount(), readyBefore + 1L);
    } finally {
      for (long entry : entries) {
        memory.releaseEntry(entry, 112L);
      }
      memory.closeArenas();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(2L, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static final class FailingAllocator extends NativeAllocator {
    private int failures;

    private FailingAllocator() {
      super(AllocatorType.UNSAFE);
    }

    private void failNextAllocations(int count) {
      failures = count;
    }

    @Override
    public long allocate(long bytes) {
      if (failures > 0) {
        failures--;
        throw new OutOfMemoryError("injected native allocation failure");
      }
      return super.allocate(bytes);
    }
  }

}
