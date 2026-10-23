package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public final class IdlePageTrimTest {
  @Test
  public void emergencyTrimStillReleasesAllEmptyPages() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      emptyPages(memory, 10);
      assertEquals(memory.pageReadyCount(), 10L);
      assertEquals(memory.trimAvailablePages(), 20L << 20);
      assertEquals(memory.pageReadyCount(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void periodicTrimRetainsWarmPagesAndBoundsRelease() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      emptyPages(memory, 10);
      assertEquals(memory.trimIdlePages(1L), 0L);
      assertEquals(memory.trimIdlePages(30_000_000_001L), 8L << 20);
      assertEquals(memory.pageReadyCount(), 6L);
      assertEquals(memory.trimIdlePages(31_000_000_001L), 8L << 20);
      assertEquals(memory.pageReadyCount(), 2L);
      assertEquals(memory.trimIdlePages(32_000_000_001L), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void partialPagesAtReadyHeadDoNotStarveOlderEmptyPages() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      long[] emptyAddresses = new long[8];
      long[] partialAddresses = new long[20];
      WriterArena[] emptyArenas = allocatedPages(memory, emptyAddresses);
      WriterArena[] partialArenas = allocatedPages(memory, partialAddresses);

      detach(emptyArenas);
      for (int index = 0; index < emptyArenas.length; index++) {
        memory.releaseEntry(emptyAddresses[index], 32_700L);
      }
      detach(partialArenas);

      assertEquals(memory.pageReadyCount(), 28L);
      assertEquals(memory.trimIdlePages(1L), 0L);
      assertEquals(memory.trimIdlePages(1L), 0L);
      assertEquals(memory.trimIdlePages(30_000_000_001L), 8L << 20);
      assertEquals(memory.pageReadyCount(), 24L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void continuousReadyPageArrivalsDoNotStarveAgedEmptyPages() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    try {
      long[] partialAddresses = new long[40];
      long[] emptyAddresses = new long[8];
      WriterArena[] partialArenas = allocatedPages(memory, partialAddresses, 1L);
      WriterArena[] emptyArenas = allocatedPages(memory, emptyAddresses, 1L);
      WriterArena[][] arrivals = new WriterArena[10][];
      for (int batch = 0; batch < arrivals.length; batch++) {
        arrivals[batch] = allocatedPages(memory, new long[16], 1L);
      }

      detach(partialArenas);
      detach(emptyArenas);
      for (long address : emptyAddresses) {
        memory.releaseEntry(address, 1L);
      }

      assertEquals(memory.trimIdlePages(1L), 0L);
      for (int batch = 0; batch < 4; batch++) {
        detach(arrivals[batch]);
        assertEquals(memory.trimIdlePages(1L), 0L);
      }

      long releasedBytes = 0L;
      for (int batch = 4; batch < arrivals.length; batch++) {
        detach(arrivals[batch]);
        releasedBytes += memory.trimIdlePages(30_000_000_001L);
      }

      assertEquals(releasedBytes, 6L * SizeClasses.pageBytes(SizeClasses.indexForEntry(1L)));
    } finally {
      memory.closeArenas();
    }
  }

  private static void emptyPages(NativeMemory.Memory memory, int count) {
    long[] addresses = new long[count];
    WriterArena[] arenas = allocatedPages(memory, addresses);
    detach(arenas);
    for (int index = 0; index < addresses.length; index++) {
      memory.releaseEntry(addresses[index], 32_700L);
    }
  }

  private static WriterArena[] allocatedPages(NativeMemory.Memory memory, long[] addresses) {
    return allocatedPages(memory, addresses, 32_700L);
  }

  private static WriterArena[] allocatedPages(
      NativeMemory.Memory memory, long[] addresses, long entryBytes) {
    WriterArena[] arenas = new WriterArena[addresses.length];
    for (int index = 0; index < addresses.length; index++) {
      arenas[index] = memory.newWriterArena();
      addresses[index] = arenas[index].allocate(entryBytes);
    }
    return arenas;
  }

  private static void detach(WriterArena[] arenas) {
    for (WriterArena arena : arenas) {
      arena.detach();
    }
  }
}
