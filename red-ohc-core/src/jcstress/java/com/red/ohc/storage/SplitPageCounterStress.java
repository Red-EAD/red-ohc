package com.red.ohc.storage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;


/** Owner allocation and independent single/batch frees must retain an exact live-slot count. */
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "all slots and the live count are preserved")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "a release or counter update was lost")
@State
public class SplitPageCounterStress {
  private static final long ENTRY_BYTES = 112L;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final WriterArena.Page page;
  private final long firstBlock;
  private final long firstHandle;
  private final long secondBlock;
  private final long secondHandle;
  private final long thirdEntry;
  private final long[] batchEntries;
  private final long[] batchHandles;
  private final int[] batchIndexes = {0};
  private long ownerAllocation;

  public SplitPageCounterStress() {
    int sizeClass = SizeClasses.indexForEntry(ENTRY_BYTES);
    long firstEntry = arena.allocate(ENTRY_BYTES);
    long secondEntry = arena.allocate(ENTRY_BYTES);
    thirdEntry = arena.allocate(ENTRY_BYTES);
    page = arena.sizeClassState(sizeClass).currentPage;
    firstBlock = firstEntry - WriterArena.PREFIX_BYTES;
    firstHandle = NativeMemory.getLong(firstBlock + 56L);
    secondBlock = secondEntry - WriterArena.PREFIX_BYTES;
    secondHandle = NativeMemory.getLong(secondBlock + 56L);
    batchEntries = new long[] {thirdEntry};
    batchHandles = new long[] {NativeMemory.getLong(thirdEntry - 8L)};
    page.freeSlot(firstBlock, firstHandle);
  }

  @Actor
  public void allocateByOwner() {
    ownerAllocation = page.allocateSlot();
  }

  @Actor
  public void freeFromWorkerRollback() {
    page.freeSlot(secondBlock, secondHandle);
  }

  @Actor
  public void freeFromActorBatch() {
    page.freeEntries(batchEntries, batchHandles, batchIndexes, 0, 1);
  }

  @Arbiter
  public void observe(I_Result result) {
    try {
      long secondAllocation = page.allocateSlot();
      long thirdAllocation = page.allocateSlot();
      long ownerBlock = ownerAllocation;
      if (ownerBlock == 0L
          || secondAllocation == 0L
          || thirdAllocation == 0L
          || ownerBlock == secondAllocation
          || ownerBlock == thirdAllocation
          || secondAllocation == thirdAllocation) {
        result.r1 = 0;
        return;
      }
      int firstRemaining = page.freeSlot(ownerBlock, NativeMemory.getLong(ownerBlock + 56L));
      int secondRemaining =
          page.freeSlot(secondAllocation, NativeMemory.getLong(secondAllocation + 56L));
      int lastRemaining =
          page.freeSlot(thirdAllocation, NativeMemory.getLong(thirdAllocation + 56L));
      arena.detach();
      boolean trimmable = page.beginTrimming();
      result.r1 =
          firstRemaining == 2 && secondRemaining == 1 && lastRemaining == 0 && trimmable ? 1 : 0;
    } catch (RuntimeException | Error failure) {
      result.r1 = 0;
    } finally {
      memory.closeArenas();
    }
  }
}
