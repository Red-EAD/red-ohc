package com.red.ohc.storage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;


/** A concurrent free cannot strand a bitmap word while the owner clears its summary bit. */
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "both freed slots remain discoverable")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "a free bit was stranded without a summary")
@State
public class FreeBitmapPublicationStress {
  private static final long ENTRY_BYTES = 112L;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final WriterArena.Page page;
  private final long firstBlock;
  private final long firstHandle;
  private final long secondBlock;
  private final long secondHandle;
  private long ownerAllocation;

  public FreeBitmapPublicationStress() {
    int sizeClass = SizeClasses.indexForEntry(ENTRY_BYTES);
    int slots = SizeClasses.pageBytes(sizeClass) / SizeClasses.slotBytes(sizeClass);
    long firstEntry = arena.allocate(ENTRY_BYTES);
    long secondEntry = arena.allocate(ENTRY_BYTES);
    firstBlock = firstEntry - WriterArena.PREFIX_BYTES;
    firstHandle = NativeMemory.getLong(firstBlock + 56L);
    secondBlock = secondEntry - WriterArena.PREFIX_BYTES;
    secondHandle = NativeMemory.getLong(secondBlock + 56L);
    for (int slot = 2; slot < slots; slot++) {
      arena.allocate(ENTRY_BYTES);
    }
    page = arena.sizeClassState(sizeClass).currentPage;
    if (page == null || page.allocateSlot() != 0L) {
      throw new AssertionError("expected one full allocator page");
    }
    page.freeSlot(firstBlock, firstHandle);
  }

  @Actor
  public void publishSecondFree() {
    page.freeSlot(secondBlock, secondHandle);
  }

  @Actor
  public void allocateFirstFree() {
    ownerAllocation = page.allocateSlot();
  }

  @Arbiter
  public void observe(I_Result result) {
    long remainingAllocation = page.allocateSlot();
    result.r1 =
        ownerAllocation != 0L
                && remainingAllocation != 0L
                && ownerAllocation != remainingAllocation
                && (ownerAllocation == firstBlock || ownerAllocation == secondBlock)
                && (remainingAllocation == firstBlock || remainingAllocation == secondBlock)
            ? 1
            : 0;
    memory.closeArenas();
  }
}
