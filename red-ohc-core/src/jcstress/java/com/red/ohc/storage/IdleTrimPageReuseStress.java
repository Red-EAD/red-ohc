package com.red.ohc.storage;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/** Periodic scanning can race with a writer claiming a page from either ready stack. */
@JCStressTest
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "claimed page stays live and ready tokens balance")
@Outcome(expect = Expect.FORBIDDEN, desc = "trimmed active page or lost/duplicated ready token")
@State
public class IdleTrimPageReuseStress {
  private static final long VALUE = 0x1234_5678_9abc_def0L;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena writer;
  private long address;

  public IdleTrimPageReuseStress() {
    WriterArena[] arenas = new WriterArena[3];
    long[] addresses = new long[arenas.length];
    for (int index = 0; index < arenas.length; index++) {
      arenas[index] = memory.newWriterArena();
      addresses[index] = arenas[index].allocate(32_700L);
    }
    for (int index = 0; index < arenas.length; index++) {
      arenas[index].detach();
      memory.releaseEntry(addresses[index], 32_700L);
    }
    memory.trimIdlePages(1L);
    writer = memory.newWriterArena();
  }

  @Actor
  public void trim() {
    for (int turn = 0; turn < 4; turn++) {
      memory.trimIdlePages(31_000_000_001L);
    }
  }

  @Actor
  public void allocate() {
    address = writer.allocate(32_700L);
    NativeMemory.putLong(address, VALUE);
  }

  @Arbiter
  public void arbitrate(II_Result result) {
    result.r1 = NativeMemory.getLong(address) == VALUE ? 1 : 0;
    result.r2 = memory.pageReadyCount() + 1L == memory.pooledPageCount() ? 1 : 0;
    memory.closeArenas();
  }
}
