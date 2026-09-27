package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import com.red.ohc.storage.NativeMemory;

/**
 * The odd-sequence entry store must close the Dekker race with the actor's arm: if the reader's
 * native loads executed, the actor's armed snapshot must observe the odd value.
 */
@JCStressTest
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "arm observes the odd entry")
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE, desc = "reader observes the flag first")
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "both publications observed")
@Outcome(
    id = "0, 0",
    expect = Expect.FORBIDDEN,
    desc = "reader's loads ran while the arm missed its odd entry")
@State
public class ReaderSeqEntryFenceStress {
  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final ReaderRegistry registry = new ReaderRegistry(memory);
  private final ReaderSlot slot = new ReaderSlot();
  private final int index = registry.register(slot);
  private final long[] snapshot = new long[registry.slotCapacity()];
  private final AtomicInteger flag = new AtomicInteger();

  @Actor
  public void reader(II_Result result) {
    registry.beginOpForTest(index, true);
    NativeMemory.getLong(memory.allocate(8L));
    result.r1 = flag.get();
  }

  @Actor
  public void arm(II_Result result) {
    flag.set(1);
    registry.armReaderQuiescence(snapshot);
    result.r2 = (snapshot[index] & 1L) != 0L ? 1 : 0;
  }

  @Arbiter
  public void close() {
    registry.endOpForTest(index);
    registry.clear();
    registry.close();
    memory.closeArenas();
  }
}
