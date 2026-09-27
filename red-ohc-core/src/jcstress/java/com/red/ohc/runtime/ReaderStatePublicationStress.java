package com.red.ohc.runtime;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

import com.red.ohc.storage.NativeMemory;

/** Verifies that the combined reader state is observed as one valid long value. */
@JCStressTest
@Outcome(
    id = {"0", "-9223372036854775807"},
    expect = Expect.ACCEPTABLE,
    desc = "the actor observes either an even word or the odd value-protecting entry")
@State
public class ReaderStatePublicationStress {
  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final ReaderRegistry registry = new ReaderRegistry(memory);
  private final int slot = registry.register(new ReaderSlot());

  @Actor
  public void publish() {
    registry.beginOpForTest(slot, true);
  }

  @Actor
  public void observe(J_Result result) {
    result.r1 = registry.readerSequence(registry.slotAt(slot));
  }

  @Arbiter
  public void close() {
    registry.clear();
    registry.close();
    memory.closeArenas();
  }
}
