package com.red.ohc.runtime;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/** Verifies that the combined reader state is observed as one valid long value. */
@JCStressTest
@Outcome(
    id = {"0", "-9223372036854775807"},
    expect = Expect.ACCEPTABLE,
    desc = "the actor observes either the old quiescent state or the fully published value state")
@State
public class ReaderStatePublicationStress {
  private final ReaderRegistry registry = new ReaderRegistry();
  private final int slot = registry.register(new ReaderSlot());

  @Actor
  public void publish() {
    registry.setReaderState(slot, ReaderRegistry.VALUE_PROTECTION_BIT | 1L);
  }

  @Actor
  public void observe(J_Result result) {
    result.r1 = registry.readerState(slot);
  }
}
