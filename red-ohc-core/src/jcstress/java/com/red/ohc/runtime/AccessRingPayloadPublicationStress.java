package com.red.ohc.runtime;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import com.red.ohc.index.Entry;

/** Verifies that the release-published producer head makes every payload column visible. */
@JCStressTest
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "consumer ran before publication")
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "published payload is complete")
@Outcome(expect = Expect.FORBIDDEN, desc = "head became visible before the payload")
@State
public class AccessRingPayloadPublicationStress {
  private static final long ADDRESS = 0x1122_3344_5566_7788L;
  private static final long GENERATION = 0x7766_5544_3322_1100L;
  private static final int POLICY_STATE = 0x1357_2468;
  private static final Entry ENTRY = new Entry(0L, 0, 0L);
  private final AccessRing ring = new AccessRing();

  @Actor
  public void publish() {
    ring.offer(ENTRY, ADDRESS, GENERATION, POLICY_STATE);
  }

  @Actor
  public void consume(II_Result result) {
    result.r1 =
        ring.poll(
                (entry, address, generation, policyState) ->
                    result.r2 =
                        entry == ENTRY
                                && address == ADDRESS
                                && generation == GENERATION
                                && policyState == POLICY_STATE
                            ? 1
                            : -1)
            ? 1
            : 0;
  }
}
