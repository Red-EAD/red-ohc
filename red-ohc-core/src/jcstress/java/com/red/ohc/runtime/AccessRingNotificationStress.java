package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

/** Records the remaining best-effort notification race after an accepted record is published. */
@JCStressTest
@Outcome(
    id = "0, 0, 1",
    expect = Expect.ACCEPTABLE,
    desc = "stale signaled state can leave a pending record without a new signal")
@Outcome(
    id = "0, 1, 1",
    expect = Expect.ACCEPTABLE,
    desc = "the producer signals after the empty scan arms")
@Outcome(
    id = "1, 0, 1",
    expect = Expect.ACCEPTABLE,
    desc = "the armed scan observes the pending record while the stale signal remains quiet")
@Outcome(
    id = "1, 1, 1",
    expect = Expect.ACCEPTABLE,
    desc = "the armed scan observes a record and a prior signal")
@Outcome(expect = Expect.FORBIDDEN, desc = "an accepted record must not disappear")
@State
public class AccessRingNotificationStress {
  private final AtomicInteger signals = new AtomicInteger();
  private final AccessRing ring;

  public AccessRingNotificationStress() {
    ring = new AccessRing(signals::incrementAndGet);
    ring.offer(null, 0L, 0L, 0);
    ring.poll((entry, address, generation, policyState) -> {});
    signals.set(0);
  }

  @Actor
  public void publish() {
    ring.offer(null, 1L, 1L, 0);
  }

  @Actor
  public void armAndScan(III_Result result) {
    result.r1 = ring.armNotificationAndCheckPending() ? 1 : 0;
  }

  @Arbiter
  public void observe(III_Result result) {
    result.r2 = signals.get();
    result.r3 = ring.size();
  }
}
