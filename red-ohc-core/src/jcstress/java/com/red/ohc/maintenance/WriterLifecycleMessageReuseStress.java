package com.red.ohc.maintenance;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/** Exercises reusable mailbox nodes across lifecycle segment rollover and consumer release. */
@JCStressTest
@Outcome(id = "64, 64", expect = Expect.ACCEPTABLE, desc = "every reusable lifecycle slot is dispatched once")
@State
public class WriterLifecycleMessageReuseStress {
  private static final int RECORDS = 64;

  private final WriterLifecycleLane lane = new WriterLifecycleLane(2);

  @Actor
  public void publish() {
    for (int index = 0; index < RECORDS; index++) {
      long sequence = lane.reserve();
      lane.writeMutation(
          sequence, null, 0L, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION);
      lane.commitForMailbox(sequence);
    }
  }

  @Actor
  public void consume() {
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    while (lane.completedRecordsTotal() < RECORDS) {
      if (lane.poll(record)) {
        lane.release(record);
      } else {
        Thread.onSpinWait();
      }
    }
  }

  @Arbiter
  public void observe(II_Result result) {
    result.r1 = (int) lane.publishedRecordsTotal();
    result.r2 = (int) lane.completedRecordsTotal();
  }
}
