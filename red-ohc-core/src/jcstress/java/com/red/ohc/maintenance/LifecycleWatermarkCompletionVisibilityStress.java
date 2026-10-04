package com.red.ohc.maintenance;

import java.util.concurrent.CompletableFuture;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

/** The captured reservation cannot complete before its payload is published and consumed. */
@JCStressTest
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "The observer raced before completion")
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Completion publishes the consumed payload")
@Outcome(expect = Expect.FORBIDDEN, desc = "An incomplete reservation or stale payload escaped")
@State
public class LifecycleWatermarkCompletionVisibilityStress {
  private final WriterLifecycleJournal journal = new WriterLifecycleJournal();
  private final WriterLifecycleLane lane = journal.createLane();
  private final long sequence = lane.reserve();
  private final long[] watermark = journal.captureWatermark();
  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private long observedPayload;

  @Actor
  public void producer(I_Result result) {
    lane.writeMutation(sequence, null, 17, 23L, 31L);
    lane.commit(sequence);
    if (completion.isDone()) {
      completion.join();
      result.r1 = observedPayload == 71L ? 1 : 2;
    }
  }

  @Actor
  public void consumer() {
    WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
    if (lane.poll(record)) {
      observedPayload = record.valueAddress + record.allocation + record.generation;
      lane.release(record);
    }
    if (journal.watermarkComplete(watermark)) {
      completion.complete(null);
    }
  }
}
