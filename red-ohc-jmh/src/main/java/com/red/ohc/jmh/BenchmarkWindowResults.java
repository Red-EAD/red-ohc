package com.red.ohc.jmh;

import java.util.Locale;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.IterationType;

/** Emits raw process and actor CPU measurements for each measured JMH iteration. */
@State(Scope.Thread)
public class BenchmarkWindowResults {
  private boolean capture;
  private BenchmarkCpuWindow.Snapshot before;

  @Setup(Level.Iteration)
  public void start(ThreadParams params) {
    capture = params.getThreadIndex() == 0;
    before = capture ? BenchmarkCpuWindow.capture() : null;
  }

  @TearDown(Level.Iteration)
  public void finish(IterationParams params) {
    if (!capture || params.getType() != IterationType.MEASUREMENT) {
      return;
    }
    BenchmarkCpuWindow.Delta delta = BenchmarkCpuWindow.delta(before, BenchmarkCpuWindow.capture());
    System.out.printf(
        Locale.ROOT,
        "measurement-window: wallNanos=%d, processCpuNanos=%d, actorCpuNanos=%d, "
            + "processCpuKnown=%s, actorCpuKnown=%s%n",
        delta.wallNanos,
        delta.processCpuNanos,
        delta.actorCpuNanos,
        delta.processCpuKnown,
        delta.actorCpuKnown);
  }
}
