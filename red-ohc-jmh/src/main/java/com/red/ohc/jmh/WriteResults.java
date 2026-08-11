package com.red.ohc.jmh;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/** Reports attempted writes separately from accepted and rejected admissions. */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class WriteResults {
  public long attempted;
  public long accepted;
  public long rejected;

  @Setup(Level.Iteration)
  public void reset() {
    attempted = 0L;
    accepted = 0L;
    rejected = 0L;
  }

  public void record(boolean wasAccepted) {
    attempted++;
    if (wasAccepted) {
      accepted++;
    } else {
      rejected++;
    }
  }
}
