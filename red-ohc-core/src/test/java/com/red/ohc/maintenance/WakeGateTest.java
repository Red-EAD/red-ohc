package com.red.ohc.maintenance;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class WakeGateTest {
  @Test
  public void collapsesRepeatedSignalsUntilTheActorReturnsToIdle() {
    WakeGate gate = new WakeGate();

    assertTrue(gate.signal(), "the idle to required transition owns the single unpark");
    assertFalse(gate.signal(), "additional queued work must not unpark again");
    assertFalse(gate.signal(), "the worker remains required until it arms an idle park");

    assertTrue(gate.armIdle());
    assertTrue(gate.finishIdle());
    assertTrue(gate.signal(), "a later idle period needs one new wakeup");
  }

  @Test
  public void signalDuringParkArmingPreventsTheActorFromParking() {
    WakeGate gate = new WakeGate();
    assertTrue(gate.signal());

    assertTrue(gate.armIdle());
    assertFalse(gate.signal(), "the actor is still running, so no unpark is needed");
    assertFalse(gate.finishIdle(), "the racing signal must keep the actor required");
    assertTrue(
        gate.isRequired(), "the required state is re-established before the next park attempt");
  }
}
