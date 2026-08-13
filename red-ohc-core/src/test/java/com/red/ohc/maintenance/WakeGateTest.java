package com.red.ohc.maintenance;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

  @Test(timeOut = 30_000L)
  public void signalRacingTheFinalIdleTransitionNeverStrandsTheGate() throws Exception {
    WakeGate gate = new WakeGate();
    Phaser phase = new Phaser(3);
    AtomicBoolean running = new AtomicBoolean(true);

    Thread actor =
        new Thread(() -> race(gate::finishIdle, phase, running), "wake-gate-test-actor");
    Thread producer =
        new Thread(() -> race(gate::signal, phase, running), "wake-gate-test-producer");
    actor.start();
    producer.start();
    try {
      for (int iteration = 0; iteration < 10_000; iteration++) {
        assertTrue(gate.signal(), "each round starts from an idle gate");
        assertTrue(gate.armIdle(), "the actor owns the idle transition");

        awaitAdvance(phase);
        awaitAdvance(phase);

        assertTrue(
            gate.armIdle(),
            "a racing signal must leave REQUIRED, never PROCESSING_TO_REQUIRED");
        assertTrue(gate.finishIdle(), "the gate must return to idle for the next round");
      }
    } finally {
      running.set(false);
      phase.forceTermination();
      actor.join();
      producer.join();
    }
  }

  private static void race(
      Runnable operation, Phaser phase, AtomicBoolean running) {
    while (!phase.isTerminated()) {
      if (phase.arriveAndAwaitAdvance() < 0 || !running.get()) {
        return;
      }
      operation.run();
      if (phase.arriveAndAwaitAdvance() < 0) {
        return;
      }
    }
  }

  private static void awaitAdvance(Phaser phase) throws Exception {
    int current = phase.arrive();
    phase.awaitAdvanceInterruptibly(current, 5L, TimeUnit.SECONDS);
  }
}
