package com.red.ohc.runtime;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class ReaderSlotTest {
  @Test
  public void producerRearmsAccessSignalAfterActorClearsPending() {
    ReaderSlot slot = new ReaderSlot();

    assertTrue(slot.markAccessPending(), "the first producer must claim the wake transition");
    assertFalse(slot.markAccessPending(), "coalesced producers must not signal repeatedly");
    assertTrue(slot.clearAccessPending(), "the actor must clear the pending state before draining");
    assertTrue(
        slot.markAccessPending(),
        "a producer entering after actor clear must claim a fresh wake transition");
  }
}
