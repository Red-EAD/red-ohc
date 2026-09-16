package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.storage.NativeMemory;

/** Unit tests for the lock-free logical capacity ledger. */
public final class LogicalAdmissionTest {
  @Test
  public void fittingChargeUsesTheAtomicFastPath() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);

    assertTrue(admission.tryChargeDelta(60L));
    assertEquals(admission.logicalCharge(), 60L);
    assertTrue(admission.tryChargeDelta(-20L));
    assertEquals(admission.logicalCharge(), 40L);
  }

  @Test
  public void anOversizedSingleMappingIsAcceptedAndMarkedOverTarget() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);

    assertTrue(admission.tryChargeDelta(101L));
    assertTrue(admission.needsCapacityWake());
    admission.refreshActorSnapshot();
    assertTrue(admission.isOverTarget());
    assertTrue(
        admission.needsCapacityWake(),
        "an actor parked in capacity-retry backoff must stay wakeable while over target");
    assertEquals(admission.logicalCharge(), 101L);
  }

  @Test
  public void releasingAChargeCannotUnderflowTheLedger() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);

    assertTrue(admission.tryChargeDelta(25L));
    admission.releaseUnpublishedCharge(25L);
    assertEquals(admission.logicalCharge(), 0L);
  }

  @Test
  public void concurrentLedgerAcceptsSignedDeltasAndRemainsEventuallyExact() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);

    assertTrue(admission.tryChargeDelta(Long.MAX_VALUE));
    assertTrue(admission.tryChargeDelta(-Long.MAX_VALUE));
    assertEquals(admission.logicalCharge(), 0L);
  }

  @Test
  public void actorCapacitySampleSubtractsWithoutChangingTheSharedLedger() {
    LogicalAdmission admission = new LogicalAdmission(Long.MAX_VALUE - 1L, false);

    assertTrue(admission.tryChargeDelta(Long.MAX_VALUE));
    assertEquals(admission.actorBeginCapacityPass(), Long.MAX_VALUE);
    assertTrue(admission.isOverTarget());

    admission.actorSubtractReleasedCharge(Long.MAX_VALUE);

    assertFalse(admission.isOverTarget(), "the actor-local sample must not underflow");
    assertEquals(
        admission.logicalCharge(),
        Long.MAX_VALUE,
        "physical removal accounting must not mutate the shared writer ledger");

    admission.actorReconcileCapacity();
    assertTrue(admission.isOverTarget(), "reconcile must observe the still-over-target ledger");
  }

  @Test
  public void externalShrinkAndReservationRollbackInvalidateTheCapacitySample() {
    assertExternalReductionObserved(admission -> admission.tryChargeDelta(-20L));
    assertExternalReductionObserved(admission -> admission.releaseUnpublishedCharge(20L));
    assertExternalReductionObserved(admission -> admission.rollbackUnpublishedDelta(20L));
  }

  private void assertExternalReductionObserved(Consumer<LogicalAdmission> reduction) {
    LogicalAdmission admission = new LogicalAdmission(100L, false);
    admission.tryChargeDelta(110L);
    admission.actorBeginCapacityPass();
    reduction.accept(admission);
    admission.actorReconcileExternalReductions();
    assertFalse(admission.isOverTarget());
    assertEquals(admission.logicalCharge(), 90L);
  }

  @Test
  public void actorAbsentTransitionIsCountedOnce() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
    Entry entry = EntryTestSupport.entry(memory, 0, 7, 0x77L, 0L);
    try {
      entry.currentValueAllocation(64L);
      LogicalAdmission admission = new LogicalAdmission(1_000L, false);
      long charge = admission.chargeOf(entry);
      assertTrue(admission.tryChargeDelta(charge));
      assertTrue(admission.markPresentAfterCharge(entry));
      assertTrue(entry.claimWriter());

      assertEquals(admission.markAbsentByActor(entry), charge);
      assertEquals(admission.markAbsentByActor(entry), 0L);
      assertEquals(admission.logicalMappingCount(), 0L);
      assertEquals(admission.logicalCharge(), 0L);
      entry.finishWriter();
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void minimumDeltaIsRejectedWithoutChangingTheLedger() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);

    try {
      admission.tryChargeDelta(Long.MIN_VALUE);
    } catch (IllegalArgumentException expected) {
      assertEquals(admission.logicalCharge(), 0L);
      return;
    }
    throw new AssertionError("Long.MIN_VALUE must be rejected");
  }

  @Test
  public void admissionDoesNotOwnABlockingLockOrCondition() {
    boolean atomicLedger = false;
    for (java.lang.reflect.Field field : LogicalAdmission.class.getDeclaredFields()) {
      atomicLedger |= field.getType() == LongAdder.class;
      assertFalse(ReentrantLock.class.isAssignableFrom(field.getType()));
      assertFalse(Condition.class.isAssignableFrom(field.getType()));
    }
    assertTrue(atomicLedger);
  }

  @Test
  public void concurrentWriteDuringReconciliationIsNotTerminalFailure() {
    LogicalAdmission admission = new LogicalAdmission(100L, false);
    ConcurrentHashMap<Entry, Entry> mappings = new ConcurrentHashMap<>();
    AtomicInteger probes = new AtomicInteger();

    boolean sampled =
        admission.assertStableIfQuiescent(
            mappings,
            () -> {
              if ((probes.getAndIncrement() & 1) == 0) {
                admission.tryChargeDelta(1L);
              }
              return true;
            });

    assertFalse(sampled, "an unstable concurrent snapshot must be retried by the caller");
    assertEquals(probes.get(), 2, "one diagnostic probe must be enough; the caller owns retrying");
  }
}
