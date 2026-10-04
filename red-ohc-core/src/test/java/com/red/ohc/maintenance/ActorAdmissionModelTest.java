package com.red.ohc.maintenance;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.annotations.Test;

/** Architectural contracts for the actor-owned FIFO admission model. */
public final class ActorAdmissionModelTest {
  @Test
  public void logicalAdmissionUsesAtomicLedgerWithoutBlockingPrimitives() {
    boolean atomicLedger = false;
    for (Field field : LogicalAdmission.class.getDeclaredFields()) {
      atomicLedger |= field.getType() == LongAdder.class;
      assertFalse(
          ReentrantLock.class.isAssignableFrom(field.getType()),
          "logical admission must not own a global blocking lock");
      assertFalse(
          Condition.class.isAssignableFrom(field.getType()),
          "logical admission must not own a completion condition");
    }
    assertTrue(atomicLedger, "logical admission must expose an atomic charge ledger");
  }

  @Test
  public void admissionDoesNotRetainWriterVictimTransactionObjects() {
    for (Class<?> nested : LogicalAdmission.class.getDeclaredClasses()) {
      assertFalse(
          nested.getSimpleName().equals("VictimHandler"),
          "victim physical work must be actor-owned instead of a writer callback transaction");
    }
  }

  @Test
  public void maintenanceDoesNotOwnACommonPoolReclaimer() {
    for (Field field : MaintenanceEventLoop.class.getDeclaredFields()) {
      assertFalse(
          Executor.class.isAssignableFrom(field.getType()),
          "maintenance actor must not retain a common-pool reclaimer");
      assertFalse(
          ForkJoinPool.class.isAssignableFrom(field.getType()),
          "maintenance actor must not retain ForkJoinPool state");
      assertFalse(
          field.getName().startsWith("safeReclaimTask"),
          "safe reclaim must be represented by actor messages");
      assertFalse(
          field.getName().equals("safeSegmentReclaimTask"),
          "safe reclaim must not use a Runnable submitted to another executor");
      assertFalse(
          field.getName().equals("asyncSubmissionLock"),
          "flush watermarks must not require the common-pool submission lock");
    }
    assertFalse(
        hasDeclaredNestedClass("SafeSegmentReclaimTask"),
        "safe reclaim must run on the maintenance actor");
  }

  @Test
  public void actorDoesNotPauseBetweenImmediateContinuations() {
    assertFalse(hasDeclaredField("CONTINUATION_PAUSE_NANOS"));
    assertFalse(hasDeclaredMethod("pauseBeforeContinuation"));
  }

  @Test
  public void linkRecordStoresActorMetadataWithoutChangingEntryLayout() throws Exception {
    assertTrue(
        EntryLinks.RECORD_BYTES == 32,
        "actor metadata must live in the fixed 32-byte intrusive record");
    assertTrue(
        EntryLinks.RECORDS_PER_LOGICAL_PAGE == 128,
        "32-byte records must preserve the 4 KiB logical page boundary");
  }

  private static boolean hasDeclaredField(String name) {
    for (Field field : MaintenanceEventLoop.class.getDeclaredFields()) {
      if (field.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasDeclaredMethod(String name) {
    for (Method method : MaintenanceEventLoop.class.getDeclaredMethods()) {
      if (method.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasDeclaredNestedClass(String simpleName) {
    for (Class<?> nested : MaintenanceEventLoop.class.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(simpleName)) {
        return true;
      }
    }
    return false;
  }
}
