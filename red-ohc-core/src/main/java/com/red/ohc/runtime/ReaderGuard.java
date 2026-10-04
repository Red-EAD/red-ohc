package com.red.ohc.runtime;

import java.util.Objects;

import com.red.ohc.maintenance.MaintenanceEventLoop;

/**
 * Publishes a per-op sequence word (odd = inside an op) and keeps native payloads protected until
 * the depth-0 exit store.
 */
public final class ReaderGuard {
  private final MaintenanceEventLoop worker;

  public ReaderGuard(MaintenanceEventLoop worker) {
    this.worker = Objects.requireNonNull(worker, "worker");
  }

  public void enter(ThreadContext context) {
    enter(context, true);
  }

  /** Enters a value reader admitted by an operation-level lifecycle guard. */
  public void enterAfterAdmission(ThreadContext context) {
    enter(context, true);
  }

  /** Protects native-key lookup only; the caller does not dereference a replaceable value. */
  public void enterLookupAfterAdmission(ThreadContext context) {
    enter(context, false);
  }

  private void enter(ThreadContext context, boolean protectsValues) {
    if (!context.isRegistered()) {
      worker.registerReader(context);
    }
    if (context.readerDepth() != 0) {
      // Nested scopes only bump depth; the first value protector re-stores the odd word with
      // the protection bit, and a lookup-only inner scope inherits whatever the outer set.
      if (context.enterReader(protectsValues)) {
        context.upgradeReaderOpValueBit();
      }
      return;
    }
    context.beginReaderOp(protectsValues);
    context.enterReader(protectsValues);
  }

  public void exit(ThreadContext context) {
    int exited = context.exitReader();
    if ((exited & ThreadContext.READER_EXITED_LOOKUP) != 0) {
      // The value bit deliberately persists until depth 0: a mid-stack downgrade is a no-op so
      // the actor never sees an unprotecting store inside one op.
      context.endReaderOp();
      worker.readerQuiescent(context.slot);
    }
  }
}
