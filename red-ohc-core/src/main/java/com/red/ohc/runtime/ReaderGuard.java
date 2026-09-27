package com.red.ohc.runtime;

import java.util.Objects;

import com.red.ohc.maintenance.MaintenanceEventLoop;

/**
 * Publishes a per-op sequence word (odd = inside an op) and keeps native payloads protected until
 * the depth-0 exit store. Close is a quiesced-only teardown, so this guard owns
 * reader-vs-reclaimer safety, not close racing.
 */
public final class ReaderGuard {
  private final MaintenanceEventLoop worker;

  public ReaderGuard(MaintenanceEventLoop worker) {
    this.worker = Objects.requireNonNull(worker, "worker");
  }

  public boolean enter(ThreadContext context) {
    return enter(context, true);
  }

  /** Enters a value reader admitted by an operation-level lifecycle guard. */
  public boolean enterAfterAdmission(ThreadContext context) {
    return enter(context, true);
  }

  /** Protects native-key lookup only; the caller does not dereference a replaceable value. */
  public boolean enterLookupAfterAdmission(ThreadContext context) {
    return enter(context, false);
  }

  private boolean enter(ThreadContext context, boolean protectsValues) {
    if (!context.isRegistered()) {
      if (!worker.registerReader(context)) {
        return false;
      }
    }
    if (context.readerDepth() != 0) {
      // Nested scopes only bump depth; the first value protector re-stores the odd word with
      // the protection bit, and a lookup-only inner scope inherits whatever the outer set.
      if (context.enterReader(protectsValues)) {
        context.upgradeReaderOpValueBit();
      }
      return true;
    }
    context.beginReaderOp(protectsValues);
    context.enterReader(protectsValues);
    return true;
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
