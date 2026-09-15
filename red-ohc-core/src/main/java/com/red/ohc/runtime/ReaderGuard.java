package com.red.ohc.runtime;

import java.util.Objects;
import java.util.function.BooleanSupplier;

import com.red.ohc.maintenance.MaintenanceEventLoop;

/** Publishes a reader epoch and keeps native payloads protected until exit. */
public final class ReaderGuard {
  private final MaintenanceEventLoop worker;
  private final BooleanSupplier closing;

  public ReaderGuard(MaintenanceEventLoop worker) {
    this(worker, () -> false);
  }

  public ReaderGuard(MaintenanceEventLoop worker, BooleanSupplier closing) {
    this.worker = Objects.requireNonNull(worker, "worker");
    this.closing = Objects.requireNonNull(closing, "closing");
  }

  public boolean enter(ThreadContext context) {
    return enter(context, true, true);
  }

  /** Enters a value reader admitted by an operation-level lifecycle guard before close began. */
  public boolean enterAfterAdmission(ThreadContext context) {
    return enter(context, false, true);
  }

  /** Protects native-key lookup only; the caller does not dereference a replaceable value. */
  public boolean enterLookupAfterAdmission(ThreadContext context) {
    return enter(context, false, false);
  }

  private boolean enter(
      ThreadContext context, boolean rejectClosing, boolean protectsValues) {
    if (!context.isRegistered()) {
      if (rejectClosing && closing.getAsBoolean()) {
        return false;
      }
      if (!worker.registerReader(context)) {
        return false;
      }
    }
    if (context.readerDepth() != 0) {
      if (rejectClosing && context.isWriterEntered() && closing.getAsBoolean()) {
        return false;
      }
      if (context.enterReader(protectsValues)) {
        publishReaderState(
            context, ReaderRegistry.VALUE_PROTECTION_BIT | context.readerPublishedEpoch());
      }
      return true;
    }
    while (!rejectClosing || !closing.getAsBoolean()) {
      long observed = worker.epoch();
      try {
        publishReaderState(
            context, protectsValues ? ReaderRegistry.VALUE_PROTECTION_BIT | observed : observed);
      } catch (IllegalArgumentException failure) {
        if (closing.getAsBoolean()) {
          context.readerPublishedEpoch(0L);
          return false;
        }
        throw failure;
      }
      // This closes the admission race with shutdown: a close that starts after the loop
      // condition but before epoch publication must observe us as quiescent, not let this
      // reader pass through to a native pointer that its actor has already freed.
      if (rejectClosing && closing.getAsBoolean()) {
        worker.clearReaderStateIfRegistered(context);
        return false;
      }
      if (observed == worker.epoch()) {
        context.enterReader(protectsValues);
        return true;
      }
      worker.clearReaderStateIfRegistered(context);
    }
    worker.clearReaderStateIfRegistered(context);
    return false;
  }

  private void publishReaderState(ThreadContext context, long state) {
    worker.publishReaderStateKnownEpoch(context, state);
  }

  public void exit(ThreadContext context) {
    int exited = context.exitReader();
    long exitedEpoch = context.readerPublishedEpoch();
    if ((exited & ThreadContext.READER_EXITED_VALUES) != 0) {
      if ((exited & ThreadContext.READER_EXITED_LOOKUP) == 0) {
        // A nested value reader may be released while an outer lookup-only guard remains active.
        worker.publishReaderState(context, exitedEpoch);
        worker.readerQuiescent(context.slot);
      }
    }
    if ((exited & ThreadContext.READER_EXITED_LOOKUP) == 0) {
      return;
    }
    worker.clearReaderState(context);
    worker.readerQuiescent(context.slot);
  }
}
