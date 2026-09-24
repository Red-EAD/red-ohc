package com.red.ohc.runtime;

import java.util.Objects;

import com.red.ohc.maintenance.MaintenanceEventLoop;

/**
 * Publishes a reader epoch and keeps native payloads protected until exit. Close is a
 * quiesced-only teardown, so this guard owns reader-vs-reclaimer safety, not close racing.
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
      if (context.enterReader(protectsValues)) {
        publishReaderState(
            context, ReaderRegistry.VALUE_PROTECTION_BIT | context.readerPublishedEpoch());
      }
      return true;
    }
    for (;;) {
      long observed = worker.epoch();
      try {
        publishReaderState(
            context, protectsValues ? ReaderRegistry.VALUE_PROTECTION_BIT | observed : observed);
      } catch (IllegalArgumentException failure) {
        // A slot that unbound concurrently can only belong to teardown; reject the reader.
        context.readerPublishedEpoch(0L);
        return false;
      }
      if (observed == worker.epoch()) {
        context.enterReader(protectsValues);
        return true;
      }
      worker.clearReaderStateIfRegistered(context);
    }
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
