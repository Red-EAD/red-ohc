package com.red.ohc.runtime;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.concurrent.locks.LockSupport;

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
        if (!context.isRegistered()) {
            context.bindMaintenance(worker);
            context.markRegistered();
            worker.registerReader(context.slot);
        }
        if (context.readerDepth() != 0) {
            context.enterReader();
            return true;
        }
        while (!closing.getAsBoolean()) {
            long observed = worker.epoch();
            context.slot.epoch = observed;
            // This closes the admission race with shutdown: a close that starts after the loop
            // condition but before epoch publication must observe us as quiescent, not let this
            // reader pass through to a native pointer that its actor has already freed.
            if (closing.getAsBoolean()) {
                context.slot.epoch = 0L;
                worker.readerQuiescent();
                return false;
            }
            if (observed == worker.epoch()) {
                context.enterReader();
                return true;
            }
            context.slot.epoch = 0L;
            LockSupport.parkNanos(this, 1_000L);
        }
        context.slot.epoch = 0L;
        return false;
    }

    public void exit(ThreadContext context) {
        if (!context.exitReader()) return;
        context.slot.epoch = 0L;
        worker.readerQuiescent();
    }
}
