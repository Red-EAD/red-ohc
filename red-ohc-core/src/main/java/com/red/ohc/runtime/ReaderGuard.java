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
            context.markRegistered();
            worker.registerReader(context.slot);
        }
        while (!closing.getAsBoolean()) {
            long observed = worker.epoch();
            context.slot.epoch = observed;
            if (observed == worker.epoch()) return true;
            context.slot.epoch = 0L;
            LockSupport.parkNanos(this, 1_000L);
        }
        context.slot.epoch = 0L;
        return false;
    }

    public void exit(ThreadContext context) {
        context.slot.epoch = 0L;
    }
}
