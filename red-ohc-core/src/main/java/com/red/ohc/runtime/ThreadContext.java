package com.red.ohc.runtime;

import java.nio.ByteBuffer;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
    public byte[] keyBytes = new byte[64];
    public byte[] valueBytes = new byte[64];
    public ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
    public ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
    public final LookupKey lookupKey = new LookupKey();
    public final ReaderSlot slot = new ReaderSlot();
    public final DirectValueView valueView = new DirectValueView();
    /** Reusable native-retirement reservation; it is active only across one writer critical section. */
    public final RetirementQueue.Reservation retirement = new RetirementQueue.Reservation();
    private final WriterArena writerArena;
    private final Budget.Stripe budgetStripe;
    private long readSequence;
    private MaintenanceEventLoop maintenance;
    /** Last actor idle generation this writer has already signalled. */
    private long maintenanceWakeGeneration = Long.MIN_VALUE;
    boolean registered;

    public ThreadContext(WriterArena writerArena, Budget.Stripe budgetStripe) {
        this.writerArena = writerArena;
        this.budgetStripe = budgetStripe;
    }

    public void ensureKey(int length) {
        if (keyBytes.length < length) {
            keyBytes = new byte[round(length)];
            keyBuffer = ByteBuffer.wrap(keyBytes);
        }
    }

    public void ensureValue(int length) {
        if (valueBytes.length < length) {
            valueBytes = new byte[round(length)];
            valueBuffer = ByteBuffer.wrap(valueBytes);
        }
    }

    public ByteBuffer keyBuffer(int length) {
        keyBuffer.clear();
        keyBuffer.limit(length);
        return keyBuffer;
    }

    public ByteBuffer valueBuffer(int length) {
        valueBuffer.clear();
        valueBuffer.limit(length);
        return valueBuffer;
    }

    public WriterArena writer() { return writerArena; }
    public Budget.Stripe budgetStripe() { return budgetStripe; }

    public boolean isRegistered() {
        return registered;
    }

    public void markRegistered() {
        registered = true;
    }

    public long hit() { slot.localHits++; return ++readSequence; }
    public long miss() { slot.localMisses++; return ++readSequence; }
    public void dropped() { slot.localAccessDropped++; }

    /** Delivers every hit to the policy stream; only an empty-to-nonempty transition wakes it. */
    public void access(Entry entry) {
        boolean wasEmpty = slot.access.isEmpty();
        if (!slot.access.offer(entry, entry.generation())) {
            dropped();
            return;
        }
        if (!wasEmpty) return;
        slot.accessPending = true;
        MaintenanceEventLoop loop = maintenance;
        if (loop != null) loop.signalAccess(slot);
    }

    public void finishRead(long sequence) {
        if ((sequence & 1023L) == 0L) {
            publish();
            // Global counters are deliberately decoupled from the policy stream. This bounded
            // stats publication may wake the actor even when an earlier access burst was already
            // drained; hit delivery itself still signals only on an empty-to-nonempty ring edge.
            slot.accessPending = true;
            MaintenanceEventLoop loop = maintenance;
            if (loop != null) loop.signalAccess(slot);
        }
    }

    public void bindMaintenance(MaintenanceEventLoop maintenance) {
        this.maintenance = maintenance;
    }

    public void publish() {
        slot.publishedHits = slot.localHits;
        slot.publishedMisses = slot.localMisses;
        slot.publishedAccessDropped = slot.localAccessDropped;
    }

    /**
     * The maintenance actor increments its idle generation only as it commits to an idle park.
     * A writer signals at most once per such generation, removing a global WakeGate access from
     * steady replacement traffic while retaining the park-before-publish handshake.
     */
    public boolean needsMaintenanceWake(long idleGeneration) {
        if (maintenanceWakeGeneration == idleGeneration) return false;
        maintenanceWakeGeneration = idleGeneration;
        return true;
    }

    private static int round(int value) {
        int size = 64;
        while (size < value) {
            if (size > (1 << 30)) throw new IllegalArgumentException("serialized value is too large");
            size <<= 1;
        }
        return size;
    }
}
