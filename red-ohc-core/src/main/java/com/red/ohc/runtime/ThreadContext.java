package com.red.ohc.runtime;

import java.nio.ByteBuffer;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.ReliableRemovalQueue;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
    public byte[] keyBytes = new byte[64];
    public byte[] valueBytes = new byte[64];
    public ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
    public ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
    public final LookupKey lookupKey = new LookupKey();
    public final ReaderSlot slot = new ReaderSlot();
    private DirectValueView[] directViews = new DirectValueView[4];
    private int directViewDepth;
    private int readerDepth;
    private byte[] bulkPayload = new byte[0];
    private int[] bulkOffsets = new int[64];
    private int[] bulkLengths = new int[64];
    private Object[] bulkKeys = new Object[64];
    /** Reusable native-retirement reservation; it is active only across one writer critical section. */
    public final RetirementQueue.Reservation retirement = new RetirementQueue.Reservation();
    public final ReliableRemovalQueue.Reservation reliableRemoval = new ReliableRemovalQueue.Reservation();
    private final WriterArena writerArena;
    private final Budget.Lease budgetLease;
    private long readSequence;
    private long accessSequence;
    private MaintenanceEventLoop maintenance;
    private int bulkReadDepth;
    private boolean bulkReadChanged;
    private boolean retirementPublished;
    /** Last actor idle generation this writer has already signalled. */
    private long maintenanceWakeGeneration = Long.MIN_VALUE;
    boolean registered;

    public ThreadContext(WriterArena writerArena, Budget.Lease budgetLease) {
        this.writerArena = writerArena;
        this.budgetLease = budgetLease;
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
    public Budget.Lease budgetLease() { return budgetLease; }

    public void activateWriter() { budgetLease.activate(); }

    public void deactivateWriter() { budgetLease.deactivate(); }

    public boolean isRegistered() {
        return registered;
    }

    public int readerDepth() { return readerDepth; }
    public void enterReader() { readerDepth++; }
    public boolean exitReader() {
        if (readerDepth <= 0) throw new IllegalStateException("reader guard is not entered");
        return --readerDepth == 0;
    }

    public DirectValueView pushDirectView(long address, int length) {
        if (directViewDepth == directViews.length) {
            DirectValueView[] expanded = new DirectValueView[directViews.length << 1];
            System.arraycopy(directViews, 0, expanded, 0, directViews.length);
            directViews = expanded;
        }
        DirectValueView view = directViews[directViewDepth];
        if (view == null) {
            view = new DirectValueView();
            directViews[directViewDepth] = view;
        }
        directViewDepth++;
        view.reset(address, length);
        return view;
    }

    public void popDirectView() {
        if (directViewDepth <= 0) throw new IllegalStateException("direct view is not entered");
        directViews[--directViewDepth].reset(0L, 0);
    }

    public byte[] ensureBulkPayload(int required) {
        if (required < 0) throw new IllegalArgumentException("bulk payload is too large");
        if (bulkPayload.length < required) {
            int previous = bulkPayload.length;
            int next = bulkPayload.length == 0 ? 256 : bulkPayload.length;
            while (next < required) {
                int grown = next << 1;
                if (grown <= next) { next = required; break; }
                next = grown;
            }
            byte[] expanded = new byte[next];
            if (previous != 0) System.arraycopy(bulkPayload, 0, expanded, 0, previous);
            bulkPayload = expanded;
        }
        return bulkPayload;
    }

    public void ensureBulkSlots(int required) {
        if (required <= bulkKeys.length) return;
        int next = bulkKeys.length;
        while (next < required) next <<= 1;
        int[] offsets = new int[next];
        int[] lengths = new int[next];
        Object[] keys = new Object[next];
        System.arraycopy(bulkOffsets, 0, offsets, 0, bulkOffsets.length);
        System.arraycopy(bulkLengths, 0, lengths, 0, bulkLengths.length);
        System.arraycopy(bulkKeys, 0, keys, 0, bulkKeys.length);
        bulkOffsets = offsets;
        bulkLengths = lengths;
        bulkKeys = keys;
    }

    public byte[] bulkPayload() { return bulkPayload; }
    public int[] bulkOffsets() { return bulkOffsets; }
    public int[] bulkLengths() { return bulkLengths; }
    public Object[] bulkKeys() { return bulkKeys; }

    public void clearBulk(int count) {
        for (int i = 0; i < count; i++) bulkKeys[i] = null;
    }

    public void markRegistered() {
        registered = true;
    }

    public long hit() { slot.localHits++; return ++readSequence; }
    public long miss() { slot.localMisses++; return ++readSequence; }
    public void dropped() { slot.localAccessDropped++; }

    public void beginBulkRead() {
        if (bulkReadDepth == 0) bulkReadChanged = false;
        bulkReadDepth++;
    }

    public void bulkHit(Entry entry) {
        slot.localHits++;
        readSequence++;
        if ((++accessSequence & 15L) == 0L
                && !slot.access.offer(entry, entry.generation())) slot.localAccessDropped++;
        bulkReadChanged = true;
    }

    public void bulkMiss() {
        slot.localMisses++;
        readSequence++;
        bulkReadChanged = true;
    }

    /** Publishes one batch of read counters and one worker hint instead of one per key. */
    public void finishBulkRead() {
        if (bulkReadDepth == 0) return;
        if (--bulkReadDepth != 0) return;
        if (bulkReadChanged) {
            publish();
            MaintenanceEventLoop loop = maintenance;
            if (loop != null && slot.markAccessPending()) loop.signalAccess(slot);
        }
        bulkReadChanged = false;
    }

    /** Delivers every hit to the policy stream; only an empty-to-nonempty transition wakes it. */
    public void access(Entry entry) {
        if ((++accessSequence & 15L) != 0L) return;
        if (!slot.access.offer(entry, entry.generation())) {
            dropped();
            return;
        }
        MaintenanceEventLoop loop = maintenance;
        if (loop != null && slot.markAccessPending()) loop.signalAccess(slot);
    }

    public void finishRead(long sequence) {
        if ((sequence & 1023L) == 0L) {
            publish();
            // Global counters are deliberately decoupled from the policy stream. This bounded
            // stats publication may wake the actor even when an earlier access burst was already
            // drained; hit delivery itself still signals only on an empty-to-nonempty ring edge.
            MaintenanceEventLoop loop = maintenance;
            if (loop != null && slot.markAccessPending()) loop.signalAccess(slot);
        }
    }

    /** Flushes sub-threshold read counters when a control-plane barrier is requested. */
    public void flushRead() {
        publish();
        MaintenanceEventLoop loop = maintenance;
        if (loop != null && slot.markAccessPending()) loop.signalAccess(slot);
    }

    public void bindMaintenance(MaintenanceEventLoop maintenance) {
        this.maintenance = maintenance;
    }

    public void markRetirementPublished() {
        retirementPublished = true;
    }

    public boolean consumeRetirementPublished() {
        boolean published = retirementPublished;
        retirementPublished = false;
        return published;
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
        if (value < 0 || value > (1 << 30)) {
            throw new IllegalArgumentException("serialized value is too large: " + value);
        }
        long size = 64L;
        while (size < value) size <<= 1;
        return (int) size;
    }
}
