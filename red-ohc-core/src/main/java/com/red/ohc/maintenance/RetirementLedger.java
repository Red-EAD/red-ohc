package com.red.ohc.maintenance;

import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.Budget;

/**
 * Thread-confined native retirement records. The producer appends records and the maintenance
 * loop drains them; no Java object is allocated for an individual retired block.
 */
public final class RetirementLedger {
    private static final int RECORD_BYTES = 24;
    private static final int RECORDS_PER_CHUNK = 256;
    private static final long CHUNK_BYTES = (long) RECORD_BYTES * RECORDS_PER_CHUNK;

    private final NativeMemory.Memory memory;
    private final Budget budget;
    private Chunk head;
    private Chunk tail;
    private final java.util.concurrent.atomic.AtomicLong pendingBytes = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger pendingRecords = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong allocatedBytes = new java.util.concurrent.atomic.AtomicLong();

    public RetirementLedger(NativeMemory.Memory memory, Budget budget) {
        this.memory = memory;
        this.budget = budget;
    }

    public void append(long address, long allocation) {
        if (address == 0L || allocation <= 0L) return;
        prepare(1);
        appendPrepared(address, allocation);
    }

    void prepare(int records) {
        if (records <= 0) return;
        if (records > RECORDS_PER_CHUNK) {
            throw new IllegalArgumentException("retirement batch is too large: " + records);
        }
        Chunk current = tail;
        if (current == null || current.count > RECORDS_PER_CHUNK - records) {
            if (!budget.reserveLedger(CHUNK_BYTES)) {
                throw new IllegalStateException("retirement ledger budget exhausted");
            }
            try {
                current = new Chunk(memory.allocate(CHUNK_BYTES));
            } catch (Throwable failure) {
                budget.releaseLedger(CHUNK_BYTES);
                throw failure;
            }
            allocatedBytes.addAndGet(CHUNK_BYTES);
            if (tail == null) head = current;
            else tail.next = current;
            tail = current;
        }
    }

    private void appendPrepared(long address, long allocation) {
        Chunk current = tail;
        long record = current.address + (long) current.count * RECORD_BYTES;
        NativeMemory.putLong(record, address);
        NativeMemory.putLong(record + 8L, allocation);
        NativeMemory.putLong(record + 16L, 1L);
        current.count++;
        pendingBytes.addAndGet(allocation);
        pendingRecords.incrementAndGet();
    }

    int drainTo(EpochReclaimer reclaimer, long epoch, int limit) {
        int drained = 0;
        while (drained < limit && head != null) {
            Chunk current = head;
            if (current.read == current.count) {
                if (current != tail) {
                    head = current.next;
                    memory.free(current.address, CHUNK_BYTES);
                    budget.releaseLedger(CHUNK_BYTES);
                    allocatedBytes.addAndGet(-CHUNK_BYTES);
                    continue;
                }
                break;
            }
            long record = current.address + (long) current.read * RECORD_BYTES;
            long address = NativeMemory.getLong(record);
            long allocation = NativeMemory.getLong(record + 8L);
            reclaimer.retireEntry(address, allocation, epoch);
            pendingBytes.addAndGet(-allocation);
            pendingRecords.decrementAndGet();
            current.read++;
            drained++;
        }
        return drained;
    }

    boolean hasPending() {
        return pendingRecords.get() != 0;
    }

    long pendingBytes() {
        return pendingBytes.get();
    }

    long allocatedBytes() {
        return allocatedBytes.get();
    }

    void freeAll() {
        Chunk current = head;
        while (current != null) {
            Chunk next = current.next;
            memory.free(current.address, CHUNK_BYTES);
            budget.releaseLedger(CHUNK_BYTES);
            current = next;
        }
        head = null;
        tail = null;
        pendingBytes.set(0L);
        pendingRecords.set(0);
        allocatedBytes.set(0L);
    }

    private static final class Chunk {
        final long address;
        volatile int count;
        int read;
        volatile Chunk next;

        Chunk(long address) {
            this.address = address;
        }
    }
}
