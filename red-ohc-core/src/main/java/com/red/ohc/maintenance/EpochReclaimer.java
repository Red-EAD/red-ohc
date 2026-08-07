package com.red.ohc.maintenance;

import java.util.List;

import com.red.ohc.index.Entry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;

/** Maintenance-owned QSBR FIFO with native fixed-size chunks. */
public final class EpochReclaimer {
    private static final int RECORD_BYTES = 32;
    private static final int RECORDS_PER_CHUNK = 256;
    private static final long CHUNK_BYTES = (long) RECORD_BYTES * RECORDS_PER_CHUNK;
    private static final long ENTRY = 1L;
    private static final long RAW = 2L;

    private final NativeMemory.Memory memory;
    private Chunk head;
    private Chunk tail;
    private long bytes;
    private int entries;

    public EpochReclaimer(NativeMemory.Memory memory) {
        this.memory = memory;
    }

    public void retireEntry(long address, long allocation, long epoch) {
        append(address, allocation, epoch, ENTRY);
    }

    public void retireRaw(long address, long allocation, long epoch) {
        append(address, allocation, epoch, RAW);
    }

    public int reclaim(NativeMemory.Memory ignored, List<ReaderSlot> readers, int limit) {
        int reclaimed = 0;
        while (reclaimed < limit && head != null) {
            if (head.read == head.count) discardHead();
            if (head == null) break;
            long record = head.address + (long) head.read * RECORD_BYTES;
            long epoch = NativeMemory.getLong(record + 16L);
            if (!allQuiescentAfter(readers, epoch)) break;
            long address = NativeMemory.getLong(record);
            long allocation = NativeMemory.getLong(record + 8L);
            if (NativeMemory.getLong(record + 24L) == ENTRY) memory.releaseEntry(address, allocation);
            else memory.free(address, allocation);
            bytes -= allocation;
            entries--;
            head.read++;
            reclaimed++;
        }
        if (head != null && head.read == head.count) discardHead();
        return reclaimed;
    }

    public void freeAll(NativeMemory.Memory ignored) {
        while (head != null) {
            while (head.read < head.count) {
                long record = head.address + (long) head.read * RECORD_BYTES;
                long address = NativeMemory.getLong(record);
                long allocation = NativeMemory.getLong(record + 8L);
                if (NativeMemory.getLong(record + 24L) == ENTRY) memory.releaseEntry(address, allocation);
                else memory.free(address, allocation);
                head.read++;
            }
            discardHead();
        }
        bytes = 0L;
        entries = 0;
    }

    public long bytes() { return bytes; }
    public int entries() { return entries; }
    public long oldestEpoch() {
        return head == null || head.read == head.count ? 0L : NativeMemory.getLong(head.address + (long) head.read * RECORD_BYTES + 16L);
    }

    private void append(long address, long allocation, long epoch, long type) {
        if (tail == null || tail.count == RECORDS_PER_CHUNK) {
            Chunk chunk = new Chunk(memory.allocate(CHUNK_BYTES));
            if (tail == null) head = chunk;
            else tail.next = chunk;
            tail = chunk;
        }
        long record = tail.address + (long) tail.count * RECORD_BYTES;
        NativeMemory.putLong(record, address);
        NativeMemory.putLong(record + 8L, allocation);
        NativeMemory.putLong(record + 16L, epoch);
        NativeMemory.putLong(record + 24L, type);
        tail.count++;
        bytes += allocation;
        entries++;
    }

    private void discardHead() {
        Chunk chunk = head;
        if (chunk == null || chunk.read != chunk.count) return;
        head = chunk.next;
        if (head == null) tail = null;
        memory.free(chunk.address, CHUNK_BYTES);
    }

    private static boolean allQuiescentAfter(List<ReaderSlot> readers, long retireEpoch) {
        for (ReaderSlot reader : readers) {
            long activeEpoch = reader.epoch;
            if (activeEpoch != 0L && activeEpoch <= retireEpoch) return false;
        }
        return true;
    }

    private static final class Chunk {
        final long address;
        int count;
        int read;
        Chunk next;
        Chunk(long address) { this.address = address; }
    }
}
