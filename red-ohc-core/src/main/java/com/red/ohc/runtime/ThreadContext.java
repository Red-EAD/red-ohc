package com.red.ohc.runtime;

import java.nio.ByteBuffer;

import com.red.ohc.codec.LookupKey;
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
    WriterArena writerArena;
    private long readSequence;
    boolean registered;

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

    public WriterArena writer(NativeMemory.Memory memory) {
        if (writerArena == null) writerArena = memory.newWriterArena();
        return writerArena;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void markRegistered() {
        registered = true;
    }

    public long hit() { slot.localHits++; return ++readSequence; }
    public long miss() { slot.localMisses++; return ++readSequence; }
    public void dropped() { slot.localAccessDropped++; }

    public void finishRead(long sequence) {
        if ((sequence & 1023L) == 0L) publish();
    }

    public void publish() {
        slot.publishedHits = slot.localHits;
        slot.publishedMisses = slot.localMisses;
        slot.publishedAccessDropped = slot.localAccessDropped;
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
