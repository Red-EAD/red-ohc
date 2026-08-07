package com.red.ohc.codec;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Reusable serialized lookup key. The xxHash64 value is calculated once per operation. */
public final class LookupKey {
    private byte[] bytes;
    private int length;
    private long hash;
    private boolean hashed;
    private int hashComputations;

    public void set(byte[] bytes, int length) {
        this.bytes = bytes;
        this.length = length;
        this.hashed = false;
    }

    public void setPrecomputed(byte[] bytes, int length, long hash) {
        set(bytes, length);
        this.hash = hash;
        this.hashed = true;
    }

    public byte[] bytes() { return bytes; }
    public int length() { return length; }
    public long hash() { ensureHashed(); return hash; }
    public int hashComputations() { return hashComputations; }

    private void ensureHashed() {
        if (!hashed) {
            hash = Hashing.xxHash64(bytes, 0, length);
            hashed = true;
            hashComputations++;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Entry)) return false;
        Entry entry = (Entry) other;
        return entry.hash == hash()
                && entry.keyLength() == length
                && NativeMemory.equals(entry.nativeKeyAddress(), bytes, 0, length);
    }
}
