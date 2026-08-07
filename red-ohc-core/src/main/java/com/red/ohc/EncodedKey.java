package com.red.ohc;

import java.util.Arrays;
import java.util.Objects;

import com.red.ohc.codec.Hashing;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Immutable, pre-serialized key for direct reads. */
public final class EncodedKey {
    final byte[] bytes;
    final long hash;

    private EncodedKey(byte[] bytes, long hash) {
        this.bytes = bytes;
        this.hash = hash;
    }

    public static EncodedKey copyOf(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        byte[] copy = Arrays.copyOf(bytes, bytes.length);
        return new EncodedKey(copy, Hashing.xxHash64(copy, 0, copy.length));
    }

    public static EncodedKey copyOf(byte[] bytes, long hash) {
        Objects.requireNonNull(bytes, "bytes");
        return new EncodedKey(Arrays.copyOf(bytes, bytes.length), hash);
    }

    public int length() {
        return bytes.length;
    }

    /** Returns a defensive copy; the precomputed key remains immutable after construction. */
    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public long hash() { return hash; }

    /** Allocation-free native-key match used by the cache's specialized encoded lookup. */
    public boolean matches(Entry entry) {
        return hash == entry.hash && bytes.length == entry.keyLength
                && NativeMemory.equals(entry.nativeKeyAddress, bytes, 0, bytes.length);
    }

    @Override
    public int hashCode() {
        return (int) (hash ^ (hash >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other instanceof EncodedKey) {
            EncodedKey key = (EncodedKey) other;
            return hash == key.hash && Arrays.equals(bytes, key.bytes);
        }
        return false;
    }
}
