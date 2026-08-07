package com.red.ohc;

import java.util.Arrays;
import java.util.Objects;

import com.red.ohc.codec.Hashing;

/** Immutable, pre-serialized key with the folded CHM hash cached at construction. */
public final class EncodedKey {
    final byte[] bytes;
    final int hash;

    private EncodedKey(byte[] bytes, int hash) {
        this.bytes = bytes;
        this.hash = hash;
    }

    public static EncodedKey copyOf(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        byte[] copy = Arrays.copyOf(bytes, bytes.length);
        return new EncodedKey(copy, Hashing.xxHash64Folded(copy, 0, copy.length));
    }

    public int length() {
        return bytes.length;
    }

    /** Returns a defensive copy; cache hot paths use the immutable backing array internally. */
    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public int hash() {
        return hash;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof EncodedKey)) return false;
        EncodedKey key = (EncodedKey) other;
        return hash == key.hash && Arrays.equals(bytes, key.bytes);
    }
}
