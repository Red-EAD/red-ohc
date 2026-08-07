package com.red.ohc.codec;

import java.nio.ByteOrder;

import com.red.ohc.storage.NativeMemory;

/** Allocation-free seed-zero xxHash64 folded to the int hash used by CHM. */
public final class Hashing {
    private static final long PRIME1 = 0x9e3779b185ebca87L;
    private static final long PRIME2 = 0xc2b2ae3d27d4eb4fL;
    private static final long PRIME3 = 0x165667b19e3779f9L;
    private static final long PRIME4 = 0x85ebca77c2b2ae63L;
    private static final long PRIME5 = 0x27d4eb2f165667c5L;
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private Hashing() { }

    public static int xxHash64Folded(byte[] bytes, int offset, int length) {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException("offset=" + offset + ", length=" + length);
        }
        int index = offset;
        int end = offset + length;
        long hash;
        if (length >= 32) {
            int limit = end - 32;
            long v1 = PRIME1 + PRIME2;
            long v2 = PRIME2;
            long v3 = 0L;
            long v4 = -PRIME1;
            do {
                v1 = round(v1, readLong(bytes, index));
                v2 = round(v2, readLong(bytes, index + 8));
                v3 = round(v3, readLong(bytes, index + 16));
                v4 = round(v4, readLong(bytes, index + 24));
                index += 32;
            } while (index <= limit);
            hash = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            hash = mergeRound(hash, v1);
            hash = mergeRound(hash, v2);
            hash = mergeRound(hash, v3);
            hash = mergeRound(hash, v4);
        } else {
            hash = PRIME5;
        }
        hash += length;
        while (index <= end - 8) {
            long lane = round(0L, readLong(bytes, index));
            hash ^= lane;
            hash = Long.rotateLeft(hash, 27) * PRIME1 + PRIME4;
            index += 8;
        }
        if (index <= end - 4) {
            hash ^= (readInt(bytes, index) & 0xffffffffL) * PRIME1;
            hash = Long.rotateLeft(hash, 23) * PRIME2 + PRIME3;
            index += 4;
        }
        while (index < end) {
            hash ^= (bytes[index] & 0xffL) * PRIME5;
            hash = Long.rotateLeft(hash, 11) * PRIME1;
            index++;
        }
        hash ^= hash >>> 33;
        hash *= PRIME2;
        hash ^= hash >>> 29;
        hash *= PRIME3;
        hash ^= hash >>> 32;
        return (int) (hash ^ (hash >>> 32));
    }

    private static long round(long accumulator, long input) {
        accumulator += input * PRIME2;
        accumulator = Long.rotateLeft(accumulator, 31);
        return accumulator * PRIME1;
    }

    private static long mergeRound(long accumulator, long value) {
        accumulator ^= round(0L, value);
        return accumulator * PRIME1 + PRIME4;
    }

    private static long readLong(byte[] bytes, int offset) {
        long value = NativeMemory.getLong(bytes, offset);
        return LITTLE_ENDIAN ? value : Long.reverseBytes(value);
    }

    private static int readInt(byte[] bytes, int offset) {
        int value = NativeMemory.getInt(bytes, offset);
        return LITTLE_ENDIAN ? value : Integer.reverseBytes(value);
    }
}
