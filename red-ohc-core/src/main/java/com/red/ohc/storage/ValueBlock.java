package com.red.ohc.storage;

/** Native value block. Metadata is kept beside the serialized value, never in Entry. */
public final class ValueBlock {
    static final int EXPIRE_AT = 0;
    static final int LENGTH = 8;
    static final int HEADER = 16;

    private ValueBlock() { }

    public static long allocationLength(int valueLength) {
        return HEADER + CacheMath.roundUpTo8(valueLength);
    }

    public static void initialize(long address, long expireAtMillis, int valueLength) {
        NativeMemory.putLong(address + EXPIRE_AT, expireAtMillis);
        NativeMemory.putInt(address + LENGTH, valueLength);
        NativeMemory.putInt(address + 12L, 0);
    }

    public static long expireAtMillis(long address) {
        return NativeMemory.getLongVolatile(address + EXPIRE_AT);
    }

    public static void expireAtMillis(long address, long value) {
        NativeMemory.putLong(address + EXPIRE_AT, value);
    }

    public static int length(long address) {
        return NativeMemory.getInt(address + LENGTH);
    }

    public static long payloadAddress(long address) {
        return address + HEADER;
    }

    public static boolean expired(long address, long nowMillis) {
        long expire = expireAtMillis(address);
        return expire > 0L && expire <= nowMillis;
    }
}
