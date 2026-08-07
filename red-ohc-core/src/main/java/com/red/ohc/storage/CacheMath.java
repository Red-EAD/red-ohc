package com.red.ohc.storage;

/** Native allocation alignment primitives. */
public final class CacheMath {
    private CacheMath() { }

    public static long roundUpTo8(long value) {
        return (value + 7L) & ~7L;
    }

}
