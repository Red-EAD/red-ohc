package com.red.ohc;

/** Raised when a cache write is waiting for maintenance progress and is interrupted. */
public final class CacheWriteInterruptedException extends RuntimeException {
    public CacheWriteInterruptedException(InterruptedException cause) {
        super("cache write interrupted while waiting for maintenance progress", cause);
    }
}
