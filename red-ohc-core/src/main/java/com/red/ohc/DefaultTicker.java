package com.red.ohc;

final class DefaultTicker implements Ticker {
    @Override
    public long nanos() {
        return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
