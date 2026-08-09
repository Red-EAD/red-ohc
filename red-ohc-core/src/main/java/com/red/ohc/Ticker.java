package com.red.ohc;

public interface Ticker {
    Ticker DEFAULT = new DefaultTicker();

    long nanos();

    long currentTimeMillis();
}
