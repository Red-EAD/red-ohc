package com.red.ohc;

public interface Ticker
{
    Ticker DEFAULT = new DefaultTicker();

    long nanos();

    long currentTimeMillis();
}

final class DefaultTicker implements Ticker
{
    static final DefaultTicker INSTANCE = new DefaultTicker();

    public long nanos()
    {
        return System.nanoTime();
    }

    public long currentTimeMillis()
    {
        return System.currentTimeMillis();
    }
}
