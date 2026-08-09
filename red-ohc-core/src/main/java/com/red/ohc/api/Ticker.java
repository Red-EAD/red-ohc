package com.red.ohc.api;

public interface Ticker {
  Ticker DEFAULT = new DefaultTicker();

  long nanos();

  long currentTimeMillis();
}
