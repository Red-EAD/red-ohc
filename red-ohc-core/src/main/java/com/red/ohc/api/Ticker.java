package com.red.ohc.api;

public interface Ticker {
  Ticker DEFAULT =
      new Ticker() {
        @Override
        public long nanos() {
          return System.nanoTime();
        }

        @Override
        public long currentTimeMillis() {
          return System.currentTimeMillis();
        }
      };

  long nanos();

  long currentTimeMillis();
}
