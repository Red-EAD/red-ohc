package com.red.ohc.api;

/** Supplies the elapsed-time and wall-clock sources used by a cache. */
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

  /** Returns a monotonic elapsed-time reading suitable for deadlines and latency measurements. */
  long nanos();

  /** Returns wall-clock milliseconds, used for absolute-expiry conversion and diagnostics. */
  long currentTimeMillis();
}
