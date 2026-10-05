package com.red.ohc.cache;

import java.util.Objects;

import com.red.ohc.api.Ticker;

/** Cache-internal clock that converts wall-clock expiries into monotonic deadlines once. */
final class MonotonicDeadlineClock implements Ticker {
  /** Sentinel stored only in Java-side write plumbing; a TTL deadline may legitimately be zero. */
  static final long NO_DEADLINE = Long.MIN_VALUE;

  private static final long NANOS_PER_MILLI = 1_000_000L;

  private final Ticker ticker;
  private final long originNanos;
  /** Approximate wall-clock origin for relative-TTL residence statistics. */
  private final long originWallMillis;
  private final boolean systemTicker;

  MonotonicDeadlineClock(Ticker ticker) {
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.originNanos = ticker.nanos();
    this.systemTicker = ticker == Ticker.DEFAULT;
    this.originWallMillis = systemTicker ? ticker.currentTimeMillis() : 0L;
  }

  /** Returns elapsed nanoseconds relative to this cache's clock origin. */
  long nowNanos() {
    return ticker.nanos() - originNanos;
  }

  /**
   * Estimated wall-clock millis for relative-TTL statistics, reusing a monotonic sample. Absolute
   * expiries must sample currentTimeMillis instead because the system wall clock can be adjusted.
   */
  long wallMillisAt(long monotonicNowNanos) {
    if (!systemTicker) {
      return ticker.currentTimeMillis();
    }
    return originWallMillis + monotonicNowNanos / NANOS_PER_MILLI;
  }

  @Override
  public long nanos() {
    return nowNanos();
  }

  @Override
  public long currentTimeMillis() {
    return ticker.currentTimeMillis();
  }

  /** Converts a positive duration into a deadline; non-positive durations mean no TTL. */
  long deadlineAfterMillis(long ttlMillis) {
    return deadlineAfterMillis(ttlMillis, nowNanos());
  }

  /** Converts a duration using a caller-owned monotonic sample. */
  long deadlineAfterMillis(long ttlMillis, long monotonicNowNanos) {
    if (ttlMillis <= 0L) {
      return NO_DEADLINE;
    }
    return saturatingAdd(monotonicNowNanos, millisToNanos(ttlMillis));
  }

  /**
   * Converts an absolute wall-clock expiry at the publication boundary. Later wall-clock changes
   * do not affect the resulting deadline.
   */
  long deadlineFromEpochMillis(long expireAtMillis) {
    if (expireAtMillis <= 0L) {
      return NO_DEADLINE;
    }
    long wallNowMillis = ticker.currentTimeMillis();
    long monotonicNowNanos = nowNanos();
    return deadlineFromEpochMillis(expireAtMillis, wallNowMillis, monotonicNowNanos);
  }

  /** Converts an absolute expiry from caller-owned wall and monotonic samples. */
  long deadlineFromEpochMillis(
      long expireAtMillis, long wallNowMillis, long monotonicNowNanos) {
    if (expireAtMillis <= 0L) {
      return NO_DEADLINE;
    }
    if (expireAtMillis <= wallNowMillis) {
      return monotonicNowNanos;
    }
    long remainingMillis = positiveDifference(expireAtMillis, wallNowMillis);
    return saturatingAdd(monotonicNowNanos, millisToNanos(remainingMillis));
  }

  static boolean expired(long deadlineNanos, long nowNanos) {
    return deadlineNanos != NO_DEADLINE && deadlineNanos <= nowNanos;
  }

  private static long millisToNanos(long millis) {
    return millis > Long.MAX_VALUE / NANOS_PER_MILLI
        ? Long.MAX_VALUE
        : millis * NANOS_PER_MILLI;
  }

  private static long positiveDifference(long high, long low) {
    return low < 0L && high > Long.MAX_VALUE + low ? Long.MAX_VALUE : high - low;
  }

  private static long saturatingAdd(long left, long right) {
    if (right > 0L && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    if (right < 0L && left < Long.MIN_VALUE - right) {
      return Long.MIN_VALUE;
    }
    return left + right;
  }
}
