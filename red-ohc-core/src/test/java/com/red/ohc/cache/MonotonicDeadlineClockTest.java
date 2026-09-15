package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.api.Ticker;

public final class MonotonicDeadlineClockTest {
  @Test
  public void absoluteExpiryIsConvertedOnceAndIgnoresLaterWallClockChanges() {
    MutableTicker ticker = new MutableTicker(10L, 1_000L);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);

    long deadline = clock.deadlineFromEpochMillis(2_000L);
    ticker.wallMillis = 100_000L;
    assertFalse(MonotonicDeadlineClock.expired(deadline, clock.nowNanos()));

    ticker.nanos = 1_000_000_010L;
    assertTrue(MonotonicDeadlineClock.expired(deadline, clock.nowNanos()));
  }

  @Test
  public void durationDeadlineUsesMonotonicTime() {
    MutableTicker ticker = new MutableTicker(100L, 5_000L);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);

    long deadline = clock.deadlineAfterMillis(2L);
    assertEquals(deadline, 2_000_000L);
    ticker.nanos = 2_000_099L;
    assertFalse(MonotonicDeadlineClock.expired(deadline, clock.nowNanos()));
    ticker.nanos = 2_000_100L;
    assertTrue(MonotonicDeadlineClock.expired(deadline, clock.nowNanos()));
  }

  @Test
  public void pastExpiryBecomesAnImmediateDeadlineAndZeroMeansNoTtl() {
    MutableTicker ticker = new MutableTicker(7L, 10_000L);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);

    long immediate = clock.deadlineFromEpochMillis(9_999L);
    assertEquals(immediate, 0L);
    assertEquals(clock.deadlineAfterMillis(0L), MonotonicDeadlineClock.NO_DEADLINE);
    assertFalse(MonotonicDeadlineClock.expired(MonotonicDeadlineClock.NO_DEADLINE, Long.MAX_VALUE));
  }

  @Test
  public void durationConversionSaturates() {
    MutableTicker ticker = new MutableTicker(Long.MAX_VALUE - 1L, 1L);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);

    assertEquals(clock.deadlineAfterMillis(Long.MAX_VALUE), Long.MAX_VALUE);
  }

  @Test
  public void absoluteConversionSaturatesWhenWallClockDifferenceWouldOverflow() {
    MutableTicker ticker = new MutableTicker(0L, Long.MIN_VALUE);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);

    assertEquals(clock.deadlineFromEpochMillis(Long.MAX_VALUE), Long.MAX_VALUE);
  }

  @Test
  public void sampledConversionDoesNotReadTickerAgain() {
    MutableTicker ticker = new MutableTicker(100L, 1_000L);
    MonotonicDeadlineClock clock = new MonotonicDeadlineClock(ticker);
    int nanosCallsAfterConstruction = ticker.nanosCalls;
    int wallCallsAfterConstruction = ticker.wallCalls;

    assertEquals(clock.deadlineAfterMillis(2L, 0L), 2_000_000L);
    assertEquals(clock.deadlineFromEpochMillis(2_000L, 1_000L, 0L), 1_000_000_000L);
    assertEquals(ticker.nanosCalls, nanosCallsAfterConstruction);
    assertEquals(ticker.wallCalls, wallCallsAfterConstruction);
  }

  private static final class MutableTicker implements Ticker {
    private long nanos;
    private long wallMillis;
    private int nanosCalls;
    private int wallCalls;

    private MutableTicker(long nanos, long wallMillis) {
      this.nanos = nanos;
      this.wallMillis = wallMillis;
    }

    @Override
    public long nanos() {
      nanosCalls++;
      return nanos;
    }

    @Override
    public long currentTimeMillis() {
      wallCalls++;
      return wallMillis;
    }
  }
}
