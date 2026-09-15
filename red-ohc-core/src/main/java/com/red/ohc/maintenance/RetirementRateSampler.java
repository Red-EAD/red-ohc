package com.red.ohc.maintenance;

/** Actor-local wall-time sampler for retirement production and completion rates. */
final class RetirementRateSampler {
  private static final long MIN_SAMPLE_NANOS = 100_000_000L;

  private long lastSampleNanos;
  private boolean initialized;
  private long lastGeneratedBytes;
  private long lastCompletedBytes;
  private volatile double generatedBytesPerSecond;
  private volatile double completedBytesPerSecond;

  boolean shouldSample(long nowNanos) {
    return !initialized || nowNanos - lastSampleNanos >= MIN_SAMPLE_NANOS;
  }

  void observe(long nowNanos, long generatedBytes, long completedBytes) {
    if (!initialized) {
      initialized = true;
      lastSampleNanos = nowNanos;
      lastGeneratedBytes = generatedBytes;
      lastCompletedBytes = completedBytes;
      return;
    }
    if (!shouldSample(nowNanos)) {
      return;
    }
    long elapsedNanos = nowNanos - lastSampleNanos;
    long generatedDelta = Math.max(0L, generatedBytes - lastGeneratedBytes);
    long completedDelta = Math.max(0L, completedBytes - lastCompletedBytes);
    double seconds = elapsedNanos / 1_000_000_000.0d;
    generatedBytesPerSecond =
        ewma(generatedBytesPerSecond, generatedDelta / seconds);
    completedBytesPerSecond =
        ewma(completedBytesPerSecond, completedDelta / seconds);
    lastSampleNanos = nowNanos;
    lastGeneratedBytes = generatedBytes;
    lastCompletedBytes = completedBytes;
  }

  double generatedBytesPerSecond() {
    return generatedBytesPerSecond;
  }

  double completedBytesPerSecond() {
    return completedBytesPerSecond;
  }

  private static double ewma(double previous, double sample) {
    return previous == 0.0d ? sample : previous * 0.75d + sample * 0.25d;
  }
}
