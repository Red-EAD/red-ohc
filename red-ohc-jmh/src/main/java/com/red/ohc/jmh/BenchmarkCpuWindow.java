package com.red.ohc.jmh;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.sun.management.OperatingSystemMXBean;

/** Captures process and maintenance-actor CPU deltas over one JMH iteration. */
final class BenchmarkCpuWindow {
  static final String ACTOR_THREAD_NAME = "red-ohc-maintenance-event-loop";

  private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();
  private static final OperatingSystemMXBean PROCESS = processBean();
  private static final boolean THREAD_CPU_TIME_AVAILABLE = enableThreadCpuTime();

  private BenchmarkCpuWindow() {}

  static Snapshot capture() {
    long processCpuNanos = PROCESS == null ? -1L : PROCESS.getProcessCpuTime();
    return new Snapshot(System.nanoTime(), processCpuNanos, actorCpuNanos());
  }

  static Delta delta(Snapshot before, Snapshot after) {
    long wallNanos = after.wallNanos - before.wallNanos;
    if (wallNanos < 0L) {
      wallNanos = -1L;
    }

    boolean processCpuKnown =
        before.processCpuNanos >= 0L
            && after.processCpuNanos >= before.processCpuNanos;
    long processCpuNanos =
        processCpuKnown ? after.processCpuNanos - before.processCpuNanos : -1L;

    boolean actorCpuKnown =
        !before.actorCpuNanosById.isEmpty()
            && before.actorCpuNanosById.size() == after.actorCpuNanosById.size();
    long actorCpuNanos = 0L;
    if (actorCpuKnown) {
      for (Map.Entry<Long, Long> entry : before.actorCpuNanosById.entrySet()) {
        Long afterNanos = after.actorCpuNanosById.get(entry.getKey());
        if (afterNanos == null || afterNanos < entry.getValue()) {
          actorCpuKnown = false;
          break;
        }
        actorCpuNanos += afterNanos - entry.getValue();
      }
    }
    if (!actorCpuKnown) {
      actorCpuNanos = -1L;
    }
    return new Delta(
        wallNanos, processCpuNanos, actorCpuNanos, processCpuKnown, actorCpuKnown);
  }

  private static OperatingSystemMXBean processBean() {
    java.lang.management.OperatingSystemMXBean bean =
        ManagementFactory.getOperatingSystemMXBean();
    return bean instanceof OperatingSystemMXBean ? (OperatingSystemMXBean) bean : null;
  }

  private static boolean enableThreadCpuTime() {
    if (!THREADS.isThreadCpuTimeSupported()) {
      return false;
    }
    if (!THREADS.isThreadCpuTimeEnabled()) {
      try {
        THREADS.setThreadCpuTimeEnabled(true);
      } catch (SecurityException | UnsupportedOperationException ignored) {
        return false;
      }
    }
    return THREADS.isThreadCpuTimeEnabled();
  }

  private static Map<Long, Long> actorCpuNanos() {
    if (!THREAD_CPU_TIME_AVAILABLE) {
      return Collections.emptyMap();
    }
    Map<Long, Long> values = new HashMap<>();
    for (long threadId : THREADS.getAllThreadIds()) {
      ThreadInfo info = THREADS.getThreadInfo(threadId);
      if (info == null || !ACTOR_THREAD_NAME.equals(info.getThreadName())) {
        continue;
      }
      long cpuNanos = THREADS.getThreadCpuTime(threadId);
      if (cpuNanos >= 0L) {
        values.put(threadId, cpuNanos);
      }
    }
    return values.isEmpty() ? Collections.emptyMap() : values;
  }

  static final class Snapshot {
    final long wallNanos;
    final long processCpuNanos;
    final Map<Long, Long> actorCpuNanosById;

    Snapshot(long wallNanos, long processCpuNanos, Map<Long, Long> actorCpuNanosById) {
      this.wallNanos = wallNanos;
      this.processCpuNanos = processCpuNanos;
      this.actorCpuNanosById =
          actorCpuNanosById.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new HashMap<>(actorCpuNanosById));
    }

    static Snapshot forTest(
        long wallNanos, long processCpuNanos, Map<Long, Long> actorCpuNanosById) {
      return new Snapshot(wallNanos, processCpuNanos, actorCpuNanosById);
    }
  }

  static final class Delta {
    final long wallNanos;
    final long processCpuNanos;
    final long actorCpuNanos;
    final boolean processCpuKnown;
    final boolean actorCpuKnown;

    Delta(
        long wallNanos,
        long processCpuNanos,
        long actorCpuNanos,
        boolean processCpuKnown,
        boolean actorCpuKnown) {
      this.wallNanos = wallNanos;
      this.processCpuNanos = processCpuNanos;
      this.actorCpuNanos = actorCpuNanos;
      this.processCpuKnown = processCpuKnown;
      this.actorCpuKnown = actorCpuKnown;
    }
  }
}
