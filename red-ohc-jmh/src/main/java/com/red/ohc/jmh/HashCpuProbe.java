package com.red.ohc.jmh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import com.red.ohc.codec.KeyHash;

/** Fixed-work hash probe; optional perf FIFOs gate hardware counters around the measured loop. */
public final class HashCpuProbe {
  static final int KEY_COUNT = 1_024;
  private static final int MAX_KEY_BYTES = 1 << 20;
  private static final long WARMUP_OPERATIONS = 2_000_000L;
  private static final int WARMUP_ITERATIONS = 10;
  private static final long SEED = 0x51ceL;

  private static volatile long checksumSink;

  private HashCpuProbe() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 3 && args.length != 5) {
      throw new IllegalArgumentException(
          "usage: HashCpuProbe keyBytes operations hash|control [perfControl perfAck]");
    }
    try (PerfControl perf = new PerfControl(args)) {
      int keyBytes = Integer.parseInt(args[0]);
      long operations = Long.parseLong(args[1]);
      String mode = args[2];
      boolean hash = "hash".equals(mode);
      if (keyBytes < 0 || keyBytes > MAX_KEY_BYTES || operations <= 0L
          || (!hash && !"control".equals(mode))) {
        throw new IllegalArgumentException("invalid probe arguments");
      }

      byte[][] keys = createKeys(keyBytes);
      for (int iteration = 0; iteration < WARMUP_ITERATIONS; iteration++) {
        checksumSink = run(keys, WARMUP_OPERATIONS, hash);
      }

      ThreadMXBean threadMxBean = threadCpuTimeBean();
      perf.command("enable");
      long cpuStart = threadMxBean == null ? -1L : threadMxBean.getCurrentThreadCpuTime();
      long wallStart = System.nanoTime();
      checksumSink = run(keys, operations, hash);
      long wallNanos = System.nanoTime() - wallStart;
      long cpuEnd = threadMxBean == null ? -1L : threadMxBean.getCurrentThreadCpuTime();
      long cpuNanos = cpuStart >= 0L && cpuEnd >= cpuStart ? cpuEnd - cpuStart : -1L;
      perf.command("disable");
      System.out.println(
          "{\"keyBytes\":" + keyBytes + ",\"operations\":" + operations
              + ",\"mode\":\"" + mode + "\",\"wallNanos\":" + wallNanos
              + ",\"threadCpuNanos\":" + (cpuNanos < 0L ? "null" : cpuNanos)
              + ",\"checksum\":" + checksumSink + "}");
    }
  }

  static byte[][] createKeys(int keyBytes) {
    byte[][] keys = new byte[KEY_COUNT][keyBytes];
    Random random = new Random(SEED);
    for (byte[] key : keys) {
      random.nextBytes(key);
    }
    return keys;
  }

  static long run(byte[][] keys, long operations, boolean hash) {
    long checksum = 0L;
    for (long operation = 0; operation < operations; operation++) {
      byte[] key = keys[(int) operation & (KEY_COUNT - 1)];
      checksum += hash ? KeyHash.hash(key, 0, key.length) : key.length;
    }
    return checksum;
  }

  private static ThreadMXBean threadCpuTimeBean() {
    ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    if (!threadMxBean.isCurrentThreadCpuTimeSupported()) {
      return null;
    }
    if (!threadMxBean.isThreadCpuTimeEnabled()) {
      try {
        threadMxBean.setThreadCpuTimeEnabled(true);
      } catch (SecurityException | UnsupportedOperationException unavailable) {
        return null;
      }
    }
    return threadMxBean.isThreadCpuTimeEnabled() ? threadMxBean : null;
  }

  private static final class PerfControl implements AutoCloseable {
    private final BufferedWriter control;
    private final BufferedReader acknowledgement;

    PerfControl(String[] args) throws IOException {
      if (args.length == 5) {
        control = Files.newBufferedWriter(
            Paths.get(args[3]), StandardCharsets.US_ASCII, StandardOpenOption.WRITE);
        try {
          acknowledgement = Files.newBufferedReader(
              Paths.get(args[4]), StandardCharsets.US_ASCII);
        } catch (IOException failure) {
          control.close();
          throw failure;
        }
      } else {
        control = null;
        acknowledgement = null;
      }
    }

    void command(String command) throws IOException {
      if (control == null) {
        return;
      }
      control.write(command);
      control.newLine();
      control.flush();
      if (!"ack".equals(acknowledgement.readLine())) {
        throw new IOException("perf did not acknowledge " + command);
      }
    }

    @Override
    public void close() throws IOException {
      if (control == null) {
        return;
      }
      try {
        control.close();
      } finally {
        acknowledgement.close();
      }
    }
  }
}
