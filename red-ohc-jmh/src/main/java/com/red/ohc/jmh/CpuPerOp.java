package com.red.ohc.jmh;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.ValueView;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.cache.OffHeapCache;

public final class CpuPerOp {
  static final int KEY_BYTES = 16;
  static final int VALUE_BYTES = 256;
  static final ThreadMXBean MX = ManagementFactory.getThreadMXBean();

  static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        public void serialize(byte[] v, ByteBuffer b) {
          b.put(v);
        }

        public byte[] deserialize(ByteBuffer b) {
          byte[] o = new byte[b.remaining()];
          b.get(o);
          return o;
        }

        public int serializedSize(byte[] v) {
          return v.length;
        }
      };

  static byte[] bytes(int len, int seed) {
    byte[] b = new byte[len];
    for (int i = 0; i < len; i++) {
      b[i] = (byte) (seed * 31 + i);
    }
    return b;
  }

  static String group(String name) {
    if (name.startsWith("bench-worker")) {
      return "workers";
    }
    if (name.contains("red-ohc-maintenance-event-loop")) {
      return "actor";
    }
    if (name.contains("commonPool")) {
      return "commonPool";
    }
    if (name.contains("Compiler") || name.contains("GC") || name.contains("G1")) {
      return "jvm";
    }
    return "other";
  }

  static Map<Long, long[]> snapshot() {
    Map<Long, long[]> m = new HashMap<>();
    for (long id : MX.getAllThreadIds()) {
      long cpu = MX.getThreadCpuTime(id);
      if (cpu >= 0) {
        m.put(id, new long[] {cpu});
      }
    }
    return m;
  }

  static Map<String, Long> delta(Map<Long, long[]> before) {
    Map<String, Long> out = new HashMap<>();
    for (long id : MX.getAllThreadIds()) {
      long cpu = MX.getThreadCpuTime(id);
      if (cpu < 0) {
        continue;
      }
      ThreadInfo info = MX.getThreadInfo(id);
      if (info == null) {
        continue;
      }
      long[] prev = before.get(id);
      long d = cpu - (prev == null ? 0L : prev[0]);
      if (d <= 0) {
        continue;
      }
      out.merge(group(info.getThreadName()), d, Long::sum);
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    String mode = args[0];
    int threads = Integer.parseInt(args[1]);
    long opsPerRep = Long.parseLong(args[2]);
    int reps = args.length > 3 ? Integer.parseInt(args[3]) : 5;
    MX.setThreadCpuTimeEnabled(true);

    int workingSet = "read".equals(mode) ? 24_576 : 1 << 15;
    byte[][] keys = new byte[workingSet][];
    byte[][] values = new byte[workingSet][];
    for (int i = 0; i < workingSet; i++) {
      keys[i] = bytes(KEY_BYTES, i);
      values[i] = bytes(VALUE_BYTES, i * 31 + 7);
    }
    long capacity =
        "read".equals(mode)
            ? (long) workingSet * (KEY_BYTES + VALUE_BYTES) * 4L
            : (long) workingSet * (KEY_BYTES + VALUE_BYTES) * 2L;

    OffHeapCache<byte[], byte[]> cache =
        (OffHeapCache<byte[], byte[]>)
            OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(capacity)
                .keySerializer(BYTES)
                .valueSerializer(BYTES)
                .eviction(Eviction.S3_FIFO)
                .build();
    for (int i = 0; i < workingSet; i++) {
      cache.put(keys[i], values[i]);
      if ((i & 1023) == 1023) {
        cache.flushAsync().join();
      }
    }
    cache.flushAsync().join();

    int[] seq = zipf(workingSet);

    List<double[]> results = new ArrayList<>();
    for (int rep = 0; rep < reps + 2; rep++) {
      boolean warmup = rep < 2;
      Map<Long, long[]> before = snapshot();
      WORKER_CPU.set(0);
      long t0 = System.nanoTime();
      runOps(cache, mode, threads, opsPerRep, keys, values, seq);
      cache.flushAsync().join();
      long wall = System.nanoTime() - t0;
      Map<String, Long> d = delta(before);
      d.put("workers", WORKER_CPU.get());
      if (warmup) {
        continue;
      }
      long total = d.values().stream().mapToLong(Long::longValue).sum();
      long nonJvm = total - d.getOrDefault("jvm", 0L);
      results.add(
          new double[] {
            (double) d.getOrDefault("workers", 0L) / opsPerRep,
            (double) d.getOrDefault("actor", 0L) / opsPerRep,
            (double) d.getOrDefault("commonPool", 0L) / opsPerRep,
            (double) d.getOrDefault("other", 0L) / opsPerRep,
            (double) nonJvm / opsPerRep,
            opsPerRep * 1e9 / wall
          });
    }
    String[] names = {"workers", "actor", "commonPool", "other", "TOTAL(non-jvm)", "ops/s"};
    System.out.printf("%n=== %s, %d thread(s), %,d ops x %d reps ===%n", mode, threads, opsPerRep, reps);
    System.out.println("CPU nanoseconds per operation (exact, ThreadMXBean):");
    for (int c = 0; c < names.length; c++) {
      final int col = c;
      double[] v = results.stream().mapToDouble(r -> r[col]).sorted().toArray();
      double med = v[v.length / 2];
      System.out.printf(
          "  %-16s median %10.1f   min %10.1f   max %10.1f   spread %5.1f%%%n",
          names[c], med, v[0], v[v.length - 1], med == 0 ? 0 : 100 * (v[v.length - 1] - v[0]) / med);
    }
    cache.close();
  }

  static final AtomicLong WORKER_CPU = new AtomicLong();

  static void runOps(
      OffHeapCache<byte[], byte[]> cache, String mode, int threads, long ops,
      byte[][] keys, byte[][] values, int[] seq) throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    Thread[] ts = new Thread[threads];
    long per = ops / threads;
    for (int t = 0; t < threads; t++) {
      final int id = t;
      ts[t] =
          new Thread(
              () -> {
                Consumer sink = new Consumer();
                try {
                  start.await();
                } catch (InterruptedException e) {
                  return;
                }
                long cpu0 = MX.getCurrentThreadCpuTime();
                int cursor = id * 7919;
                if ("read".equals(mode)) {
                  for (long i = 0; i < per; i++) {
                    cache.getDirect(keys[seq[(cursor++) & (seq.length - 1)]], sink);
                  }
                } else {
                  for (long i = 0; i < per; i++) {
                    int k = (cursor++) & (keys.length - 1);
                    cache.put(keys[k], values[k]);
                  }
                }
                WORKER_CPU.addAndGet(MX.getCurrentThreadCpuTime() - cpu0);
              },
              "bench-worker-" + t);
      ts[t].start();
    }
    start.countDown();
    for (Thread t : ts) {
      t.join();
    }
  }

  static final class Consumer implements DirectValueConsumer {
    long sink;

    public void accept(ValueView value) {
      sink += value.getLong(0);
    }
  }

  static int[] zipf(int n) {
    int len = 1 << 16;
    int[] out = new int[len];
    Random r = new Random(42);
    double[] cdf = new double[n];
    double sum = 0;
    for (int i = 0; i < n; i++) {
      sum += 1.0 / Math.pow(i + 1, 0.99);
      cdf[i] = sum;
    }
    for (int i = 0; i < n; i++) {
      cdf[i] /= sum;
    }
    for (int i = 0; i < len; i++) {
      int idx = Arrays.binarySearch(cdf, r.nextDouble());
      if (idx < 0) {
        idx = -idx - 1;
      }
      out[i] = Math.min(idx, n - 1);
    }
    return out;
  }
}
