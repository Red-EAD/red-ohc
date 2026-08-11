package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.SkipException;
import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;

public final class VirtualThreadLifecycleTest {
  private static final int READERS = 128;
  private static final int WRITERS = 128;
  private static final int TOTAL = READERS + WRITERS;

  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(StandardCharsets.UTF_8).length;
        }
      };

  @Test(timeOut = 30_000L)
  public void shortLivedVirtualThreadsDoNotAccumulateReaderOrWriterRegistrations()
      throws Exception {
    Method startVirtualThread = virtualThreadStarter();
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(64L << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      assertTrue(cache.put("seed", "value"));
      cache.flushAsync().join();
      int baselineReaders = readerCount(cache);
      int baselineStripes = budgetStripeCount(cache);
      WorkloadResult workload = runWorkload(cache, startVirtualThread);
      assertEquals(workload.readHits, READERS);
      assertEquals(workload.acceptedWrites, WRITERS);

      awaitCollectionAndCleanup(cache, workload.terminatedThreads, baselineReaders, baselineStripes);

      int remainingReaders = readerCount(cache);
      int remainingStripes = budgetStripeCount(cache);
      int liveThreads = liveReferences(workload.terminatedThreads);
      assertTrue(
          remainingReaders <= baselineReaders + 4,
          "reader registrations="
              + remainingReaders
              + ", baseline="
              + baselineReaders
              + ", live virtual threads="
              + liveThreads);
      assertEquals(remainingStripes, baselineStripes);
    }
  }

  private static Method virtualThreadStarter() {
    try {
      return Thread.class.getMethod("startVirtualThread", Runnable.class);
    } catch (NoSuchMethodException unavailable) {
      throw new SkipException("virtual threads require JDK 21");
    }
  }

  private static Thread startVirtualThread(Method starter, Runnable task) throws Exception {
    return (Thread) starter.invoke(null, task);
  }

  private static WorkloadResult runWorkload(OffHeapCache<String, String> cache, Method starter)
      throws Exception {
    AtomicInteger readHits = new AtomicInteger();
    AtomicInteger acceptedWrites = new AtomicInteger();
    Thread[] threads = new Thread[TOTAL];
    for (int index = 0; index < READERS; index++) {
      threads[index] =
          startVirtualThread(
              starter,
              () -> {
                if ("value".equals(cache.get("seed"))) {
                  readHits.incrementAndGet();
                }
              });
    }
    for (int index = 0; index < WRITERS; index++) {
      final int key = index;
      threads[READERS + index] =
          startVirtualThread(
              starter,
              () -> {
                if (cache.put("writer-" + key, "value")) {
                  acceptedWrites.incrementAndGet();
                }
              });
    }
    for (Thread thread : threads) {
      thread.join();
    }
    List<WeakReference<Thread>> terminated = new ArrayList<>(TOTAL);
    for (Thread thread : threads) {
      terminated.add(new WeakReference<>(thread));
    }
    return new WorkloadResult(terminated, readHits.get(), acceptedWrites.get());
  }

  private static void awaitCollectionAndCleanup(
      OffHeapCache<?, ?> cache,
      List<WeakReference<Thread>> terminated,
      int baselineReaders,
      int baselineStripes)
      throws Exception {
    for (int attempt = 0; attempt < 100; attempt++) {
      System.gc();
      cache.flushAsync().join();
      if (readerCount(cache) <= baselineReaders + 4
          && budgetStripeCount(cache) == baselineStripes
          && liveReferences(terminated) <= 4) {
        return;
      }
      Thread.sleep(10L);
    }
  }

  private static int liveReferences(List<WeakReference<Thread>> references) {
    int live = 0;
    for (WeakReference<Thread> reference : references) {
      if (reference.get() != null) {
        live++;
      }
    }
    return live;
  }

  private static int budgetStripeCount(OffHeapCache<?, ?> cache) throws Exception {
    Field budgetField = OffHeapCache.class.getDeclaredField("budget");
    budgetField.setAccessible(true);
    Object budget = budgetField.get(cache);
    Method count = budget.getClass().getDeclaredMethod("stripeCount");
    count.setAccessible(true);
    return (Integer) count.invoke(budget);
  }

  private static int readerCount(OffHeapCache<?, ?> cache) throws Exception {
    Field readersField = OffHeapCache.class.getDeclaredField("readers");
    readersField.setAccessible(true);
    Object readers = readersField.get(cache);
    Method count = readers.getClass().getDeclaredMethod("registeredCount");
    count.setAccessible(true);
    return (Integer) count.invoke(readers);
  }

  private static final class WorkloadResult {
    final List<WeakReference<Thread>> terminatedThreads;
    final int readHits;
    final int acceptedWrites;

    private WorkloadResult(
        List<WeakReference<Thread>> terminatedThreads, int readHits, int acceptedWrites) {
      this.terminatedThreads = terminatedThreads;
      this.readHits = readHits;
      this.acceptedWrites = acceptedWrites;
    }
  }
}
