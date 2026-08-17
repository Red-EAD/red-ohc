package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Ticker;
import com.red.ohc.storage.Budget;

public class WriteAdmissionTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
      };

  @Test
  public void closedPutThrowsInsteadOfReturningResourceRejection() {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    cache.close();
    expectThrows(IllegalStateException.class, () -> cache.put("key", "value"));
  }

  @Test
  public void oversizePutThrowsInsteadOfReturningFalse() {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(128)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      expectThrows(
          IllegalArgumentException.class, () -> cache.put("this-key-is-too-large", "value"));
    }
  }

  @Test
  public void capacityOversizeSkipsDefaultTtlAndNativeAdmission() throws Exception {
    AtomicInteger serializedSizes = new AtomicInteger();
    AtomicInteger currentTimeMillisCalls = new AtomicInteger();
    Thread writer = Thread.currentThread();
    CacheSerializer<String> countingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            serializedSizes.incrementAndGet();
            return STRING.serializedSize(value);
          }
        };
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return System.nanoTime();
          }

          @Override
          public long currentTimeMillis() {
            if (Thread.currentThread() == writer) {
              currentTimeMillisCalls.incrementAndGet();
            }
            return 1_000_000L;
          }
        };

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(512)
            .defaultTTLmillis(1_000L)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(countingValueSerializer)
            .buildTyped()) {
      assertTrue(cache.put("baseline", "value", 0L));
      cache.flushAsync().join();
      long allocatedBeforeOversize = cache.totalAllocatedBytes();
      long reservedBeforeOversize = budgetReserved(cache);
      serializedSizes.set(0);
      currentTimeMillisCalls.set(0);
      expectThrows(
          IllegalArgumentException.class,
          () -> cache.put("oversized", "x".repeat(4_096)));

      assertEquals(serializedSizes.get(), 1);
      assertEquals(currentTimeMillisCalls.get(), 0);
      assertEquals(cache.totalAllocatedBytes(), allocatedBeforeOversize);
      assertEquals(budgetReserved(cache), reservedBeforeOversize);
      assertTrue(cache.put("baseline", "value"));
    }
  }

  @Test
  public void capacityOversizeReplacementSkipsDefaultTtlAndNativeAdmission() throws Exception {
    AtomicInteger serializedSizes = new AtomicInteger();
    AtomicInteger currentTimeMillisCalls = new AtomicInteger();
    Thread writer = Thread.currentThread();
    CacheSerializer<String> countingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            serializedSizes.incrementAndGet();
            return STRING.serializedSize(value);
          }
        };
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return System.nanoTime();
          }

          @Override
          public long currentTimeMillis() {
            if (Thread.currentThread() == writer) {
              currentTimeMillisCalls.incrementAndGet();
            }
            return 1_000_000L;
          }
        };

    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(512)
            .defaultTTLmillis(1_000L)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(countingValueSerializer)
            .buildTyped()) {
      assertTrue(cache.put("baseline", "value", 0L));
      cache.flushAsync().join();
      long allocatedBeforeOversize = cache.totalAllocatedBytes();
      long reservedBeforeOversize = budgetReserved(cache);
      serializedSizes.set(0);
      currentTimeMillisCalls.set(0);

      expectThrows(
          IllegalArgumentException.class,
          () -> cache.put("baseline", "x".repeat(4_096)));

      assertEquals(serializedSizes.get(), 1);
      assertEquals(currentTimeMillisCalls.get(), 0);
      assertEquals(cache.totalAllocatedBytes(), allocatedBeforeOversize);
      assertEquals(budgetReserved(cache), reservedBeforeOversize);
      assertTrue(cache.put("baseline", "replacement"));
      assertEquals(cache.get("baseline"), "replacement");
    }
  }

  @Test(timeOut = 5_000L)
  public void readOnlyThreadDoesNotAllocateWriterState() throws Exception {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      assertTrue(cache.put("seed", "value"));
      cache.flushAsync().join();
      AtomicReference<String> observed = new AtomicReference<>();

      Thread reader = new Thread(() -> observed.set(cache.get("seed")), "read-only-cache-thread");
      reader.start();
      reader.join();

      assertEquals(observed.get(), "value");
    }
  }

  @Test
  public void firstWriteUsesTheCacheFixedBudgetStripeSet() throws Exception {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      int before = budgetStripeCount(cache);
      assertTrue(cache.put("first", "value"));
      assertTrue(cache.put("second", "value"));
      assertEquals(budgetStripeCount(cache), before);
    }
  }

  @Test
  public void closeClearsReaderRegistryAndBudget() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    assertTrue(cache.put("key", "value"));
    assertEquals(cache.get("key"), "value");
    assertTrue(readerCount(cache) > 0);

    cache.close();

    assertEquals(readerCount(cache), 0);
    assertEquals(budgetReserved(cache), 0L);
  }

  @Test(timeOut = 30_000L)
  public void writerReturnsWithoutWaitingForReaderQuiescence() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(8L << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.ExecutorService readers =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    java.util.concurrent.Future<Boolean> direct = null;
    try {
      assertTrue(cache.put("key", "initial"));
      cache.flushAsync().join();
      direct =
          readers.submit(
              () ->
                  cache.getDirect(
                      "key",
                      view -> {
                        entered.countDown();
                        try {
                          release.await();
                        } catch (InterruptedException interrupted) {
                          Thread.currentThread().interrupt();
                          throw new AssertionError(interrupted);
                        }
                        view.getByte(0);
                      }));
      assertTrue(entered.await(2L, java.util.concurrent.TimeUnit.SECONDS));

      int attempts = Math.toIntExact(retirementQueueCapacity(cache)) + 64;
      java.util.concurrent.Future<Integer> writer =
          readers.submit(
              () -> {
                int accepted = 0;
                for (int i = 0; i < attempts; i++) {
                  if (cache.put("key", "value-" + i)) {
                    accepted++;
                  }
                }
                return accepted;
              });
      assertTrue(
          writer.get(8L, java.util.concurrent.TimeUnit.SECONDS) < attempts,
          "writer must return bounded admission failures instead of waiting for the reader");
      release.countDown();
      assertTrue(direct.get(2L, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      release.countDown();
      if (direct != null) {
        direct.cancel(true);
      }
      readers.shutdownNow();
      cache.close();
    }
  }

  private static int budgetStripeCount(OffHeapCache<?, ?> cache) throws Exception {
    Field budgetField = OffHeapCache.class.getDeclaredField("budget");
    budgetField.setAccessible(true);
    Budget budget = (Budget) budgetField.get(cache);
    Method count = Budget.class.getDeclaredMethod("stripeCount");
    count.setAccessible(true);
    return (Integer) count.invoke(budget);
  }

  private static long retirementQueueCapacity(OffHeapCache<?, ?> cache) throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    com.red.ohc.maintenance.MaintenanceEventLoop worker =
        (com.red.ohc.maintenance.MaintenanceEventLoop) workerField.get(cache);
    return worker.retirementQueueCapacity();
  }

  private static long budgetReserved(OffHeapCache<?, ?> cache) throws Exception {
    Field budgetField = OffHeapCache.class.getDeclaredField("budget");
    budgetField.setAccessible(true);
    return ((Budget) budgetField.get(cache)).reserved();
  }

  private static int readerCount(OffHeapCache<?, ?> cache) throws Exception {
    Field readersField = OffHeapCache.class.getDeclaredField("readers");
    readersField.setAccessible(true);
    Object readers = readersField.get(cache);
    Method count = readers.getClass().getDeclaredMethod("registeredCount");
    count.setAccessible(true);
    return (Integer) count.invoke(readers);
  }
}
