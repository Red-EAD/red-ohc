package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.runtime.ThreadContext;

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

      int attempts = (int) cache.stats().getRetirementQueueCapacity() + 64;
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

  @Test(timeOut = 10_000L)
  public void failedBudgetReservationTriggersIdleLeaseReclaim() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(4 << 10)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    int leaseCount = 1;
    java.util.concurrent.ExecutorService holders =
        java.util.concurrent.Executors.newFixedThreadPool(leaseCount);
    java.util.concurrent.CountDownLatch ready =
        new java.util.concurrent.CountDownLatch(leaseCount);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    AtomicInteger accepted = new AtomicInteger();
    try {
      for (int index = 0; index < leaseCount; index++) {
        final int key = index;
        holders.submit(
            () -> {
              if (cache.put("holder-" + key, "value")) {
                accepted.incrementAndGet();
              }
              ready.countDown();
              release.await();
              return null;
            });
      }
      assertTrue(ready.await(2L, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(accepted.get(), leaseCount);
      assertFalse(
          cache.put("pressure", "first-attempt"),
          "idle writer leases must retain all refill credit before budget pressure");

      boolean recovered = false;
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
      while (!recovered && System.nanoTime() < deadline) {
        recovered = cache.put("pressure", "recovered");
        Thread.yield();
      }
      assertTrue(recovered, "budget pressure must reclaim idle lease credit");
      assertEquals(cache.get("pressure"), "recovered");
    } finally {
      release.countDown();
      holders.shutdownNow();
      holders.awaitTermination(2L, java.util.concurrent.TimeUnit.SECONDS);
      cache.close();
    }
  }

  @Test
  public void failedWriterLeaseActivationReturnsWithoutSettingCloseMarker() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    ThreadContext context = null;
    try {
      Field contextsField = OffHeapCache.class.getDeclaredField("contexts");
      contextsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      ThreadLocal<ThreadContext> contexts = (ThreadLocal<ThreadContext>) contextsField.get(cache);
      context = contexts.get();

      Field leaseField = ThreadContext.class.getDeclaredField("budgetLease");
      leaseField.setAccessible(true);
      Object lease = leaseField.get(context);
      Field stateField = lease.getClass().getDeclaredField("state");
      stateField.setAccessible(true);
      AtomicInteger state = (AtomicInteger) stateField.get(lease);
      state.set(2); // Budget.Lease.RECLAIMING

      Method enterWriter = OffHeapCache.class.getDeclaredMethod("enterWriter");
      enterWriter.setAccessible(true);
      assertEquals(enterWriter.invoke(cache), null);
      state.set(0); // Budget.Lease.IDLE
      assertFalse(
          context.slot.writerActive, "failed lease activation must not block close forever");
    } finally {
      // Keep cleanup independent of the intentionally injected activation failure.
      if (context != null) {
        context.slot.writerActive = false;
      }
      cache.close();
    }
  }
}
