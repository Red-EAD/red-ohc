package com.red.ohc.cache;

import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;

public class DirectValueEpochTest {
  private static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] copy = new byte[buffer.remaining()];
          buffer.get(copy);
          return copy;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Test(timeOut = 10_000L)
  public void removeCannotReclaimAnEntryWhileDirectValueConsumerIsRunning() throws Exception {
    ExecutorService reader = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    byte[] key = new byte[] {7, 8, 9, 10};
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      assertTrue(cache.put(key, new byte[16]));
      cache.flushAsync().join();
      Future<Boolean> directRead =
          reader.submit(() -> waitForDirectHit(cache, key, entered, release));
      try {
        assertTrue(entered.await(2L, TimeUnit.SECONDS), "direct consumer did not start");
        assertTrue(cache.removeAsync(key).get(2L, TimeUnit.SECONDS));
        assertTrue(
            cache.totalAllocatedBytes() > 0L,
            "native storage must remain allocated during the direct callback");
      } finally {
        release.countDown();
        assertTrue(directRead.get(2L, TimeUnit.SECONDS));
      }
    } finally {
      reader.shutdownNow();
    }
  }

  private static boolean waitForDirectHit(
      OHCache<byte[], byte[]> cache, byte[] key, CountDownLatch entered, CountDownLatch release) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
    while (System.nanoTime() < deadline) {
      if (cache.getDirect(
          key,
          value -> {
            entered.countDown();
            try {
              if (!release.await(2L, TimeUnit.SECONDS)) {
                throw new AssertionError("direct callback was not released");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
            value.getLong(0);
          })) {
        return true;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
    }
    return false;
  }

}
