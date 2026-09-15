package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;

public final class PermanentReadClockTest {
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

  @Test
  public void permanentDirectHitDoesNotReadTheBusinessThreadClock() {
    Thread reader = Thread.currentThread();
    AtomicInteger readerClockCalls = new AtomicInteger();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return System.nanoTime();
          }

          @Override
          public long currentTimeMillis() {
            if (Thread.currentThread() == reader) {
              readerClockCalls.incrementAndGet();
            }
            return System.currentTimeMillis();
          }
        };
    byte[] key = {1, 2, 3, 4};
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(key, new byte[16]);
      cache.flushAsync().join();
      readerClockCalls.set(0);
      assertTrue(cache.getDirect(key, value -> value.getLong(0)));
      assertEquals(readerClockCalls.get(), 0);
    }
  }

  @Test
  public void ttlReadUsesTheCurrentTickerEvenBeforeTheWorkerPhysicallyRemovesTheEntry() {
    AtomicInteger now = new AtomicInteger();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return now.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return now.get();
          }
        };
    byte[] key = {9, 8, 7, 6};
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(key, new byte[16], 64L);
      cache.flushAsync().join();
      now.set(64);
      cache.flushAsync().join();
      assertTrue(!cache.getDirect(key, value -> value.getLong(0)));
    }
  }

  @Test
  public void ttlLivenessUsesTheCurrentTickerWithoutAFlush() {
    AtomicInteger now = new AtomicInteger();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return now.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return now.get();
          }
        };
    byte[] readKey = {1, 2, 3, 4};
    byte[] absentKey = {5, 6, 7, 8};
    byte[] replaceKey = {9, 10, 11, 12};
    try (OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped()) {
      assertPutEventually(cache, readKey, new byte[16], 64L);
      assertPutEventually(cache, absentKey, new byte[] {1}, 64L);
      assertPutEventually(cache, replaceKey, new byte[] {2}, 64L);

      now.set(64);

      assertEquals(cache.get(readKey), null);
      assertFalse(cache.containsKey(readKey));
      assertFalse(cache.getDirect(readKey, value -> value.getByte(0)));
      assertEquals(
          cache.getDirectAll(Collections.singletonList(readKey), (key, value) -> {}), 0);
      assertTrue(cache.putIfAbsent(absentKey, new byte[] {3}, 0L) == null);
      assertFalse(cache.replace(replaceKey, new byte[] {2}, new byte[] {4}, 0L));
    }
  }

  @Test
  public void ttlReadUsesTheMonotonicDeadlineAfterPublication() {
    Thread reader = Thread.currentThread();
    AtomicLong readerWallMillis = new AtomicLong(1_000L);
    AtomicLong monotonicNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return monotonicNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return Thread.currentThread() == reader ? readerWallMillis.get() : 1_000L;
          }
        };
    byte[] key = {11, 12, 13, 14};
    try (OHCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .build()) {
      cache.put(key, new byte[16], 2_000L);

      readerWallMillis.set(3_000L);
      assertTrue(cache.getDirect(key, value -> value.getLong(0)));

      monotonicNanos.set(1_000_000_000L);
      assertFalse(cache.getDirect(key, value -> value.getLong(0)));
    }
  }

  private static void assertPutEventually(
      OHCache<byte[], byte[]> cache, byte[] key, byte[] value, long expireAtMillis) {
    for (int attempt = 0; attempt < 1_000; attempt++) {
      cache.put(key, value, expireAtMillis);
      return;
    }
    assertTrue(false, "put admission did not succeed within 1000 attempts");
  }
}
