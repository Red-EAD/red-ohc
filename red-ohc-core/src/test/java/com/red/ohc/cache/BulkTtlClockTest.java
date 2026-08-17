package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.DirectEntryConsumer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;

public final class BulkTtlClockTest {
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
  public void getAllReadsTheTtlClockOncePerChunk() {
    CountingTicker ticker = new CountingTicker();
    try (OHCache<String, String> cache = newCache(ticker)) {
      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("two", "value-two"));
      cache.flushAsync().join();
      ticker.resetReaderCalls();

      Map<String, String> values = cache.getAll(Arrays.asList("one", "two"));

      assertEquals(values.size(), 2);
      assertEquals(ticker.readerCalls(), 1);
    }
  }

  @Test
  public void getDirectAllReadsTheTtlClockOncePerChunk() {
    CountingTicker ticker = new CountingTicker();
    try (OHCache<String, String> cache = newCache(ticker)) {
      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("two", "value-two"));
      cache.flushAsync().join();
      ticker.resetReaderCalls();
      AtomicInteger hits = new AtomicInteger();
      DirectEntryConsumer<String> consumer = (key, value) -> hits.incrementAndGet();

      assertEquals(cache.getDirectAll(Arrays.asList("one", "two"), consumer), 2);

      assertEquals(hits.get(), 2);
      assertEquals(ticker.readerCalls(), 1);
    }
  }

  @Test
  public void getAllKeepsThePublishedValueObservedBeforeTheTtlClockRead() {
    ReentrantReplacementTicker ticker = new ReentrantReplacementTicker();
    try (OHCache<String, String> cache = newCache(ticker)) {
      assertTrue(cache.put("key", "old", 1_060_000L));
      cache.flushAsync().join();
      ticker.arm(cache);

      Map<String, String> values = cache.getAll(Arrays.asList("key"));

      assertEquals(values.get("key"), "old");
      assertEquals(cache.get("key"), "new");
      assertEquals(ticker.readerCalls(), 1);
    }
  }

  @Test
  public void noTtlWritesDoNotReadTheWallClock() {
    CountingTicker ticker = new CountingTicker();
    try (OHCache<String, String> cache = newNoTtlCache(ticker)) {
      ticker.resetReaderCalls();

      assertTrue(cache.put("one", "value-one"));
      assertTrue(cache.put("one", "value-two"));

      assertEquals(ticker.readerCalls(), 0);
    }
  }

  private static OHCache<String, String> newCache(Ticker ticker) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .defaultTTLmillis(60_000L)
        .ticker(ticker)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static OHCache<String, String> newNoTtlCache(CountingTicker ticker) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .ticker(ticker)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }

  private static final class CountingTicker implements Ticker {
    private final Thread reader = Thread.currentThread();
    private final AtomicInteger readerCalls = new AtomicInteger();

    @Override
    public long nanos() {
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      if (Thread.currentThread() == reader) {
        readerCalls.incrementAndGet();
      }
      return 1_000_000L;
    }

    void resetReaderCalls() {
      readerCalls.set(0);
    }

    int readerCalls() {
      return readerCalls.get();
    }
  }

  private static final class ReentrantReplacementTicker implements Ticker {
    private final Thread reader = Thread.currentThread();
    private final AtomicInteger readerCalls = new AtomicInteger();
    private OHCache<String, String> cache;
    private boolean replaceOnNextRead;

    @Override
    public long nanos() {
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      if (Thread.currentThread() == reader) {
        int call = readerCalls.incrementAndGet();
        if (replaceOnNextRead && call == 1) {
          replaceOnNextRead = false;
          assertTrue(cache.put("key", "new", 0L));
        }
      }
      return 1_000_000L;
    }

    void arm(OHCache<String, String> cache) {
      this.cache = cache;
      readerCalls.set(0);
      replaceOnNextRead = true;
    }

    int readerCalls() {
      return readerCalls.get();
    }
  }
}
