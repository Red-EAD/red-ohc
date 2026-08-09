package com.red.ohc.cache;

import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;

public class TtlJitterTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes());
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          return "unused";
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes().length;
        }
      };

  @Test
  public void defaultTtlJitterSpreadsNativeExpiryTimes() {
    AtomicLong clock = new AtomicLong(10_000L);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return clock.get() * 1_000_000L;
          }

          @Override
          public long currentTimeMillis() {
            return clock.get();
          }
        };
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .defaultTTLmillis(1_000L)
            .ttlJitterPercent(.5d)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      for (int i = 0; i < 100; i++) {
        assertTrue(cache.put("key-" + i, "value"));
      }
      Set<Long> expiries = new HashSet<>();
      for (Entry entry : cache.data.values()) {
        expiries.add(ValueBlock.expireAtMillis(Entry.rawValueAddress(entry.valueAddress)));
      }
      assertTrue(expiries.size() > 1, "default TTLs were not jittered");
      for (Long expiry : expiries) {
        assertTrue(expiry >= 10_500L && expiry <= 11_500L);
      }
    }
  }
}
