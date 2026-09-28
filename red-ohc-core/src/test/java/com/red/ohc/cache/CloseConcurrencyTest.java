package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;

/**
 * Close is a quiesced-only teardown: the cache is process-lifetime in production and callers
 * must have no concurrent readers, writers, or bulk operations when close runs. These tests
 * cover only what the quiesced contract still guarantees.
 */
public class CloseConcurrencyTest {
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

  @Test(timeOut = 10_000L)
  public void closeOnAQuiescedCacheFreesEverythingAndRejectsLaterOperations() {
    OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build();
    cache.put("key", "value");
    assertEquals(cache.get("key"), "value");
    cache.close();
    assertEquals(cache.get("key"), null, "a closed cache must miss");
    try {
      cache.put("key", "value");
      org.testng.Assert.fail("a closed cache must reject writes");
    } catch (IllegalStateException expected) {
      // Closed caches fail fast on writes.
    }
  }

  @Test(timeOut = 10_000L)
  public void closeIsIdempotentForConcurrentQuiescedCallers() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    cache.put("key", "value");
    Thread second =
        new Thread(
            () -> {
              cache.close();
              cache.close();
            });
    second.start();
    cache.close();
    second.join(5_000L);
    assertFalse(second.isAlive());
    assertEquals(cache.get("key"), null);
  }
}
