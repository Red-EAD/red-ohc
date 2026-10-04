package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;

public class MaintenanceLifecycleTest {
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

  @Test
  public void internalStopReclaimsAllNativeMemoryAfterAcceptedMutations() {
    OHCache<String, String> cache = newCache();
    Throwable primaryFailure = null;
    try {
      cache.put("alpha", "value");
      cache.flushAsync().join();
      assertTrue(cache.totalAllocatedBytes() > 0L);

      CacheTestSupport.stop(cache);

      assertEquals(cache.totalAllocatedBytes(), 0L);

    } catch (Throwable operationFailure) {
      primaryFailure = operationFailure;
      throw operationFailure;
    } finally {
      CacheTestSupport.stop(cache, primaryFailure);
    }
  }

  private static OHCache<String, String> newCache() {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .keySerializer(STRING)
        .valueSerializer(STRING)
        .build();
  }
}
