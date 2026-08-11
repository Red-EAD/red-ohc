package com.red.ohc.cache;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.runtime.ThreadContext;

public final class ListenerContextLifecycleTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, java.nio.ByteBuffer buffer) {
          buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(java.nio.ByteBuffer buffer) {
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
  public void cacheWithoutListenerDoesNotCreateAnEvictionContext() throws Exception {
    try (OffHeapCache<String, String> cache = newCache(null)) {
      assertNull(evictionContext(cache));
    }
  }

  @Test
  public void configuredListenerUsesOneActorContext() throws Exception {
    try (OffHeapCache<String, String> cache = newCache((key, value, cause) -> {})) {
      assertTrue(evictionContext(cache) instanceof ThreadContext);
    }
  }

  @Test
  public void listenerActorContextIsSeparateFromNestedBusinessContext() throws Exception {
    try (OffHeapCache<String, String> cache = newCache((key, value, cause) -> {})) {
      assertNotSame(evictionContext(cache), businessContext(cache));
    }
  }

  private static OffHeapCache<String, String> newCache(
      com.red.ohc.api.EvictionListener<String, String> listener) {
    OHCacheBuilder<String, String> builder =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING);
    if (listener != null) {
      builder.evictionListener(listener);
    }
    return builder.buildTyped();
  }

  private static Object evictionContext(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("evictionContexts");
    field.setAccessible(true);
    return field.get(cache);
  }

  private static Object businessContext(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("contexts");
    field.setAccessible(true);
    return ((ThreadLocal<?>) field.get(cache)).get();
  }
}
