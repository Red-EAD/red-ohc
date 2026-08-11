package com.red.ohc.cache;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheInput;
import com.red.ohc.api.CacheOutput;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.runtime.ThreadContext;

public final class ListenerContextLifecycleTest {
  private static final CacheSerializer<String> STRING = CacheTestSerializers.string();

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
}
