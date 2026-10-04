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
    {
      OffHeapCache<String, String> cache = newCache(null);
      Throwable cacheFailure3 = null;
      try {
        assertNull(evictionContext(cache));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
    }
  }

  @Test
  public void configuredListenerUsesOneActorContext() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache((key, value, cause) -> {});
      Throwable cacheFailure2 = null;
      try {
        assertTrue(evictionContext(cache) instanceof ThreadContext);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
    }
  }

  @Test
  public void listenerActorContextIsSeparateFromNestedBusinessContext() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache((key, value, cause) -> {});
      Throwable cacheFailure1 = null;
      try {
        assertNotSame(evictionContext(cache), businessContext(cache));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
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
