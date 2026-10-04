package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.runtime.WriterResourceRegistry;

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
  public void reentrantPutFailsExplicitlyInsteadOfSilentlyDroppingTheNestedWrite() {
    AtomicReference<OHCache<String, String>> holder = new AtomicReference<>();
    CacheSerializer<String> reentrant =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("outer".equals(value)) {
              holder.get().put("nested", "inner");
            }
            buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(reentrant)
              .build();
      Throwable cacheFailure3 = null;
      try {
        holder.set(cache);
        expectThrows(IllegalStateException.class, () -> cache.put("outer", "outer"));
        assertEquals(cache.size(), 0L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
    }
  }

  @Test(timeOut = 5_000L)
  public void readOnlyThreadDoesNotAllocateWriterState() throws Exception {
    {
      OffHeapCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .buildTyped();
      Thread reader = null;
      Throwable cacheFailure2 = null;
      try {
        cache.put("seed", "value");
        cache.flushAsync().join();
        AtomicReference<String> observed = new AtomicReference<>();

        reader = new Thread(() -> observed.set(cache.get("seed")), "read-only-cache-thread");
        reader.start();
        CacheTestSupport.awaitCaller(reader);

        assertEquals(observed.get(), "value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stopAfterCallers(cache, cacheFailure2, reader);
      }
    }
  }

  @Test
  public void oneLiveWriterKeepsOneExclusiveResource() throws Exception {
    {
      OffHeapCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .buildTyped();
      Throwable cacheFailure1 = null;
      try {
        int before = writerResourceCount(cache);
        cache.put("first", "value");
        cache.put("second", "value");
        assertEquals(writerResourceCount(cache), before + 1);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
    }
  }

  @Test
  public void internalStopClearsReaderRegistryAndNativeMemory() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    Throwable primaryFailure = null;
    try {
      cache.put("key", "value");
      assertEquals(cache.get("key"), "value");
      assertTrue(readerCount(cache) > 0);

      CacheTestSupport.stop(cache);

      assertEquals(readerCount(cache), 0);
      assertEquals(cache.totalAllocatedBytes(), 0L);

    } catch (Throwable operationFailure) {
      primaryFailure = operationFailure;
      throw operationFailure;
    } finally {
      CacheTestSupport.stop(cache, primaryFailure);
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
    {
      Throwable explicitCacheFailure1 = null;
      try {

        cache.put("key", "initial");
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

        int attempts = 64;
        java.util.concurrent.Future<Integer> writer =
            readers.submit(
                () -> {
                  for (int i = 0; i < attempts; i++) {
                    cache.put("key", "value-" + i);
                  }
                  return attempts;
                });
        Thread.sleep(100L);
        assertTrue(writer.isDone(), "writer must not wait for reader-backed retirement");
        release.countDown();
        assertEquals(writer.get(8L, java.util.concurrent.TimeUnit.SECONDS).intValue(), attempts);
        assertTrue(direct.get(2L, java.util.concurrent.TimeUnit.SECONDS));

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure1 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        if (direct != null) {
          direct.cancel(true);
        }
        CacheTestSupport.stop(cache, explicitCacheFailure1, readers);
      }
    }
  }

  private static int writerResourceCount(OffHeapCache<?, ?> cache) throws Exception {
    Field resourceField = OffHeapCache.class.getDeclaredField("writerResources");
    resourceField.setAccessible(true);
    return ((WriterResourceRegistry) resourceField.get(cache)).resourceCount();
  }

  private static int readerCount(OffHeapCache<?, ?> cache) throws Exception {
    Field readersField = OffHeapCache.class.getDeclaredField("readers");
    readersField.setAccessible(true);
    Object readers = readersField.get(cache);
    Method count = readers.getClass().getDeclaredMethod("registeredCount");
    count.setAccessible(true);
    return (Integer) count.invoke(readers);
  }
}
