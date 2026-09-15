package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.runtime.ReaderRegistry;

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

  @Test
  public void closeDoesNotKeepAWaiterReferenceOnTheWriterExitPath() throws Exception {
    try {
      OffHeapCache.class.getDeclaredField("closeWaiter");
      fail("close must not require a per-writer waiter check");
    } catch (NoSuchFieldException expected) {
      // Close uses the cache-local lifecycle condition instead of a per-writer waiter.
    }
  }

  @Test(timeOut = 5_000L)
  public void concurrentCloseSharesOneTerminationWithoutAMonitor() throws Exception {
    Method close = OffHeapCache.class.getDeclaredMethod("close");
    assertFalse(
        Modifier.isSynchronized(close.getModifiers()),
        "close must use the OPEN/CLOSING/CLOSED gate");

    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    cache.put("key", "value");
    cache.flushAsync().join();

    ExecutorService callers = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<?> first =
          callers.submit(
              () -> {
                await(start);
                cache.close();
              });
      Future<?> second =
          callers.submit(
              () -> {
                await(start);
                cache.close();
              });
      start.countDown();
      first.get(2L, TimeUnit.SECONDS);
      second.get(2L, TimeUnit.SECONDS);
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      callers.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 5_000L)
  public void flushAndCloseAreLinearized() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    CountDownLatch flushCheckedOpen = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    cache.setFlushLifecycleHookForTest(
        () -> {
          flushCheckedOpen.countDown();
          await(releaseFlush);
        });
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<?> flush = callers.submit(() -> cache.flushAsync().join());
      assertTrue(flushCheckedOpen.await(2L, TimeUnit.SECONDS));
      Future<?> close = callers.submit(cache::close);
      Thread.sleep(100L);
      assertFalse(close.isDone(), "close must not pass the flush lifecycle boundary while the lock is held");

      releaseFlush.countDown();
      flush.get(2L, TimeUnit.SECONDS);
      close.get(2L, TimeUnit.SECONDS);
    } finally {
      releaseFlush.countDown();
      callers.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 5_000L)
  public void closeWaitsForAWriterAdmittedBeforeClosing() throws Exception {
    CountDownLatch serializeStarted = new CountDownLatch(1);
    CountDownLatch releaseSerialize = new CountDownLatch(1);
    CacheSerializer<String> blockingValue =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            serializeStarted.countDown();
            await(releaseSerialize);
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
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(blockingValue)
            .buildTyped();
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<?> write = callers.submit(() -> cache.put("key", "value"));
      assertTrue(serializeStarted.await(2L, TimeUnit.SECONDS), "writer was not admitted");
      Field readersField = OffHeapCache.class.getDeclaredField("readers");
      readersField.setAccessible(true);
      assertFalse(
          ((ReaderRegistry) readersField.get(cache)).hasActiveReader(),
          "a slow serializer must not hold a reader epoch before native publication");

      Future<?> close = callers.submit(cache::close);
      Thread.sleep(100L);
      assertFalse(
          close.isDone(),
          "close must not release native arenas while an admitted writer is active");

      releaseSerialize.countDown();
      write.get(2L, TimeUnit.SECONDS);
      close.get(2L, TimeUnit.SECONDS);
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      releaseSerialize.countDown();
      callers.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 5_000L)
  public void closeWaitsForNativeBackedDeserialization() throws Exception {
    CountDownLatch deserializeStarted = new CountDownLatch(1);
    CountDownLatch releaseDeserialize = new CountDownLatch(1);
    CacheSerializer<String> blockingValue =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializeStarted.countDown();
            await(releaseDeserialize);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .closeTimeoutMillis(100L)
            .keySerializer(STRING)
            .valueSerializer(blockingValue)
            .buildTyped();
    ExecutorService reader = Executors.newSingleThreadExecutor();
    Future<String> read = null;
    boolean closed = false;
    try {
      cache.put("key", "value");
      cache.flushAsync().join();
      read = reader.submit(() -> cache.get("key"));
      assertTrue(deserializeStarted.await(2L, TimeUnit.SECONDS), "deserialize did not start");

      try {
        cache.close();
        fail("close must wait for the active native-backed deserialization");
      } catch (IllegalStateException expected) {
        // The configured close timeout expires while the serializer is still inside the reader
        // epoch. Releasing it below allows the second close call to finish cleanup.
      }
    } finally {
      releaseDeserialize.countDown();
      if (read != null) {
        assertEquals(read.get(2L, TimeUnit.SECONDS), "value");
      }
      cache.close();
      closed = true;
      reader.shutdownNow();
      if (!closed) {
        cache.close();
      }
    }
  }

  @Test(timeOut = 5_000L)
  public void closeCompletesAfterAnActiveDirectReaderBecomesQuiescent() throws Exception {
    CountDownLatch directEntered = new CountDownLatch(1);
    CountDownLatch releaseDirect = new CountDownLatch(1);
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .closeTimeoutMillis(1_000L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      cache.put("key", "value");
      cache.flushAsync().join();
      Future<Boolean> direct =
          callers.submit(
              () ->
                  cache.getDirect(
                      "key",
                      value -> {
                        directEntered.countDown();
                        await(releaseDirect);
                        value.getByte(0);
                      }));
      assertTrue(directEntered.await(2L, TimeUnit.SECONDS));

      Future<?> close = callers.submit(cache::close);
      Thread.sleep(100L);
      assertFalse(close.isDone(), "close must wait for the active native reader");
      releaseDirect.countDown();

      assertTrue(direct.get(2L, TimeUnit.SECONDS));
      close.get(2L, TimeUnit.SECONDS);
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      releaseDirect.countDown();
      callers.shutdownNow();
    }
  }

  @Test(timeOut = 5_000L)
  public void closeWaitsForBlockingGetAllDeserializer() throws Exception {
    CountDownLatch deserializeStarted = new CountDownLatch(1);
    CountDownLatch releaseDeserialize = new CountDownLatch(1);
    CacheSerializer<String> blockingValue =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializeStarted.countDown();
            await(releaseDeserialize);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .closeTimeoutMillis(100L)
            .keySerializer(STRING)
            .valueSerializer(blockingValue)
            .buildTyped();
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      cache.put("key", "value");
      cache.flushAsync().join();
      Future<Map<String, String>> read =
          callers.submit(() -> cache.getAll(Collections.singletonList("key")));
      assertTrue(deserializeStarted.await(2L, TimeUnit.SECONDS));
      try {
        cache.close();
        fail("close must wait for the active getAll reader");
      } catch (IllegalStateException expected) {
        // The timeout is expected while the deserializer owns the reader epoch.
      }
      releaseDeserialize.countDown();
      assertEquals(read.get(2L, TimeUnit.SECONDS).get("key"), "value");
      cache.close();
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      releaseDeserialize.countDown();
      callers.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void closeRaceBeforeReaderPublicationReturnsOrdinaryMisses() throws Exception {
    for (String operation : new String[] {"get", "containsKey", "getDirect"}) {
      CountDownLatch serializationStarted = new CountDownLatch(1);
      CountDownLatch releaseSerialization = new CountDownLatch(1);
      AtomicBoolean blockSerialization = new AtomicBoolean();
      CacheSerializer<String> blockingKey =
          new CacheSerializer<String>() {
            @Override
            public void serialize(String value, ByteBuffer buffer) {
              if (blockSerialization.get()) {
                serializationStarted.countDown();
                await(releaseSerialization);
              }
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
      OffHeapCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(blockingKey)
              .valueSerializer(STRING)
              .buildTyped();
      AtomicReference<Throwable> closeFailure = new AtomicReference<>();
      Thread closer =
          new Thread(
              () -> {
                try {
                  if (!serializationStarted.await(2L, TimeUnit.SECONDS)) {
                    throw new AssertionError("read did not reach key serialization");
                  }
                  cache.close();
                } catch (Throwable failure) {
                  closeFailure.set(failure);
                } finally {
                  releaseSerialization.countDown();
                }
              },
              "close-before-reader-publication");
      Throwable readFailure = null;
      Object result = null;
      try {
        // Register this caller first; close will clear its slot while the next operation is still
        // serializing its key and has not entered a reader epoch.
        assertEquals(cache.get("initial"), null);
        blockSerialization.set(true);
        closer.start();
        if ("get".equals(operation)) {
          result = cache.get("racing");
        } else if ("containsKey".equals(operation)) {
          result = cache.containsKey("racing");
        } else {
          result = cache.getDirect("racing", value -> fail("unexpected hit"));
        }
      } catch (Throwable failure) {
        readFailure = failure;
      } finally {
        releaseSerialization.countDown();
        closer.join(4_000L);
        cache.close();
      }
      assertEquals(readFailure, null, operation + " must not expose an unbound reader slot");
      assertEquals(closeFailure.get(), null, operation + " close must complete cleanly");
      assertEquals(result, "get".equals(operation) ? null : Boolean.FALSE);
    }
  }

  @Test(timeOut = 5_000L)
  public void closeWaitsForBulkReadBetweenReaderChunks() throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .closeTimeoutMillis(1_000L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    ExecutorService callers = Executors.newFixedThreadPool(2);
    CountDownLatch boundaryReached = new CountDownLatch(1);
    CountDownLatch releaseBoundary = new CountDownLatch(1);
    List<String> keys = new ArrayList<>();
    for (int index = 0; index < 8_192; index++) {
      keys.add("missing-bulk-" + index);
    }
    Collection<String> blockingKeys =
        new AbstractCollection<String>() {
          @Override
          public Iterator<String> iterator() {
            Iterator<String> delegate = keys.iterator();
            return new Iterator<String>() {
              private int seen;

              @Override
              public boolean hasNext() {
                if (seen == 4_096) {
                  boundaryReached.countDown();
                  await(releaseBoundary);
                }
                return delegate.hasNext();
              }

              @Override
              public String next() {
                seen++;
                return delegate.next();
              }
            };
          }

          @Override
          public int size() {
            return keys.size();
          }
        };
    try {
      Future<Map<String, String>> read = callers.submit(() -> cache.getAll(blockingKeys));
      assertTrue(boundaryReached.await(2L, TimeUnit.SECONDS));

      Future<?> close = callers.submit(cache::close);
      Thread.sleep(100L);
      assertFalse(close.isDone(), "close must wait for the complete bulk operation");

      releaseBoundary.countDown();
      assertTrue(read.get(2L, TimeUnit.SECONDS).isEmpty());
      close.get(2L, TimeUnit.SECONDS);
    } finally {
      releaseBoundary.countDown();
      callers.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 5_000L)
  public void deserializeFailureLeavesReaderEpochQuiescent() {
    CacheSerializer<String> failingValue =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            throw new IllegalStateException("deserialize failure");
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .closeTimeoutMillis(100L)
            .keySerializer(STRING)
            .valueSerializer(failingValue)
            .buildTyped();
    try {
      cache.put("key", "value");
      cache.flushAsync().join();
      try {
        cache.get("key");
        fail("deserialize should fail");
      } catch (IllegalStateException expected) {
        // The ReaderGuard must already have been exited by get().
      }
      cache.close();
      assertEquals(cache.totalAllocatedBytes(), 0L);
    } finally {
      cache.close();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
