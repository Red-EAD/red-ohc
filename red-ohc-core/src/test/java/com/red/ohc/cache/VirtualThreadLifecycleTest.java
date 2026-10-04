package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.SkipException;
import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.runtime.WriterResourceRegistry;

public final class VirtualThreadLifecycleTest {
  private static final int READERS = 128;
  private static final int WRITERS = 128;
  private static final int TOTAL = READERS + WRITERS;

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

  @Test(timeOut = 30_000L)
  public void shortLivedVirtualThreadsDoNotAccumulateReaderOrWriterRegistrations()
      throws Exception {
    Method startVirtualThread = virtualThreadStarter();
    {
      OffHeapCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(64L << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .buildTyped();
      Thread[] callers = new Thread[TOTAL];
      Throwable cacheFailure1 = null;
      try {
        cache.put("seed", "value");
        cache.flushAsync().join();
        int baselineReaders = readerCount(cache);
        int baselineResources = writerResourceActiveCount(cache);
        WorkloadResult workload = runWorkload(cache, startVirtualThread, callers);
        assertEquals(workload.readHits, READERS);
        assertEquals(workload.acceptedWrites, WRITERS);

        awaitCollectionAndCleanup(cache, baselineReaders, baselineResources);

        int remainingReaders = readerCount(cache);
        int remainingResources = writerResourceActiveCount(cache);
        assertTrue(
            remainingReaders <= baselineReaders + 4,
            "reader registrations=" + remainingReaders + ", baseline=" + baselineReaders);
        assertTrue(
            remainingResources <= baselineResources + 4,
            "active writer resources=" + remainingResources + ", baseline=" + baselineResources);
        WriterResourceRegistry resources = writerResources(cache);
        assertEquals(resources.retiringCount(), 0);
        assertTrue(
            resources.resourceCount() <= baselineResources + WRITERS,
            "resource descriptors must grow only to the writer concurrency high-water mark");
        assertTrue(resources.pooledCount() >= resources.resourceCount() - remainingResources - 1);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stopAfterCallers(cache, cacheFailure1, callers);
      }
    }
  }

  private static Method virtualThreadStarter() {
    try {
      return Thread.class.getMethod("startVirtualThread", Runnable.class);
    } catch (NoSuchMethodException unavailable) {
      throw new SkipException("virtual threads require JDK 21");
    }
  }

  private static Thread startVirtualThread(Method starter, Runnable task) throws Exception {
    return (Thread) starter.invoke(null, task);
  }

  private static WorkloadResult runWorkload(
      OffHeapCache<String, String> cache, Method starter, Thread[] threads) throws Exception {
    AtomicInteger readHits = new AtomicInteger();
    AtomicInteger acceptedWrites = new AtomicInteger();
    for (int index = 0; index < READERS; index++) {
      threads[index] =
          startVirtualThread(
              starter,
              () -> {
                if ("value".equals(cache.get("seed"))) {
                  readHits.incrementAndGet();
                }
              });
    }
    for (int index = 0; index < WRITERS; index++) {
      final int key = index;
      threads[READERS + index] =
          startVirtualThread(
              starter,
              () -> {
                cache.put("writer-" + key, "value");
                acceptedWrites.incrementAndGet();
              });
    }
    for (Thread thread : threads) {
      CacheTestSupport.awaitCaller(thread);
    }
    return new WorkloadResult(readHits.get(), acceptedWrites.get());
  }

  private static void awaitCollectionAndCleanup(
      OffHeapCache<?, ?> cache, int baselineReaders, int baselineResources) throws Exception {
    for (int attempt = 0; attempt < 100; attempt++) {
      cache.flushAsync().join();
      if (readerCount(cache) <= baselineReaders + 4
          && writerResourceActiveCount(cache) <= baselineResources + 4
          && writerResources(cache).retiringCount() == 0) {
        return;
      }
      Thread.sleep(10L);
    }
  }

  private static int writerResourceActiveCount(OffHeapCache<?, ?> cache) throws Exception {
    return writerResources(cache).activeCount();
  }

  private static WriterResourceRegistry writerResources(OffHeapCache<?, ?> cache) throws Exception {
    Field resourceField = OffHeapCache.class.getDeclaredField("writerResources");
    resourceField.setAccessible(true);
    return (WriterResourceRegistry) resourceField.get(cache);
  }

  private static int readerCount(OffHeapCache<?, ?> cache) throws Exception {
    Field readersField = OffHeapCache.class.getDeclaredField("readers");
    readersField.setAccessible(true);
    Object readers = readersField.get(cache);
    Method count = readers.getClass().getDeclaredMethod("registeredCount");
    count.setAccessible(true);
    return (Integer) count.invoke(readers);
  }

  private static final class WorkloadResult {
    final int readHits;
    final int acceptedWrites;

    private WorkloadResult(int readHits, int acceptedWrites) {
      this.readHits = readHits;
      this.acceptedWrites = acceptedWrites;
    }
  }
}
