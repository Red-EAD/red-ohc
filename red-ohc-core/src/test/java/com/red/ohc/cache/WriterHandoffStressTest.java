package com.red.ohc.cache;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;

public final class WriterHandoffStressTest {
  private static final CacheSerializer<Integer> KEY = fixedInteger(64);
  private static final CacheSerializer<Integer> VALUE = fixedInteger(128);

  @DataProvider(name = "allocators")
  public Object[][] allocators() {
    return new Object[][] {{AllocatorType.JNA}, {AllocatorType.UNSAFE}};
  }

  @Test(dataProvider = "allocators", timeOut = 60_000L)
  public void contendedWritersOnOneKeyAlwaysMakeProgress(AllocatorType allocator)
      throws Exception {
    int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
    int writesPerThread = 20_000;
    OHCache<Integer, Integer> cache =
        OHCacheBuilder.<Integer, Integer>newBuilder()
            .capacity(1L << 22)
            .keySerializer(KEY)
            .valueSerializer(VALUE)
            .eviction(Eviction.S3_FIFO)
            .allocator(allocator)
            .buildTyped();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread[] workers = new Thread[threads];
    try {
      for (int index = 0; index < threads; index++) {
        int id = index;
        workers[index] =
            new Thread(
                () -> {
                  try {
                    start.await();
                    for (int i = 0; i < writesPerThread; i++) {
                      cache.put(7, id * writesPerThread + i);
                    }
                  } catch (Throwable error) {
                    failure.compareAndSet(null, error);
                  } finally {
                    done.countDown();
                  }
                },
                "handoff-writer-" + index);
        workers[index].start();
      }
      start.countDown();
      assertTrue(
          done.await(45, TimeUnit.SECONDS),
          "a writer never woke after a release cleared the waiter flag");
      if (failure.get() != null) {
        throw new AssertionError(failure.get());
      }
      assertNotNull(cache.get(7));
    } finally {
      for (Thread worker : workers) {
        if (worker != null) {
          worker.join(5_000L);
        }
      }
      cache.close();
    }
  }

  private static CacheSerializer<Integer> fixedInteger(int bytes) {
    return new CacheSerializer<Integer>() {
      @Override
      public void serialize(Integer value, ByteBuffer buffer) {
        buffer.putInt(value);
        for (int index = Integer.BYTES; index < bytes; index++) {
          buffer.put((byte) 0);
        }
      }

      @Override
      public Integer deserialize(ByteBuffer buffer) {
        return buffer.getInt();
      }

      @Override
      public int serializedSize(Integer value) {
        return bytes;
      }
    };
  }
}
