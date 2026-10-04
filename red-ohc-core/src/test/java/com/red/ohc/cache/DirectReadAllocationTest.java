package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.OHCache;

public class DirectReadAllocationTest {
  private static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] copy = new byte[buffer.remaining()];
          buffer.get(copy);
          return copy;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Test
  public void primitiveLongMatchesTheBigEndianByteBufferView() {
    byte[] key = new byte[] {1, 2, 3, 4};
    byte[] value = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    {
      OHCache<byte[], byte[]> cache =
          OHCacheBuilder.<byte[], byte[]>newBuilder()
              .capacity(1 << 20)
              .keySerializer(BYTES)
              .valueSerializer(BYTES)
              .build();
      Throwable cacheFailure2 = null;
      try {
        cache.put(key, value);

        assertTrue(
            cache.getDirect(
                key,
                view -> {
                  assertEquals(view.getLong(0), 0x0102030405060708L);
                  assertEquals(view.getLong(0), view.asReadOnlyByteBuffer().getLong(0));
                }));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
    }
  }

  @Test
  public void steadyDirectHitHasNoCacheSideThreadAllocation() {
    com.sun.management.ThreadMXBean bean =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    if (!bean.isThreadAllocatedMemorySupported()) {
      return;
    }
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    byte[] key = new byte[] {1, 2, 3, 4};
    byte[] value = new byte[16];
    {
      OHCache<byte[], byte[]> cache =
          OHCacheBuilder.<byte[], byte[]>newBuilder()
              .capacity(1 << 20)
              .keySerializer(BYTES)
              .valueSerializer(BYTES)
              .build();
      Throwable cacheFailure1 = null;
      try {
        cache.put(key, value);
        cache.flushAsync().join();
        DirectValueConsumer consumer =
            view -> {
              if (view.getLong(0) != 0L) {
                throw new AssertionError();
              }
            };
        for (int i = 0; i < 65_536; i++) {
          cache.getDirect(key, consumer);
        }

        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < 65_536; i++) {
          if (!cache.getDirect(key, consumer)) {
            throw new AssertionError("direct hit unexpectedly missed");
          }
        }
        long after = bean.getThreadAllocatedBytes(threadId);

        assertEquals(after - before, 0L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
    }
  }
}
