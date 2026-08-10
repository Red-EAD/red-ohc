package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;

public final class NativeSerializationContractTest {
  private static final CacheSerializer<Integer> INT =
      new CacheSerializer<Integer>() {
        @Override
        public void serialize(Integer value, ByteBuffer buffer) {
          buffer.putInt(value);
        }

        @Override
        public Integer deserialize(ByteBuffer buffer) {
          assertTrue(buffer.isDirect());
          assertTrue(buffer.isReadOnly());
          assertFalse(buffer.hasArray());
          assertEquals(buffer.order(), ByteOrder.BIG_ENDIAN);
          return buffer.getInt();
        }

        @Override
        public int serializedSize(Integer value) {
          return Integer.BYTES;
        }
      };

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
  public void genericReadsDeserializeNativePayloadAndReturnOwnedObjects() {
    try (OHCache<String, Integer> cache = newIntCache()) {
      assertTrue(cache.put("one", 0x01020304));
      cache.flushAsync().join();

      assertEquals(cache.get("one").intValue(), 0x01020304);
      Map<String, Integer> values = cache.getAll(Collections.singleton("one"));
      assertEquals(values.get("one").intValue(), 0x01020304);
      assertTrue(cache.getDirect("one", view -> assertEquals(view.getByte(0), (byte) 1)));
    }
  }

  @Test
  public void nestedGenericGetKeepsTheOuterReadOnlyViewUsable() {
    AtomicReference<OHCache<String, String>> owner = new AtomicReference<>();
    AtomicInteger nestedReads = new AtomicInteger();
    CacheSerializer<String> nested =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            if (nestedReads.getAndIncrement() == 0) {
              assertEquals(owner.get().get("inner"), "inner-value");
            }
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(nested)
            .build()) {
      owner.set(cache);
      assertTrue(cache.put("outer", "outer-value"));
      assertTrue(cache.put("inner", "inner-value"));
      assertEquals(cache.get("outer"), "outer-value");
    }
  }

  @Test
  public void putIfAbsentDoesNotSerializeWhenAValueIsAlreadyLive() {
    AtomicInteger serializedSizes = new AtomicInteger();
    CacheSerializer<String> tracking =
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
            serializedSizes.incrementAndGet();
            return value.length();
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(tracking)
            .build()) {
      assertTrue(cache.put("key", "old"));
      assertEquals(cache.putIfAbsentAsync("key", "new", 0L).join(), Boolean.FALSE);
      assertEquals(serializedSizes.get(), 1);
    }
  }

  @Test
  public void shortOverwritingAndArrayAssumingSerializersReleaseNativeBlocks() {
    CacheSerializer<byte[]> shortWriter = failingWriter(1, false);
    CacheSerializer<byte[]> overWriter = failingWriter(-1, false);
    CacheSerializer<byte[]> arrayWriter = failingWriter(0, true);
    assertRejectedWriter(shortWriter);
    assertRejectedWriter(overWriter);
    assertRejectedWriter(arrayWriter);
  }

  @Test
  public void serializerFailureLeavesTheWriterReusable() {
    AtomicInteger calls = new AtomicInteger();
    CacheSerializer<String> flaky =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if (calls.getAndIncrement() == 0) {
              throw new IllegalStateException("synthetic serializer failure");
            }
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
            return value.length();
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(flaky)
            .build()) {
      expectThrows(IllegalStateException.class, () -> cache.put("failed", "value"));
      assertTrue(cache.put("after", "value"));
      assertEquals(cache.get("after"), "value");
    }
  }

  @Test
  public void replaceUsesNativeExpectedScratchAndReleasesItOnMismatch() {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped()) {
      assertTrue(cache.put("key", "old"));
      cache.flushAsync().join();
      assertEquals(cache.replaceAsync("key", "wrong", "new", 0L).join(), Boolean.FALSE);
      assertEquals(cache.replaceAsync("key", "old", "new", 0L).join(), Boolean.TRUE);
      assertEquals(cache.get("key"), "new");
    }
  }

  private static void assertRejectedWriter(CacheSerializer<byte[]> serializer) {
    OHCache<String, byte[]> cache =
        OHCacheBuilder.<String, byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .build();
    try {
      expectThrows(RuntimeException.class, () -> cache.put("key", new byte[] {1, 2, 3, 4}));
    } finally {
      cache.close();
      assertEquals(cache.totalAllocatedBytes(), 0L);
    }
  }

  private static CacheSerializer<byte[]> failingWriter(int sizeAdjustment, boolean callArray) {
    return new CacheSerializer<byte[]>() {
      @Override
      public void serialize(byte[] value, ByteBuffer buffer) {
        if (callArray) {
          buffer.array();
        } else {
          buffer.put(value);
        }
      }

      @Override
      public byte[] deserialize(ByteBuffer buffer) {
        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);
        return copy;
      }

      @Override
      public int serializedSize(byte[] value) {
        return sizeAdjustment < 0 ? value.length - 1 : value.length + sizeAdjustment;
      }
    };
  }

  private static OHCache<String, Integer> newIntCache() {
    return OHCacheBuilder.<String, Integer>newBuilder()
        .capacity(1 << 20)
        .keySerializer(STRING)
        .valueSerializer(INT)
        .build();
  }
}
