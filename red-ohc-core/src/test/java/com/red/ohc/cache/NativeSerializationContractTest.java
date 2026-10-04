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
    {
      OHCache<String, Integer> cache = newIntCache();
      Throwable cacheFailure6 = null;
      try {
        cache.put("one", 0x01020304);
        cache.flushAsync().join();

        assertEquals(cache.get("one").intValue(), 0x01020304);
        Map<String, Integer> values = cache.getAll(Collections.singleton("one"));
        assertEquals(values.get("one").intValue(), 0x01020304);
        assertTrue(cache.getDirect("one", view -> assertEquals(view.getByte(0), (byte) 1)));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure6 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure6);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(nested)
              .build();
      Throwable cacheFailure5 = null;
      try {
        owner.set(cache);
        cache.put("outer", "outer-value");
        cache.put("inner", "inner-value");
        assertEquals(cache.get("outer"), "outer-value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure5 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure5);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(tracking)
              .build();
      Throwable cacheFailure4 = null;
      try {
        cache.put("key", "old");
        assertTrue(cache.putIfAbsent("key", "new", 0L) != null);
        assertEquals(serializedSizes.get(), 1);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure4 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure4);
      }
    }
  }

  @Test
  public void unconditionalPutAndRemoveDoNotDeserializePreviousValue() {
    AtomicInteger deserializations = new AtomicInteger();
    CacheSerializer<String> tracking =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializations.incrementAndGet();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
          }

          @Override
          public int serializedSize(String value) {
            return value.length();
          }
        };
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(tracking)
              .build();
      Throwable cacheFailure3 = null;
      try {
        cache.put("key", "old");
        cache.put("key", "new");
        cache.remove("key");
        assertEquals(deserializations.get(), 0);

        cache.put("key", "conditional");
        assertEquals(cache.putIfAbsent("key", "ignored"), "conditional");
        assertEquals(deserializations.get(), 1);
        assertTrue(cache.remove("key", "conditional"));
        assertEquals(deserializations.get(), 2);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(flaky)
              .build();
      Throwable cacheFailure2 = null;
      try {
        expectThrows(IllegalStateException.class, () -> cache.put("failed", "value"));
        cache.put("after", "value");
        assertEquals(cache.get("after"), "value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
    }
  }

  @Test
  public void replaceUsesNativeExpectedScratchAndReleasesItOnMismatch() {
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 20)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .buildTyped();
      Throwable cacheFailure1 = null;
      try {
        cache.put("key", "old");
        cache.flushAsync().join();
        assertFalse(cache.replace("key", "wrong", "new", 0L));
        assertTrue(cache.replace("key", "old", "new", 0L));
        assertEquals(cache.get("key"), "new");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
    }
  }

  private static void assertRejectedWriter(CacheSerializer<byte[]> serializer) {
    OHCache<String, byte[]> cache =
        OHCacheBuilder.<String, byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .build();
    {
      Throwable explicitCacheFailure1 = null;
      try {

        expectThrows(RuntimeException.class, () -> cache.put("key", new byte[] {1, 2, 3, 4}));

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure1 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        CacheTestSupport.stop(cache, explicitCacheFailure1);
        assertEquals(cache.totalAllocatedBytes(), 0L);
      }
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
