package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;

public final class ByteBufferCacheTest {
  private static final CacheSerializer<String> STRING = new StringSerializer();
  private static final CacheSerializer<ByteBuffer> BYTE_BUFFER = new OwnedByteBufferSerializer();

  @Test
  public void ownedByteBufferValuesRoundTrip() {
    ByteBuffer value = ByteBuffer.wrap(new byte[] {1, 2, 3});
    {
      OHCache<String, ByteBuffer> cache =
          OHCacheBuilder.<String, ByteBuffer>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(BYTE_BUFFER)
              .build();
      Throwable cacheFailure2 = null;
      try {
        cache.put("key", value);

        ByteBuffer loaded = cache.get("key");
        assertEquals(loaded.remaining(), 3);
        assertEquals(loaded.get(), (byte) 1);
        assertEquals(loaded.get(), (byte) 2);
        assertEquals(loaded.get(), (byte) 3);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
    }
  }

  @Test
  public void byteBufferKeysRoundTrip() {
    {
      OHCache<ByteBuffer, String> cache =
          OHCacheBuilder.<ByteBuffer, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(BYTE_BUFFER)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure1 = null;
      try {
        cache.put(ByteBuffer.wrap(new byte[] {4, 5}), "value");

        assertEquals(cache.get(ByteBuffer.wrap(new byte[] {4, 5})), "value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
    }
  }

  private static final class StringSerializer implements CacheSerializer<String> {
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
  }

  private static final class OwnedByteBufferSerializer implements CacheSerializer<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer buffer) {
      buffer.put(value.duplicate());
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer buffer) {
      ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
      copy.put(buffer.duplicate()).flip();
      return copy;
    }

    @Override
    public int serializedSize(ByteBuffer value) {
      return value.remaining();
    }
  }
}
