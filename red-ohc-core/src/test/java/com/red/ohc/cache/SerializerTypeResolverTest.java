package com.red.ohc.cache;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;

public final class SerializerTypeResolverTest {
  @Test
  public void resolvesDirectByteBufferSerializer() {
    assertTrue(SerializerTypeResolver.resolvesToByteBuffer(new DirectBufferSerializer()));
  }

  @Test
  public void resolvesByteBufferThroughAConcreteGenericBaseClass() {
    assertTrue(SerializerTypeResolver.resolvesToByteBuffer(new BufferBaseSerializer()));
  }

  @Test
  public void resolvesByteBufferThroughAParentInterface() {
    assertTrue(SerializerTypeResolver.resolvesToByteBuffer(new InterfaceBufferSerializer()));
  }

  @Test
  public void resolvesOrdinaryPojoThroughAConcreteGenericBaseClassAsNonByteBuffer() {
    assertFalse(SerializerTypeResolver.resolvesToByteBuffer(new PojoSerializer()));
  }

  @Test
  public void rawSerializerIsAllowedAtBuildTime() {
    assertFalse(SerializerTypeResolver.resolvesToByteBuffer(new RawSerializer()));
  }

  @Test
  public void genericVariableCannotBeProvenToBeByteBuffer() {
    assertFalse(SerializerTypeResolver.resolvesToByteBuffer(new GenericSerializer<String>()));
  }

  @Test
  public void builderRejectsDirectAndInheritedByteBufferSerializers() {
    assertRejected(new DirectBufferSerializer());
    assertRejected(new BufferBaseSerializer());
  }

  private static void assertRejected(CacheSerializer<ByteBuffer> serializer) {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            OHCacheBuilder.<String, ByteBuffer>newBuilder()
                .keySerializer(new StringSerializer())
                .valueSerializer(serializer)
                .weakValues(true)
                .build());
  }

  private static class BaseSerializer<T> implements CacheSerializer<T> {
    @Override
    public void serialize(T value, ByteBuffer buffer) {}

    @Override
    public T deserialize(ByteBuffer buffer) {
      return null;
    }

    @Override
    public int serializedSize(T value) {
      return 0;
    }
  }

  private static final class BufferBaseSerializer extends BaseSerializer<ByteBuffer> {}

  private interface SerializerBase<T> extends CacheSerializer<T> {}

  private static final class InterfaceBufferSerializer implements SerializerBase<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer buffer) {}

    @Override
    public ByteBuffer deserialize(ByteBuffer buffer) {
      return buffer;
    }

    @Override
    public int serializedSize(ByteBuffer value) {
      return 0;
    }
  }

  private static final class PojoSerializer extends BaseSerializer<String> {}

  private static final class DirectBufferSerializer implements CacheSerializer<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer buffer) {}

    @Override
    public ByteBuffer deserialize(ByteBuffer buffer) {
      return buffer;
    }

    @Override
    public int serializedSize(ByteBuffer value) {
      return 0;
    }
  }

  private static final class GenericSerializer<T> implements CacheSerializer<T> {
    @Override
    public void serialize(T value, ByteBuffer buffer) {}

    @Override
    public T deserialize(ByteBuffer buffer) {
      return null;
    }

    @Override
    public int serializedSize(T value) {
      return 0;
    }
  }

  @SuppressWarnings("rawtypes")
  private static final class RawSerializer implements CacheSerializer {
    @Override
    public void serialize(Object value, ByteBuffer buffer) {}

    @Override
    public Object deserialize(ByteBuffer buffer) {
      return null;
    }

    @Override
    public int serializedSize(Object value) {
      return 0;
    }
  }

  private static final class StringSerializer implements CacheSerializer<String> {
    @Override
    public void serialize(String value, ByteBuffer buffer) {}

    @Override
    public String deserialize(ByteBuffer buffer) {
      return "";
    }

    @Override
    public int serializedSize(String value) {
      return 0;
    }
  }
}
