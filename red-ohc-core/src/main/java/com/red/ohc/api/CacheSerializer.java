package com.red.ohc.api;

import java.nio.ByteBuffer;

/** Serialize and deserialize cached data using {@link java.nio.ByteBuffer}. */
public interface CacheSerializer<T> {
  /**
   * Serialize the specified type into the specified {@code ByteBuffer} instance.
   *
   * @param value non-{@code null} object that needs to be serialized
   * @param buf bounded {@code ByteBuffer} into which serialization needs to happen. Value
   *     serialization receives a direct, non-array buffer; key serialization may use the cache's
   *     reusable heap lookup buffer. In both cases the position starts at {@code 0}, and byte
   *     order is {@link java.nio.ByteOrder#BIG_ENDIAN}. Limit and capacity equal
   *     {@link #serializedSize}. Implementations must use relative writes so that the final
   *     position is the number of bytes written, must not call {@link ByteBuffer#array()}, and
   *     must not retain the buffer after returning.
   */
  void serialize(T value, ByteBuffer buf);

  /**
   * Deserialize from the specified {@code DataInput} instance.
   *
   * <p>Implementations of this method should never return {@code null}. Although there
   * <em>might</em> be no explicit runtime checks, a violation would break the contract of several
   * API methods in {@link OHCache}. For example users of {@link OHCache#get(Object)} might not be
   * able to distinguish between a non-existing entry or the "value" {@code null}. Instead, consider
   * returning a singleton replacement object.
   *
   * @param buf direct, read-only, non-array {@code ByteBuffer} from which deserialization needs to
   *     happen. The buffer is invalidated immediately after this method returns.
   * @return the type that was deserialized. Must not return {@code null}, and must not retain the
   *     input buffer, a {@code slice()}, {@code duplicate()}, {@code asReadOnlyBuffer()} view, or
   *     any native address. Generic cache APIs return owned objects; use the direct cache APIs for
   *     an explicitly scoped zero-copy view.
   */
  T deserialize(ByteBuffer buf);

  /**
   * Calculate the number of bytes that will be produced by {@link #serialize(Object,
   * java.nio.ByteBuffer)} for given object {@code t}.
   *
   * @param value non-{@code null} object to calculate serialized size for
   * @return serialized size of {@code t}
   * @implNote The result must be deterministic for the value and agree with the number of bytes
   *     written by {@link #serialize(Object, ByteBuffer)}. Native allocation, lifecycle, or
   *     conditional-operation failures can still prevent publication; callers must not rely on
   *     serializer side effects for an unpublished write.
   */
  int serializedSize(T value);

}
