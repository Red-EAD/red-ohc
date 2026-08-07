package com.red.ohc;

import java.nio.ByteBuffer;

/**
 * CacheSerializer implementation for ByteBuffer values.
 *
 * <p>This serializer provides optimized serialization for ByteBuffer values:
 * <ul>
 *   <li>PUT: copies the source buffer's remaining bytes without changing its position</li>
 *   <li>GET: returns a read-only view over the cache reader's reusable materialization buffer</li>
 *   <li>Supports both heap and direct ByteBuffers</li>
 * </ul>
 *
 * <p><strong>Usage Guidelines:</strong>
 * <ul>
 *   <li>Generic {@link OHCache#get(Object)} materializes values; it is not a direct-value API</li>
 *   <li>Use {@link OHCache#withDirectValue(Object, DirectValueConsumer)} for zero-copy inspection</li>
 * </ul>
 */
public class ByteBufferSerializer implements CacheSerializer<ByteBuffer>
{
    /**
     * Singleton instance for convenience.
     */
    public static final ByteBufferSerializer INSTANCE = new ByteBufferSerializer();

    /**
     * Default constructor.
     */
    public ByteBufferSerializer()
    {
        // No state to initialize
    }

    @Override
    public void serialize(ByteBuffer value, ByteBuffer buf)
    {
        if (value == null)
            throw new IllegalArgumentException("ByteBuffer value cannot be null");
        if (buf == null)
            throw new IllegalArgumentException("Target buffer cannot be null");

        // Create duplicate to avoid mutating original ByteBuffer's position/limit
        ByteBuffer src = value.duplicate();

        // Ensure we have enough space in target buffer
        int remaining = src.remaining();
        if (buf.remaining() < remaining)
            throw new IllegalArgumentException("Target buffer has insufficient space: required=" +
                                             remaining + ", available=" + buf.remaining());

        // Direct copy of the bytes
        buf.put(src);
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer buf)
    {
        if (buf == null)
            throw new IllegalArgumentException("Source buffer cannot be null");

        // Return a read-only view for zero-copy access
        return buf.asReadOnlyBuffer();
    }

    @Override
    public int serializedSize(ByteBuffer value)
    {
        if (value == null)
            throw new IllegalArgumentException("ByteBuffer value cannot be null");

        return value.remaining();
    }
}
