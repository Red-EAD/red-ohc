package com.red.ohc.codec;

import java.nio.ByteBuffer;

import com.red.ohc.CacheSerializer;
import com.red.ohc.runtime.ThreadContext;

/** Serializes a user key into the calling thread's reusable lookup buffer. */
public final class KeyEncoder {
    @SuppressWarnings("rawtypes")
    public static int encode(CacheSerializer serializer, Object key, ThreadContext context) {
        int length = serializer.serializedSize(key);
        if (length < 0) throw new IllegalArgumentException("negative serialized key length");
        context.ensureKey(length);
        ByteBuffer buffer = context.keyBuffer(length);
        serializer.serialize(key, buffer);
        context.lookupKey.set(context.keyBytes, length);
        return length;
    }
}
