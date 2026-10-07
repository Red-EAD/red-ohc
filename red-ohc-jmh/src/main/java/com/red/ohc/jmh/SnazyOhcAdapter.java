package com.red.ohc.jmh;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public final class SnazyOhcAdapter implements FairCache {
  private OHCache<byte[], byte[]> cache;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttlMillis) {
    cache = OHCacheBuilder.<byte[], byte[]>newBuilder().capacity(bytes).chunkSize(0)
        .timeouts(ttlMillis > 0).defaultTTLmillis(ttlMillis)
        .keySerializer(new Bytes()).valueSerializer(new Bytes()).build();
  }

  @Override
  public byte[] get(byte[] key) {
    return cache.get(key);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    return cache.put(key, value);
  }

  @Override
  public void close() {
    OHCache<byte[], byte[]> owned = cache;
    cache = null;
    if (owned != null) {
      try {
        owned.close();
      } catch (IOException failure) {
        throw new IllegalStateException("OHC teardown failed", failure);
      }
    }
  }

  private static final class Bytes implements CacheSerializer<byte[]> {
    @Override
    public int serializedSize(byte[] value) {
      return value.length;
    }

    @Override
    public void serialize(byte[] value, ByteBuffer target) {
      target.put(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer source) {
      byte[] result = new byte[source.remaining()];
      source.get(result);
      return result;
    }
  }
}
