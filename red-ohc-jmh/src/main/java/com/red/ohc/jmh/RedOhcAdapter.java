package com.red.ohc.jmh;

import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;

public final class RedOhcAdapter implements FairCache {
  private OHCache<byte[], byte[]> cache;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttlMillis) {
    cache = OHCacheBuilder.<byte[], byte[]>newBuilder().capacity(bytes)
        .defaultTTLmillis(ttlMillis).keySerializer(Utils.byteArraySerializer)
        .valueSerializer(Utils.byteArraySerializer).build();
  }

  @Override
  public byte[] get(byte[] key) {
    return cache.get(key);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    cache.put(key, value); return true;
  }

  @Override
  public void close() {
    OHCache<byte[], byte[]> owned = cache;
    cache = null;
    RedOhcBenchmarkSupport.stopOHC(owned);
  }
}
