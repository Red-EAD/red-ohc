package com.red.ohc.jmh;

public final class EhcacheAdapter implements FairCache {
  private EhcacheBenchmarkSupport.EhcacheStore store;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttlMillis) {
    store = EhcacheBenchmarkSupport.newEhcache(bytes, ttlMillis);
  }

  @Override
  public byte[] get(byte[] key) {
    return store.cache().get(key);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    store.cache().put(key, value); return true;
  }

  @Override
  public void close() {
    EhcacheBenchmarkSupport.EhcacheStore owned = store;
    store = null;
    if (owned != null) {
      owned.close();
    }
  }
}
