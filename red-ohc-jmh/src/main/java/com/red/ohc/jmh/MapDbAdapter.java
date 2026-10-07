package com.red.ohc.jmh;

public final class MapDbAdapter implements FairCache {
  private MapDbBenchmarkSupport.MapDbStore store;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttlMillis) {
    store = MapDbBenchmarkSupport.newMapDb(entries, ttlMillis);
  }

  @Override
  public byte[] get(byte[] key) {
    return store.map().get(key);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    store.map().put(key, value); return true;
  }

  @Override
  public void close() {
    MapDbBenchmarkSupport.MapDbStore owned = store;
    store = null;
    if (owned != null) {
      owned.close();
    }
  }
}
