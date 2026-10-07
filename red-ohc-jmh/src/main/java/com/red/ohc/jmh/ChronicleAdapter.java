package com.red.ohc.jmh;

public final class ChronicleAdapter implements FairCache {
  private ChronicleBenchmarkSupport.ChronicleMapStore store;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttlMillis) {
    if (ttlMillis != 0) {
      throw new IllegalArgumentException("Chronicle Map has no native TTL");
    }
    store = ChronicleBenchmarkSupport.newChronicleMap(keyBytes, valueBytes, entries);
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
    ChronicleBenchmarkSupport.ChronicleMapStore owned = store;
    store = null;
    if (owned != null) {
      owned.close();
    }
  }
}
