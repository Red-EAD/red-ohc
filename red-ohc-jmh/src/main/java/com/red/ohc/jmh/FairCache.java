package com.red.ohc.jmh;

/** Ordinary serialized APIs; adapters do not patch engine internals. */
public interface FairCache extends AutoCloseable {
  byte[] get(byte[] key);

  /** Returns false only when the backend reports rejection through its native API. */
  boolean put(byte[] key, byte[] value);

  void open(long capacityBytes, long capacityEntries, int keyBytes, int valueBytes, long ttlMillis);

  @Override
  void close();

  static FairCache create(String backend, long bytes, long entries, int keyBytes,
      int valueBytes, long ttlMillis) {
    String implementation;
    switch (backend) {
      case "RED_OHC": implementation = "RedOhcAdapter"; break;
      case "SNAZY_OHC": implementation = "SnazyOhcAdapter"; break;
      case "EHCACHE": implementation = "EhcacheAdapter"; break;
      case "MAPDB": implementation = "MapDbAdapter"; break;
      case "CHRONICLE": implementation = "ChronicleAdapter"; break;
      case "REDIS": implementation = "RedisAdapter"; break;
      default: throw new IllegalArgumentException("unsupported backend: " + backend);
    }
    FairCache cache;
    try {
      cache = (FairCache) Class.forName("com.red.ohc.jmh." + implementation)
          .getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("backend is missing from its isolated classpath", failure);
    }
    try {
      cache.open(bytes, entries, keyBytes, valueBytes, ttlMillis);
      return cache;
    } catch (RuntimeException | Error failure) {
      close(cache, failure);
      throw failure;
    }
  }

  static void close(FairCache cache, Throwable primary) {
    if (cache != null) {
      try {
        cache.close();
      } catch (RuntimeException | Error failure) {
        if (primary == null) {
          throw failure;
        }
        primary.addSuppressed(failure);
      }
    }
  }
}
