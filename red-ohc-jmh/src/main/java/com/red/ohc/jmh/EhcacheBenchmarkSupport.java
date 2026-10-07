package com.red.ohc.jmh;

import java.nio.ByteBuffer;
import java.time.Duration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.Serializer;

/** Backend-specific support keeps unrelated libraries out of isolated runtime graphs. */
public final class EhcacheBenchmarkSupport {
  private static final int EHCACHE_ENTRY_OVERHEAD_BYTES = 256;
  private EhcacheBenchmarkSupport() {}

  public static EhcacheStore newEhcache(long offHeapBytes, long ttlMillis) {
    CacheConfigurationBuilder<byte[], byte[]> configuration =
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
                byte[].class, byte[].class,
                ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(offHeapBytes, MemoryUnit.B))
            .withKeySerializer(new RawByteArraySerializer())
            .withValueSerializer(new RawByteArraySerializer());
    if (ttlMillis > 0) {
      configuration = configuration.withExpiry(
          ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(ttlMillis)));
    }
    CacheManager manager =
        CacheManagerBuilder.newCacheManagerBuilder()
            .withCache("serialized", configuration).build(false);
    try {
      manager.init();
      return new EhcacheStore(manager, manager.getCache("serialized", byte[].class, byte[].class));
    } catch (RuntimeException | Error failure) {
      try {
        manager.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  public static long ehcacheCapacityBytes(int keyBytes, int valueBytes) {
    return (long) SerializedBenchmarkSupport.CAPACITY_ENTRIES * (keyBytes + valueBytes + EHCACHE_ENTRY_OVERHEAD_BYTES);
  }

  public static final class EhcacheStore implements AutoCloseable {
    private final CacheManager manager;
    private final Cache<byte[], byte[]> cache;

    private EhcacheStore(CacheManager manager, Cache<byte[], byte[]> cache) {
      this.manager = manager;
      this.cache = cache;
    }

    public Cache<byte[], byte[]> cache() {
      return cache;
    }

    @Override
    public void close() {
      manager.close();
    }
  }

  public static final class RawByteArraySerializer implements Serializer<byte[]> {
    @Override
    public ByteBuffer serialize(byte[] value) {
      return ByteBuffer.wrap(value);
    }

    @Override
    public byte[] read(ByteBuffer binary) {
      ByteBuffer source = binary.duplicate();
      byte[] value = new byte[source.remaining()];
      source.get(value);
      return value;
    }

    @Override
    public boolean equals(byte[] value, ByteBuffer binary) {
      if (value.length != binary.remaining()) {
        return false;
      }
      int offset = binary.position();
      for (int i = 0; i < value.length; i++) {
        if (value[i] != binary.get(offset + i)) {
          return false;
        }
      }
      return true;
    }
  }
}
