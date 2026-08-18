package com.red.ohc.jmh;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.Serializer;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/** Shared raw-byte serialization helpers for the OHC and Ehcache JMH states. */
public final class SerializedBenchmarkSupport {
  public static final int WORKING_SET = 24_576;
  public static final int DEFAULT_KEY_BYTES = 32;
  public static final int DEFAULT_VALUE_BYTES = 5 * 1024;
  public static final int CAPACITY_ENTRIES = WORKING_SET * 4 / 5;
  public static final int MAPDB_SEGMENTS = 16;
  private static final int EHCACHE_ENTRY_OVERHEAD_BYTES = 256;
  public static final long TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);
  public static final int ACCESS_SEQUENCE_LENGTH = 1 << 16;

  private SerializedBenchmarkSupport() {}

  public static EhcacheStore newEhcache(long offHeapBytes, long ttlMillis) {
    CacheManager manager =
        CacheManagerBuilder.newCacheManagerBuilder()
            .withCache(
                "serialized",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(
                        byte[].class,
                        byte[].class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                            .offheap(offHeapBytes, MemoryUnit.B))
                    .withKeySerializer(new RawByteArraySerializer())
                    .withValueSerializer(new RawByteArraySerializer())
                    .withExpiry(
                        ExpiryPolicyBuilder.timeToLiveExpiration(
                            Duration.ofMillis(ttlMillis))))
            .build(true);
    return new EhcacheStore(manager, manager.getCache("serialized", byte[].class, byte[].class));
  }

  public static MapDbStore newMapDb(long capacityEntries, long ttlMillis) {
    ScheduledExecutorService expiryExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "mapdb-expiry");
              thread.setDaemon(true);
              return thread;
            });
    @SuppressWarnings("unchecked")
    org.mapdb.DB.HashMapMaker<byte[], byte[]> maker =
        (org.mapdb.DB.HashMapMaker<byte[], byte[]>)
            (org.mapdb.DB.HashMapMaker<?, ?>) DBMaker.memoryShardedHashMap(MAPDB_SEGMENTS);
    HTreeMap<byte[], byte[]> map =
        maker
            .keySerializer(org.mapdb.Serializer.BYTE_ARRAY)
            .valueSerializer(org.mapdb.Serializer.BYTE_ARRAY)
            .expireAfterCreate(ttlMillis, TimeUnit.MILLISECONDS)
            .expireAfterUpdate(ttlMillis, TimeUnit.MILLISECONDS)
            .expireMaxSize(capacityEntries)
            .expireExecutor(expiryExecutor)
            .expireExecutorPeriod(TimeUnit.SECONDS.toMillis(1L))
            .create();
    return new MapDbStore(map, expiryExecutor);
  }

  public static ChronicleMapStore newChronicleMap(int keyBytes, int valueBytes, long entries) {
    ChronicleMap<byte[], byte[]> map =
        ChronicleMapBuilder.of(byte[].class, byte[].class)
            .entries(entries)
            .averageKeySize(keyBytes)
            .averageValueSize(valueBytes)
            .create();
    return new ChronicleMapStore(map);
  }

  public static Dataset dataset(int keyBytes, int valueBytes, String distribution) {
    byte[][] keys = new byte[WORKING_SET][];
    byte[][] values = new byte[WORKING_SET][];
    for (int i = 0; i < WORKING_SET; i++) {
      keys[i] = bytes(keyBytes, i);
      values[i] = bytes(valueBytes, i * 31 + 7);
    }
    return new Dataset(
        keys,
        values,
        "ZIPF_099".equals(distribution) ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET));
  }

  public static boolean isWrite(String workload, long operation) {
    if ("READ_100".equals(workload)) {
      return false;
    }
    if ("WRITE_100".equals(workload)) {
      return true;
    }
    if ("READ_90_WRITE_10".equals(workload)) {
      return Math.floorMod(operation, 10L) == 0L;
    }
    throw new IllegalArgumentException("unsupported workload: " + workload);
  }

  public static int threadStartOffset(int threadIndex, int threadCount, int sequenceLength) {
    if (threadIndex < 0 || threadCount <= 0 || sequenceLength <= 0) {
      throw new IllegalArgumentException("invalid thread sequence parameters");
    }
    return (int) ((long) sequenceLength * threadIndex / threadCount);
  }

  /**
   * Ehcache's off-heap tier charges serialized mapping metadata in addition to key and value
   * bytes. The fixed headroom keeps the target 32B/5KiB preload resident at the same entry cap.
   */
  public static long ehcacheCapacityBytes(int keyBytes, int valueBytes) {
    return (long) CAPACITY_ENTRIES
        * (keyBytes + valueBytes + EHCACHE_ENTRY_OVERHEAD_BYTES);
  }

  public static long ohcCapacityBytes(int keyBytes, int valueBytes) {
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyBytes);
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    long entryWeight =
        WriterArena.allocationWeight(keyAllocation) + WriterArena.allocationWeight(valueAllocation);
    return entryWeight * CAPACITY_ENTRIES;
  }

  public static int benchmarkThreadCount() {
    int configured = Integer.getInteger("redohc.benchmark.threads", Runtime.getRuntime().availableProcessors());
    if (configured <= 0) {
      throw new IllegalArgumentException("redohc.benchmark.threads must be positive");
    }
    return configured;
  }

  public static byte[] ownedCopy(byte[] value) {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  /** Benchmark-local content key; it keeps raw-key semantics without using OHC's EncodedKey API. */
  public static final class RawKey {
    private final byte[] bytes;
    private final int hashCode;

    private RawKey(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
    }

    public static RawKey copyOf(byte[] source) {
      if (source == null) {
        throw new NullPointerException("source");
      }
      return new RawKey(Arrays.copyOf(source, source.length));
    }

    @Override
    public boolean equals(Object other) {
      return other == this
          || (other instanceof RawKey && Arrays.equals(bytes, ((RawKey) other).bytes));
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static byte[] bytes(int length, int seed) {
    byte[] bytes = new byte[length];
    long value = seed * 0x9e3779b97f4a7c15L;
    for (int i = 0; i < length; i++) {
      value ^= value >>> 12;
      value ^= value << 25;
      value ^= value >>> 27;
      bytes[i] = (byte) value;
    }
    return bytes;
  }

  private static int[] uniformSequence(int bound) {
    int[] sequence = new int[1 << 16];
    long seed = 1L;
    for (int i = 0; i < sequence.length; i++) {
      seed ^= seed << 13;
      seed ^= seed >>> 7;
      seed ^= seed << 17;
      sequence[i] = (int) Long.remainderUnsigned(seed, bound);
    }
    return sequence;
  }

  private static int[] zipfSequence(int bound) {
    double[] cdf = new double[bound];
    double sum = 0d;
    for (int rank = 1; rank <= bound; rank++) {
      sum += 1d / Math.pow(rank, .99d);
    }
    double running = 0d;
    for (int rank = 1; rank <= bound; rank++) {
      running += 1d / Math.pow(rank, .99d) / sum;
      cdf[rank - 1] = running;
    }
    int[] sequence = new int[1 << 16];
    long seed = 7L;
    for (int i = 0; i < sequence.length; i++) {
      seed ^= seed << 13;
      seed ^= seed >>> 7;
      seed ^= seed << 17;
      double sample = (seed >>> 11) * 0x1.0p-53d;
      int low = 0;
      int high = cdf.length - 1;
      while (low < high) {
        int middle = (low + high) >>> 1;
        if (cdf[middle] < sample) {
          low = middle + 1;
        } else {
          high = middle;
        }
      }
      sequence[i] = low;
    }
    return sequence;
  }

  public static final class Dataset {
    public final byte[][] keys;
    public final byte[][] values;
    public final int[] accessSequence;

    private Dataset(byte[][] keys, byte[][] values, int[] accessSequence) {
      this.keys = keys;
      this.values = values;
      this.accessSequence = accessSequence;
    }
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

  public static final class MapDbStore implements AutoCloseable {
    private final HTreeMap<byte[], byte[]> map;
    private final ScheduledExecutorService expiryExecutor;

    private MapDbStore(HTreeMap<byte[], byte[]> map, ScheduledExecutorService expiryExecutor) {
      this.map = map;
      this.expiryExecutor = expiryExecutor;
    }

    public HTreeMap<byte[], byte[]> map() {
      return map;
    }

    public int segmentCount() {
      return map.getStores().length;
    }

    @Override
    public void close() {
      try {
        map.close();
      } finally {
        expiryExecutor.shutdownNow();
      }
    }
  }

  public static final class ChronicleMapStore implements AutoCloseable {
    private final ChronicleMap<byte[], byte[]> map;

    private ChronicleMapStore(ChronicleMap<byte[], byte[]> map) {
      this.map = map;
    }

    public ChronicleMap<byte[], byte[]> map() {
      return map;
    }

    @Override
    public void close() {
      map.close();
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
