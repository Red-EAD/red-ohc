package com.red.ohc.cache;

import java.util.Objects;
import java.util.concurrent.Executor;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.EvictionListener;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;

/** Builder for the off-heap cache. */
public final class OHCacheBuilder<K, V> {
  private long capacity = 64L << 20;
  private long maxEntrySize;
  private long expectedEntries;
  private CacheSerializer<K> keySerializer;
  private CacheSerializer<V> valueSerializer;
  private long defaultTtlMillis;
  private double ttlJitterPercent = 0.01d;
  private Ticker ticker = Ticker.DEFAULT;
  private Eviction eviction = Eviction.S3_FIFO;
  private EvictionListener<K, V> evictionListener;
  private boolean weakValues;
  private AllocatorType allocatorType = AllocatorType.JNA;
  private Executor loaderExecutor;
  private long closeTimeoutMillis = 30_000L;

  private OHCacheBuilder() {}

  public static <K, V> OHCacheBuilder<K, V> newBuilder() {
    return new OHCacheBuilder<>();
  }

  public OHCacheBuilder<K, V> capacity(long capacity) {
    if (capacity <= 0L) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.capacity = capacity;
    return this;
  }

  public OHCacheBuilder<K, V> maxEntrySize(long maxEntrySize) {
    if (maxEntrySize <= 0L) {
      throw new IllegalArgumentException("maxEntrySize must be positive");
    }
    this.maxEntrySize = maxEntrySize;
    return this;
  }

  public OHCacheBuilder<K, V> expectedEntries(long expectedEntries) {
    if (expectedEntries <= 0L) {
      throw new IllegalArgumentException("expectedEntries must be positive");
    }
    this.expectedEntries = expectedEntries;
    return this;
  }

  public OHCacheBuilder<K, V> keySerializer(CacheSerializer<K> serializer) {
    this.keySerializer = Objects.requireNonNull(serializer, "keySerializer");
    return this;
  }

  public OHCacheBuilder<K, V> valueSerializer(CacheSerializer<V> serializer) {
    this.valueSerializer = Objects.requireNonNull(serializer, "valueSerializer");
    return this;
  }

  public OHCacheBuilder<K, V> defaultTTLmillis(long millis) {
    if (millis < 0L) {
      throw new IllegalArgumentException("defaultTTLmillis must not be negative");
    }
    this.defaultTtlMillis = millis;
    return this;
  }

  public OHCacheBuilder<K, V> ttlJitterPercent(double percent) {
    if (percent < 0d || percent > .5d) {
      throw new IllegalArgumentException("ttlJitterPercent must be in [0, 0.5]");
    }
    this.ttlJitterPercent = percent;
    return this;
  }

  public OHCacheBuilder<K, V> ticker(Ticker ticker) {
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    return this;
  }

  public OHCacheBuilder<K, V> eviction(Eviction eviction) {
    this.eviction = Objects.requireNonNull(eviction, "eviction");
    return this;
  }

  public OHCacheBuilder<K, V> evictionListener(EvictionListener<K, V> listener) {
    this.evictionListener = Objects.requireNonNull(listener, "evictionListener");
    return this;
  }

  /**
   * Enables weak reuse of deserialized Java values.
   *
   * <p>The option is disabled by default. A value serializer whose generic type resolves to
   * {@link java.nio.ByteBuffer} is rejected during {@link #build()}. Java generic erasure can make
   * that type undecidable; such serializers are checked again when values are written or
   * deserialized. ByteBuffer values are never accepted while this option is enabled. A weak hit
   * may return the same Java object supplied to a cache write, so callers should prefer immutable
   * values. Mutating a value in place does not update the native authoritative copy; write a new
   * value again to synchronize it.
   */
  public OHCacheBuilder<K, V> weakValues(boolean enabled) {
    this.weakValues = enabled;
    return this;
  }

  public OHCacheBuilder<K, V> allocator(AllocatorType allocatorType) {
    this.allocatorType = Objects.requireNonNull(allocatorType, "allocatorType");
    return this;
  }

  public OHCacheBuilder<K, V> loaderExecutor(Executor loaderExecutor) {
    this.loaderExecutor = Objects.requireNonNull(loaderExecutor, "loaderExecutor");
    return this;
  }

  public OHCacheBuilder<K, V> closeTimeoutMillis(long timeoutMillis) {
    if (timeoutMillis <= 0L) {
      throw new IllegalArgumentException("closeTimeoutMillis must be positive");
    }
    this.closeTimeoutMillis = timeoutMillis;
    return this;
  }

  public OHCache<K, V> build() {
    return buildTyped();
  }

  OffHeapCache<K, V> buildTyped() {
    if (keySerializer == null || valueSerializer == null) {
      throw new IllegalStateException("both keySerializer and valueSerializer are required");
    }
    if (weakValues && SerializerTypeResolver.resolvesToByteBuffer(valueSerializer)) {
      throw new IllegalArgumentException(
          "weakValues(true) does not support ByteBuffer values; use weakValues(false) or a non-ByteBuffer value type");
    }
    long effectiveMaxEntrySize = maxEntrySize == 0L ? capacity : maxEntrySize;
    return new OffHeapCache<>(
        keySerializer,
        valueSerializer,
        defaultTtlMillis,
        ttlJitterPercent,
        loaderExecutor,
        closeTimeoutMillis,
        allocatorType,
        ticker,
        eviction,
        evictionListener,
        weakValues,
        capacity,
        effectiveMaxEntrySize,
        expectedEntries);
  }

  long capacityValue() {
    return capacity;
  }

  long maxEntrySizeValue() {
    return maxEntrySize;
  }

  long expectedEntriesValue() {
    return expectedEntries;
  }

  CacheSerializer<K> keySerializerValue() {
    return keySerializer;
  }

  CacheSerializer<V> valueSerializerValue() {
    return valueSerializer;
  }

  long defaultTtlMillisValue() {
    return defaultTtlMillis;
  }

  double ttlJitterPercentValue() {
    return ttlJitterPercent;
  }

  Ticker tickerValue() {
    return ticker;
  }

  Eviction evictionValue() {
    return eviction;
  }

  AllocatorType allocatorTypeValue() {
    return allocatorType;
  }

  Executor loaderExecutorValue() {
    return loaderExecutor;
  }

  long closeTimeoutMillisValue() {
    return closeTimeoutMillis;
  }
}
