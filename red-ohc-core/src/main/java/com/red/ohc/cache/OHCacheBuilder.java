package com.red.ohc.cache;

import java.util.Objects;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.EvictionListener;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;

/** Builder for the off-heap cache. */
public final class OHCacheBuilder<K, V> {
  private static final long DEFAULT_NATIVE_DEBT_BUDGET_BYTES = 64L << 20;
  private long capacity;
  private long maxSize;
  private boolean capacityConfigured;
  private boolean maxSizeConfigured;
  private long nativeMemoryBudgetBytes;
  private boolean nativeMemoryBudgetConfigured;
  private CacheSerializer<K> keySerializer;
  private CacheSerializer<V> valueSerializer;
  private long defaultTtlMillis;
  private Ticker ticker = Ticker.DEFAULT;
  private Eviction eviction = Eviction.S3_FIFO;
  private EvictionListener<K, V> evictionListener;
  private long closeTimeoutMillis = 30_000L;

  private OHCacheBuilder() {}

  public static <K, V> OHCacheBuilder<K, V> newBuilder() {
    return new OHCacheBuilder<>();
  }

  /**
   * Selects a logical serialized-entry byte capacity. Each entry charges its serialized key
   * allocation plus its serialized value allocation, including OHC's fixed headers and alignment;
   * allocator pages, size-class rounding, and shared maintenance structures are excluded. Actual
   * native usage is exposed separately by {@link OHCache#totalAllocatedBytes()}. This value is the
   * actor's steady-state eviction target, not a synchronous write admission limit: a writer may
   * temporarily exceed it while the actor claims logical victims. Retirement backlog is observed
   * separately through the native memory-debt budget; it does not throttle writes.
   */
  public OHCacheBuilder<K, V> capacity(long capacity) {
    if (capacity <= 0L) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    if (maxSizeConfigured) {
      throw new IllegalStateException("capacity and maxSize are mutually exclusive");
    }
    this.capacity = capacity;
    this.capacityConfigured = true;
    return this;
  }

  /**
   * Selects an entry-count target. The maintenance actor asynchronously claims logical victims to
   * approach this target; writes are not rejected merely because the current count is above it.
   * This mode is mutually exclusive with {@link #capacity(long)}; use
   * {@link #nativeMemoryBudgetBytes(long)} to observe native retirement backlog independently of
   * the entry-count target.
   */
  public OHCacheBuilder<K, V> maxSize(long maxSize) {
    if (maxSize <= 0L) {
      throw new IllegalArgumentException("maxSize must be positive");
    }
    if (capacityConfigured) {
      throw new IllegalStateException("capacity and maxSize are mutually exclusive");
    }
    this.maxSize = maxSize;
    this.maxSizeConfigured = true;
    return this;
  }

  /**
   * Sets the native retirement-debt budget used for diagnostics. Physical retirement is always
   * asynchronous and this setting never blocks, rejects, or asks a writer to reclaim native
   * memory. Live logical capacity is controlled by {@link #capacity(long)} or {@link #maxSize(long)}.
   */
  public OHCacheBuilder<K, V> nativeMemoryBudgetBytes(long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException("nativeMemoryBudgetBytes must be positive");
    }
    nativeMemoryBudgetBytes = bytes;
    nativeMemoryBudgetConfigured = true;
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
    if (!capacityConfigured && !maxSizeConfigured) {
      throw new IllegalStateException("capacity or maxSize is required");
    }
    if (keySerializer == null || valueSerializer == null) {
      throw new IllegalStateException("both keySerializer and valueSerializer are required");
    }
    long nativeDebtBudget =
        nativeMemoryBudgetConfigured
            ? nativeMemoryBudgetBytes
            : defaultNativeDebtBudget(capacityConfigured ? capacity : -1L);
    return new OffHeapCache<>(
        keySerializer,
        valueSerializer,
        defaultTtlMillis,
        closeTimeoutMillis,
        ticker,
        eviction,
        evictionListener,
        capacity,
        maxSize,
        nativeDebtBudget);
  }

  private static long defaultNativeDebtBudget(long configuredCapacity) {
    return configuredCapacity <= 0L
        ? DEFAULT_NATIVE_DEBT_BUDGET_BYTES
        : Math.max(DEFAULT_NATIVE_DEBT_BUDGET_BYTES, configuredCapacity);
  }

}
