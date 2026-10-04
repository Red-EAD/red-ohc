package com.red.ohc.api;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A weakly-consistent off-heap cache with synchronous data-plane writes and asynchronous target
 * maintenance.
 *
 * <p>The CHM is the authoritative Java index. Values and keys are serialized into native memory,
 * so reads and callbacks return deserialized heap snapshots rather than preserving Java object
 * identity. Key matching uses the key serializer's bytes; value matching for conditional methods
 * uses {@link java.util.Objects#equals(Object, Object)} on deserialized values.
 *
 * <p>TTL expiry is logical first and physical later. An expired mapping is absent from reads,
 * conditional operations, views, and bulk callbacks, but can remain in the CHM-backed physical
 * index until the maintenance actor retires it. {@link #size()}, {@link #isEmpty()}, and view
 * sizes use the logical mapping count; {@link #mappingCount()} exposes the constant-time physical
 * CHM count and may temporarily include expired entries. Logical expiry is observed by reads or
 * maintenance, so these weakly-consistent counts can lag an unobserved clock transition. Closing
 * makes reads and views empty; writes, including writes through views, reject the operation with
 * {@link IllegalStateException}.
 *
 * <p>{@link #capacity()} is the steady-state byte target and {@code maxSize} is the corresponding
 * entry-count target. Neither target synchronously limits a write: logical occupancy and native
 * usage can temporarily grow while the actor evicts asynchronously. If maintenance remains
 * behind the write rate, native allocation can eventually fail with an {@link OutOfMemoryError};
 * that failure makes the cache terminal.
 */
public interface OHCache<K, V> extends AutoCloseable {
  /**
   * Publishes the serialized entry synchronously. {@link #capacity()} is an asynchronous eviction
   * target rather than a write admission limit, so the logical mapping count and live weight may
   * temporarily exceed it while the maintenance actor catches up. Listener delivery, policy
   * repair, and native reclamation remain asynchronous. The method is intentionally void: an
   * unconditional write does not materialize or expose the previous value.
   */
  void put(K key, V value);

  /**
   * Publishes an entry with an absolute wall-clock expiry time. The method is intentionally void:
   * an unconditional write does not materialize or expose the previous value. A non-positive
   * expiry has the same permanent-entry meaning as the existing OHC TTL API.
   */
  void put(K key, V value, long expireAtMillis);

  /** Conditionally publishes a value only when the key is absent or expired. */
  V putIfAbsent(K key, V value);

  /**
   * Conditionally publishes a value only when the key is absent or expired, returning the previous
   * live value when the mapping was not inserted.
   */
  V putIfAbsent(K key, V value, long expireAtMillis);

  /**
   * Conditionally replaces a value when the current live value equals expected. The expiry is an
   * absolute wall-clock time; a non-positive expiry creates a permanent mapping.
   */
  boolean replace(K key, V expected, V value, long expireAtMillis);

  V get(Object key);

  boolean containsKey(Object key);

  /**
   * Returns the logical live mapping count as an {@code int}, like {@link Map#size()}. The result
   * may temporarily exceed the configured capacity or {@code maxSize} target while asynchronous
   * eviction catches up.
   */
  int size();

  /** Returns the constant-time physical CHM mapping count; it may include expired entries. */
  long mappingCount();

  boolean isEmpty();

  boolean equals(Object object);

  int hashCode();

  String toString();

  boolean containsValue(Object value);

  void putAll(Map<? extends K, ? extends V> entries);

  /**
   * Removes the live mapping for a key if present. Missing keys are a no-op and the method does
   * not materialize or expose the removed value.
   */
  void remove(Object key);

  boolean remove(Object key, Object value);

  boolean replace(K key, V oldValue, V newValue);

  V replace(K key, V value);

  void clear();

  Set<K> keySet();

  Collection<V> values();

  Set<Map.Entry<K, V>> entrySet();

  V getOrDefault(Object key, V defaultValue);

  void forEach(BiConsumer<? super K, ? super V> action);

  void replaceAll(BiFunction<? super K, ? super V, ? extends V> function);

  V computeIfAbsent(K key, Function<? super K, ? extends V> function);

  V computeIfPresent(
      K key, BiFunction<? super K, ? super V, ? extends V> function);

  V compute(K key, BiFunction<? super K, ? super V, ? extends V> function);

  V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> function);
  Map<K, V> getAll(Collection<? extends K> keys);

  int removeAll(Collection<? extends K> keys);

  /**
   * Reads a live value without deserialization and invokes the consumer synchronously on the
   * calling thread. The callback-scoped {@link ValueView} and any direct buffer obtained from it
   * must not escape the callback.
   */
  boolean getDirect(K key, DirectValueConsumer consumer);

  /**
   * Reads live values without deserialization and invokes the consumer synchronously on the
   * calling thread once per unique hit. Each callback-scoped {@link ValueView} and direct buffer
   * must not escape its callback.
   */
  int getDirectAll(Collection<? extends K> keys, DirectEntryConsumer<K> consumer);

  /**
   * Completes after async tasks queued before the call and their maintenance work are drained. The
   * capacity/maxSize target is included: when the logical occupancy is over target, completion
   * waits for the actor to bring it back to target. Concurrent writes after the FIFO fence may
   * leave later occupancy outside this guarantee.
   */
  CompletableFuture<Void> flushAsync();

  /**
   * Returns the configured logical serialized-entry byte target. It is not a synchronous write
   * admission limit; a cache can temporarily exceed it and can grow until native allocation
   * fails if maintenance cannot keep up.
   */
  long capacity();

  /** Returns current physical native allocation, including allocator and shared structures. */
  long totalAllocatedBytes();

  OHCacheStats stats();

  void close();
}
