package com.red.ohc.api;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** A weakly-consistent off-heap cache with synchronous data-plane writes. */
public interface OHCache<K, V> extends AutoCloseable {
  /**
   * Publishes the serialized entry synchronously. A successful return is immediately visible to
   * {@link #get(Object)}; resource or contention admission failure returns {@code false}.
   */
  boolean put(K key, V value);

  boolean put(K key, V value, long expireAtMillis);

  boolean remove(K key);

  V get(K key);

  boolean containsKey(K key);

  int putAll(Map<? extends K, ? extends V> entries);

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
   * Enqueues a mutation for this cache's maintenance event-loop. The key and value must not be
   * modified until the returned future completes.
   */
  CompletableFuture<Boolean> putIfAbsentAsync(K key, V value, long expireAtMillis);

  /** The key, expected value, and replacement value must remain unchanged until completion. */
  CompletableFuture<Boolean> replaceAsync(K key, V expected, V value, long expireAtMillis);

  /** The key must remain unchanged until completion. */
  CompletableFuture<Boolean> removeAsync(K key);

  CompletableFuture<V> getOrLoadAsync(K key, CacheLoader<K, V> loader, long expireAtMillis);

  /** Completes after async tasks queued before the call and their maintenance work are drained. */
  CompletableFuture<Void> flushAsync();

  long size();

  long capacity();

  long totalAllocatedBytes();

  OHCacheStats stats();

  @Override
  void close();
}
