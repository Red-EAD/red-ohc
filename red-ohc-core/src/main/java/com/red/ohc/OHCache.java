package com.red.ohc;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** A weakly-consistent, asynchronous off-heap cache. */
public interface OHCache<K, V> extends AutoCloseable {
    boolean put(K key, V value);
    boolean put(K key, V value, long expireAtMillis);
    boolean remove(K key);

    V get(K key);
    boolean containsKey(K key);

    int putAll(Map<? extends K, ? extends V> entries);
    Map<K, V> getAll(Collection<? extends K> keys);
    int removeAll(Collection<? extends K> keys);

    /** Reads a live value without deserialization while the callback is running. */
    boolean getDirect(K key, DirectValueConsumer consumer);

    /** Reads live values without deserialization, invoking the callback once per hit. */
    int getDirectAll(Collection<? extends K> keys, DirectEntryConsumer<K> consumer);

    CompletableFuture<Boolean> putIfAbsentAsync(K key, V value, long expireAtMillis);
    CompletableFuture<Boolean> replaceAsync(K key, V expected, V value, long expireAtMillis);
    CompletableFuture<Boolean> removeAsync(K key);
    CompletableFuture<V> getOrLoadAsync(K key, CacheLoader<K, V> loader, long expireAtMillis);
    CompletableFuture<Void> flushAsync();

    long size();
    long capacity();
    long totalAllocatedBytes();
    OHCacheStats stats();

    @Override
    void close();
}
