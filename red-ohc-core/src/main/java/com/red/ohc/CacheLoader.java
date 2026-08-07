package com.red.ohc;

@FunctionalInterface
public interface CacheLoader<K, V> {
    V load(K key) throws Exception;
}
