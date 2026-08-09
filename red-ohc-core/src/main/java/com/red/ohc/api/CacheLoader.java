package com.red.ohc.api;

@FunctionalInterface
public interface CacheLoader<K, V> {
  V load(K key) throws Exception;
}
