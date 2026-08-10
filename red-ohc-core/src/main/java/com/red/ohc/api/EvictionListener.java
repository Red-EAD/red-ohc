package com.red.ohc.api;

import java.util.function.Supplier;

/** Listener for automatic cache evictions. */
@FunctionalInterface
public interface EvictionListener<K, V> {
  /**
   * Called synchronously on the maintenance thread after an automatic removal succeeds.
   *
   * <p>The key and value suppliers deserialize at most once and are valid only until this method
   * returns. A supplier that is not used does not deserialize its payload.
   */
  void onEviction(Supplier<K> key, Supplier<V> value, RemovalCause cause);
}
