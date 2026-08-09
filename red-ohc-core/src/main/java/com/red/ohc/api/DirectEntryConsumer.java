package com.red.ohc.api;

/** Consumes one cache key and its temporary native-backed value view. */
@FunctionalInterface
public interface DirectEntryConsumer<K> {
  /** Receives a value view that is valid only for the duration of this call. */
  void accept(K key, ValueView value);
}
