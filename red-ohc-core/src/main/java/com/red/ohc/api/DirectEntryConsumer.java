package com.red.ohc.api;

/** Consumes one cache key and its temporary native-backed value view synchronously. */
@FunctionalInterface
public interface DirectEntryConsumer<K> {
  /**
   * Receives a value view and any native {@code ByteBuffer} obtained from it. They are borrowed and
   * valid only for the duration of this call; derived buffer views must not escape it either.
   */
  void accept(K key, ValueView value);
}
