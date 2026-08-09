package com.red.ohc.api;

import java.util.Arrays;
import java.util.Objects;

import com.red.ohc.codec.Hashing;

/** Immutable, pre-serialized key with one xxHash64 result and its folded CHM hash cached. */
public final class EncodedKey {
  private final byte[] bytes;
  final int hash;
  final long hash64;

  private EncodedKey(byte[] bytes, long hash64) {
    this.bytes = bytes;
    this.hash64 = hash64;
    this.hash = (int) (hash64 ^ (hash64 >>> 32));
  }

  public static EncodedKey fromSerialized(byte[] bytes, int length, long hash64) {
    if (length < 0 || length > bytes.length) {
      throw new IllegalArgumentException("invalid encoded key length");
    }
    return new EncodedKey(Arrays.copyOf(bytes, length), hash64);
  }

  public static EncodedKey copyOf(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    byte[] copy = Arrays.copyOf(bytes, bytes.length);
    return new EncodedKey(copy, Hashing.xxHash64(copy, 0, copy.length));
  }

  public int length() {
    return bytes.length;
  }

  /** Returns a defensive copy; cache hot paths use the immutable backing array internally. */
  public byte[] bytes() {
    return Arrays.copyOf(bytes, bytes.length);
  }

  /** Returns the immutable backing bytes for cache-internal use. */
  public byte[] backingBytes() {
    return bytes;
  }

  public int hash() {
    return hash;
  }

  public long hash64() {
    return hash64;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof EncodedKey)) {
      return false;
    }
    EncodedKey key = (EncodedKey) other;
    return hash64 == key.hash64 && Arrays.equals(bytes, key.bytes);
  }
}
