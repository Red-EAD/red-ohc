package com.red.ohc.api;

import java.util.Arrays;
import java.util.Objects;

/** Immutable, pre-serialized key with the 32-bit CHM hash cached. */
public final class EncodedKey {
  private final byte[] bytes;
  final int hash;

  private EncodedKey(byte[] bytes, int hash) {
    this.bytes = bytes;
    this.hash = hash;
  }

  public static EncodedKey copyOf(byte[] bytes, int length) {
    Objects.requireNonNull(bytes, "bytes");
    if (length < 0 || length > bytes.length) {
      throw new IllegalArgumentException("invalid encoded key length");
    }
    byte[] copy = Arrays.copyOf(bytes, length);
    return new EncodedKey(copy, Arrays.hashCode(copy));
  }

  public static EncodedKey copyOf(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    return copyOf(bytes, bytes.length);
  }

  public int length() {
    return bytes.length;
  }

  /** Returns a defensive copy; cache hot paths use the immutable backing array internally. */
  public byte[] bytes() {
    return Arrays.copyOf(bytes, bytes.length);
  }

  /** Copies the encoded key into caller-owned storage without exposing the backing array. */
  public void copyTo(byte[] target, int targetOffset) {
    if (targetOffset < 0 || target.length - targetOffset < bytes.length) {
      throw new IndexOutOfBoundsException();
    }
    System.arraycopy(bytes, 0, target, targetOffset, bytes.length);
  }

  public int hash() {
    return hash;
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
    return hash == key.hash && Arrays.equals(bytes, key.bytes);
  }
}