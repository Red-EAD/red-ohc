package com.red.ohc.api;

import java.util.Arrays;
import java.util.Objects;

import com.red.ohc.codec.KeyHash;

/** Immutable, pre-serialized key with the packed identity (hash and length) cached. */
public final class EncodedKey {
  /** Serialized keys beyond this length are rejected by every cache operation. */
  public static final int MAX_KEY_LENGTH = 0xff;

  private final byte[] bytes;
  final int keyIndex;

  private EncodedKey(byte[] bytes, int keyIndex) {
    this.bytes = bytes;
    this.keyIndex = keyIndex;
  }

  public static EncodedKey copyOf(byte[] bytes, int length) {
    Objects.requireNonNull(bytes, "bytes");
    if (length < 0 || length > bytes.length) {
      throw new IllegalArgumentException("invalid encoded key length");
    }
    if (length > MAX_KEY_LENGTH) {
      throw new IllegalArgumentException("encoded key exceeds " + MAX_KEY_LENGTH + " bytes");
    }
    byte[] copy = Arrays.copyOf(bytes, length);
    return new EncodedKey(copy, (KeyHash.hash(copy, 0, copy.length) << 8) | copy.length);
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

  public int keyIndex() {
    return keyIndex;
  }

  public int hash() {
    return keyIndex >>> 8;
  }

  @Override
  public int hashCode() {
    return keyIndex >>> 8;
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
    return keyIndex == key.keyIndex && Arrays.equals(bytes, key.bytes);
  }
}
