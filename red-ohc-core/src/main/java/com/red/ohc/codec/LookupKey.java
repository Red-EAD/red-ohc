package com.red.ohc.codec;

import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Reusable serialized lookup key; its CHM hash is computed at most once per operation. */
public final class LookupKey {
  private byte[] bytes;
  private int length;
  private int hash;

  public void set(byte[] bytes, int length) {
    if (this.bytes != bytes) {
      this.bytes = bytes;
    }
    this.length = length;
    this.hash = hash(bytes, 0, length);
  }

  /** Binds an immutable encoded key without hashing its bytes a second time. */
  public void set(byte[] target, EncodedKey key) {
    key.copyTo(target, 0);
    this.bytes = target;
    this.length = key.length();
    this.hash = key.hash();
  }

  public byte[] bytes() {
    return bytes;
  }

  public int length() {
    return length;
  }

  public int hash() {
    return hash;
  }

  private static int hash(byte[] bytes, int offset, int length) {
    return KeyHash.hash(bytes, offset, length);
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
    if (!(other instanceof Entry)) {
      return false;
    }
    Entry entry = (Entry) other;
    return entry.keyLength() == length
        && NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, length);
  }
}