package com.red.ohc.codec;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Reusable serialized lookup key; its CHM hash is computed at most once per operation. */
public final class LookupKey {
  private byte[] bytes;
  private int length;
  private int hash;
  private long hash64;

  public void set(byte[] bytes, int length) {
    this.bytes = bytes;
    this.length = length;
    long hash64 = Hashing.farmHashUo(bytes, 0, length);
    this.hash64 = hash64;
    this.hash = (int) (hash64 ^ (hash64 >>> 32));
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
    if (!(other instanceof Entry)) {
      return false;
    }
    Entry entry = (Entry) other;
    return entry.keyHash() == hash
        && entry.keyHash64() == hash64
        && entry.keyLength() == length
        && NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, length);
  }
}
