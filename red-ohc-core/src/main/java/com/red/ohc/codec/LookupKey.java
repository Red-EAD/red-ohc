package com.red.ohc.codec;

import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Reusable serialized lookup key; its packed identity is computed at most once per operation. */
public final class LookupKey {
  private byte[] bytes;
  private int keyIndex;

  public void set(byte[] bytes, int length) {
    if (this.bytes != bytes) {
      this.bytes = bytes;
    }
    this.keyIndex = (KeyHash.hash(bytes, 0, length) << 8) | length;
  }

  /** Binds an immutable encoded key without hashing its bytes a second time. */
  public void set(byte[] target, EncodedKey key) {
    key.copyTo(target, 0);
    this.bytes = target;
    this.keyIndex = key.keyIndex();
  }

  public byte[] bytes() {
    return bytes;
  }

  public int length() {
    return keyIndex & 0xff;
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
    if (!(other instanceof Entry)) {
      return false;
    }
    Entry entry = (Entry) other;
    int length = keyIndex & 0xff;
    return entry.keyIndex() == keyIndex
        && (length == 0 || NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, length));
  }
}
