package com.red.ohc.codec;

import com.red.ohc.api.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;

/** Reusable serialized lookup key; its CHM hash is computed at most once per operation. */
public final class LookupKey {
  private byte[] bytes;
  private int length;
  private int hash;
  private Entry comparedEntry;
  private long comparedAbsenceWord;

  public void set(byte[] bytes, int length) {
    if (this.bytes != bytes) {
      this.bytes = bytes;
    }
    this.length = length;
    this.hash = KeyHash.hash(bytes, 0, length);
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
    if (entry.keyLength() != length) {
      return false;
    }
    if (length == 0) {
      return true;
    }
    // Warm the logical-absence line while the key bytes load is still in flight.
    comparedEntry = entry;
    comparedAbsenceWord = entry.currentValueAllocationAndAbsent();
    return NativeMemory.equals(entry.nativeKeyBytesAddress(), bytes, 0, length);
  }

  /**
   * Absence word observed during the CHM equals walk for this entry, else a fresh read. Reading at
   * comparison time linearizes the get before any racing remove, so no fresh read is required.
   */
  public long absenceWordFor(Entry entry) {
    if (entry == null) {
      return 0L;
    }
    if (entry == comparedEntry) {
      return comparedAbsenceWord;
    }
    return entry.currentValueAllocationAndAbsent();
  }
}
