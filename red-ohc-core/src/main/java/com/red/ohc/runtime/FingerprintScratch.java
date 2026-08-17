package com.red.ohc.runtime;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Adler32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/** Reusable direct storage and checksum state for one fingerprint validation lease. */
final class FingerprintScratch {
  private static final int VALUE_FINGERPRINT_SWITCH_BYTES = 256;
  private static final int INITIAL_FINGERPRINT_BYTES = VALUE_FINGERPRINT_SWITCH_BYTES;

  private ByteBuffer storage;
  private ByteBuffer exposed;
  private final CRC32C crc32c = new CRC32C();
  private final Adler32 adler32 = new Adler32();

  FingerprintScratch() {}

  FingerprintScratch(int capacity) {
    if (!ensureCapacity(capacity)) {
      throw new IllegalArgumentException("invalid fingerprint scratch capacity: " + capacity);
    }
  }

  boolean ensureCapacity(int length) {
    if (length < 0) {
      return false;
    }
    if (storage != null && storage.capacity() >= length) {
      return true;
    }
    int capacity = INITIAL_FINGERPRINT_BYTES;
    while (capacity < length) {
      if (capacity > Integer.MAX_VALUE / 2) {
        return false;
      }
      capacity <<= 1;
    }
    storage = ByteBuffer.allocateDirect(capacity).order(ByteOrder.BIG_ENDIAN);
    exposed = null;
    return true;
  }

  ByteBuffer prepare(int length) {
    if (!ensureCapacity(length)) {
      throw new IllegalArgumentException("invalid fingerprint scratch length: " + length);
    }
    if (exposed == null || exposed.capacity() != length) {
      ByteBuffer view = storage.duplicate();
      view.clear();
      view.limit(length);
      exposed = view.slice().order(ByteOrder.BIG_ENDIAN);
    }
    exposed.order(ByteOrder.BIG_ENDIAN);
    exposed.clear();
    return exposed;
  }

  long checksum(ByteBuffer input, int length) {
    input.position(0);
    input.limit(length);
    Checksum checksum = length <= VALUE_FINGERPRINT_SWITCH_BYTES ? crc32c : adler32;
    checksum.reset();
    checksum.update(input);
    return checksum.getValue();
  }

  int capacity() {
    return storage == null ? 0 : storage.capacity();
  }

  void discard() {
    storage = null;
    exposed = null;
  }
}
