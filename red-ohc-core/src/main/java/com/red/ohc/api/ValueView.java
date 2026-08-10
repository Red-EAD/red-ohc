package com.red.ohc.api;

import java.nio.ByteBuffer;

/**
 * A callback-scoped view over a serialized value in cache-owned native memory.
 *
 * <p>Every method, including {@link #length()}, is valid only while the direct-read callback is
 * running. The view does not own the underlying memory and must not be retained after the
 * callback returns.
 */
public interface ValueView {
  int length();

  byte getByte(int offset);

  long getLong(int offset);

  void copyTo(byte[] target, int targetOffset);

  /**
   * Returns a read-only direct view over the serialized value without copying it.
   *
   * <p>The returned buffer is direct, non-array, starts at position {@code 0}, has limit and
   * capacity equal to {@link #length()}, and uses {@link java.nio.ByteOrder#BIG_ENDIAN}. It is
   * borrowed and valid only until the current direct-read callback returns. It must not be
   * retained, or have a {@code slice()}, {@code duplicate()}, or another view retained, after the
   * callback returns.
   */
  ByteBuffer asReadOnlyByteBuffer();
}
