package com.red.ohc.api;

/** Valid only while passed to a direct-read callback. */
public interface ValueView {
  int length();

  byte getByte(int offset);

  long getLong(int offset);

  void copyTo(byte[] target, int targetOffset);
}
