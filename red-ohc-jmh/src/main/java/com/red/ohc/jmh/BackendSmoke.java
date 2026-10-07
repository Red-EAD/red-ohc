package com.red.ohc.jmh;

import java.util.Arrays;

/** A few correctness operations in an isolated runtime, with no benchmark timing. */
public final class BackendSmoke {
  private BackendSmoke() {}

  public static void main(String[] args) {
    try (FairCache cache = FairCache.create(args[0], 8 * 1024 * 1024, 32, 8, 32, 0)) {
      byte[] key = new byte[8];
      byte[] value = new byte[32];
      value[31] = 123;
      if (!cache.put(key, value)) {
        throw new IllegalStateException("put rejected");
      }
      byte[] result = cache.get(key);
      if (!Arrays.equals(result, value)) {
        throw new IllegalStateException("full value differs");
      }
      result[31] = 0;
      if (!Arrays.equals(cache.get(key), value)) {
        throw new IllegalStateException("returned value is not owned");
      }
      value[31] = 42;
      if (!cache.put(key, value) || !Arrays.equals(cache.get(key), value)) {
        throw new IllegalStateException("replacement differs");
      }
    }
    System.out.println(args[0] + " isolated correctness smoke passed");
  }
}
