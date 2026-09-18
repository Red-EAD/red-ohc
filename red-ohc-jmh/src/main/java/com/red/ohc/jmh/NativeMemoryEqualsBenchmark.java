package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.storage.NativeMemory;

/**
 * Compares the historical word-at-a-time equality loop with NativeMemory's block8 path.
 *
 * <p>All input data is prepared before measurement. A mismatch is injected into the native
 * left-hand side only, so both methods observe the same bytes and the benchmark measures only
 * comparison work.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 2,
    jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@State(Scope.Benchmark)
public class NativeMemoryEqualsBenchmark {
  @Param({"8", "24", "32", "64", "127", "128", "129", "256", "512", "4096"})
  public int keyBytes;

  @Param({"EQUAL", "FIRST_BYTE", "FIRST_WORD", "BLOCK_TAIL"})
  public String mismatch;

  private static final int ARRAY_OFFSET = 11;
  private NativeMemory.Memory memory;
  private byte[] expected;
  private long left;
  private long right;

  @Setup(Level.Trial)
  public void setup() {
    memory = new NativeMemory.Memory();
    expected = bytes(keyBytes + ARRAY_OFFSET, keyBytes * 17 + 3);
    left = memory.allocate(keyBytes);
    right = memory.allocate(keyBytes);
    NativeMemory.copy(expected, ARRAY_OFFSET, left, keyBytes);
    NativeMemory.copy(expected, ARRAY_OFFSET, right, keyBytes);

    int mismatchOffset = mismatchOffset();
    if (mismatchOffset >= 0) {
      NativeMemory.putByte(
          left + mismatchOffset, (byte) (NativeMemory.getByte(left + mismatchOffset) ^ 1));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    memory.free(left, keyBytes);
    memory.free(right, keyBytes);
    memory.closeArenas();
  }

  @Benchmark
  public void historicalHeapLoop(Blackhole blackhole) {
    blackhole.consume(historicalHeapEquals(left, expected, ARRAY_OFFSET, keyBytes));
  }

  @Benchmark
  public void block8HeapLoop(Blackhole blackhole) {
    blackhole.consume(NativeMemory.equals(left, expected, ARRAY_OFFSET, keyBytes));
  }

  @Benchmark
  public void historicalNativeLoop(Blackhole blackhole) {
    blackhole.consume(historicalNativeEquals(left, right, keyBytes));
  }

  @Benchmark
  public void block8NativeLoop(Blackhole blackhole) {
    blackhole.consume(NativeMemory.equals(left, right, keyBytes));
  }

  private int mismatchOffset() {
    if ("EQUAL".equals(mismatch)) {
      return -1;
    }
    if ("FIRST_BYTE".equals(mismatch)) {
      return 0;
    }
    if ("FIRST_WORD".equals(mismatch)) {
      return Math.min(4, keyBytes - 1);
    }
    return keyBytes >= 64 ? Math.min(56, keyBytes - 1) : Math.max(0, keyBytes - 1);
  }

  private static boolean historicalHeapEquals(long address, byte[] bytes, int offset, int length) {
    int i = 0;
    for (; i + 8 <= length; i += 8) {
      if (NativeMemory.getLong(address + i) != NativeMemory.getLong(bytes, offset + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (NativeMemory.getByte(address + i) != bytes[offset + i]) {
        return false;
      }
    }
    return true;
  }

  private static boolean historicalNativeEquals(long left, long right, int length) {
    int i = 0;
    for (; i + 8 <= length; i += 8) {
      if (NativeMemory.getLong(left + i) != NativeMemory.getLong(right + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (NativeMemory.getByte(left + i) != NativeMemory.getByte(right + i)) {
        return false;
      }
    }
    return true;
  }

  private static byte[] bytes(int length, int seed) {
    byte[] bytes = new byte[length];
    long value = seed * 0x9e3779b97f4a7c15L;
    for (int i = 0; i < length; i++) {
      value ^= value >>> 12;
      value ^= value << 25;
      value ^= value >>> 27;
      bytes[i] = (byte) value;
    }
    return bytes;
  }
}
