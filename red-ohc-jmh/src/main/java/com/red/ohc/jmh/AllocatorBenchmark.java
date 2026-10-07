package com.red.ohc.jmh;

import java.util.concurrent.TimeUnit;

import com.sun.jna.Native;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/**
 * Allocation-path cost ladder in steady state: raw JNA malloc/free vs the project's un-pooled
 * allocator layer (JNA malloc plus accounting) vs the production page-pool slot path. The arena
 * arm consumes a slot of the same footprint as the malloc arms (the 64-byte allocator prefix is
 * internal to the arena call).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class AllocatorBenchmark {
  /** Total native footprint per block: 96 = 32B key block, 5200 = 5120B value block. */
  @Param({"96", "5200"})
  public int footprint;

  private static final NativeMemory.Memory SHARED_MEMORY = new NativeMemory.Memory();

  @State(Scope.Thread)
  public static class ArenaState {
    NativeMemory.Memory memory;
    WriterArena arena;

    @Setup
    public void setup() {
      memory = new NativeMemory.Memory();
      arena = memory.newWriterArena();
    }

    @TearDown
    public void tearDown() {
      memory.closeArenas();
    }
  }

  @Benchmark
  public long jnaMallocFree() {
    long address = Native.malloc(footprint);
    Native.free(address);
    return address;
  }

  @Benchmark
  public long rawAllocFree() {
    long address = SHARED_MEMORY.allocateRaw(footprint);
    SHARED_MEMORY.free(address, footprint);
    return address;
  }

  @Benchmark
  public long arenaAllocFree(ArenaState state) {
    long block = state.arena.allocate(footprint - 64L);
    state.memory.releaseEntry(block, footprint - 64L);
    return block;
  }
}
