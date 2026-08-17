package com.red.ohc.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.index.Entry;
import com.red.ohc.index.WeakValueStateStore;

/** Compares the generic get path with and without native-deserialization reuse. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 7, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class WeakValuesBenchmark {
  private static final CacheSerializer<Integer> INT_SERIALIZER =
      new CacheSerializer<Integer>() {
        @Override
        public void serialize(Integer value, ByteBuffer buffer) {
          buffer.putInt(value);
        }

        @Override
        public Integer deserialize(ByteBuffer buffer) {
          return buffer.getInt();
        }

        @Override
        public int serializedSize(Integer value) {
          return Integer.BYTES;
        }
      };

  private static final CacheSerializer<byte[]> BYTES_SERIALIZER =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] result = new byte[buffer.remaining()];
          buffer.get(result);
          return result;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Param({"64", "255", "256", "257", "1024", "5120", "65536", "65537"})
  public int valueBytes;

  @Param({"false", "true"})
  public boolean ttl;

  private OffHeapCache<Integer, byte[]> weakCache;
  private OffHeapCache<Integer, byte[]> strongCache;
  private Entry weakEntry;
  private Entry mixedNativeEntry;
  private byte[] value;
  private byte[] weakValue;
  private byte[] secondValue;

  @Setup(Level.Trial)
  public void setup() {
    value = new byte[valueBytes];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) index;
    }
    secondValue = Arrays.copyOf(value, value.length);
    secondValue[0]++;
    long expiry = ttl ? System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10) : 0L;
    weakCache = newCache(true);
    strongCache = newCache(false);
    weakCache.put(1, value, expiry);
    weakCache.put(2, secondValue, expiry);
    strongCache.put(1, value, expiry);
    strongCache.put(2, secondValue, expiry);
    weakCache.flushAsync().join();
    strongCache.flushAsync().join();
    WeakValueStateStore stateStore = weakCache.weakValueStateStoreForTest();
    weakEntry =
        weakCache.dataForTest().values().stream()
            .filter(entry -> entry.nativeKeyAddress != 0L)
            .filter(entry -> stateStore.get(entry) != null)
            .filter(entry -> stateStore.get(entry).weakValue() != null)
            .filter(entry -> stateStore.get(entry).weakValue().get() == value)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("weak benchmark entry missing"));
    mixedNativeEntry =
        weakCache.dataForTest().values().stream()
            .filter(entry -> entry.nativeKeyAddress != 0L)
            .filter(entry -> entry != weakEntry)
            .filter(entry -> stateStore.get(entry) != null)
            .filter(entry -> stateStore.get(entry).weakValue() != null)
            .filter(entry -> stateStore.get(entry).weakValue().get() == secondValue)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("mixed benchmark entry missing"));
    weakValue = weakCache.get(1);
    WeakMissState.entryForBenchmark = weakEntry;
    WeakMissState.stateStoreForBenchmark = stateStore;
    MixedGetAllState.entryForBenchmark = mixedNativeEntry;
    MixedGetAllState.stateStoreForBenchmark = stateStore;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    weakCache.close();
    strongCache.close();
  }

  @Benchmark
  @Threads(1)
  public void weakHit(Blackhole blackhole) {
    blackhole.consume(weakCache.get(1));
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void weakHitCpuThreads(Blackhole blackhole) {
    blackhole.consume(weakCache.get(1));
  }

  @Benchmark
  @Threads(Threads.MAX)
  public void strongGetCpuThreads(Blackhole blackhole) {
    blackhole.consume(strongCache.get(1));
  }

  @Benchmark
  @Threads(1)
  public void weakMiss(Blackhole blackhole, WeakMissState state) {
    blackhole.consume(weakCache.get(1));
  }

  @Benchmark
  @Threads(1)
  public void strongGet(Blackhole blackhole) {
    blackhole.consume(strongCache.get(1));
  }

  @Benchmark
  @Threads(1)
  public void weakGetAllMixed(Blackhole blackhole, MixedGetAllState state) {
    blackhole.consume(weakCache.getAll(Arrays.asList(1, 2)));
  }

  @Benchmark
  @Threads(1)
  public void strongGetAllMixed(Blackhole blackhole) {
    blackhole.consume(strongCache.getAll(Arrays.asList(1, 2)));
  }

  @Benchmark
  @Threads(1)
  public void weakPut(Blackhole blackhole) {
    blackhole.consume(weakCache.put(1, value));
  }

  @Benchmark
  @Threads(1)
  public void strongPut(Blackhole blackhole) {
    blackhole.consume(strongCache.put(1, value));
  }

  @Benchmark
  @Threads(1)
  public void weakReplace(Blackhole blackhole) {
    blackhole.consume(weakCache.replaceAsync(1, weakValue, value, 0L).join());
  }

  @Benchmark
  @Threads(1)
  public void strongReplace(Blackhole blackhole) {
    blackhole.consume(strongCache.replaceAsync(1, value, value, 0L).join());
  }

  private OffHeapCache<Integer, byte[]> newCache(boolean weakValues) {
    return (OffHeapCache<Integer, byte[]>)
        OHCacheBuilder.<Integer, byte[]>newBuilder()
            .capacity(1 << 20)
            .keySerializer(INT_SERIALIZER)
            .valueSerializer(BYTES_SERIALIZER)
            .eviction(Eviction.S3_FIFO)
            .weakValues(weakValues)
            .build();
  }

  @State(Scope.Thread)
  public static class WeakMissState {
    private static volatile Entry entryForBenchmark;
    private static volatile WeakValueStateStore stateStoreForBenchmark;

    @Setup(Level.Invocation)
    public void clearWeakSlot() {
      Entry entry = entryForBenchmark;
      WeakValueStateStore store = stateStoreForBenchmark;
      if (entry != null && store != null) {
        store.clearWeakValueSlot(entry);
      }
    }
  }

  @State(Scope.Thread)
  public static class MixedGetAllState {
    private static volatile Entry entryForBenchmark;
    private static volatile WeakValueStateStore stateStoreForBenchmark;

    @Setup(Level.Invocation)
    public void clearWeakSlot() {
      Entry entry = entryForBenchmark;
      WeakValueStateStore store = stateStoreForBenchmark;
      if (entry != null && store != null) {
        store.clearWeakValueSlot(entry);
      }
    }
  }
}
