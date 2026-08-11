package com.red.ohc.jmh;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;

/**
 * Generic API comparison for batch reads and writes.
 *
 * <p>Both implementations use the same integer keys and byte[] payloads. The read benchmark copies
 * every Caffeine value before returning it so that OHC's generic getAll materialization cost is
 * included on both sides. The write benchmark intentionally measures each cache's real putAll
 * contract: OHC serializes into native memory while Caffeine retains the supplied arrays.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 7, time = 3)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCGetAllPutAllBenchmark {
  private static final int KEY_COUNT = 1 << 14;
  private static final int BATCH_SIZE = 64;
  private static final long CAPACITY = 128L << 20;
  private static final AtomicInteger NEXT_FRESH_KEY = new AtomicInteger(KEY_COUNT);

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
          byte[] value = new byte[buffer.remaining()];
          buffer.get(value);
          return value;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Param({"OHC", "CAFFEINE"})
  public String implementation;

  @Param({"256", "1024"})
  public int valueBytes;

  @Param({"JNA"})
  public AllocatorType allocator;

  private Integer[] keys;
  private byte[][] values;
  private List<Integer> batchKeys;
  private Map<Integer, byte[]> batch;
  private final ThreadLocal<FreshBatch> freshBatches = ThreadLocal.withInitial(FreshBatch::new);
  private OHCache<Integer, byte[]> ohc;
  private Cache<Integer, byte[]> caffeine;

  @Setup(Level.Trial)
  public void setup() {
    keys = new Integer[KEY_COUNT];
    values = new byte[KEY_COUNT][];
    for (int index = 0; index < KEY_COUNT; index++) {
      keys[index] = index;
      values[index] = bytes(valueBytes, index * 31 + 7);
    }
    batchKeys = new ArrayList<>(BATCH_SIZE);
    batch = new HashMap<>(BATCH_SIZE * 2);
    for (int index = 0; index < BATCH_SIZE; index++) {
      batchKeys.add(keys[index]);
      batch.put(keys[index], values[index]);
    }
    if ("OHC".equals(implementation)) {
      ohc =
          OHCacheBuilder.<Integer, byte[]>newBuilder()
              .capacity(CAPACITY)
              .expectedEntries(KEY_COUNT)
              .keySerializer(INT_SERIALIZER)
              .valueSerializer(BYTES_SERIALIZER)
              .eviction(Eviction.S3_FIFO)
              .allocator(allocator)
              .build();
      for (int index = 0; index < KEY_COUNT; index += BATCH_SIZE) {
        ohc.putAll(batch(index, BATCH_SIZE));
      }
      // Stabilize the preload once. Repeated barriers here would measure setup cost and
      // make the benchmark needlessly sensitive to maintenance wake-up latency.
      ohc.flushAsync().join();
      if (ohc.size() != KEY_COUNT || ohc.stats().getMaintenanceUnhealthy()) {
        throw new IllegalStateException(
            "OHC preload did not converge: size="
                + ohc.size()
                + ", unhealthy="
                + ohc.stats().getMaintenanceUnhealthy());
      }
    } else {
      caffeine =
          Caffeine.<Integer, byte[]>newBuilder()
              .maximumWeight(CAPACITY)
              .weigher((Integer key, byte[] value) -> Integer.BYTES + value.length)
              .build();
      for (int index = 0; index < KEY_COUNT; index += BATCH_SIZE) {
        caffeine.putAll(batch(index, BATCH_SIZE));
      }
      if (caffeine.estimatedSize() != KEY_COUNT) {
        throw new IllegalStateException(
            "Caffeine preload did not converge: size=" + caffeine.estimatedSize());
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (ohc != null) {
      ohc.close();
    }
    if (caffeine != null) {
      caffeine.invalidateAll();
      caffeine.cleanUp();
    }
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void getAllOneThread(Blackhole blackhole) {
    getAll(blackhole);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void getAllCpuThreads(Blackhole blackhole) {
    getAll(blackhole);
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(BATCH_SIZE)
  public void putAllOneThread(Blackhole blackhole, WriteResults results) {
    putAll(blackhole, results);
  }

  @Benchmark
  @Threads(Threads.MAX)
  @OperationsPerInvocation(BATCH_SIZE)
  public void putAllCpuThreads(Blackhole blackhole, WriteResults results) {
    putAll(blackhole, results);
  }

  private void getAll(Blackhole blackhole) {
    if ("OHC".equals(implementation)) {
      blackhole.consume(ohc.getAll(batchKeys));
      return;
    }
    Map<Integer, byte[]> present = caffeine.getAllPresent(batchKeys);
    Map<Integer, byte[]> result = new HashMap<>(present.size() * 2);
    // OHC copies native bytes into a temporary payload and the generic deserializer copies
    // that payload again. Keep the two ownership copies explicit on the heap side.
    present.forEach(
        (key, value) -> {
          byte[] payload = Arrays.copyOf(value, value.length);
          result.put(key, Arrays.copyOf(payload, payload.length));
        });
    blackhole.consume(result);
  }

  private void putAll(Blackhole blackhole, WriteResults results) {
    Map<Integer, byte[]> freshBatch = freshBatches.get().next(values);
    int accepted;
    if ("OHC".equals(implementation)) {
      accepted = ohc.putAll(freshBatch);
    } else {
      caffeine.putAll(freshBatch);
      accepted = BATCH_SIZE;
    }
    results.attempted += BATCH_SIZE;
    results.accepted += accepted;
    results.rejected += BATCH_SIZE - accepted;
    blackhole.consume(accepted);
  }

  private Map<Integer, byte[]> batch(int start, int count) {
    Map<Integer, byte[]> batch = new HashMap<>(count * 2);
    for (int offset = 0; offset < count; offset++) {
      int index = (start + offset) & (KEY_COUNT - 1);
      batch.put(keys[index], values[index]);
    }
    return batch;
  }

  private static byte[] bytes(int length, int seed) {
    byte[] result = new byte[length];
    long value = seed * 0x9e3779b97f4a7c15L;
    for (int index = 0; index < length; index++) {
      value ^= value >>> 12;
      value ^= value << 25;
      value ^= value >>> 27;
      result[index] = (byte) value;
    }
    return result;
  }

  /**
   * Reusable Map view whose next batch occupies a globally fresh integer-key range. Keeping the
   * view thread-local avoids measuring a HashMap allocation on every invocation while preserving
   * the real Map.entrySet() iteration used by both cache implementations.
   */
  private static final class FreshBatch extends AbstractMap<Integer, byte[]> {
    private final EntrySet entries = new EntrySet();
    private byte[][] values;
    private int base;

    private Map<Integer, byte[]> next(byte[][] values) {
      this.values = values;
      this.base = NEXT_FRESH_KEY.getAndAdd(BATCH_SIZE);
      return this;
    }

    @Override
    public Set<Entry<Integer, byte[]>> entrySet() {
      return entries;
    }

    @Override
    public int size() {
      return BATCH_SIZE;
    }

    private final class EntrySet extends AbstractSet<Entry<Integer, byte[]>> {
      @Override
      public Iterator<Entry<Integer, byte[]>> iterator() {
        return new Iterator<Entry<Integer, byte[]>>() {
          private int index;
          private final Entry<Integer, byte[]> entry = new BatchEntry();

          @Override
          public boolean hasNext() {
            return index < BATCH_SIZE;
          }

          @Override
          public Entry<Integer, byte[]> next() {
            ((BatchEntry) entry).index = index++;
            return entry;
          }
        };
      }

      @Override
      public int size() {
        return BATCH_SIZE;
      }
    }

    private final class BatchEntry implements Entry<Integer, byte[]> {
      private int index;

      @Override
      public Integer getKey() {
        return base + index;
      }

      @Override
      public byte[] getValue() {
        return values[index & (values.length - 1)];
      }

      @Override
      public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
