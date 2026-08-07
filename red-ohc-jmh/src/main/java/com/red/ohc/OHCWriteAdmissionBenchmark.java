package com.red.ohc;

import java.util.concurrent.TimeUnit;

import com.red.ohc.jmh.Utils;
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
import org.openjdk.jmh.infra.ThreadParams;

/** Measures producer admission only: no serializer and no periodic flush. */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class OHCWriteAdmissionBenchmark {
    private static final int KEY_COUNT = 1 << 15;
    private static final int KEY_MASK = KEY_COUNT - 1;
    private static final int BATCH_SIZE = 1 << 8;

    @Param({"JNA", "UNSAFE"}) public AllocatorType allocator;
    @Param({"16", "64"}) public int keyBytes;
    @Param({"256", "1024"}) public int valueBytes;

    private EncodedKey[] keys;
    private byte[][] values;
    private OffHeapCache<byte[], byte[]> cache;

    @Setup(Level.Trial)
    public void setup() {
        keys = new EncodedKey[KEY_COUNT];
        values = new byte[KEY_COUNT][];
        long payloadCapacity = (long) KEY_COUNT * (keyBytes + valueBytes) * 2L;
        cache = (OffHeapCache<byte[], byte[]>) OHCacheBuilder.<byte[], byte[]>newBuilder()
                .capacity(payloadCapacity)
                .expectedEntries(KEY_COUNT)
                .keySerializer(Utils.byteArraySerializer)
                .valueSerializer(Utils.byteArraySerializer)
                .eviction(Eviction.S3_FIFO)
                .allocator(allocator)
                .build();
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = EncodedKey.copyOf(bytes(keyBytes, i));
            values[i] = bytes(valueBytes, i * 31 + 7);
            if (!cache.putEncoded(keys[i], values[i])) throw new IllegalStateException("OHC preload rejected");
        }
        cache.flushAsync().join();
        assertHealthyAndDrained();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cache.flushAsync().join();
        try {
            assertHealthyAndDrained();
        } finally {
            cache.close();
        }
    }

    @Benchmark @Threads(1) @OperationsPerInvocation(BATCH_SIZE)
    public void oneThread(WriteCursor cursor) { write(cursor); }

    @Benchmark @Threads(Threads.MAX) @OperationsPerInvocation(BATCH_SIZE)
    public void cpuThreads(WriteCursor cursor) { write(cursor); }

    void write(WriteCursor cursor) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            int index = cursor.next();
            if (!cache.putEncoded(keys[index], values[index])) throw new IllegalStateException("OHC encoded write rejected");
        }
    }

    private void assertHealthyAndDrained() {
        OHCacheStats stats = cache.stats();
        if (stats.maintenanceUnhealthy || stats.mutationRejectedQueue != 0L || stats.mutationRejectedBudget != 0L
                || stats.maintenanceDropped != 0L || stats.maintenanceQueueDepth != 0L) {
            throw new IllegalStateException("invalid OHC write measurement");
        }
    }

    @State(Scope.Thread)
    public static class WriteCursor {
        private int cursor;
        @Setup(Level.Trial) public void setup(ThreadParams params) { cursor = params.getThreadIndex() * BATCH_SIZE; }
        int next() { return cursor++ & KEY_MASK; }
    }

    public static byte[] bytes(int length, int seed) {
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
