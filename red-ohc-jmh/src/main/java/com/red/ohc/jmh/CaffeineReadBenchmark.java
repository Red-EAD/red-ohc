package com.red.ohc.jmh;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.red.ohc.EncodedKey;
import com.red.ohc.OHCBenchmark;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Caffeine-only counterpart to {@link OHCBenchmark}; no OHC worker is created here. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class CaffeineReadBenchmark {
    private static final int WORKING_SET = 24_576;
    @Param({"16", "64"}) public int keyBytes;
    @Param({"256", "1024"}) public int valueBytes;
    @Param({"READ_100", "READ_99_WRITE_1", "READ_95_WRITE_5"}) public String workload;
    @Param({"HIT_ONLY", "EVICTION_120"}) public String residency;
    @Param({"UNIFORM", "ZIPF_099"}) public String distribution;
    private EncodedKey[] keys;
    private byte[][] values;
    private int[] accessSequence;
    private Cache<EncodedKey, byte[]> cache;

    @Setup(Level.Trial)
    public void setup() {
        long payloadCapacity = (long) ("HIT_ONLY".equals(residency) ? WORKING_SET : WORKING_SET * 5 / 6)
                * (keyBytes + valueBytes);
        keys = new EncodedKey[WORKING_SET];
        values = new byte[WORKING_SET][];
        for (int i = 0; i < WORKING_SET; i++) {
            keys[i] = EncodedKey.copyOf(bytes(keyBytes, i));
            values[i] = bytes(valueBytes, i * 31 + 7);
        }
        accessSequence = "ZIPF_099".equals(distribution) ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET);
        cache = Caffeine.<EncodedKey, byte[]>newBuilder().maximumWeight(payloadCapacity)
                .weigher((EncodedKey key, byte[] value) -> key.length() + value.length).build();
        for (int i = 0; i < WORKING_SET; i++) cache.put(keys[i], values[i]);
        cache.cleanUp();
        if ("HIT_ONLY".equals(residency) && cache.estimatedSize() != WORKING_SET) {
            throw new IllegalStateException("HIT_ONLY preload was evicted: size=" + cache.estimatedSize());
        }
    }

    @Benchmark @Threads(1)
    public void oneThread(ThreadState state, Blackhole blackhole) { access(state, blackhole); }

    @Benchmark @Threads(Threads.MAX)
    public void cpuThreads(ThreadState state, Blackhole blackhole) { access(state, blackhole); }

    private void access(ThreadState state, Blackhole blackhole) {
        int index = state.next(accessSequence);
        if (state.write(workload)) {
            cache.put(keys[index], values[index]);
            return;
        }
        byte[] value = cache.getIfPresent(keys[index]);
        blackhole.consume(value == null ? 0L : firstLong(value));
    }

    @State(Scope.Thread)
    public static class ThreadState {
        private int cursor;
        private int writes;
        int next(int[] sequence) { return sequence[cursor++ & (sequence.length - 1)]; }
        boolean write(String mix) {
            if ("READ_100".equals(mix)) return false;
            int every = "READ_99_WRITE_1".equals(mix) ? 100 : 20;
            return ++writes % every == 0;
        }
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

    private static int[] uniformSequence(int bound) {
        int[] sequence = new int[1 << 16];
        long seed = 1L;
        for (int i = 0; i < sequence.length; i++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            sequence[i] = (int) Long.remainderUnsigned(seed, bound);
        }
        return sequence;
    }

    private static int[] zipfSequence(int bound) {
        double[] cdf = new double[bound];
        double sum = 0d;
        for (int rank = 1; rank <= bound; rank++) sum += 1d / Math.pow(rank, .99d);
        double running = 0d;
        for (int rank = 1; rank <= bound; rank++) {
            running += 1d / Math.pow(rank, .99d) / sum;
            cdf[rank - 1] = running;
        }
        int[] sequence = new int[1 << 16];
        long seed = 7L;
        for (int i = 0; i < sequence.length; i++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            double sample = (seed >>> 11) * 0x1.0p-53d;
            int position = Arrays.binarySearch(cdf, sample);
            sequence[i] = position >= 0 ? position : -position - 1;
        }
        return sequence;
    }

    private static long firstLong(byte[] value) {
        return ((long) value[0] & 0xffL)
             | (((long) value[1] & 0xffL) << 8)
             | (((long) value[2] & 0xffL) << 16)
             | (((long) value[3] & 0xffL) << 24)
             | (((long) value[4] & 0xffL) << 32)
             | (((long) value[5] & 0xffL) << 40)
             | (((long) value[6] & 0xffL) << 48)
             | (((long) value[7] & 0xffL) << 56);
    }
}
