package com.red.ohc.jmh;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.red.ohc.AllocatorType;
import com.red.ohc.EncodedKey;
import com.red.ohc.index.Entry;
import com.red.ohc.index.FlatConcurrentMap;
import com.red.ohc.storage.NativeMemory;
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

/**
 * Fair index-only comparison. Both maps store the same native-key Entries and use distinct,
 * preallocated Entry probes, so serializer, allocator, timer, policy and value work are absent.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 10)
@Measurement(iterations = 10, time = 10)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class FlatIndexBenchmark {
    private static final int WORKING_SET = 52_428; // 80% of FlatConcurrentMap's 65,536-slot table
    private static final int SEQUENCE_MASK = (1 << 16) - 1;

    @Param({"16", "64"}) public int keyBytes;
    @Param({"UNIFORM", "ZIPF_099"}) public String distribution;

    private NativeMemory.Memory memory;
    private long[] keyAddresses;
    private int addressCount;
    private Entry[] entries;
    private Entry[] probes;
    private Entry[] misses;
    private int[] accessSequence;
    private FlatConcurrentMap flat;
    private ConcurrentHashMap<Entry, Entry> chm;

    @Setup(Level.Trial)
    public void setup() {
        memory = new NativeMemory.Memory(AllocatorType.JNA);
        keyAddresses = new long[WORKING_SET * 3];
        entries = new Entry[WORKING_SET];
        probes = new Entry[WORKING_SET];
        misses = new Entry[WORKING_SET];
        for (int index = 0; index < WORKING_SET; index++) {
            byte[] key = bytes(keyBytes, index);
            EncodedKey encoded = EncodedKey.copyOf(key);
            entries[index] = entry(key, encoded);
            probes[index] = entry(key, encoded);

            byte[] missKey = bytes(keyBytes, index + WORKING_SET);
            EncodedKey miss = EncodedKey.copyOf(missKey);
            misses[index] = entry(missKey, miss);
        }
        accessSequence = "ZIPF_099".equals(distribution)
                ? zipfSequence(WORKING_SET) : uniformSequence(WORKING_SET);
        flat = new FlatConcurrentMap(WORKING_SET, 0L);
        chm = new ConcurrentHashMap<>(WORKING_SET);
        for (Entry entry : entries) {
            if (flat.putIfAbsent(entry, entry) != null) throw new IllegalStateException("flat preload duplicate");
            if (chm.putIfAbsent(entry, entry) != null) throw new IllegalStateException("CHM preload duplicate");
        }
        while (flat.helpResize(1_024) != 0) {
            // Preload must not leave resize migration inside measured operations.
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (flat != null) flat.clear();
        if (chm != null) chm.clear();
        if (memory != null) {
            for (int index = 0; index < addressCount; index++) memory.free(keyAddresses[index], keyBytes);
            memory.closeArenas();
        }
    }

    @Benchmark @Threads(1)
    public Entry flatHitOneThread(Cursor cursor) {
        return flat.get(probes[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(Threads.MAX)
    public Entry flatHitCpuThreads(Cursor cursor) {
        return flat.get(probes[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(1)
    public Entry chmHitOneThread(Cursor cursor) {
        return chm.get(probes[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(Threads.MAX)
    public Entry chmHitCpuThreads(Cursor cursor) {
        return chm.get(probes[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(1)
    public Entry flatMissOneThread(Cursor cursor) {
        return flat.get(misses[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(Threads.MAX)
    public Entry flatMissCpuThreads(Cursor cursor) {
        return flat.get(misses[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(1)
    public Entry chmMissOneThread(Cursor cursor) {
        return chm.get(misses[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(Threads.MAX)
    public Entry chmMissCpuThreads(Cursor cursor) {
        return chm.get(misses[cursor.next(accessSequence)]);
    }

    @Benchmark @Threads(1)
    public boolean flatConditionalRemoveAndReinsertOneThread(Cursor cursor) {
        return removeAndReinsertFlat(cursor.next(accessSequence));
    }

    @Benchmark @Threads(Threads.MAX)
    public boolean flatConditionalRemoveAndReinsertCpuThreads(Cursor cursor) {
        return removeAndReinsertFlat(cursor.next(accessSequence));
    }

    @Benchmark @Threads(1)
    public boolean chmConditionalRemoveAndReinsertOneThread(Cursor cursor) {
        return removeAndReinsertChm(cursor.next(accessSequence));
    }

    @Benchmark @Threads(Threads.MAX)
    public boolean chmConditionalRemoveAndReinsertCpuThreads(Cursor cursor) {
        return removeAndReinsertChm(cursor.next(accessSequence));
    }

    private boolean removeAndReinsertFlat(int index) {
        Entry probe = probes[index];
        Entry entry = entries[index];
        boolean removed = flat.remove(probe, entry);
        if (removed && flat.putIfAbsent(entry, entry) != null) {
            throw new IllegalStateException("flat conditional reinsertion lost its mapping");
        }
        return removed;
    }

    private boolean removeAndReinsertChm(int index) {
        Entry probe = probes[index];
        Entry entry = entries[index];
        boolean removed = chm.remove(probe, entry);
        if (removed && chm.putIfAbsent(entry, entry) != null) {
            throw new IllegalStateException("CHM conditional reinsertion lost its mapping");
        }
        return removed;
    }

    private Entry entry(byte[] bytes, EncodedKey encoded) {
        long address = memory.allocate(keyBytes);
        NativeMemory.copy(bytes, 0, address, keyBytes);
        keyAddresses[addressCount++] = address;
        return new Entry(address, keyBytes, encoded.hash(), 0L);
    }

    private static byte[] bytes(int length, int seed) {
        byte[] bytes = new byte[length];
        long value = seed * 0x9e3779b97f4a7c15L;
        for (int index = 0; index < length; index++) {
            value ^= value >>> 12;
            value ^= value << 25;
            value ^= value >>> 27;
            bytes[index] = (byte) value;
        }
        return bytes;
    }

    private static int[] uniformSequence(int bound) {
        int[] sequence = new int[SEQUENCE_MASK + 1];
        long seed = 1L;
        for (int index = 0; index < sequence.length; index++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            sequence[index] = (int) Long.remainderUnsigned(seed, bound);
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
        int[] sequence = new int[SEQUENCE_MASK + 1];
        long seed = 7L;
        for (int index = 0; index < sequence.length; index++) {
            seed ^= seed << 13;
            seed ^= seed >>> 7;
            seed ^= seed << 17;
            double sample = (seed >>> 11) * 0x1.0p-53d;
            int position = Arrays.binarySearch(cdf, sample);
            sequence[index] = position >= 0 ? position : -position - 1;
        }
        return sequence;
    }

    @State(Scope.Thread)
    public static class Cursor {
        private int cursor;

        int next(int[] sequence) {
            return sequence[cursor++ & SEQUENCE_MASK];
        }
    }
}
