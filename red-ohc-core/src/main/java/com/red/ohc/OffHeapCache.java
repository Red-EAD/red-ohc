package com.red.ohc;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.codec.KeyEncoder;
import com.red.ohc.index.Entry;
import com.red.ohc.index.FlatConcurrentMap;
import com.red.ohc.index.FlatIndexStats;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/** Flat concurrent authority with native payloads and one asynchronous maintenance worker. */
public final class OffHeapCache<K, V> implements OHCache<K, V> {
    private static final long DEFAULT_TTL = Long.MIN_VALUE;

    final FlatConcurrentMap data;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;
    private final Ticker ticker;
    private final long defaultTtlMillis;
    private final double ttlJitterPercent;
    private final java.util.concurrent.Executor loaderExecutor;
    private final long closeTimeoutMillis;
    private final long capacity;
    private final long maxEntrySize;
    private final NativeMemory.Memory memory;
    private final Budget budget;
    private final CopyOnWriteArrayList<ReaderSlot> readers = new CopyOnWriteArrayList<>();
    private final ThreadLocal<ThreadContext> contexts = ThreadLocal.withInitial(ThreadContext::new);
    private final MaintenanceEventLoop worker;
    private final ReaderGuard readerGuard;
    private final AtomicLong liveEntryBytes = new AtomicLong();
    private final int initialCapacity;
    private final int tableCapacity;
    private volatile boolean closing;
    private volatile boolean closed;

    OffHeapCache(CacheSerializer<K> keySerializer, CacheSerializer<V> valueSerializer,
                   long defaultTtlMillis, double ttlJitterPercent,
                   java.util.concurrent.Executor loaderExecutor, long closeTimeoutMillis,
                   AllocatorType allocatorType, Ticker ticker, Eviction eviction,
                   long capacity, long maxEntrySize, long expectedEntries) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.ticker = ticker;
        this.defaultTtlMillis = defaultTtlMillis;
        this.ttlJitterPercent = ttlJitterPercent;
        this.loaderExecutor = loaderExecutor;
        this.closeTimeoutMillis = closeTimeoutMillis;
        this.capacity = capacity;
        this.maxEntrySize = maxEntrySize;
        long initialEntries = estimatedIndexEntries(expectedEntries, capacity, maxEntrySize);
        this.data = new FlatConcurrentMap(initialEntries);
        FlatIndexStats indexStats = data.snapshot();
        this.initialCapacity = indexStats.slotCapacity;
        this.tableCapacity = indexStats.slotCapacity;
        this.memory = new NativeMemory.Memory(allocatorType);
        this.budget = new Budget(capacity);
        this.worker = new MaintenanceEventLoop(data, memory, budget, ticker, capacity,
                                             eviction, maintenanceQueueCapacity(initialEntries), liveEntryBytes, readers);
        this.readerGuard = new ReaderGuard(worker, () -> closing);
        worker.start();
    }

    @Override
    public boolean put(K key, V value) {
        return putInternal(key, value, DEFAULT_TTL);
    }

    @Override
    public boolean put(K key, V value, long expireAtMillis) {
        return putInternal(key, value, expireAtMillis <= 0L ? 0L : expireAtMillis);
    }

    private boolean putInternal(K key, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (closing) return false;
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        int valueLength = encode(valueSerializer, value, context.valueBytes, context, false);
        return putSerialized(context, context.lookupKey, context.keyBytes, keyLength,
                             context.valueBytes, valueLength, expireAtMillis);
    }

    /** Encoded benchmark path: neither serializer is touched and the precomputed int hash is reused. */
    boolean putEncoded(EncodedKey key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (closing) return false;
        ThreadContext context = contexts.get();
        context.lookupKey.setPrecomputed(key.bytes(), key.length(), key.hash());
        return putSerialized(context, context.lookupKey, key.bytes(), key.length(), value, value.length,
                             DEFAULT_TTL);
    }

    private boolean putSerialized(ThreadContext context, LookupKey lookup, byte[] keyBytes, int keyLength,
                                  byte[] valueBytes, int valueLength, long requestedExpiry) {
        if (keyLength < 0 || valueLength < 0 || (long) keyLength + valueLength > maxEntrySize) return false;
        long expireAtMillis = requestedExpiry == DEFAULT_TTL
                ? defaultExpiry(lookup.hash()) : requestedExpiry;
        for (;;) {
            Entry existing;
            if (!enter(context)) return false;
            try {
                existing = data.get(lookup);
            } finally {
                exit(context);
            }
            if (existing != null) {
                int result = replaceExisting(context, lookup, existing, valueBytes, valueLength, expireAtMillis);
                if (result > 0) return true;
                if (result < 0) return false;
                continue;
            }
            Entry candidate = allocateEntry(context, lookup.hash(), keyBytes, keyLength,
                                             valueBytes, valueLength, expireAtMillis);
            if (candidate == null) return false;
            if (!enter(context)) {
                freeEntry(candidate, ValueBlock.allocationLength(valueLength));
                return false;
            }
            Entry winner;
            try {
                winner = data.putIfAbsent(candidate, candidate);
            } finally {
                exit(context);
            }
            if (winner == null) {
                liveEntryBytes.addAndGet(candidate.keyAllocationLength()
                        + ValueBlock.allocationLength(valueLength));
                worker.recordAccepted();
                worker.enqueue(candidate, Entry.PENDING_UPDATE);
                return true;
            }
            freeEntry(candidate, ValueBlock.allocationLength(valueLength));
            existing = winner;
            int result = replaceExisting(context, lookup, existing, valueBytes, valueLength, expireAtMillis);
            if (result > 0) return true;
            if (result < 0) return false;
        }
    }

    private int replaceExisting(ThreadContext context, LookupKey lookup, Entry entry,
                                byte[] valueBytes, int valueLength, long expireAtMillis) {
        long newAllocation = ValueBlock.allocationLength(valueLength);
        if (!budget.reserve(newAllocation)) {
            worker.recordBudgetRejected();
            return -1;
        }
        long replacement = 0L;
        boolean locked = false;
        boolean published = false;
        try {
            replacement = context.writer(memory).allocate(newAllocation);
            ValueBlock.initialize(replacement, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(replacement), valueLength);
            if (!worker.prepareRetirement(1)) {
                freeBlock(replacement, newAllocation);
                budget.refund(newAllocation);
                worker.recordBudgetRejected();
                return -1;
            }
            if (!claimWriter(entry)) {
                freeBlock(replacement, newAllocation);
                budget.refund(newAllocation);
                return -1;
            }
            locked = true;
            if (!mappingIsCurrent(context, lookup, entry)) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, newAllocation);
                budget.refund(newAllocation);
                return 0;
            }
            long old = Entry.rawValueAddress(entry.valueAddress);
            long oldAllocation = old == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(old));
            entry.valueAddress = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
            published = true;
            entry.finishWriter();
            locked = false;
            if (old != 0L) worker.retireValue(old, oldAllocation);
            worker.enqueue(entry, Entry.PENDING_UPDATE);
            liveEntryBytes.addAndGet(newAllocation - oldAllocation);
            worker.recordAccepted();
            return 1;
        } catch (Throwable failure) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                freeBlock(replacement, newAllocation);
                budget.refund(newAllocation);
            }
            worker.markUnhealthy();
            return -1;
        }
    }

    private Entry allocateEntry(ThreadContext context, long hash, byte[] keyBytes, int keyLength,
                                byte[] valueBytes, int valueLength, long expireAtMillis) {
        long keyAllocation = Math.max(8L, CacheMath.roundUpTo8(keyLength));
        long valueAllocation = ValueBlock.allocationLength(valueLength);
        long total = keyAllocation + valueAllocation;
        if (!budget.reserve(total)) {
            worker.recordBudgetRejected();
            return null;
        }
        long keyAddress = 0L;
        long valueAddress = 0L;
        try {
            WriterArena arena = context.writer(memory);
            keyAddress = arena.allocate(keyAllocation);
            NativeMemory.copy(keyBytes, 0, keyAddress, keyLength);
            valueAddress = arena.allocate(valueAllocation);
            ValueBlock.initialize(valueAddress, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(valueAddress), valueLength);
            return new Entry(keyAddress, keyLength, hash, Entry.tagValueAddress(valueAddress, expireAtMillis > 0L));
        } catch (Throwable failure) {
            if (valueAddress != 0L) freeBlock(valueAddress, valueAllocation);
            if (keyAddress != 0L) freeBlock(keyAddress, keyAllocation);
            budget.refund(total);
            throw failure;
        }
    }

    @Override
    public boolean remove(K key) {
        Objects.requireNonNull(key, "key");
        if (closing) return false;
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        for (;;) {
            if (!enter(context)) return false;
            Entry entry;
            try {
                entry = data.get(context.lookupKey);
            } finally {
                exit(context);
            }
            if (entry == null) return false;
            if (!claimWriter(entry)) return false;
            if (!worker.prepareRetirement(2)) {
                entry.finishWriter();
                worker.recordBudgetRejected();
                return false;
            }
            boolean removed;
            if (!enter(context)) {
                entry.finishWriter();
                return false;
            }
            try {
                removed = data.removeIfSame(entry);
            } finally {
                exit(context);
            }
            if (removed) {
                long value = Entry.rawValueAddress(entry.valueAddress);
                long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
                entry.valueAddress = 0L;
                entry.finishWriter();
                if (value != 0L) worker.retireValue(value, valueAllocation);
                worker.retireValue(entry.nativeKeyAddress, entry.keyAllocationLength());
                liveEntryBytes.addAndGet(-(entry.keyAllocationLength() + valueAllocation));
                worker.enqueue(entry, Entry.PENDING_REMOVE);
                return true;
            }
            entry.finishWriter();
        }
    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key");
        if (closing) return null;
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        if (!enter(context)) return null;
        try {
            Entry entry = data.get(context.lookupKey);
            long value = valueIfLive(entry);
            if (value == 0L) {
                miss(context);
                return null;
            }
            hit(context, entry);
            int length = ValueBlock.length(value);
            context.ensureValue(length);
            NativeMemory.copy(ValueBlock.payloadAddress(value), context.valueBytes, 0, length);
            return valueSerializer.deserialize(context.valueBuffer(length));
        } finally {
            exit(context);
        }
    }

    @Override
    public boolean containsKey(K key) {
        Objects.requireNonNull(key, "key");
        if (closing) return false;
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        if (!enter(context)) return false;
        try {
            Entry entry = data.get(context.lookupKey);
            if (valueIfLive(entry) == 0L) { miss(context); return false; }
            hit(context, entry);
            return true;
        } finally {
            exit(context);
        }
    }

    @Override
    public boolean withDirectValue(K key, DirectValueConsumer consumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(consumer, "consumer");
        if (closing) return false;
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        return withDirect(context, consumer);
    }

    @Override
    public boolean withDirectValue(EncodedKey key, DirectValueConsumer consumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(consumer, "consumer");
        if (closing) return false;
        ThreadContext context = contexts.get();
        if (!enter(context)) return false;
        try {
            return withDirectEntered(context, data.get(key), consumer);
        } finally {
            exit(context);
        }
    }

    private boolean withDirect(ThreadContext context, DirectValueConsumer consumer) {
        if (!enter(context)) return false;
        try {
            return withDirectEntered(context, data.get(context.lookupKey), consumer);
        } finally {
            exit(context);
        }
    }

    private boolean withDirectEntered(ThreadContext context, Entry entry, DirectValueConsumer consumer) {
        long value = valueIfLive(entry);
        if (value == 0L) { miss(context); return false; }
        hit(context, entry);
        context.valueView.reset(ValueBlock.payloadAddress(value), ValueBlock.length(value));
        try {
            consumer.accept(context.valueView);
            return true;
        } finally {
            context.valueView.reset(0L, 0);
        }
    }

    @Override
    public int putAll(Map<? extends K, ? extends V> entries) {
        Objects.requireNonNull(entries, "entries");
        int accepted = 0;
        for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
            if (put(entry.getKey(), entry.getValue())) accepted++;
        }
        return accepted;
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {
        Objects.requireNonNull(keys, "keys");
        int expected = Math.max(1, keys.size());
        Map<K, V> result = new HashMap<>(expected);
        ObjectOpenHashSet<K> unique = new ObjectOpenHashSet<>(expected);
        if (closing) return result;
        ThreadContext context = contexts.get();
        boolean guarded = false;
        int inEpoch = 0;
        try {
            for (K key : keys) {
                Objects.requireNonNull(key, "key");
                if (!unique.add(key)) continue;
                if (inEpoch == 512) {
                    exit(context);
                    guarded = false;
                    inEpoch = 0;
                    if (closing) break;
                }
                if (!guarded) {
                    if (!enter(context)) break;
                    guarded = true;
                }
                int keyLength = KeyEncoder.encode(keySerializer, key, context);
                context.lookupKey.set(context.keyBytes, keyLength);
                Entry entry = data.get(context.lookupKey);
                long value = valueIfLive(entry);
                if (value == 0L) {
                    miss(context);
                } else {
                    hit(context, entry);
                    int length = ValueBlock.length(value);
                    context.ensureValue(length);
                    NativeMemory.copy(ValueBlock.payloadAddress(value), context.valueBytes, 0, length);
                    result.put(key, valueSerializer.deserialize(context.valueBuffer(length)));
                }
                inEpoch++;
            }
        } finally {
            if (guarded) exit(context);
        }
        return result;
    }

    @Override
    public int removeAll(Collection<? extends K> keys) {
        Objects.requireNonNull(keys, "keys");
        int removed = 0;
        for (K key : keys) if (remove(key)) removed++;
        return removed;
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(K key, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (closing) return CompletableFuture.completedFuture(false);
        ThreadContext context = contexts.get();
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        int valueLength = encode(valueSerializer, value, context.valueBytes, context, false);
        return CompletableFuture.completedFuture(putIfAbsentSerialized(context, context.lookupKey,
                context.keyBytes, keyLength, context.valueBytes, valueLength, expireAtMillis));
    }

    private boolean putIfAbsentSerialized(ThreadContext context, LookupKey lookup, byte[] keyBytes, int keyLength,
                                          byte[] valueBytes, int valueLength, long expireAtMillis) {
        if (keyLength < 0 || valueLength < 0 || (long) keyLength + valueLength > maxEntrySize) return false;
        for (;;) {
            Entry current;
            boolean live;
            if (!enter(context)) return false;
            try {
                current = data.get(lookup);
                live = valueIfLive(current) != 0L;
            } finally {
                exit(context);
            }
            if (current != null) {
                if (live) return false;
                int removal = removeExpiredEntry(context, lookup, current,
                        current.generation(), current.valueAddress);
                if (removal < 0) return false;
                continue;
            }
            Entry candidate = allocateEntry(context, lookup.hash(), keyBytes, keyLength,
                                             valueBytes, valueLength, expireAtMillis);
            if (candidate == null) return false;
            if (!enter(context)) {
                freeEntry(candidate, ValueBlock.allocationLength(valueLength));
                return false;
            }
            Entry winner;
            try {
                winner = data.putIfAbsent(candidate, candidate);
            } finally {
                exit(context);
            }
            if (winner == null) {
                liveEntryBytes.addAndGet(candidate.keyAllocationLength()
                        + ValueBlock.allocationLength(valueLength));
                worker.recordAccepted();
                worker.enqueue(candidate, Entry.PENDING_UPDATE);
                return true;
            }
            freeEntry(candidate, ValueBlock.allocationLength(valueLength));
            if (!enter(context)) return false;
            boolean winnerLive;
            try {
                winnerLive = valueIfLive(winner) != 0L;
            } finally {
                exit(context);
            }
            if (winnerLive) return false;
        }
    }

    private int removeExpiredEntry(ThreadContext context, LookupKey lookup, Entry entry,
                                   long expectedGeneration, long expectedValueAddress) {
        if (!claimWriter(entry)) return -1;
        if (!worker.prepareRetirement(2)) {
            entry.finishWriter();
            worker.recordBudgetRejected();
            return -1;
        }
        boolean removed = false;
        long value = 0L;
        if (!enter(context)) {
            entry.finishWriter();
            return -1;
        }
        try {
            long taggedValue = entry.valueAddress;
            value = Entry.rawValueAddress(taggedValue);
            removed = entry.generation() == expectedGeneration
                    && taggedValue == expectedValueAddress
                    && value != 0L
                    && Entry.hasTtl(taggedValue)
                    && ValueBlock.expired(value, worker.nowMillis())
                    && data.removeIfSame(entry);
        } finally {
            exit(context);
        }
        if (removed) {
            long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
            entry.valueAddress = 0L;
            entry.finishWriter();
            if (value != 0L) worker.retireValue(value, valueAllocation);
            worker.retireValue(entry.nativeKeyAddress, entry.keyAllocationLength());
            liveEntryBytes.addAndGet(-(entry.keyAllocationLength() + valueAllocation));
            worker.enqueue(entry, Entry.PENDING_REMOVE);
            return 1;
        } else {
            entry.finishWriter();
            return 0;
        }
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(K key, V expected, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(value, "value");
        if (closing) return CompletableFuture.completedFuture(false);
        ThreadContext context = contexts.get();
        int expectedLength = encode(valueSerializer, expected, context.valueBytes, context, false);
        byte[] expectedBytes = Arrays.copyOf(context.valueBytes, expectedLength);
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        context.lookupKey.set(context.keyBytes, keyLength);
        Entry entry;
        boolean live;
        if (!enter(context)) return CompletableFuture.completedFuture(false);
        try {
            entry = data.get(context.lookupKey);
            live = valueIfLive(entry) != 0L;
        } finally {
            exit(context);
        }
        if (entry == null || !live) return CompletableFuture.completedFuture(false);
        int valueLength = encode(valueSerializer, value, context.valueBytes, context, false);
        return CompletableFuture.completedFuture(replaceExpected(context, context.lookupKey, entry, expectedBytes,
                valueLength, context.valueBytes, expireAtMillis));
    }

    private boolean replaceExpected(ThreadContext context, LookupKey lookup, Entry entry, byte[] expected,
                                    int valueLength, byte[] valueBytes, long expireAtMillis) {
        long allocation = ValueBlock.allocationLength(valueLength);
        if (!budget.reserve(allocation)) { worker.recordBudgetRejected(); return false; }
        long replacement = 0L;
        boolean locked = false;
        boolean published = false;
        try {
            replacement = context.writer(memory).allocate(allocation);
            ValueBlock.initialize(replacement, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(replacement), valueLength);
            if (!worker.prepareRetirement(1)) {
                freeBlock(replacement, allocation);
                budget.refund(allocation);
                worker.recordBudgetRejected();
                return false;
            }
            if (!claimWriter(entry)) {
                freeBlock(replacement, allocation);
                budget.refund(allocation);
                return false;
            }
            locked = true;
            long oldTagged = entry.valueAddress;
            long old = Entry.rawValueAddress(oldTagged);
            boolean match = mappingIsCurrent(context, lookup, entry) && old != 0L
                    && (!Entry.hasTtl(oldTagged) || !ValueBlock.expired(old, worker.nowMillis()))
                    && ValueBlock.length(old) == expected.length
                    && NativeMemory.equals(ValueBlock.payloadAddress(old), expected, 0, expected.length);
            if (!match) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, allocation);
                budget.refund(allocation);
                return false;
            }
            long oldAllocation = ValueBlock.allocationLength(ValueBlock.length(old));
            entry.valueAddress = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
            published = true;
            entry.finishWriter();
            locked = false;
            worker.retireValue(old, oldAllocation);
            worker.enqueue(entry, Entry.PENDING_UPDATE);
            liveEntryBytes.addAndGet(allocation - oldAllocation);
            worker.recordAccepted();
            return true;
        } catch (Throwable failure) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                freeBlock(replacement, allocation);
                budget.refund(allocation);
            }
            worker.markUnhealthy();
            return false;
        }
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(K key) {
        return CompletableFuture.completedFuture(remove(key));
    }

    @Override
    public CompletableFuture<V> getOrLoadAsync(K key, CacheLoader<K, V> loader, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        V existing = get(key);
        if (existing != null) return CompletableFuture.completedFuture(existing);
        if (loaderExecutor == null) {
            CompletableFuture<V> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("loaderExecutor is not configured"));
            return failed;
        }
        return CompletableFuture.supplyAsync(() -> {
            try { return loader.load(key); }
            catch (Exception e) { throw new java.util.concurrent.CompletionException(e); }
        }, loaderExecutor).thenCompose(value -> {
            if (value == null) return CompletableFuture.completedFuture(null);
            return putIfAbsentAsync(key, value, expireAtMillis).thenApply(ignored -> value);
        });
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        if (closed) return CompletableFuture.completedFuture(null);
        return worker.flush();
    }

    @Override public long size() { return data.size(); }
    @Override public long capacity() { return capacity; }
    @Override public long totalAllocatedBytes() { return memory.allocated(); }
    long rawNativeAllocations() { return memory.rawAllocationCount(); }
    long entryNativeAllocations() { return memory.entryAllocationCount(); }
    boolean mutationBacklogExceeds() {
        long watermark = worker.queueCapacity() - Math.max(1L, worker.queueCapacity() / 8L);
        return worker.queueDepth() > watermark || budget.reserved() > capacity - capacity / 8L;
    }
    FlatConcurrentMap indexForTest() { return data; }

    @Override
    public OHCacheStats stats() {
        final long unavailable = -1L;
        long pending = budget.reserved();
        MaintenanceEventLoop.Snapshot snapshot = worker.snapshot();
        FlatIndexStats indexStats = data.snapshot();
        return new OHCacheStats(snapshot.hits, snapshot.misses, snapshot.accessDropped,
                snapshot.accepted, snapshot.rejectedQueue, snapshot.rejectedBudget, snapshot.applied, 0L,
                snapshot.queueDepth, unavailable, pending, snapshot.maintenanceLoopNanos, snapshot.unhealthy,
                initialCapacity, tableCapacity == 0 ? 0d : (double) data.size() / tableCapacity,
                indexStats.maxProbe, indexStats.resizeInProgress, indexStats.heapPayloadBytes,
                indexStats.overflowSize, indexStats.fallbackHeapBytes,
                snapshot.logicalExpired, snapshot.physicalExpired, snapshot.timeoutLagMillis,
                snapshot.evicted, unavailable, unavailable, unavailable, unavailable, unavailable,
                snapshot.retiredEntries, snapshot.retiredBytes, snapshot.oldestRetireEpoch,
                liveEntryBytes.get(), snapshot.queueCapacity * Long.BYTES,
                snapshot.timerBytes, snapshot.sketchBytes, snapshot.ledgerBytes, memory.allocated());
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closing = true;
        worker.stop();
        long timeout = Math.max(1L, closeTimeoutMillis);
        try {
            worker.join(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing", e);
        }
        if (worker.isAlive()) throw new IllegalStateException("close timed out with active readers");
        closed = true;
    }

    private boolean enter(ThreadContext context) {
        return readerGuard.enter(context);
    }

    private void exit(ThreadContext context) {
        readerGuard.exit(context);
    }

    private boolean mappingIsCurrent(ThreadContext context, LookupKey lookup, Entry entry) {
        return data.isCurrent(entry);
    }

    private boolean claimWriter(Entry entry) {
        while (!entry.claimWriter()) {
            if (closing) return false;
            LockSupport.parkNanos(this, 1_000L);
        }
        return true;
    }

    private static void hit(ThreadContext context, Entry entry) {
        long sequence = context.hit();
        if ((sequence & 15L) == 0L && !context.slot.access.offer(entry, entry.generation())) context.dropped();
        context.finishRead(sequence);
    }

    private static void miss(ThreadContext context) {
        long sequence = context.miss();
        context.finishRead(sequence);
    }

    private long valueIfLive(Entry entry) {
        if (entry == null) return 0L;
        long taggedValue = entry.valueAddress;
        if (taggedValue == 0L) return 0L;
        long value = Entry.rawValueAddress(taggedValue);
        if (!Entry.hasTtl(taggedValue)) return value;
        return ValueBlock.expired(value, worker.nowMillis()) ? 0L : value;
    }

    private long defaultExpiry(long hash) {
        if (defaultTtlMillis <= 0L) return 0L;
        long now = ticker.currentTimeMillis();
        long jitterRange = (long) (defaultTtlMillis * ttlJitterPercent);
        if (jitterRange > 0L) {
            long span = jitterRange > (Long.MAX_VALUE - 1L) / 2L
                    ? Long.MAX_VALUE : jitterRange * 2L + 1L;
            long mixed = mix64(hash);
            long jitter = Math.floorMod(mixed, span) - jitterRange;
            if (jitter > 0L && defaultTtlMillis > Long.MAX_VALUE - jitter) {
                return Long.MAX_VALUE;
            }
            long duration = defaultTtlMillis + jitter;
            return now > Long.MAX_VALUE - duration ? Long.MAX_VALUE : now + duration;
        }
        return Long.MAX_VALUE - now < defaultTtlMillis ? Long.MAX_VALUE : now + defaultTtlMillis;
    }

    private static long estimatedIndexEntries(long expectedEntries, long capacity, long maxEntrySize) {
        if (expectedEntries > 0L) return Math.max(64L, expectedEntries);
        long divisor = Math.max(1L, maxEntrySize + Math.min(128L, Long.MAX_VALUE - maxEntrySize));
        return Math.max(64L, Math.max(1L, capacity / divisor));
    }

    private static int maintenanceQueueCapacity(long entries) {
        long requested = Math.max(1024L, Math.min(1L << 20, Math.max(1L, entries / 64L)));
        int capacity = 1;
        while (capacity < requested && capacity < (1 << 20)) capacity <<= 1;
        return capacity;
    }

    private static long mix64(long value) {
        value += 0x9e3779b97f4a7c15L;
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private int encode(CacheSerializer serializer, Object value, byte[] existing,
                       ThreadContext context, boolean key) {
        int length = serializer.serializedSize(value);
        if (length < 0) throw new IllegalArgumentException("negative serialized length");
        if (key) context.ensureKey(length); else context.ensureValue(length);
        ByteBuffer buffer = key ? context.keyBuffer(length) : context.valueBuffer(length);
        serializer.serialize(value, buffer);
        return length;
    }

    private void freeEntry(Entry entry, long valueAllocation) {
        freeBlock(Entry.rawValueAddress(entry.valueAddress), valueAllocation);
        freeBlock(entry.nativeKeyAddress, entry.keyAllocationLength());
        budget.refund(entry.keyAllocationLength() + valueAllocation);
    }

    private void freeBlock(long address, long allocation) {
        if (address != 0L) memory.releaseEntry(address, allocation);
    }
}
