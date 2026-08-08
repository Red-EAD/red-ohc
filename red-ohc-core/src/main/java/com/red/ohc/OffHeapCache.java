package com.red.ohc;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.codec.KeyEncoder;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/** CHM authority with native payloads and one asynchronous maintenance worker. */
public final class OffHeapCache<K, V> implements OHCache<K, V> {
    private static final long DEFAULT_TTL = Long.MIN_VALUE;
    private static final int BULK_BATCH_SIZE = 512;
    private static final int OPEN = 0;
    private static final int CLOSING = 1;
    private static final int CLOSED = 2;

    final ConcurrentHashMap<Entry, Entry> data;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;
    private final Ticker ticker;
    private final long defaultTtlMillis;
    private final double ttlJitterPercent;
    private final Executor loaderExecutor;
    private final long closeTimeoutMillis;
    private final long capacity;
    private final long maxEntrySize;
    private final long residentHardLimit;
    private final long nativeHardLimit;
    private final NativeMemory.Memory memory;
    private final Budget budget;
    private final ReaderRegistry readers = new ReaderRegistry();
    private final ThreadLocal<ThreadContext> contexts;
    private final MaintenanceEventLoop worker;
    private final ReaderGuard readerGuard;
    private final AtomicInteger closeState = new AtomicInteger(OPEN);
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final AtomicReference<Thread> closeLeader = new AtomicReference<>();
    private volatile Thread closeWaiter;
    private volatile boolean closing;

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
        long maxValueWeight = allocationWeight(ValueBlock.allocationLength((int) Math.min(maxEntrySize, Integer.MAX_VALUE - 16L)));
        long headroom = Math.max(maxValueWeight, capacity / 8L);
        this.residentHardLimit = saturatedAdd(capacity, headroom);
        long allocatorSlack = Math.min(8L << 20, Math.max(256L << 10, capacity / 16L));
        this.nativeHardLimit = saturatedAdd(saturatedAdd(residentHardLimit, allocatorSlack), 512L << 10);
        int initialCapacity = ChmSizing.constructorCapacity(expectedEntries, capacity, maxEntrySize);
        this.data = new ConcurrentHashMap<>(initialCapacity, 0.75f, 1);
        Entry bootstrap = Entry.bootstrap();
        data.put(bootstrap, bootstrap);
        data.remove(bootstrap, bootstrap);
        this.memory = new NativeMemory.Memory(allocatorType, nativeHardLimit);
        this.budget = new Budget(residentHardLimit);
        this.contexts = ThreadLocal.withInitial(() -> new ThreadContext(
                memory.writerForCurrentThread(), budget.stripeForCurrentThread()));
        this.worker = new MaintenanceEventLoop(data, memory, budget, ticker, capacity,
                                             eviction, readers);
        this.readerGuard = new ReaderGuard(worker, this::isClosing);
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
        ThreadContext context = enterWriter();
        if (context == null) return false;
        try {
            boolean accepted = putOne(context, key, value, expireAtMillis);
            finishWrite(context, accepted);
            return accepted;
        } finally {
            exitWriter(context);
        }
    }

    private boolean putOne(ThreadContext context, K key, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        int keyLength = KeyEncoder.encode(keySerializer, key, context);
        int valueLength = encode(valueSerializer, value, context.valueBytes, context, false);
        return putSerialized(context, context.lookupKey, context.keyBytes, keyLength,
                             context.valueBytes, valueLength, expireAtMillis);
    }

    private void finishWrite(ThreadContext context, boolean accepted) {
        if (!accepted) return;
        worker.recordAccepted();
        worker.afterWrite(context);
    }

    /** Encoded benchmark path: neither serializer is touched and the precomputed int hash is reused. */
    boolean putEncoded(EncodedKey key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        ThreadContext context = enterWriter();
        if (context == null) return false;
        try {
            context.lookupKey.setPrecomputed(key.bytes, key.length(), key.hash64());
            return putSerialized(context, context.lookupKey, key.bytes, key.length(), value, value.length,
                                 DEFAULT_TTL);
        } finally {
            exitWriter(context);
        }
    }

    private boolean putSerialized(ThreadContext context, LookupKey lookup, byte[] keyBytes, int keyLength,
                                  byte[] valueBytes, int valueLength, long requestedExpiry) {
        if (keyLength < 0 || valueLength < 0 || (long) keyLength + valueLength > maxEntrySize) return false;
        long expireAtMillis = requestedExpiry == DEFAULT_TTL
                ? defaultExpiry(lookup.hash64()) : requestedExpiry;
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
            Entry candidate = allocateEntry(context, lookup.hash(), lookup.hash64(), keyBytes, keyLength,
                                             valueBytes, valueLength, expireAtMillis);
            if (candidate == null) return false;
            if (!worker.reserveMutation(candidate, Entry.PENDING_ADD)) {
                freeEntry(candidate, ValueBlock.allocationLength(valueLength));
                return false;
            }
            if (!enter(context)) {
                worker.cancelMutation(candidate);
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
                worker.publishMutation(candidate);
                return true;
            }
            worker.cancelMutation(candidate);
            freeEntry(candidate, ValueBlock.allocationLength(valueLength));
            candidate.markDead();
            existing = winner;
            int result = replaceExisting(context, lookup, existing, valueBytes, valueLength, expireAtMillis);
            if (result > 0) return true;
            if (result < 0) return false;
        }
    }

    private int replaceExisting(ThreadContext context, LookupKey lookup, Entry entry,
                                byte[] valueBytes, int valueLength, long expireAtMillis) {
        long newAllocation = ValueBlock.allocationLength(valueLength);
        long newWeight = allocationWeight(newAllocation);
        if (!budget.reserve(context.budgetStripe(), newWeight)) {
            worker.recordBudgetRejected();
            return -1;
        }
        long replacement = 0L;
        boolean locked = false;
        boolean published = false;
        boolean mutationReserved = false;
        try {
            replacement = context.writer().allocate(newAllocation);
            ValueBlock.initialize(replacement, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(replacement), valueLength);
            if (!claimWriter(entry)) {
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
                // A reader may have observed this Entry immediately before the actor retired
                // and removed it from CHM. Retired Entries can never release their writer bit,
                // so retry the lookup rather than treating that stale observation as contention.
                return entry.isAlive() ? -1 : 0;
            }
            locked = true;
            if (!mappingIsCurrent(entry)) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
                return 0;
            }
            long old = Entry.rawValueAddress(entry.valueAddress);
            long oldAllocation = old == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(old));
            long oldWeight = allocationWeight(oldAllocation);
            boolean requiresMutation = maintenanceUpdateRequired(entry, entry.valueAddress,
                    old, oldWeight, newWeight, expireAtMillis);
            if (!worker.prepareRetirement(context, 1)) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
                return -1;
            }
            if (requiresMutation && !worker.reserveMutation(entry, Entry.PENDING_UPDATE)) {
                worker.cancelRetirement(context);
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
                return -1;
            }
            mutationReserved = requiresMutation;
            entry.valueAddress = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
            published = true;
            entry.finishWriter();
            locked = false;
            if (mutationReserved) worker.publishMutation(entry);
            worker.retireValue(context, old, oldAllocation);
            return 1;
        } catch (NativeMemory.AllocationLimitException rejected) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                worker.cancelRetirement(context);
                if (mutationReserved) worker.cancelMutation(entry);
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
            }
            worker.recordBudgetRejected();
            return -1;
        } catch (Throwable failure) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                worker.cancelRetirement(context);
                if (mutationReserved) worker.cancelMutation(entry);
                freeBlock(replacement, newAllocation);
                budget.refund(newWeight);
            }
            worker.markUnhealthy();
            return -1;
        }
    }

    private Entry allocateEntry(ThreadContext context, int hash, long hash64, byte[] keyBytes, int keyLength,
                                byte[] valueBytes, int valueLength, long expireAtMillis) {
        long keyAllocation = Math.max(8L, CacheMath.roundUpTo8((long) keyLength + Long.BYTES));
        long valueAllocation = ValueBlock.allocationLength(valueLength);
        long totalWeight = allocationWeight(keyAllocation) + allocationWeight(valueAllocation);
        if (totalWeight > capacity || !budget.reserve(context.budgetStripe(), totalWeight)) {
            worker.recordBudgetRejected();
            return null;
        }
        long keyAddress = 0L;
        long valueAddress = 0L;
        try {
            WriterArena arena = context.writer();
            keyAddress = arena.allocate(keyAllocation);
            NativeMemory.putLong(keyAddress, hash64);
            NativeMemory.copy(keyBytes, 0, keyAddress + Long.BYTES, keyLength);
            valueAddress = arena.allocate(valueAllocation);
            ValueBlock.initialize(valueAddress, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(valueAddress), valueLength);
            return new Entry(keyAddress, keyLength, hash, hash64,
                    Entry.tagValueAddress(valueAddress, expireAtMillis > 0L));
        } catch (NativeMemory.AllocationLimitException rejected) {
            if (valueAddress != 0L) freeBlock(valueAddress, valueAllocation);
            if (keyAddress != 0L) freeBlock(keyAddress, keyAllocation);
            budget.refund(totalWeight);
            worker.recordBudgetRejected();
            return null;
        } catch (Throwable failure) {
            if (valueAddress != 0L) freeBlock(valueAddress, valueAllocation);
            if (keyAddress != 0L) freeBlock(keyAddress, keyAllocation);
            budget.refund(totalWeight);
            throw failure;
        }
    }

    @Override
    public boolean remove(K key) {
        Objects.requireNonNull(key, "key");
        ThreadContext context = enterWriter();
        if (context == null) return false;
        try {
            KeyEncoder.encode(keySerializer, key, context);
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
                if (!worker.prepareRetirement(context, 2)) {
                    entry.finishWriter();
                    return false;
                }
                if (!worker.reserveMutation(entry, Entry.PENDING_REMOVE)) {
                    worker.cancelRetirement(context);
                    entry.finishWriter();
                    return false;
                }
                boolean removed = removeCurrent(entry);
                if (removed) {
                    long value = Entry.rawValueAddress(entry.valueAddress);
                    long valueAllocation = value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
                    entry.valueAddress = 0L;
                    entry.finishWriter();
                    worker.publishMutation(entry);
                    worker.retireValue(context, value, valueAllocation);
                    worker.retireValue(context, entry.nativeKeyAddress, entry.keyAllocationLength());
                    worker.afterWrite(context);
                    return true;
                }
                worker.cancelMutation(entry);
                worker.cancelRetirement(context);
                entry.finishWriter();
            }
        } finally {
            exitWriter(context);
        }
    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key");
        if (closing) return null;
        ThreadContext context = contexts.get();
        KeyEncoder.encode(keySerializer, key, context);
        if (!enter(context)) return null;
        ByteBuffer serializedValue;
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
            serializedValue = context.valueBuffer(length);
        } finally {
            exit(context);
        }
        return valueSerializer.deserialize(serializedValue);
    }

    @Override
    public boolean containsKey(K key) {
        Objects.requireNonNull(key, "key");
        if (closing) return false;
        ThreadContext context = contexts.get();
        KeyEncoder.encode(keySerializer, key, context);
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
        KeyEncoder.encode(keySerializer, key, context);
        return withDirect(context, consumer);
    }

    @Override
    public boolean withDirectValue(EncodedKey key, DirectValueConsumer consumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(consumer, "consumer");
        if (closing) return false;
        ThreadContext context = contexts.get();
        context.lookupKey.setPrecomputed(key.bytes, key.length(), key.hash64());
        if (!enter(context)) return false;
        try {
            return withDirectEntered(context, data.get(context.lookupKey), consumer);
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
        if (isClosing() || entries.isEmpty()) return 0;
        int accepted = 0;
        Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext() && !isClosing()) {
            ThreadContext context = enterWriter();
            if (context == null) break;
            int acceptedBatch = 0;
            try {
                for (int count = 0; count < BULK_BATCH_SIZE && iterator.hasNext(); count++) {
                    Map.Entry<? extends K, ? extends V> entry = iterator.next();
                    K key = Objects.requireNonNull(entry.getKey(), "key");
                    V value = Objects.requireNonNull(entry.getValue(), "value");
                    if (isClosing()) break;
                    if (putOne(context, key, value, DEFAULT_TTL)) acceptedBatch++;
                }
            } finally {
                if (acceptedBatch != 0) {
                    accepted += acceptedBatch;
                    worker.recordAccepted(acceptedBatch);
                    worker.afterWrite(context);
                }
                exitWriter(context);
            }
        }
        return accepted;
    }

    @Override
    public Map<K, V> getAll(Collection<? extends K> keys) {
        Objects.requireNonNull(keys, "keys");
        if (closing || keys.isEmpty()) return new HashMap<>();
        int expected = Math.max(1, keys.size());
        Map<K, V> result = new HashMap<>(expected);
        Map<K, byte[]> payloads = new HashMap<>(expected);
        ObjectOpenHashSet<K> unique = new ObjectOpenHashSet<>(expected);
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
                KeyEncoder.encode(keySerializer, key, context);
                Entry entry = data.get(context.lookupKey);
                long value = valueIfLive(entry);
                if (value == 0L) {
                    miss(context);
                } else {
                    hit(context, entry);
                    int length = ValueBlock.length(value);
                    byte[] payload = new byte[length];
                    NativeMemory.copy(ValueBlock.payloadAddress(value), payload, 0, length);
                    payloads.put(key, payload);
                }
                inEpoch++;
            }
        } finally {
            if (guarded) exit(context);
        }
        // The native payload has been copied into an independent array. Keeping arbitrary user
        // deserialization outside QSBR prevents one slow serializer from delaying reclamation for
        // this whole reader batch.
        for (Map.Entry<K, byte[]> payload : payloads.entrySet()) {
            result.put(payload.getKey(), valueSerializer.deserialize(ByteBuffer.wrap(payload.getValue())));
        }
        return result;
    }

    @Override
    public int removeAll(Collection<? extends K> keys) {
        Objects.requireNonNull(keys, "keys");
        if (closing || keys.isEmpty()) return 0;
        int removed = 0;
        for (K key : keys) if (remove(key)) removed++;
        return removed;
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(K key, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        ThreadContext context = enterWriter();
        if (context == null) return CompletableFuture.completedFuture(false);
        try {
            int keyLength = KeyEncoder.encode(keySerializer, key, context);
            int valueLength = encode(valueSerializer, value, context.valueBytes, context, false);
            return CompletableFuture.completedFuture(putIfAbsentSerialized(context, context.lookupKey,
                    context.keyBytes, keyLength, context.valueBytes, valueLength, expireAtMillis));
        } finally {
            exitWriter(context);
        }
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
                int removal = removeExpiredEntry(context, current,
                        current.generation(), current.valueAddress);
                if (removal < 0) return false;
                continue;
            }
            Entry candidate = allocateEntry(context, lookup.hash(), lookup.hash64(), keyBytes, keyLength,
                                             valueBytes, valueLength, expireAtMillis);
            if (candidate == null) return false;
            if (!worker.reserveMutation(candidate, Entry.PENDING_ADD)) {
                freeEntry(candidate, ValueBlock.allocationLength(valueLength));
                return false;
            }
            if (!enter(context)) {
                worker.cancelMutation(candidate);
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
                worker.recordAccepted();
                worker.publishMutation(candidate);
                worker.afterWrite(context);
                return true;
            }
            worker.cancelMutation(candidate);
            freeEntry(candidate, ValueBlock.allocationLength(valueLength));
            candidate.markDead();
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

    private int removeExpiredEntry(ThreadContext context, Entry entry,
                                   long expectedGeneration, long expectedValueAddress) {
        if (!claimWriter(entry)) return -1;
        if (!worker.prepareRetirement(context, 2)) {
            entry.finishWriter();
            return -1;
        }
        if (!worker.reserveMutation(entry, Entry.PENDING_REMOVE)) {
            worker.cancelRetirement(context);
            entry.finishWriter();
            return -1;
        }
        boolean removed = false;
        long value = 0L;
        long taggedValue = entry.valueAddress;
        value = Entry.rawValueAddress(taggedValue);
        removed = entry.generation() == expectedGeneration
                && taggedValue == expectedValueAddress
                && value != 0L
                && Entry.hasTtl(taggedValue)
                && ValueBlock.expired(value, ticker.currentTimeMillis())
                && removeCurrent(entry);
        if (removed) {
            long valueAllocation = ValueBlock.allocationLength(ValueBlock.length(value));
            entry.valueAddress = 0L;
            entry.finishWriter();
            worker.publishMutation(entry);
            worker.retireValue(context, value, valueAllocation);
            worker.retireValue(context, entry.nativeKeyAddress, entry.keyAllocationLength());
            worker.afterWrite(context);
            return 1;
        } else {
            worker.cancelMutation(entry);
            worker.cancelRetirement(context);
            entry.finishWriter();
            return 0;
        }
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(K key, V expected, V value, long expireAtMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(value, "value");
        ThreadContext context = enterWriter();
        if (context == null) return CompletableFuture.completedFuture(false);
        try {
            int expectedLength = encode(valueSerializer, expected, context.valueBytes, context, false);
            byte[] expectedBytes = Arrays.copyOf(context.valueBytes, expectedLength);
            KeyEncoder.encode(keySerializer, key, context);
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
        } finally {
            exitWriter(context);
        }
    }

    private boolean replaceExpected(ThreadContext context, LookupKey lookup, Entry entry, byte[] expected,
                                    int valueLength, byte[] valueBytes, long expireAtMillis) {
        long allocation = ValueBlock.allocationLength(valueLength);
        long weight = allocationWeight(allocation);
        if (weight > capacity || !budget.reserve(context.budgetStripe(), weight)) { worker.recordBudgetRejected(); return false; }
        long replacement = 0L;
        boolean locked = false;
        boolean published = false;
        boolean mutationReserved = false;
        try {
            replacement = context.writer().allocate(allocation);
            ValueBlock.initialize(replacement, expireAtMillis, valueLength);
            NativeMemory.copy(valueBytes, 0, ValueBlock.payloadAddress(replacement), valueLength);
            if (!claimWriter(entry)) {
                freeBlock(replacement, allocation);
                budget.refund(weight);
                return false;
            }
            locked = true;
            long oldTagged = entry.valueAddress;
            long old = Entry.rawValueAddress(oldTagged);
            boolean match = mappingIsCurrent(entry) && old != 0L
                    && (!Entry.hasTtl(oldTagged) || !ValueBlock.expired(old, ticker.currentTimeMillis()))
                    && ValueBlock.length(old) == expected.length
                    && NativeMemory.equals(ValueBlock.payloadAddress(old), expected, 0, expected.length);
            if (!match) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, allocation);
                budget.refund(weight);
                return false;
            }
            long oldAllocation = ValueBlock.allocationLength(ValueBlock.length(old));
            long oldWeight = allocationWeight(oldAllocation);
            boolean requiresMutation = maintenanceUpdateRequired(entry, oldTagged,
                    old, oldWeight, weight, expireAtMillis);
            if (!worker.prepareRetirement(context, 1)) {
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, allocation);
                budget.refund(weight);
                return false;
            }
            if (requiresMutation && !worker.reserveMutation(entry, Entry.PENDING_UPDATE)) {
                worker.cancelRetirement(context);
                entry.finishWriter();
                locked = false;
                freeBlock(replacement, allocation);
                budget.refund(weight);
                return false;
            }
            mutationReserved = requiresMutation;
            entry.valueAddress = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
            published = true;
            entry.finishWriter();
            locked = false;
            if (mutationReserved) worker.publishMutation(entry);
            worker.retireValue(context, old, oldAllocation);
            worker.afterWrite(context);
            worker.recordAccepted();
            return true;
        } catch (NativeMemory.AllocationLimitException rejected) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                worker.cancelRetirement(context);
                if (mutationReserved) worker.cancelMutation(entry);
                freeBlock(replacement, allocation);
                budget.refund(weight);
            }
            worker.recordBudgetRejected();
            return false;
        } catch (Throwable failure) {
            if (locked) entry.finishWriter();
            if (!published && replacement != 0L) {
                worker.cancelRetirement(context);
                if (mutationReserved) worker.cancelMutation(entry);
                freeBlock(replacement, allocation);
                budget.refund(weight);
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
        if (closeState.get() != OPEN) return CompletableFuture.completedFuture(null);
        return worker.flush();
    }

    @Override public long size() { return data.size(); }
    @Override public long capacity() { return capacity; }
    @Override public long totalAllocatedBytes() { return memory.allocated(); }
    long rawNativeAllocations() { return memory.rawAllocationCount(); }
    long entryNativeAllocations() { return memory.entryAllocationCount(); }
    long nativeHardLimitForTest() { return nativeHardLimit; }
    boolean mutationBacklogExceeds() {
        return worker.mutationBacklogExceeds() || worker.retirementBacklogExceeds()
                || budget.reserved() > residentHardLimit - residentHardLimit / 8L;
    }
    ConcurrentHashMap<Entry, Entry> dataForTest() { return data; }

    @Override
    public OHCacheStats stats() {
        long resident = budget.reserved();
        MaintenanceEventLoop.Snapshot snapshot = worker.snapshot();
        return new OHCacheStats(snapshot.hits, snapshot.misses, snapshot.accessDropped,
                snapshot.accepted, snapshot.rejectedQueue, snapshot.rejectedBudget, snapshot.rejectedRetirement,
                snapshot.applied,
                snapshot.queueDepth, snapshot.queueCapacity, snapshot.maintenanceLoopNanos, snapshot.unhealthy,
                snapshot.logicalExpired, snapshot.physicalExpired, snapshot.timeoutLagMillis, snapshot.ttlBacklog,
                snapshot.evicted, snapshot.evictionScans, snapshot.evictionLockedSkips,
                snapshot.retiredEntries, snapshot.liveWeight, resident, snapshot.retiredBytes,
                memory.allocated(), snapshot.timerBytes, snapshot.sketchBytes, snapshot.ghostHeapBytes,
                snapshot.ledgerBytes, snapshot.retirementQueueDepth, snapshot.retirementQueueCapacity,
                snapshot.wakeSignals, snapshot.mergedWakeSignals);
    }

    @Override
    public void close() {
        if (closeState.get() == CLOSED) return;
        closeState.compareAndSet(OPEN, CLOSING);
        closing = true;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Math.max(1L, closeTimeoutMillis));
        Thread current = Thread.currentThread();
        if (closeLeader.compareAndSet(null, current)) {
            try {
                if (!awaitWriters(deadline)) {
                    throw new IllegalStateException("close timed out with active writers=" + readers.activeWriterCount());
                }
                if (shutdownStarted.compareAndSet(false, true)) worker.stop();
            } finally {
                closeLeader.compareAndSet(current, null);
            }
        }
        long remainingNanos = deadline - System.nanoTime();
        long timeout = Math.max(1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, remainingNanos)));
        try {
            worker.join(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing", e);
        }
        if (worker.isAlive()) {
            throw new IllegalStateException("close timed out with active readers or writers=" + readers.activeWriterCount());
        }
        closeState.compareAndSet(CLOSING, CLOSED);
    }

    private boolean enter(ThreadContext context) {
        return readerGuard.enter(context);
    }

    private void exit(ThreadContext context) {
        readerGuard.exit(context);
    }

    private boolean mappingIsCurrent(Entry entry) {
        return entry.isAlive();
    }

    /**
     * A pointer-only replacement leaves actor state valid when its policy weight and TTL deadline
     * are unchanged. The old block still enters QSBR retirement, but no mutation transport or
     * policy/timer pass is required.
     */
    private static boolean maintenanceUpdateRequired(Entry entry, long oldTaggedAddress, long oldAddress,
                                                     long oldWeight, long newWeight, long expireAtMillis) {
        if (entry.policyState() == Entry.POLICY_NONE || oldAddress == 0L || oldWeight != newWeight) return true;
        boolean oldHasTtl = Entry.hasTtl(oldTaggedAddress);
        boolean newHasTtl = expireAtMillis > 0L;
        return oldHasTtl != newHasTtl
                || (oldHasTtl && ValueBlock.expireAtMillis(oldAddress) != expireAtMillis);
    }

    private boolean removeCurrent(Entry entry) {
        if (!entry.isAlive()) return false;
        entry.markRetired();
        if (data.remove(entry, entry)) return true;
        entry.restoreAlive();
        return false;
    }

    private boolean claimWriter(Entry entry) {
        int spins = 0;
        while (!entry.claimWriter()) {
            // Only ALIVE Entries can eventually release an ALIVE writer mutex. A retired/dead
            // Entry is a stale CHM observation and waiting for it would park forever.
            if (closing || !entry.isAlive()) return false;
            if (spins++ < 64) Thread.onSpinWait();
            else LockSupport.parkNanos(this, 1_000L);
        }
        return true;
    }

    private boolean isClosing() {
        return closing || closeState.get() != OPEN;
    }

    /**
     * Enters the native-writing domain. The second state check closes the race with close(): a
     * writer that races after CLOSING never touches an arena, while a writer that won admission
     * keeps close from releasing cache-owned native memory until its finally block completes.
     */
    private ThreadContext enterWriter() {
        if (closeState.get() != OPEN) return null;
        ThreadContext context = contexts.get();
        if (!context.isRegistered()) {
            context.markRegistered();
            worker.registerReader(context.slot);
        }
        context.slot.writerActive = true;
        if (closeState.get() == OPEN) return context;
        context.slot.writerActive = false;
        return null;
    }

    private void exitWriter(ThreadContext context) {
        context.slot.writerActive = false;
        Thread waiter = closeWaiter;
        if (waiter != null) LockSupport.unpark(waiter);
    }

    private boolean awaitWriters(long deadlineNanos) {
        closeWaiter = Thread.currentThread();
        try {
            while (readers.hasActiveWriter()) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0L) return false;
                if (readers.hasActiveWriter()) LockSupport.parkNanos(this, remaining);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            closeWaiter = null;
        }
    }

    private static void hit(ThreadContext context, Entry entry) {
        long sequence = context.hit();
        context.access(entry);
        context.finishRead(sequence);
    }

    private static void miss(ThreadContext context) {
        long sequence = context.miss();
        context.finishRead(sequence);
    }

    private long valueIfLive(Entry entry) {
        if (entry == null || !entry.isAlive()) return 0L;
        long taggedValue = entry.valueAddress;
        if (taggedValue == 0L) return 0L;
        long value = Entry.rawValueAddress(taggedValue);
        if (!Entry.hasTtl(taggedValue)) return value;
        return ValueBlock.expired(value, ticker.currentTimeMillis()) ? 0L : value;
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
        budget.refund(entryWeight(entry, valueAllocation));
    }

    private void freeBlock(long address, long allocation) {
        if (address != 0L) memory.releaseEntry(address, allocation);
    }

    private static long allocationWeight(long allocation) {
        return allocation == 0L ? 0L : WriterArena.allocationWeight(allocation);
    }

    private static long entryWeight(Entry entry, long valueAllocation) {
        return allocationWeight(entry.keyAllocationLength()) + allocationWeight(valueAllocation);
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
