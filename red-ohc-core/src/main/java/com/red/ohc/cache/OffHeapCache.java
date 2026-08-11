package com.red.ohc.cache;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheLoader;
import com.red.ohc.api.CacheMaintenanceException;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.DirectEntryConsumer;
import com.red.ohc.api.DirectValueConsumer;
import com.red.ohc.api.EncodedKey;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.EvictionListener;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.RemovalCause;
import com.red.ohc.api.Ticker;
import com.red.ohc.codec.KeyEncoder;
import com.red.ohc.codec.LookupKey;
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
  private static final int OPEN = 0;
  private static final int CLOSING = 1;
  private static final int CLOSED = 2;

  final ConcurrentHashMap<Entry, Entry> data;
  private final CacheSerializer<K> keySerializer;
  private final CacheSerializer<V> valueSerializer;
  private final EvictionListener<K, V> evictionListener;
  private final boolean weakValues;
  private final Ticker ticker;
  private final long defaultTtlMillis;
  private final double ttlJitterPercent;
  private final Executor loaderExecutor;
  private final long closeTimeoutMillis;
  private final long capacity;
  private final long byteCapacity;
  private final long maxSizeHighWatermark;
  private final long residentHardLimit;
  private final long nativeHardLimit;
  private final NativeMemory.Memory memory;
  private final Budget budget;
  private final ReaderRegistry readers = new ReaderRegistry();
  private final ThreadLocal<ThreadContext> contexts;
  private final ThreadLocal<ThreadContext> evictionContexts;
  private final MaintenanceEventLoop worker;
  private final ReaderGuard readerGuard;
  private final ConcurrentHashMap<EncodedKey, CompletableFuture<V>> loadFlights = new ConcurrentHashMap<>();
  private final AtomicInteger closeState = new AtomicInteger(OPEN);
  private final AtomicBoolean shutdownStarted = new AtomicBoolean();
  private final AtomicReference<Thread> closeLeader = new AtomicReference<>();
  private volatile Thread closeWaiter;
  private volatile boolean closing;

  OffHeapCache(
      CacheSerializer<K> keySerializer,
      CacheSerializer<V> valueSerializer,
      long defaultTtlMillis,
      double ttlJitterPercent,
      java.util.concurrent.Executor loaderExecutor,
      long closeTimeoutMillis,
      AllocatorType allocatorType,
      Ticker ticker,
      Eviction eviction,
      EvictionListener<K, V> evictionListener,
      boolean weakValues,
      long capacity,
      long maxSize,
      long expectedEntries) {
    ThreadContext.verifyNativeByteBufferSupported();
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.evictionListener = evictionListener;
    this.weakValues = weakValues;
    this.ticker = ticker;
    this.defaultTtlMillis = defaultTtlMillis;
    this.ttlJitterPercent = ttlJitterPercent;
    this.loaderExecutor = loaderExecutor;
    this.closeTimeoutMillis = closeTimeoutMillis;
    boolean countBounded = maxSize > 0L;
    this.capacity = countBounded ? -1L : capacity;
    this.byteCapacity = countBounded ? Long.MAX_VALUE : capacity;
    this.maxSizeHighWatermark =
        countBounded ? maxSizeHighWatermark(maxSize) : Long.MAX_VALUE;
    long limit = countBounded ? maxSize : capacity;
    if (countBounded) {
      this.residentHardLimit = Long.MAX_VALUE;
      this.nativeHardLimit = Long.MAX_VALUE;
    } else {
      long maxValueWeight =
          allocationWeight(
              ValueBlock.allocationLength((int) Math.min(capacity, Integer.MAX_VALUE - 16L)));
      long headroom = Math.max(maxValueWeight, capacity / 8L);
      this.residentHardLimit = saturatedAdd(capacity, headroom);
      // Every writer stripe may retain a partially-used page for the key and value size classes.
      // This is allocator overhead, not resident cache weight, and must not turn a legal write
      // into a permanent AllocationLimitException under full-CPU churn.
      long stripePageSlack =
          saturatedMultiply(
              nextPowerOfTwo(Math.max(1, Runtime.getRuntime().availableProcessors()) * 4L),
              2L * 64L * 1024L);
      long allocatorSlack = Math.max(8L << 20, Math.max(stripePageSlack, capacity / 16L));
      this.nativeHardLimit =
          saturatedAdd(saturatedAdd(residentHardLimit, allocatorSlack), 512L << 10);
    }
    long sizingExpectedEntries =
        expectedEntries > 0L ? expectedEntries : (countBounded ? maxSize : 0L);
    int initialCapacity = ChmSizing.constructorCapacity(sizingExpectedEntries, limit);
    this.data = new ConcurrentHashMap<>(initialCapacity, 0.75f, 1);
    Entry bootstrap = Entry.bootstrap();
    data.put(bootstrap, bootstrap);
    data.remove(bootstrap, bootstrap);
    this.memory = new NativeMemory.Memory(allocatorType, nativeHardLimit);
    this.budget = new Budget(residentHardLimit);
    this.contexts = ThreadLocal.withInitial(() -> new ThreadContext(null, null));
    this.evictionContexts = ThreadLocal.withInitial(() -> new ThreadContext(null, null));
    this.worker =
        new MaintenanceEventLoop(
            data,
            memory,
            budget,
            ticker,
                limit,
            eviction,
            evictionListener == null ? null : this::notifyEviction,
            readers,
            ChmSizing.maintenanceQueueCapacity(sizingExpectedEntries, limit),
                countBounded);
    this.readerGuard = new ReaderGuard(worker, this::isClosing);
    worker.start();
  }

  private void notifyEviction(Entry entry, long valueAddress, RemovalCause cause) {
    EvictionListener<K, V> listener = evictionListener;
    if (listener == null) {
      return;
    }
    LazySupplier<K> key = null;
    LazySupplier<V> value = null;
    try {
      ThreadContext context = evictionContexts.get();
      key =
          new LazySupplier<>(
              () ->
                  deserialize(
                      context, keySerializer, entry.nativeKeyBytesAddress(), entry.keyLength()));
      int valueLength = ValueBlock.length(valueAddress);
      value =
          new LazySupplier<>(
              () ->
                  deserialize(
                      context,
                      valueSerializer,
                      ValueBlock.payloadAddress(valueAddress),
                      valueLength,
                      weakValues));
      listener.onEviction(key, value, cause);
    } catch (Throwable ignored) {
      // Listener and lazy-deserialization failures must not prevent native retirement.
    } finally {
      if (key != null) {
        key.invalidate();
      }
      if (value != null) {
        value.invalidate();
      }
    }
  }

  private static <T> T deserialize(
      ThreadContext context, CacheSerializer<T> serializer, long address, int length) {
    return deserialize(context, serializer, address, length, false);
  }

  private static <T> T deserialize(
      ThreadContext context,
      CacheSerializer<T> serializer,
      long address,
      int length,
      boolean rejectByteBuffer) {
    ByteBuffer buffer = context.readOnlyValueBuffer(address, length);
    try {
      T result = serializer.deserialize(buffer);
      if (rejectByteBuffer && result instanceof ByteBuffer) {
        throw new IllegalArgumentException(
            "weakValues(true) does not support ByteBuffer values; use weakValues(false) or a non-ByteBuffer value type");
      }
      return result;
    } finally {
      context.releaseReadOnlyValueBuffer();
    }
  }

  private static final class LazySupplier<T> implements java.util.function.Supplier<T> {
    private java.util.function.Supplier<T> delegate;
    private T value;
    private Throwable failure;
    private boolean resolved;
    private boolean active = true;

    private LazySupplier(java.util.function.Supplier<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public synchronized T get() {
      if (!active) {
        throw new IllegalStateException("eviction supplier is no longer valid");
      }
      if (!resolved) {
        try {
          value = delegate.get();
        } catch (Throwable failure) {
          this.failure = failure;
        } finally {
          resolved = true;
        }
      }
      if (failure != null) {
        OffHeapCache.throwUnchecked(failure);
      }
      return value;
    }

    private synchronized void invalidate() {
      active = false;
      delegate = null;
    }
  }

  @Override
  public boolean put(K key, V value) {
    return putInternal(key, value, DEFAULT_TTL);
  }

  @Override
  public boolean put(K key, V value, long expireAtMillis) {
    return putInternal(key, value, Math.max(expireAtMillis, 0L));
  }

  private boolean putInternal(K key, V value, long expireAtMillis) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    rejectByteBufferValue(value);
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      worker.recordNonBlockingPutFailure();
      return false;
    }
    try {
      if (expireAtMillis != DEFAULT_TTL) {
        worker.refreshClock();
      }
      boolean accepted = putOne(context, key, value, expireAtMillis, false);
      if (!accepted) {
        worker.recordNonBlockingPutFailure();
      }
      finishWrite(context, accepted);
      return accepted;
    } finally {
      exitWriter(context);
    }
  }

  private boolean putOne(
      ThreadContext context, K key, V value, long expireAtMillis, boolean deferMaintenanceWake) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    rejectByteBufferValue(value);
    int keyLength = KeyEncoder.encode(keySerializer, key, context);
    int valueLength = serializedSize(valueSerializer, value);
    return putSerialized(
        context,
        context.lookupKey,
        context.keyBytes,
        keyLength,
        value,
        null,
        valueLength,
        expireAtMillis,
        deferMaintenanceWake);
  }

  private void finishWrite(ThreadContext context, boolean accepted) {
    if (!accepted) {
      return;
    }
    worker.afterWrite(context);
  }

  /**
   * Encoded benchmark path: neither serializer is touched and the precomputed int hash is reused.
   */
  public boolean putEncoded(EncodedKey key, byte[] value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      worker.recordNonBlockingPutFailure();
      return false;
    }
    try {
      context.ensureKey(key.length());
      key.copyTo(context.keyBytes, 0);
      byte[] encodedKey = context.keyBytes;
      context.lookupKey.setPrecomputed(encodedKey, key.length(), key.hash64());
      boolean accepted =
          putSerialized(
              context,
              context.lookupKey,
              encodedKey,
              key.length(),
              null,
              value,
              value.length,
              DEFAULT_TTL,
              false);
      if (!accepted) {
        worker.recordNonBlockingPutFailure();
      }
      finishWrite(context, accepted);
      return accepted;
    } finally {
      exitWriter(context);
    }
  }

  private boolean putSerialized(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      byte[] valueBytes,
      int valueLength,
      long requestedExpiry,
      boolean deferMaintenanceWake) {
    if (value != null) {
      rejectByteBufferValue(value);
    }
    long expireAtMillis =
        requestedExpiry == DEFAULT_TTL ? defaultExpiry(lookup.hash64()) : requestedExpiry;
    worker.throwIfUnavailable();
    Entry existing;
    if (!enter(context)) {
      throw new IllegalStateException("cache is closed");
    }
    try {
      existing = data.get(lookup);
    } finally {
      exit(context);
    }
    if (existing != null) {
      int result =
          replaceExisting(
              context,
              lookup,
              existing,
              value,
              valueBytes,
              valueLength,
              expireAtMillis,
              deferMaintenanceWake);
      if (result < 0) {
        throw new IllegalStateException("cache write failed");
      }
      if (result == 0) {
        worker.recordNonBlockingReplaceFailure();
      }
      return result > 0;
    }
    if (data.mappingCount() >= maxSizeHighWatermark) {
      return false;
    }
    Entry candidate =
        allocateEntry(
            context,
            lookup.hash(),
            lookup.hash64(),
            keyBytes,
            keyLength,
            value,
            valueBytes,
            valueLength,
            expireAtMillis);
    if (candidate == null) {
      return false;
    }
    if (!enter(context)) {
      freeEntry(context, candidate, ValueBlock.allocationLength(valueLength));
      throw new IllegalStateException("cache is closed");
    }
    Entry winner;
    try {
      winner = data.putIfAbsent(candidate, candidate);
    } finally {
      exit(context);
    }
    if (winner == null) {
      worker.publishMutation(candidate, Entry.PENDING_ADD, !deferMaintenanceWake);
      return true;
    }
    freeEntry(context, candidate, ValueBlock.allocationLength(valueLength));
    candidate.markDead();
    int result =
        replaceExisting(
            context,
            lookup,
            winner,
            value,
            valueBytes,
            valueLength,
            expireAtMillis,
            deferMaintenanceWake);
    if (result < 0) {
      throw new IllegalStateException("cache write failed");
    }
    if (result == 0) {
      worker.recordNonBlockingReplaceFailure();
    }
    return result > 0;
  }

  private int replaceExisting(
      ThreadContext context,
      LookupKey lookup,
      Entry entry,
      Object value,
      byte[] valueBytes,
      int valueLength,
      long expireAtMillis,
      boolean deferMaintenanceWake) {
    long newAllocation = ValueBlock.allocationLength(valueLength);
    long newWeight = allocationWeight(newAllocation);
    ensureReplacementFitsCapacity(entry.keyAllocationLength(), newAllocation);
    if (!reserveBudget(context, newWeight)) {
      return 0;
    }
    long replacement = 0L;
    boolean locked = false;
    boolean published = false;
    try {
      replacement = context.writer().allocate(newAllocation);
      ValueBlock.initialize(replacement, expireAtMillis, valueLength);
      writeValue(context, ValueBlock.payloadAddress(replacement), value, valueBytes, valueLength);
      if (!claimWriter(entry)) {
        freeBlock(replacement, newAllocation);
        budget.refund(context.budgetLease(), newWeight);
        if (isClosing()) {
          throw new IllegalStateException("cache is closing");
        }
        return 0;
      }
      locked = true;
      if (!mappingIsCurrent(entry)) {
        entry.finishWriter();
        locked = false;
        freeBlock(replacement, newAllocation);
        budget.refund(context.budgetLease(), newWeight);
        return 0;
      }
      long old = Entry.rawValueAddress(entry.valueAddress);
      long oldAllocation = old == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(old));
      long oldWeight = allocationWeight(oldAllocation);
      boolean requiresMutation =
          maintenanceUpdateRequired(
              entry, entry.valueAddress, old, oldWeight, newWeight, expireAtMillis);
      if (!worker.prepareRetirement(context, 1)) {
        worker.recordRetirementAdmissionFailure();
        entry.finishWriter();
        locked = false;
        freeBlock(replacement, newAllocation);
        budget.refund(context.budgetLease(), newWeight);
        return 0;
      }
      long newTaggedValue = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
      Entry.WeakValueSlot newWeakValue =
          prepareWeakValue(value, valueBytes, newTaggedValue);
      entry.publishValue(newTaggedValue, newWeakValue);
      published = true;
      worker.retireValue(context, old, oldAllocation);
      entry.finishWriter();
      locked = false;
      if (requiresMutation) {
        worker.publishMutation(entry, Entry.PENDING_UPDATE, !deferMaintenanceWake);
      }
      return 1;
    } catch (NativeMemory.AllocationLimitException rejected) {
      worker.recordNativeAllocationFailure();
      if (locked) {
        entry.finishWriter();
      }
      if (!published) {
        worker.cancelRetirement(context);
        if (replacement != 0L) {
          freeBlock(replacement, newAllocation);
        }
        budget.refund(context.budgetLease(), newWeight);
      }
      worker.requestAllocationPressure();
      return 0;
    } catch (Throwable failure) {
      if (locked) {
        entry.finishWriter();
      }
      if (!published) {
        worker.cancelRetirement(context);
        if (replacement != 0L) {
          freeBlock(replacement, newAllocation);
        }
        budget.refund(context.budgetLease(), newWeight);
      }
      if (isClosing()
          || failure instanceof CacheMaintenanceException) {
        throwUnchecked(failure);
      }
      throwUnchecked(failure);
      return -1;
    }
  }

  private Entry allocateEntry(
      ThreadContext context,
      int hash,
      long hash64,
      byte[] keyBytes,
      int keyLength,
      Object value,
      byte[] valueBytes,
      int valueLength,
      long expireAtMillis) {
    long keyAllocation = Math.max(8L, CacheMath.roundUpTo8((long) keyLength + Long.BYTES));
    long valueAllocation = ValueBlock.allocationLength(valueLength);
    long totalWeight = allocationWeight(keyAllocation) + allocationWeight(valueAllocation);
    if (totalWeight > byteCapacity) {
      throw new IllegalArgumentException("serialized entry allocation exceeds cache capacity");
    }
    if (!reserveBudget(context, totalWeight)) {
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
      writeValue(context, ValueBlock.payloadAddress(valueAddress), value, valueBytes, valueLength);
      Entry entry =
          new Entry(
              keyAddress,
              keyLength,
              hash,
              hash64,
              Entry.tagValueAddress(valueAddress, expireAtMillis > 0L));
      if (weakValues) {
        entry.initializeValueState(
            entry.valueAddress, prepareWeakValue(value, valueBytes, entry.valueAddress));
      }
      return entry;
    } catch (NativeMemory.AllocationLimitException rejected) {
      worker.recordNativeAllocationFailure();
      if (valueAddress != 0L) {
        freeBlock(valueAddress, valueAllocation);
      }
      if (keyAddress != 0L) {
        freeBlock(keyAddress, keyAllocation);
      }
      budget.refund(context.budgetLease(), totalWeight);
      worker.requestAllocationPressure();
      return null;
    } catch (Throwable failure) {
      if (valueAddress != 0L) {
        freeBlock(valueAddress, valueAllocation);
      }
      if (keyAddress != 0L) {
        freeBlock(keyAddress, keyAllocation);
      }
      budget.refund(context.budgetLease(), totalWeight);
      throw failure;
    }
  }

  @Override
  public boolean remove(K key) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      worker.recordNonBlockingRemoveFailure();
      return false;
    }
    try {
      boolean removed = removeOne(context, key, false);
      if (removed) {
        worker.afterWrite(context);
      } else {
        worker.recordNonBlockingRemoveFailure();
      }
      return removed;
    } finally {
      exitWriter(context);
    }
  }

  private boolean removeOne(ThreadContext context, K key, boolean deferMaintenanceWake) {
    KeyEncoder.encode(keySerializer, key, context);
    if (!enter(context)) {
      throw new IllegalStateException("cache is closed");
    }
    Entry entry;
    try {
      entry = data.get(context.lookupKey);
    } finally {
      exit(context);
    }
    if (entry == null || !claimWriter(entry)) {
      if (entry != null && isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      return false;
    }
    boolean writerHeld = true;
    boolean removalPrepared = false;
    try {
      if (!worker.prepareReliableRemoval(context, entry)) {
        worker.recordReliableRemovalAdmissionFailure();
        entry.finishWriter();
        writerHeld = false;
        return false;
      }
      removalPrepared = true;
      boolean removed = removeCurrent(entry);
      if (removed) {
        long value = Entry.rawValueAddress(entry.valueAddress);
        long valueAllocation =
            value == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(value));
        entry.clearValue();
        entry.finishWriter();
        writerHeld = false;
        worker.publishRemovalAndRetire(
            context, entry, !deferMaintenanceWake, value, valueAllocation, null);
        removalPrepared = false;
        return true;
      }
      entry.finishWriter();
      writerHeld = false;
      worker.cancelReliableRemoval(context, entry);
      removalPrepared = false;
      return false;
    } catch (Throwable failure) {
      if (writerHeld) {
        entry.finishWriter();
      }
      if (removalPrepared) {
        worker.cancelReliableRemoval(context, entry);
      }
      throw failure;
    }
  }

  @Override
  public V get(K key) {
    Objects.requireNonNull(key, "key");
    if (closing) {
      return null;
    }
    ThreadContext context = contexts.get();
    KeyEncoder.encode(keySerializer, key, context);
    return getEncoded(context);
  }

  /** Reads the lookup key already installed in the supplied thread context. */
  private V getEncoded(ThreadContext context) {
    if (!enter(context)) {
      return null;
    }
    boolean serializedValueEntered = false;
    try {
      Entry entry = data.get(context.lookupKey);
      long value = valueIfLive(entry);
      if (value == 0L) {
        miss(context);
        return null;
      }
      hit(context, entry);
      Entry.ValueState observedState = entry.valueState();
      V cached = weakValue(entry, value, observedState);
      if (cached != null) {
        return cached;
      }
      long taggedValue = observedState == null ? 0L : observedState.taggedValueAddress();
      if (Entry.rawValueAddress(taggedValue) != value || entry.valueAddress != taggedValue) {
        taggedValue = 0L;
      }
      int length = ValueBlock.length(value);
      ByteBuffer serializedValue =
          context.readOnlyValueBuffer(ValueBlock.payloadAddress(value), length);
      serializedValueEntered = true;
      V result = valueSerializer.deserialize(serializedValue);
      rejectByteBufferValue(result);
      if (taggedValue != 0L) {
        publishWeakValueIfCurrent(entry, result, taggedValue, observedState);
      }
      return result;
    } finally {
      if (serializedValueEntered) {
        context.releaseReadOnlyValueBuffer();
      }
      exit(context);
    }
  }

  private void bindEncodedKey(ThreadContext context, EncodedKey key) {
    context.ensureKey(key.length());
    key.copyTo(context.keyBytes, 0);
    context.lookupKey.setPrecomputed(context.keyBytes, key.length(), key.hash64());
  }

  private V getEncoded(EncodedKey key) {
    ThreadContext context = contexts.get();
    bindEncodedKey(context, key);
    return getEncoded(context);
  }

  @Override
  public boolean containsKey(K key) {
    Objects.requireNonNull(key, "key");
    if (closing) {
      return false;
    }
    ThreadContext context = contexts.get();
    KeyEncoder.encode(keySerializer, key, context);
    if (!enter(context)) {
      return false;
    }
    try {
      Entry entry = data.get(context.lookupKey);
      if (valueIfLive(entry) == 0L) {
        miss(context);
        return false;
      }
      hit(context, entry);
      return true;
    } finally {
      exit(context);
    }
  }

  @Override
  public boolean getDirect(K key, DirectValueConsumer consumer) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(consumer, "consumer");
    if (closing) {
      return false;
    }
    ThreadContext context = contexts.get();
    KeyEncoder.encode(keySerializer, key, context);
    return getDirectInContext(context, consumer);
  }

  private boolean getDirectInContext(ThreadContext context, DirectValueConsumer consumer) {
    if (!enter(context)) {
      return false;
    }
    try {
      return getDirectEntered(context, data.get(context.lookupKey), consumer);
    } finally {
      exit(context);
    }
  }

  private boolean getDirectEntered(
      ThreadContext context, Entry entry, DirectValueConsumer consumer) {
    long value = valueIfLive(entry);
    if (value == 0L) {
      miss(context);
      return false;
    }
    hit(context, entry);
    com.red.ohc.runtime.DirectValueView view =
        context.pushDirectView(ValueBlock.payloadAddress(value), ValueBlock.length(value));
    try {
      consumer.accept(view);
      return true;
    } finally {
      context.popDirectView();
    }
  }

  @Override
  public int getDirectAll(Collection<? extends K> keys, DirectEntryConsumer<K> consumer) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(consumer, "consumer");
    if (closing || keys.isEmpty()) {
      return 0;
    }

    ThreadContext context = contexts.get();
    int hits = 0;
    int expected = keys.size();
    Set<Entry> uniqueEntries = context.acquireBulkEntries(expected);
    boolean guarded = false;
    try {
      if (!enter(context)) {
        return 0;
      }
      guarded = true;
      context.beginBulkRead();
      for (K key : keys) {
        Objects.requireNonNull(key, "key");
        KeyEncoder.encode(keySerializer, key, context);
        Entry entry = data.get(context.lookupKey);
        long value = valueIfLive(entry);
        if (value == 0L) {
          context.bulkMiss();
        } else {
          if (!uniqueEntries.add(entry)) {
            continue;
          }
          context.bulkHit(entry);
          com.red.ohc.runtime.DirectValueView view =
              context.pushDirectView(ValueBlock.payloadAddress(value), ValueBlock.length(value));
          try {
            consumer.accept(key, view);
            hits++;
          } finally {
            context.popDirectView();
          }
        }
      }
    } finally {
      if (guarded) {
        context.finishBulkRead();
        exit(context);
      }
      context.releaseBulkEntries(uniqueEntries);
    }
    return hits;
  }

  @Override
  public int putAll(Map<? extends K, ? extends V> entries) {
    Objects.requireNonNull(entries, "entries");
    if (isClosing()) {
      throw new IllegalStateException("cache is closed");
    }
    if (entries.isEmpty()) {
      return 0;
    }
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      return 0;
    }
    int accepted = 0;
    Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator =
        entries.entrySet().iterator();
    try {
      while (iterator.hasNext()) {
        Map.Entry<? extends K, ? extends V> entry = iterator.next();
        K key = Objects.requireNonNull(entry.getKey(), "key");
        V value = Objects.requireNonNull(entry.getValue(), "value");
        rejectByteBufferValue(value);
        if (isClosing()) {
          throw new IllegalStateException("cache is closed");
        }
        if (putOne(context, key, value, DEFAULT_TTL, true)) {
          accepted++;
        } else {
          worker.recordNonBlockingPutFailure();
        }
      }
    } finally {
      if (accepted != 0) {
        worker.afterWrite(context);
      }
      exitWriter(context);
    }
    return accepted;
  }

  @Override
  public Map<K, V> getAll(Collection<? extends K> keys) {
    Objects.requireNonNull(keys, "keys");
    if (closing || keys.isEmpty()) {
      return new HashMap<>();
    }
    int expected = keys.size();
    Map<K, V> result = new HashMap<>(resultCapacity(expected));
    ThreadContext context = contexts.get();
    Set<Entry> uniqueEntries = context.acquireBulkEntries(expected);
    boolean guarded = false;
    try {
      if (!enter(context)) {
        return result;
      }
      guarded = true;
      context.beginBulkRead();
      for (K key : keys) {
        Objects.requireNonNull(key, "key");
        KeyEncoder.encode(keySerializer, key, context);
        Entry entry = data.get(context.lookupKey);
        long value = valueIfLive(entry);
        if (value == 0L) {
          context.bulkMiss();
        } else {
          if (!uniqueEntries.add(entry)) {
            continue;
          }
          context.bulkHit(entry);
          Entry.ValueState observedState = entry.valueState();
          V cached = weakValue(entry, value, observedState);
          if (cached != null) {
            result.put(key, cached);
            continue;
          }
          long taggedValue = observedState == null ? 0L : observedState.taggedValueAddress();
          if (Entry.rawValueAddress(taggedValue) != value || entry.valueAddress != taggedValue) {
            taggedValue = 0L;
          }
          int length = ValueBlock.length(value);
          ByteBuffer serializedValue =
              context.readOnlyValueBuffer(ValueBlock.payloadAddress(value), length);
          try {
            V deserialized = valueSerializer.deserialize(serializedValue);
            rejectByteBufferValue(deserialized);
            result.put(key, deserialized);
            if (taggedValue != 0L) {
              publishWeakValueIfCurrent(entry, deserialized, taggedValue, observedState);
            }
          } finally {
            context.releaseReadOnlyValueBuffer();
          }
        }
      }
    } finally {
      if (guarded) {
        context.finishBulkRead();
        exit(context);
      }
      context.releaseBulkEntries(uniqueEntries);
    }
    return result;
  }

  private static int resultCapacity(int expectedEntries) {
    if (expectedEntries < 3) {
      return expectedEntries + 1;
    }
    long requested = ((long) expectedEntries * 4L + 2L) / 3L + 1L;
    return (int) Math.min(1L << 30, requested);
  }

  @Override
  public int removeAll(Collection<? extends K> keys) {
    Objects.requireNonNull(keys, "keys");
    if (isClosing()) {
      throw new IllegalStateException("cache is closed");
    }
    if (keys.isEmpty()) {
      return 0;
    }
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      return 0;
    }
    int removed = 0;
    try {
      for (K key : keys) {
        Objects.requireNonNull(key, "key");
        if (isClosing()) {
          throw new IllegalStateException("cache is closed");
        }
        if (removeOne(context, key, true)) {
          removed++;
        }
      }
    } finally {
      if (removed != 0) {
        worker.afterWrite(context);
      }
      exitWriter(context);
    }
    return removed;
  }

  @Override
  public CompletableFuture<Boolean> putIfAbsentAsync(K key, V value, long expireAtMillis) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    rejectByteBufferValue(value);
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    submitAsyncMutation(
        result,
        () -> {
          ThreadContext context = enterWriter();
          if (context == null) {
            if (isClosing()) {
              throw new IllegalStateException("cache is closed");
            }
            result.complete(false);
            return;
          }
          try {
            int keyLength = KeyEncoder.encode(keySerializer, key, context);
            result.complete(
                putIfAbsentValue(
                    context, context.lookupKey, context.keyBytes, keyLength, value, expireAtMillis));
          } finally {
            exitWriter(context);
          }
        });
    return result;
  }

  private boolean putIfAbsentValue(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
    long expireAtMillis) {
    rejectByteBufferValue(value);
    worker.throwIfUnavailable();
    Entry current;
    boolean live;
    if (!enter(context)) {
      throw new IllegalStateException("cache is closed");
    }
    try {
      current = data.get(lookup);
      live = valueIfLive(current) != 0L;
    } finally {
      exit(context);
    }
    if (current != null) {
      if (live) {
        return false;
      }
      int removal =
          removeExpiredEntry(context, current, current.generation(), current.valueAddress);
      if (removal <= 0) {
        return false;
      }
    }
    int valueLength = serializedSize(valueSerializer, value);
    Entry candidate =
        allocateEntry(
            context,
            lookup.hash(),
            lookup.hash64(),
            keyBytes,
            keyLength,
            value,
            null,
            valueLength,
            expireAtMillis);
    if (candidate == null) {
      return false;
    }
    if (!enter(context)) {
      freeEntry(context, candidate, ValueBlock.allocationLength(valueLength));
      throw new IllegalStateException("cache is closed");
    }
    Entry winner;
    try {
      winner = data.putIfAbsent(candidate, candidate);
    } finally {
      exit(context);
    }
    if (winner == null) {
      worker.publishMutation(candidate, Entry.PENDING_ADD);
      worker.afterWrite(context);
      return true;
    }
    freeEntry(context, candidate, ValueBlock.allocationLength(valueLength));
    candidate.markDead();
    return false;
  }

  private int removeExpiredEntry(
      ThreadContext context, Entry entry, long expectedGeneration, long expectedValueAddress) {
    if (!claimWriter(entry)) {
      return -1;
    }
    boolean writerHeld = true;
    boolean removalPrepared = false;
    try {
      if (!worker.prepareReliableRemoval(context, entry)) {
        worker.recordReliableRemovalAdmissionFailure();
        entry.finishWriter();
        writerHeld = false;
        return -1;
      }
      removalPrepared = true;
      long taggedValue = entry.valueAddress;
      long value = Entry.rawValueAddress(taggedValue);
      boolean removed =
          entry.generation() == expectedGeneration
              && taggedValue == expectedValueAddress
              && value != 0L
              && Entry.hasTtl(taggedValue)
              && ValueBlock.expired(value, ticker.currentTimeMillis())
              && removeCurrent(entry);
      if (removed) {
        long valueAllocation = ValueBlock.allocationLength(ValueBlock.length(value));
        entry.clearValue();
        entry.finishWriter();
        writerHeld = false;
        worker.publishRemovalAndRetire(
            context, entry, false, value, valueAllocation, RemovalCause.EXPIRED);
        removalPrepared = false;
        worker.afterWrite(context);
        return 1;
      }
      entry.finishWriter();
      writerHeld = false;
      worker.cancelReliableRemoval(context, entry);
      removalPrepared = false;
      return 0;
    } catch (Throwable failure) {
      if (writerHeld) {
        entry.finishWriter();
      }
      if (removalPrepared) {
        worker.cancelReliableRemoval(context, entry);
      }
      throw failure;
    }
  }

  @Override
  public CompletableFuture<Boolean> replaceAsync(K key, V expected, V value, long expireAtMillis) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(value, "value");
    rejectByteBufferValue(value);
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    submitAsyncMutation(
        result,
        () -> {
          boolean accepted = executeReplaceAsync(key, expected, value, expireAtMillis);
          if (!accepted) {
            worker.recordNonBlockingReplaceFailure();
          }
          result.complete(accepted);
        });
    return result;
  }

  private boolean executeReplaceAsync(K key, V expected, V value, long expireAtMillis) {
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      return false;
    }
    long expectedScratch = 0L;
    long expectedAllocation = 0L;
    try {
      KeyEncoder.encode(keySerializer, key, context);
      Entry entry;
      boolean live;
      if (!enter(context)) {
        throw new IllegalStateException("cache is closed");
      }
      try {
        entry = data.get(context.lookupKey);
        live = valueIfLive(entry) != 0L;
      } finally {
        exit(context);
      }
      if (entry == null || !live) {
        return false;
      }
      int expectedLength = serializedSize(valueSerializer, expected);
      expectedAllocation = ValueBlock.allocationLength(expectedLength);
      if (allocationWeight(expectedAllocation) > byteCapacity
          || expectedAllocation > nativeHardLimit) {
        throw new IllegalArgumentException("expected value exceeds cache capacity");
      }
      expectedScratch = allocateScratch(context, expectedAllocation);
      if (expectedScratch == 0L) {
        return false;
      }
      writeValue(context, ValueBlock.payloadAddress(expectedScratch), expected, null, expectedLength);
      return replaceExpected(
          context,
          context.lookupKey,
          entry,
          ValueBlock.payloadAddress(expectedScratch),
          expectedLength,
          value,
          expireAtMillis);
    } finally {
      if (expectedScratch != 0L) {
        freeBlock(expectedScratch, expectedAllocation);
      }
      exitWriter(context);
    }
  }

  private boolean replaceExpected(
      ThreadContext context,
      LookupKey lookup,
      Entry entry,
      long expectedAddress,
      int expectedLength,
      Object value,
      long expireAtMillis) {
    rejectByteBufferValue(value);
    if (!claimWriter(entry)) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closing");
      }
      return false;
    }
    long replacement = 0L;
    long allocation = 0L;
    long weight = 0L;
    boolean budgetReserved = false;
    boolean retirementPrepared = false;
    boolean published = false;
    boolean writerHeld = true;
    try {
      if (!expectedMatches(entry, expectedAddress, expectedLength)) {
        return false;
      }

      int valueLength = serializedSize(valueSerializer, value);
      allocation = ValueBlock.allocationLength(valueLength);
      weight = allocationWeight(allocation);
      ensureReplacementFitsCapacity(entry.keyAllocationLength(), allocation);
      if (weight > byteCapacity || !reserveBudget(context, weight)) {
        return false;
      }
      budgetReserved = true;
      replacement = context.writer().allocate(allocation);
      ValueBlock.initialize(replacement, expireAtMillis, valueLength);
      writeValue(context, ValueBlock.payloadAddress(replacement), value, null, valueLength);
      long oldTagged = entry.valueAddress;
      long old = Entry.rawValueAddress(oldTagged);
      long oldAllocation = ValueBlock.allocationLength(ValueBlock.length(old));
      long oldWeight = allocationWeight(oldAllocation);
      boolean requiresMutation =
          maintenanceUpdateRequired(entry, oldTagged, old, oldWeight, weight, expireAtMillis);
      if (!worker.prepareRetirement(context, 1)) {
        worker.recordRetirementAdmissionFailure();
        worker.requestMaintenance();
        return false;
      }
      retirementPrepared = true;
      long newTaggedValue = Entry.tagValueAddress(replacement, expireAtMillis > 0L);
      Entry.WeakValueSlot newWeakValue = prepareWeakValue(value, null, newTaggedValue);
      entry.publishValue(newTaggedValue, newWeakValue);
      published = true;
      worker.retireValue(context, old, oldAllocation);
      entry.finishWriter();
      writerHeld = false;
      if (requiresMutation) {
        worker.publishMutation(entry, Entry.PENDING_UPDATE);
      }
      worker.afterWrite(context);
      return true;
    } catch (NativeMemory.AllocationLimitException pressure) {
      worker.recordNativeAllocationFailure();
      worker.requestAllocationPressure();
      return false;
    } catch (Throwable failure) {
      if (isClosing() || failure instanceof CacheMaintenanceException) {
        throwUnchecked(failure);
      }
      throwUnchecked(failure);
      return false;
    } finally {
      if (!published && retirementPrepared) {
        worker.cancelRetirement(context);
      }
      if (!published && replacement != 0L) {
        freeBlock(replacement, allocation);
      }
      if (!published && budgetReserved) {
        budget.refund(context.budgetLease(), weight);
      }
      if (writerHeld) {
        entry.finishWriter();
      }
    }
  }

  @Override
  public CompletableFuture<Boolean> removeAsync(K key) {
    Objects.requireNonNull(key, "key");
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    submitAsyncMutation(result, () -> result.complete(remove(key)));
    return result;
  }

  private void submitAsyncMutation(CompletableFuture<Boolean> result, Runnable action) {
    worker.submitAsyncMutation(action, result::completeExceptionally);
  }

  @Override
  public CompletableFuture<V> getOrLoadAsync(K key, CacheLoader<K, V> loader, long expireAtMillis) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(loader, "loader");
    if (closing) {
      return CompletableFuture.completedFuture(null);
    }
    ThreadContext context = contexts.get();
    KeyEncoder.encode(keySerializer, key, context);
    V existing = getEncoded(context);
    if (existing != null) {
      return CompletableFuture.completedFuture(existing);
    }
    if (loaderExecutor == null) {
      CompletableFuture<V> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("loaderExecutor is not configured"));
      return failed;
    }
    EncodedKey flightKey =
        EncodedKey.fromSerialized(
            context.keyBytes, context.lookupKey.length(), context.lookupKey.hash64());
    CompletableFuture<V> result = new CompletableFuture<>();
    CompletableFuture<V> previous = loadFlights.putIfAbsent(flightKey, result);
    if (previous != null) {
      return previous;
    }
    result.whenComplete((value, failure) -> loadFlights.remove(flightKey, result));
    try {
      loaderExecutor.execute(() -> loadAndPublish(key, flightKey, loader, expireAtMillis, result));
    } catch (Throwable failure) {
      result.completeExceptionally(failure);
    }
    return result;
  }

  private void loadAndPublish(
      K key,
      EncodedKey encodedKey,
      CacheLoader<K, V> loader,
      long expireAtMillis,
      CompletableFuture<V> result) {
    try {
      V current = getEncoded(encodedKey);
      if (current != null) {
        result.complete(current);
        return;
      }
      V loaded = loader.load(key);
      if (loaded == null) {
        result.complete(null);
        return;
      }
      worker.refreshClock();
      boolean inserted = putIfAbsentEncoded(encodedKey, loaded, expireAtMillis);
      worker.refreshClock();
      if (inserted) {
        result.complete(
            expireAtMillis > 0L && expireAtMillis <= ticker.currentTimeMillis() ? null : loaded);
      } else {
        V winner = getEncoded(encodedKey);
        result.complete(
            winner != null
                ? winner
                : expireAtMillis > 0L && expireAtMillis <= ticker.currentTimeMillis()
                    ? null
                    : loaded);
      }
    } catch (Throwable failure) {
      result.completeExceptionally(failure);
    }
  }

  private boolean putIfAbsentEncoded(EncodedKey key, V value, long expireAtMillis) {
    ThreadContext context = enterWriter();
    if (context == null) {
      if (isClosing()) {
        throw new IllegalStateException("cache is closed");
      }
      worker.recordNonBlockingPutFailure();
      return false;
    }
    try {
      bindEncodedKey(context, key);
      return putIfAbsentValue(
          context, context.lookupKey, context.keyBytes, key.length(), value, expireAtMillis);
    } finally {
      exitWriter(context);
    }
  }

  @Override
  public CompletableFuture<Void> flushAsync() {
    if (closeState.get() != OPEN) {
      return CompletableFuture.completedFuture(null);
    }
    ThreadContext context = contexts.get();
    if (context.isRegistered()) {
      context.flushRead();
    }
    return worker.flush();
  }

  @Override
  public long size() {
    return data.size();
  }

  @Override
  public long capacity() {
    return capacity;
  }

  @Override
  public long totalAllocatedBytes() {
    return memory.allocated();
  }

  long rawNativeAllocations() {
    return memory.rawAllocationCount();
  }

  long entryNativeAllocations() {
    return memory.entryAllocationCount();
  }

  long nativeHardLimitForTest() {
    return nativeHardLimit;
  }

  ConcurrentHashMap<Entry, Entry> dataForTest() {
    return data;
  }

  @Override
  public OHCacheStats stats() {
    long resident = budget.reserved();
    MaintenanceEventLoop.Snapshot snapshot = worker.snapshot();
    return new OHCacheStats(
        snapshot.hits,
        snapshot.misses,
        snapshot.queueDepth,
        snapshot.queueCapacity,
        snapshot.unhealthy,
        snapshot.physicalExpired,
        snapshot.timeoutLagMillis,
        snapshot.ttlBacklog,
        snapshot.evicted,
        snapshot.retiredEntries,
        size(),
        snapshot.liveWeight,
        resident,
        snapshot.retiredBytes,
        memory.allocated(),
        snapshot.timerBytes,
        snapshot.sketchBytes,
        snapshot.ghostHeapBytes,
        snapshot.ledgerBytes,
        snapshot.retirementQueueDepth,
        snapshot.retirementQueueCapacity,
        snapshot.nonBlockingPutFailures,
        snapshot.nonBlockingReplaceFailures,
        snapshot.nonBlockingRemoveFailures,
        snapshot.writerContentionFailures,
        snapshot.retirementAdmissionFailures,
        snapshot.reliableRemovalAdmissionFailures,
        snapshot.nativeAllocationFailures,
        snapshot.repairQueueDepth,
        snapshot.asyncMutationQueueDepth,
        snapshot.asyncMutationFailedCount,
        snapshot.asyncMutationRejectedCount);
  }

  @Override
  public void close() {
    if (closeState.get() == CLOSED) {
      return;
    }
    closeState.compareAndSet(OPEN, CLOSING);
    closing = true;
    worker.beginClosing();
    long deadline =
        System.nanoTime()
            + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Math.max(1L, closeTimeoutMillis));
    Thread current = Thread.currentThread();
    if (closeLeader.compareAndSet(null, current)) {
      try {
        if (!awaitWriters(deadline)) {
          throw new IllegalStateException(
              "close timed out with active writers=" + readers.activeWriterCount());
        }
        if (shutdownStarted.compareAndSet(false, true)) {
          worker.stop();
        }
      } finally {
        closeLeader.compareAndSet(current, null);
      }
    }
    long remainingNanos = deadline - System.nanoTime();
    long timeout =
        Math.max(
            1L, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, remainingNanos)));
    try {
      worker.join(timeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while closing", e);
    }
    if (worker.isAlive()) {
      throw new IllegalStateException(
          "close timed out with active readers or writers=" + readers.activeWriterCount());
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
   * A pointer-only replacement leaves actor state valid when its policy weight and TTL deadline are
   * unchanged. The old block still enters QSBR retirement, but no mutation transport or
   * policy/timer pass is required.
   */
  private static boolean maintenanceUpdateRequired(
      Entry entry,
      long oldTaggedAddress,
      long oldAddress,
      long oldWeight,
      long newWeight,
      long expireAtMillis) {
    if (entry.policyState() == Entry.POLICY_NONE || oldAddress == 0L || oldWeight != newWeight) {
      return true;
    }
    boolean oldHasTtl = Entry.hasTtl(oldTaggedAddress);
    boolean newHasTtl = expireAtMillis > 0L;
    return oldHasTtl != newHasTtl
        || (oldHasTtl && ValueBlock.expireAtMillis(oldAddress) != expireAtMillis);
  }

  private boolean removeCurrent(Entry entry) {
    if (!entry.isAlive()) {
      return false;
    }
    entry.markRetired();
    if (data.remove(entry, entry)) {
      return true;
    }
    entry.restoreAlive();
    return false;
  }

  private boolean claimWriter(Entry entry) {
    boolean claimed = !closing && entry.isAlive() && entry.claimWriter();
    if (!claimed) {
      worker.recordWriterContentionFailure();
    }
    return claimed;
  }

  private boolean isClosing() {
    return closing || closeState.get() != OPEN;
  }

  private boolean prepareRetirement(ThreadContext context, int records) {
    worker.throwIfUnavailable();
    return worker.prepareRetirement(context, records);
  }

  private boolean reserveBudget(ThreadContext context, long weight) {
    if (weight <= 0L || weight > byteCapacity) {
      throw new IllegalArgumentException("entry allocation exceeds cache capacity");
    }
    worker.throwIfUnavailable();
    if (budget.tryReserve(context.budgetLease(), weight)) {
      return true;
    }
    worker.requestBudgetPressure();
    return false;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
    throw (T) failure;
  }

  /**
   * Enters the native-writing domain. The second state check closes the race with close(): a writer
   * that races after CLOSING never touches an arena, while a writer that won admission keeps close
   * from releasing cache-owned native memory until its finally block completes.
   */
  private ThreadContext enterWriter() {
    if (closeState.get() != OPEN) {
      return null;
    }
    ThreadContext context = contexts.get();
    if (!context.isRegistered()) {
      context.bindMaintenance(worker);
      context.markRegistered();
      worker.registerReader(context.slot);
    }
    context.slot.writerActive = true;
    boolean activated = false;
    boolean admitted = false;
    try {
      if (closeState.get() != OPEN) {
        return null;
      }
      if (!context.hasWriterResources()) {
        context.bindWriterResources(
            memory.writerForCurrentThread(), budget.leaseForCurrentThread());
      }
      if (!context.tryActivateWriter()) {
        return null;
      }
      activated = true;
      if (closeState.get() == OPEN) {
        admitted = true;
        return context;
      }
      return null;
    } finally {
      if (!admitted) {
        context.slot.writerActive = false;
        if (activated) {
          context.deactivateWriter();
        }
      }
    }
  }

  private void exitWriter(ThreadContext context) {
    context.slot.writerActive = false;
    context.deactivateWriter();
    Thread waiter = closeWaiter;
    if (waiter != null) {
      LockSupport.unpark(waiter);
    }
  }

  private boolean awaitWriters(long deadlineNanos) {
    closeWaiter = Thread.currentThread();
    try {
      while (readers.hasActiveWriter()) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) {
          return false;
        }
        if (readers.hasActiveWriter()) {
          LockSupport.parkNanos(this, remaining);
        }
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
    if (entry == null || !entry.isAlive()) {
      return 0L;
    }
    long taggedValue = entry.valueAddress;
    if (taggedValue == 0L) {
      return 0L;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (!Entry.hasTtl(taggedValue)) {
      return value;
    }
    return ValueBlock.expired(value, ticker.currentTimeMillis()) ? 0L : value;
  }

  private boolean expectedMatches(Entry entry, long expectedAddress, int expectedLength) {
    long taggedValue = entry.valueAddress;
    long value = Entry.rawValueAddress(taggedValue);
    return mappingIsCurrent(entry)
        && value != 0L
        && (!Entry.hasTtl(taggedValue) || !ValueBlock.expired(value, ticker.currentTimeMillis()))
        && ValueBlock.length(value) == expectedLength
        && NativeMemory.equals(ValueBlock.payloadAddress(value), expectedAddress, expectedLength);
  }

  private long defaultExpiry(long hash) {
    if (defaultTtlMillis <= 0L) {
      return 0L;
    }
    long now = ticker.currentTimeMillis();
    long jitterRange = (long) (defaultTtlMillis * ttlJitterPercent);
    if (jitterRange > 0L) {
      long span = jitterRange > (Long.MAX_VALUE - 1L) / 2L ? Long.MAX_VALUE : jitterRange * 2L + 1L;
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

  private void rejectByteBufferValue(Object value) {
    if (weakValues && value instanceof ByteBuffer) {
      throw new IllegalArgumentException(
          "weakValues(true) does not support ByteBuffer values; use weakValues(false) or a non-ByteBuffer value type");
    }
  }

  @SuppressWarnings("unchecked")
  private V weakValue(Entry entry, long rawValueAddress, Entry.ValueState state) {
    if (!weakValues) {
      return null;
    }
    if (state == null || Entry.rawValueAddress(state.taggedValueAddress()) != rawValueAddress) {
      return null;
    }
    Entry.WeakValueSlot slot = state.weakValue();
    if (slot == null || entry.valueAddress != state.taggedValueAddress()) {
      return null;
    }
    return (V) slot.get();
  }

  private Entry.WeakValueSlot prepareWeakValue(
      Object value, byte[] valueBytes, long taggedValueAddress) {
    if (!weakValues) {
      return null;
    }
    if (valueBytes != null || value == null) {
      return null;
    }
    return new Entry.WeakValueSlot(value, taggedValueAddress);
  }

  private void publishWeakValueIfCurrent(
      Entry entry, Object value, long taggedValueAddress, Entry.ValueState observedState) {
    if (!weakValues) {
      return;
    }
    try {
      if (observedState == null || observedState.taggedValueAddress() != taggedValueAddress) {
        return;
      }
      if (entry.valueAddress != taggedValueAddress) {
        return;
      }
      Entry.WeakValueSlot replacement =
          value == null ? null : new Entry.WeakValueSlot(value, taggedValueAddress);
      Entry.ValueState next = new Entry.ValueState(taggedValueAddress, replacement);
      entry.compareAndSetValueState(observedState, next);
    } catch (OutOfMemoryError ignored) {
      // Weak-value backfill is an optimization; the native-deserialized result remains valid.
    }
  }

  private static long mix64(long value) {
    value += 0x9e3779b97f4a7c15L;
    value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
    value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
    return value ^ (value >>> 31);
  }

  @SuppressWarnings("rawtypes")
  private static int serializedSize(CacheSerializer serializer, Object value) {
    int length = serializer.serializedSize(value);
    if (length < 0) {
      throw new IllegalArgumentException("negative serialized length");
    }
    return length;
  }

  @SuppressWarnings("rawtypes")
  private void writeValue(
      ThreadContext context, long payloadAddress, Object value, byte[] encoded, int length) {
    if (encoded != null) {
      NativeMemory.copy(encoded, 0, payloadAddress, length);
      return;
    }
    ByteBuffer buffer = context.writableValueBuffer(payloadAddress, length);
    try {
      valueSerializer.serialize((V) value, buffer);
      if (buffer.position() != length) {
        throw new IllegalArgumentException(
            "value serializer wrote " + buffer.position() + " bytes, expected " + length);
      }
    } finally {
      context.invalidateWritableValueBuffer(buffer);
    }
  }

  private long allocateScratch(ThreadContext context, long allocation) {
    try {
      return context.writer().allocate(allocation);
    } catch (NativeMemory.AllocationLimitException pressure) {
      worker.recordNativeAllocationFailure();
      worker.requestAllocationPressure();
      return 0L;
    }
  }

  private void freeEntry(ThreadContext context, Entry entry, long valueAllocation) {
    freeBlock(Entry.rawValueAddress(entry.valueAddress), valueAllocation);
    freeBlock(entry.nativeKeyAddress, entry.keyAllocationLength());
    budget.refund(context.budgetLease(), entryWeight(entry, valueAllocation));
  }

  private void freeBlock(long address, long allocation) {
    if (address != 0L) {
      memory.releaseEntry(address, allocation);
    }
  }

  private static long allocationWeight(long allocation) {
    return allocation == 0L ? 0L : WriterArena.allocationWeight(allocation);
  }

  private static long entryWeight(Entry entry, long valueAllocation) {
    return allocationWeight(entry.keyAllocationLength()) + allocationWeight(valueAllocation);
  }

  private void ensureReplacementFitsCapacity(long keyAllocation, long valueAllocation) {
    long keyWeight = allocationWeight(keyAllocation);
    long valueWeight = allocationWeight(valueAllocation);
    if (keyWeight > byteCapacity
        || valueWeight > byteCapacity
        || keyWeight > byteCapacity - valueWeight) {
      throw new IllegalArgumentException(
          "replacement key and value allocations exceed cache capacity");
    }
  }

  private static long saturatedAdd(long left, long right) {
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private static long maxSizeHighWatermark(long maxSize) {
    long overshoot = maxSize / 64L + (maxSize % 64L == 0L ? 0L : 1L);
    overshoot = Math.max(1L, Math.min(1_024L, overshoot));
    return saturatedAdd(maxSize, overshoot);
  }

  private static long saturatedMultiply(long left, long right) {
    return left != 0L && right > Long.MAX_VALUE / left ? Long.MAX_VALUE : left * right;
  }

  private static long nextPowerOfTwo(long value) {
    long result = 1L;
    while (result < value && result <= (1L << 30)) {
      result <<= 1;
    }
    return result;
  }
}
