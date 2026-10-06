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
import java.util.function.BiFunction;
import java.util.function.Function;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.DirectEntryConsumer;
import com.red.ohc.api.DirectValueConsumer;
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
import com.red.ohc.maintenance.EntryLinks;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.RetirementSegment;
import com.red.ohc.maintenance.TimerWheel;
import com.red.ohc.maintenance.WriterLifecycleJournal;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.runtime.DirectValueView;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.runtime.WriterResource;
import com.red.ohc.runtime.WriterResourceRegistry;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

/** CHM authority with native payloads and one asynchronous maintenance worker. */
public final class OffHeapCache<K, V> implements OHCache<K, V> {
  private static final long DEFAULT_TTL = Long.MIN_VALUE;
  private static final long VALUE_LIFECYCLE_MASK = 0b110L;
  private static final int BULK_READ_CHUNK_SIZE = 4_096;
  private static final int EXPIRED_NOOP = 0;
  private static final int EXPIRED_REMOVED = 1;
  private static final int EXPIRED_WRITER_BUSY = -2;

  final ConcurrentHashMap<Entry, Entry> data;
  private final CacheSerializer<K> keySerializer;
  private final CacheSerializer<V> valueSerializer;
  private final EvictionListener<K, V> evictionListener;
  private final Ticker ticker;
  private final MonotonicDeadlineClock deadlineClock;
  private final long defaultTtlMillis;
  private final boolean ttlEnabled;
  private final long capacity;
  private final boolean countBounded;
  private final WriterLifecycleJournal writerLifecycleJournal;
  private final RetirementJournal retirementJournal;
  private final WriterResourceRegistry writerResources;
  private final NativeMemory.Memory memory;
  private final ReaderRegistry readers;
  private final ThreadLocal<ThreadContext> contexts;
  private final ThreadContext evictionContexts;
  private final MaintenanceEventLoop worker;
  private final LogicalAdmission logicalAdmission;
  private final ReaderGuard readerGuard;
  private final MapViews<K, V> mapViews;
  private final Object lifecycleLock = new Object();
  private volatile Runnable flushLifecycleHookForTest;
  private volatile PostChargeFailurePoint postChargeFailurePointForTest;

  OffHeapCache(
      CacheSerializer<K> keySerializer,
      CacheSerializer<V> valueSerializer,
      long defaultTtlMillis,
      Ticker ticker,
      Eviction eviction,
      EvictionListener<K, V> evictionListener,
      long capacity,
      long maxSize,
      long nativeDebtBudgetBytes) {
    ThreadContext.verifyNativeByteBufferSupported();
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.evictionListener = evictionListener;
    this.ticker = ticker;
    this.deadlineClock = new MonotonicDeadlineClock(ticker);
    this.defaultTtlMillis = defaultTtlMillis;
    this.ttlEnabled = defaultTtlMillis > 0L;
    boolean countBounded = maxSize > 0L;
    this.countBounded = countBounded;
    this.capacity = countBounded ? -1L : capacity;
    long limit = countBounded ? maxSize : capacity;
    long entryEstimate = countBounded ? maxSize : 0L;
    int initialCapacity = ChmSizing.constructorCapacity(entryEstimate, limit);
    this.data = new ConcurrentHashMap<>(initialCapacity, 0.75f, 1);
    this.writerLifecycleJournal = new WriterLifecycleJournal();
    this.memory = new NativeMemory.Memory();
    this.readers = new ReaderRegistry(this.memory);
    EntryLinks links = new EntryLinks(memory);
    this.logicalAdmission = new LogicalAdmission(limit, countBounded);
    this.retirementJournal = new RetirementJournal(memory);
    this.writerResources =
        new WriterResourceRegistry(memory, writerLifecycleJournal, retirementJournal);
    this.contexts = ThreadLocal.withInitial(() -> new ThreadContext(null));
    this.evictionContexts = evictionListener == null ? null : new ThreadContext(null);
    this.worker =
        new MaintenanceEventLoop(
            data,
            memory,
            deadlineClock,
            limit,
            eviction,
            evictionListener == null ? null : this::notifyEviction,
            readers,
            countBounded,
            retirementJournal,
            nativeDebtBudgetBytes,
            links);
    worker.bindLogicalAdmission(logicalAdmission);
    worker.bindWriterLifecycleJournal(writerLifecycleJournal);
    worker.bindWriterResourceRegistry(writerResources);
    this.readerGuard = new ReaderGuard(worker);
    this.mapViews = new MapViews<>(this);
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
      ThreadContext context = evictionContexts;
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
                      valueLength));
      context.enterUserCallback();
      try {
        listener.onEviction(key, value, cause);
      } finally {
        context.exitUserCallback();
      }
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
    ByteBuffer buffer = context.readOnlyValueBuffer(address, length);
    try {
      return serializer.deserialize(buffer);
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

  private static final class PreviousValue<T> {
    private T value;
  }

  private enum ComputeKind {
    IF_ABSENT,
    IF_PRESENT,
    COMPUTE,
    MERGE
  }

  /** Package-private failure seam for deterministic post-charge ownership tests. */
  enum PostChargeFailurePoint {
    AFTER_LEDGER_COMMIT,
    BEFORE_RETIREMENT_COMMIT,
    BEFORE_WRITER_RELEASE
  }

  @FunctionalInterface
  private interface ComputeAction<K, V> {
    V apply(K key, V current, boolean present);
  }

  private static final class ComputeAttempt<T> {
    private T result;
    private T replacement;
    private boolean readerEntered;
    private boolean retry;
    private boolean writerHeld;
    private boolean removalPrepared;
    private boolean needsPreparation;
    private boolean prepared;
    private boolean insertion;
    private boolean committed;
    private boolean stale;
    private boolean probeInserted;
    private long expectedTaggedValue;
    private long expectedGeneration;
    private long oldValue;
    private long oldAllocation;
    private long oldLogicalCharge;
    private long replacementAddress;
    private long replacementAllocation;
    private long replacementLogicalCharge;
    private long chargeDelta;
    private long deadlineNanos;
    private boolean chargeReserved;
    private boolean retirementPrepared;
    private boolean published;
    private boolean allocationUpdated;
    private boolean mutationPrepared;
    private int mutationKeyHash;
    private long mutationValueAllocation;
    private long mutationVersion = WriterLifecycleLane.UNSEEDED_MUTATION_VERSION;
    private long lifecycleSequence;
    private WriterLifecycleLane lifecycleLane;
    private RetirementJournal.Lane retirementLane;
    private Entry entry;
  }

  @Override
  public void put(K key, V value) {
    putInternal(key, value, DEFAULT_TTL);
  }

  @Override
  public void put(K key, V value, long expireAtMillis) {
    putInternal(key, value, Math.max(expireAtMillis, 0L));
  }

  @Override
  public V putIfAbsent(K key, V value, long expireAtMillis) {
    return putIfAbsentInternal(key, value, Math.max(expireAtMillis, 0L));
  }

  private V putIfAbsentInternal(K key, V value, long requestedExpiry) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      int keyLength = KeyEncoder.encode(keySerializer, key, context);
      return putIfAbsentValueReturningPrevious(
          context, context.lookupKey, context.keyBytes, keyLength, value, requestedExpiry);
    } finally {
      exitWriter(context);
    }
  }

  /** Internal conditional insert that does not materialize an existing value. */
  boolean putIfAbsentWithoutPrevious(K key, V value) {
    return putIfAbsentWithoutPreviousInternal(key, value, DEFAULT_TTL);
  }

  /** Internal conditional insert with an absolute expiry. */
  boolean putIfAbsentWithoutPrevious(K key, V value, long expireAtMillis) {
    return putIfAbsentWithoutPreviousInternal(key, value, Math.max(expireAtMillis, 0L));
  }

  private boolean putIfAbsentWithoutPreviousInternal(K key, V value, long requestedExpiry) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      int keyLength = KeyEncoder.encode(keySerializer, key, context);
      return putIfAbsentValue(
          context, context.lookupKey, context.keyBytes, keyLength, value, requestedExpiry);
    } finally {
      exitWriter(context);
    }
  }

  @Override
  public boolean replace(K key, V expected, V value, long expireAtMillis) {
    return replaceConditionalInternal(key, expected, value, Math.max(expireAtMillis, 0L));
  }

  private boolean replaceConditionalInternal(K key, V expected, V value, long requestedExpiry) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      KeyEncoder.encode(keySerializer, key, context);
      int valueLength = -1;
      long valueAllocation = 0L;
      long deadlineNanos = 0L;
      while (true) {
        Entry entry;
        long expectedTaggedValue;
        long expectedGeneration;
        enterWriterValueReader(context);
        try {
          entry = data.get(context.lookupKey);
          if (entry == null || valueIfLive(entry) == 0L) {
            return false;
          }
          expectedTaggedValue = entry.valueAddress;
          expectedGeneration = entry.generation();
          V current = snapshotValueEntered(context, entry, expectedTaggedValue);
          if (!Objects.equals(expected, current)) {
            return false;
          }
        } finally {
          exit(context);
        }
        if (valueLength < 0) {
          valueLength = serializedSize(valueSerializer, value);
          valueAllocation = ValueBlock.allocationLength(valueLength);
          deadlineNanos = resolveDeadline(context, requestedExpiry);
        }
        int result =
            replaceExistingVersioned(
                context,
                entry,
                value,
                valueLength,
                deadlineNanos,
                false,
                valueAllocation,
                0L,
                expectedTaggedValue,
                expectedGeneration);
        if (result < 0) {
          throw new IllegalStateException("cache write failed");
        }
        if (result > 0) {
          return true;
        }
        // The replacement path waits for a live writer owner before returning a retry. A version
        // mismatch is an actual semantic race and should be re-read immediately, not delayed by a
        // timer wake-up.
      }
    } finally {
      exitWriter(context);
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return putIfAbsentInternal(key, value, DEFAULT_TTL);
  }

  @Override
  public boolean remove(Object key, Object value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      KeyEncoder.encode(keySerializer, key, context);
      while (true) {
        enterWriterValueReader(context);
        Entry entry;
        long expectedTaggedValue;
        long expectedGeneration;
        try {
          entry = data.get(context.lookupKey);
          if (entry == null || valueIfLive(entry) == 0L) {
            return false;
          }
          expectedTaggedValue = entry.valueAddress;
          expectedGeneration = entry.generation();
          V current = snapshotValueEntered(context, entry, expectedTaggedValue);
          if (!Objects.equals(value, current)) {
            return false;
          }
        } finally {
          exit(context);
        }
        if (removeEntryByIdentity(context, entry, false, expectedTaggedValue, expectedGeneration)) {
          return true;
        }
        // The identity helper rejects stale versions to prevent ABA. Re-read the Java mapping
        // immediately; writer conflicts have already been handled by the event-driven claim path.
      }
    } finally {
      exitWriter(context);
    }
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return replaceConditionalInternal(key, oldValue, newValue, DEFAULT_TTL);
  }

  @Override
  public V replace(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      KeyEncoder.encode(keySerializer, key, context);
      int valueLength = -1;
      long valueAllocation = 0L;
      long deadlineNanos = 0L;
      while (true) {
        Entry entry;
        long expectedTaggedValue;
        long expectedGeneration;
        enterWriterValueReader(context);
        try {
          entry = data.get(context.lookupKey);
          if (entry == null || valueIfLive(entry) == 0L) {
            return null;
          }
          expectedTaggedValue = entry.valueAddress;
          expectedGeneration = entry.generation();
        } finally {
          exit(context);
        }
        if (valueLength < 0) {
          valueLength = serializedSize(valueSerializer, value);
          valueAllocation = ValueBlock.allocationLength(valueLength);
          deadlineNanos = resolveDeadline(context, DEFAULT_TTL);
        }
        PreviousValue<Object> previous = new PreviousValue<>();
        int result =
            replaceExistingReturningPreviousVersioned(
                context,
                entry,
                value,
                valueLength,
                deadlineNanos,
                false,
                valueAllocation,
                0L,
                expectedTaggedValue,
                expectedGeneration,
                previous);
        if (result < 0) {
          throw new IllegalStateException("cache write failed");
        }
        if (result > 0) {
          @SuppressWarnings("unchecked")
          V previousValue = (V) previous.value;
          return previousValue;
        }
        // A version mismatch is re-evaluated immediately. A writer conflict is waited on by the
        // next compute pass after it has left the CHM remapping callback.
      }
    } finally {
      exitWriter(context);
    }
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> function) {
    Objects.requireNonNull(function, "function");
    return computeInternal(
        key,
        ComputeKind.IF_ABSENT,
        (computeKey, current, present) -> present ? current : function.apply(computeKey));
  }

  @Override
  public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> function) {
    Objects.requireNonNull(function, "function");
    return computeInternal(
        key,
        ComputeKind.IF_PRESENT,
        (computeKey, current, present) -> present ? function.apply(computeKey, current) : null);
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> function) {
    Objects.requireNonNull(function, "function");
    return computeInternal(
        key,
        ComputeKind.COMPUTE,
        (computeKey, current, present) -> function.apply(computeKey, current));
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> function) {
    Objects.requireNonNull(value, "value");
    Objects.requireNonNull(function, "function");
    return computeInternal(
        key,
        ComputeKind.MERGE,
        (computeKey, current, present) -> present ? function.apply(current, value) : value);
  }

  private V computeInternal(K key, ComputeKind kind, ComputeAction<K, V> action) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      int keyLength = KeyEncoder.encode(keySerializer, key, context);
      int computeKeyHash = context.lookupKey.hash();
      Entry probe = allocateKeyProbe(context, context.lookupKey, context.keyBytes, keyLength);
      boolean probeOwned = true;
      boolean probeMayBeMapped = false;
      try {
        while (true) {
          ComputeAttempt<V> attempt = new ComputeAttempt<>();
          // The callback may perform nested reads, which reuse and mutate lookupKey. Capture the
          // outer key's immutable hash before entering user code and carry it through this attempt.
          attempt.mutationKeyHash = computeKeyHash;
          try {
            runComputePass(context, key, probe, kind, action, attempt, false);
            if (attempt.retry) {
              handleComputeRetry(context, probe, attempt);
              continue;
            }

            if (attempt.needsPreparation) {
              prepareComputeReplacement(context, attempt, probe);
              attempt.prepared = true;
              // The second CHM pass only validates/claims the Entry. The logical charge is
              // committed after this callback, so the cache-local gate must not span CHM work.
              runComputePass(context, key, probe, kind, action, attempt, true);
              if (attempt.retry) {
                discardPreparedComputeAttempt(context, attempt, probe);
                handleComputeRetry(context, probe, attempt);
                continue;
              }
            }

            if (attempt.probeInserted) {
              // data.compute has made the probe visible. Complete the logical charge after the CHM
              // callback returns; victim removal must never run while a CHM bin lock is held.
              probeMayBeMapped = true;
              // CHM now owns the probe, even if a remover retires it before the hint handoff ends.
              probeOwned = false;
              commitPreparedComputeInsertion(context, probe, attempt, false);
            } else if (attempt.prepared && !attempt.insertion && attempt.writerHeld) {
              // The second compute pass only claims the current Entry. Native publication and
              // cross-key victim removal happen outside the CHM remapping callback.
              publishPreparedComputeReplacement(context, attempt.entry, attempt);
            }
            finishComputeAttempt(context, attempt);
            return attempt.result;
          } catch (Throwable failure) {
            probeMayBeMapped |= attempt.probeInserted;
            boolean probeVisible = attempt.probeInserted && isMappedEntry(probe);
            if (probeVisible) {
              // The CHM node is already visible. Preserve it for terminal shutdown instead of
              // trying to restore an old mapping after a post-publication failure.
              worker.recordTerminalFailure(failure);
              probeOwned = false;
            }
            abortComputeAttempt(context, attempt);
            discardPreparedComputeAttempt(context, attempt, probe, failure);
            throwUnchecked(failure);
            return null;
          }
        }
      } finally {
        if (probeOwned) {
          long value = Entry.rawValueAddress(probe.valueAddress);
          boolean probeVisible = probeMayBeMapped && isMappedEntry(probe);
          if (value != 0L && !probeVisible) {
            freeBlock(value, probe.currentValueAllocation());
            probe.clearValue();
          }
          if (!probeVisible) {
            freeCandidateKey(probe);
          }
        }
      }
    } finally {
      exitWriter(context);
    }
  }

  private void runComputePass(
      ThreadContext context,
      K key,
      Entry probe,
      ComputeKind kind,
      ComputeAction<K, V> action,
      ComputeAttempt<V> attempt,
      boolean prepared) {
    enterWriterReader(context);
    attempt.readerEntered = true;
    try {
      data.compute(
          probe,
          (ignored, current) ->
              prepared
                  ? applyPreparedComputeMapping(context, probe, current, attempt)
                  : computeMapping(context, key, probe, kind, action, current, attempt));
    } finally {
      if (attempt.readerEntered) {
        exit(context);
        attempt.readerEntered = false;
      }
    }
  }

  private void handleComputeRetry(ThreadContext context, Entry probe, ComputeAttempt<V> attempt) {
    if (attempt.entry != null) {
      awaitWriterRelease(attempt.entry);
    }
    if (attempt.stale) {
      // The old Entry may have been reclaimed since the CHM pass. Revalidate with our private
      // probe before reading its native metadata or using it as an identity-removal key.
      enterWriterReader(context);
      try {
        if (data.get(probe) == attempt.entry) {
          removeStaleLookupEntry(context, attempt.entry);
        }
      } finally {
        exit(context);
      }
    }
  }

  private void commitPreparedComputeInsertion(
      ThreadContext context, Entry probe, ComputeAttempt<V> attempt, boolean deferMaintenanceWake) {
    if (!attempt.chargeReserved) {
      long charge = logicalCharge(probe.keyAllocationLength(), attempt.replacementAllocation);
      attempt.chargeDelta = charge;
      attempt.chargeReserved = reserveLogicalCharge(charge);
      if (!attempt.chargeReserved) {
        throw new IllegalStateException("logical charge reservation failed");
      }
    }
    logicalAdmission.markPresentAfterCharge(probe);
    // Preserve publication history: a later removal can clear logical presence before cleanup.
    attempt.published = true;
    maybeFailPostCharge(PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
    attempt.mutationPrepared = worker.prepareMutation(probe, Entry.PENDING_ADD);
    if (attempt.mutationPrepared) {
      attempt.mutationValueAllocation = attempt.replacementAllocation;
      attempt.mutationVersion = probe.mutationVersion();
    }
    maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
    attempt.writerHeld = false;
    releaseWriter(probe, deferMaintenanceWake, false);
    publishInsertedCandidateSafely(
        context,
        probe,
        attempt.mutationKeyHash,
        attempt.mutationValueAllocation,
        attempt.mutationVersion,
        attempt.mutationPrepared,
        deferMaintenanceWake);
  }

  private Entry allocateKeyProbe(
      ThreadContext context, LookupKey lookup, byte[] keyBytes, int keyLength) {
    long allocation = keyAllocationLength(keyLength);
    long address = allocateNative(context, allocation);
    if (address == 0L) {
      nativeAllocationRejected();
      throw new IllegalStateException("native allocation failed while preparing compute key");
    }
    try {
      NativeMemory.copy(keyBytes, 0, address, keyLength);
      Entry probe = new Entry(address, keyLength, 0L);
      probe.initializeKeyHash(lookup.hash());
      probe.initializeNativeMetadata();
      probe.currentValueAllocation(0L);
      return probe;
    } catch (Throwable failure) {
      freeBlock(address, allocation);
      throw failure;
    }
  }

  private Entry computeMapping(
      ThreadContext context,
      K key,
      Entry probe,
      ComputeKind kind,
      ComputeAction<K, V> action,
      Entry current,
      ComputeAttempt<V> attempt) {
    if (current == null) {
      releaseComputeReader(context, attempt);
      V replacement = invokeUserCallback(() -> action.apply(key, null, false));
      if (replacement != null) {
        attempt.replacement = replacement;
        attempt.insertion = true;
        attempt.needsPreparation = true;
      }
      return null;
    }

    attempt.entry = current;
    if (!current.isAlive()) {
      attempt.retry = true;
      attempt.stale = true;
      return current;
    }

    boolean valueReaderEntered = false;
    try {
      // The CHM remapping callback starts with lookup-only protection. Upgrade before the first
      // native value read and capture the pointer only after the value-protection publication.
      enterWriterValueReader(context);
      valueReaderEntered = true;
      long observedTaggedValue = current.valueAddress;
      long liveValue =
          valueIfLive(
              current,
              observedTaggedValue,
              Entry.hasTtl(observedTaggedValue) ? deadlineClock.nowNanos() : 0L);
      if (kind == ComputeKind.IF_ABSENT && liveValue != 0L) {
        attempt.result = snapshotValueEntered(context, current, observedTaggedValue);
        if (attempt.result != null) {
          return current;
        }
        // The value can expire between the fast live check and materialization. Treat that race as
        // an absent mapping so computeIfAbsent does not return null while leaving a live CHM node.
        liveValue = 0L;
      }
      if (!claimWriter(current)) {
        attempt.retry = true;
        return current;
      }
      attempt.writerHeld = true;

      // TTL is part of the logical mapping contract. Re-read it after taking the writer claim so a
      // compute/merge started just before expiry cannot invoke user code with a dead value.
      liveValue = valueIfLiveWhileWriterHeld(current);

      if (kind == ComputeKind.IF_PRESENT && liveValue == 0L) {
        prepareComputeRemoval(context, current, attempt);
        attempt.result = null;
        reacquireComputeReader(context, attempt);
        return null;
      }

      long writerTaggedValue = current.valueAddress;
      V previous =
          liveValue == 0L ? null : snapshotValueEntered(context, current, writerTaggedValue);
      exit(context);
      valueReaderEntered = false;
      releaseComputeReader(context, attempt);
      boolean present = liveValue != 0L;
      V replacement = invokeUserCallback(() -> action.apply(key, previous, present));
      if (replacement == null) {
        prepareComputeRemoval(context, current, attempt);
        attempt.result = null;
        reacquireComputeReader(context, attempt);
        return null;
      }
      attempt.replacement = replacement;
      attempt.needsPreparation = true;
      attempt.expectedTaggedValue = current.valueAddress;
      attempt.oldValue = Entry.rawValueAddress(attempt.expectedTaggedValue);
      attempt.oldAllocation = currentValueAllocation(current);
      if (attempt.oldValue != 0L && attempt.oldAllocation == 0L) {
        attempt.oldAllocation = ValueBlock.allocationLength(ValueBlock.length(attempt.oldValue));
      }
      attempt.oldLogicalCharge =
          current.isLogicallyAbsent()
              ? 0L
              : logicalCharge(current.keyAllocationLength(), attempt.oldAllocation);
      attempt.writerHeld = false;
      releaseWriter(current);
      attempt.expectedGeneration = current.generation();
      return current;
    } finally {
      if (valueReaderEntered) {
        exit(context);
      }
    }
  }

  private Entry applyPreparedComputeMapping(
      ThreadContext context, Entry probe, Entry current, ComputeAttempt<V> attempt) {
    if (attempt.insertion) {
      if (current != null) {
        attempt.entry = current;
        attempt.retry = true;
        attempt.stale = !current.isAlive();
        return current;
      }
      if (!claimWriter(probe)) {
        throw new IllegalStateException("candidate writer claim failed");
      }
      attempt.entry = probe;
      attempt.writerHeld = true;
      // Keep the private probe claimed until data.compute either publishes it or proves that it
      // did not. If CHM links the node and then throws, removal cannot retire its native blocks
      // while the caller is still resolving ownership.
      attempt.probeInserted = true;
      attempt.result = attempt.replacement;
      return probe;
    }

    attempt.entry = current;
    if (current == null) {
      attempt.retry = true;
      return null;
    }
    if (!current.isAlive()
        || !Entry.samePublishedValue(current.valueAddress, attempt.expectedTaggedValue)
        || current.generation() != attempt.expectedGeneration
        || !claimWriter(current)) {
      attempt.retry = true;
      attempt.stale = !current.isAlive();
      return current;
    }
    attempt.writerHeld = true;
    if (!Entry.samePublishedValue(current.valueAddress, attempt.expectedTaggedValue)
        || current.generation() != attempt.expectedGeneration) {
      attempt.writerHeld = false;
      releaseWriter(current);
      attempt.retry = true;
      return current;
    }
    return current;
  }

  private void allocateProbeValue(
      ThreadContext context,
      Entry probe,
      V value,
      int valueLength,
      long deadlineNanos,
      long allocation) {
    long address = allocateNative(context, allocation);
    if (address == 0L) {
      nativeAllocationRejected();
      throw new IllegalStateException("native allocation failed while inserting computed value");
    }
    try {
      initializeValueBlock(context, address, deadlineNanos, valueLength);
      writeValue(context, ValueBlock.payloadAddress(address), value, valueLength);
      long taggedValue =
          Entry.tagValueAddress(address, deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE);
      probe.currentValueAllocation(allocation);
      probe.valueAddress = taggedValue | Entry.VALUE_LOGICALLY_ABSENT;
    } catch (Throwable failure) {
      freeBlock(address, allocation);
      throw failure;
    }
  }

  private void prepareComputeReplacement(
      ThreadContext context, ComputeAttempt<V> attempt, Entry entryProbe) {
    long deadlineNanos = resolveDeadline(context, DEFAULT_TTL);
    int valueLength = serializedSize(valueSerializer, attempt.replacement);
    long newAllocation = ValueBlock.allocationLength(valueLength);
    try {
      if (attempt.insertion) {
        allocateProbeValue(
            context, entryProbe, attempt.replacement, valueLength, deadlineNanos, newAllocation);
        attempt.deadlineNanos = deadlineNanos;
        attempt.replacementAllocation = newAllocation;
        return;
      }
      long replacementLogicalCharge =
          logicalCharge(attempt.entry.keyAllocationLength(), newAllocation);

      long replacement =
          allocateReplacement(
              context, attempt.replacement, valueLength, deadlineNanos, newAllocation);
      if (replacement == 0L) {
        nativeAllocationRejected();
        throw new IllegalStateException("native allocation failed while replacing computed value");
      }
      attempt.replacementAddress = replacement;
      attempt.replacementAllocation = newAllocation;
      attempt.replacementLogicalCharge = replacementLogicalCharge;
      attempt.deadlineNanos = deadlineNanos;
      long newTaggedValue =
          Entry.tagValueAddress(replacement, deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE);
      RetirementJournal.Lane retirementLane = context.retirementLane();
      if (attempt.oldValue != 0L) {
        if (!retirementLane.reserve(context.retirementReservation())) {
          throw new IllegalStateException("retirement journal is closed");
        }
        attempt.retirementPrepared = true;
        retirementLane.write(
            context.retirementReservation(), attempt.oldValue, attempt.oldAllocation);
        attempt.retirementLane = retirementLane;
      }
    } catch (Throwable failure) {
      discardPreparedComputeAttempt(context, attempt, entryProbe, failure);
      throwUnchecked(failure);
    }
  }

  /** Publishes a fully prepared replacement while holding only the final Entry writer claim. */
  private void publishPreparedComputeReplacement(
      ThreadContext context, Entry entry, ComputeAttempt<V> attempt) {
    RetirementJournal.Lane retirementLane = attempt.retirementLane;
    boolean published = false;
    boolean allocationUpdated = false;
    long oldAllocation = attempt.oldAllocation;
    Throwable operationFailure = null;
    long chargeDelta = 0L;
    try {
      long delta = attempt.replacementLogicalCharge - attempt.oldLogicalCharge;
      if (delta != 0L) {
        chargeDelta = delta;
        attempt.chargeDelta = delta;
        attempt.chargeReserved = reserveLogicalCharge(delta);
        if (!attempt.chargeReserved) {
          throw new IllegalStateException("logical charge reservation failed");
        }
      }
      entry.currentValueAllocation(attempt.replacementAllocation);
      allocationUpdated = true;
      publishValue(
          entry,
          Entry.tagValueAddress(
              attempt.replacementAddress,
              attempt.deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE));
      published = true;
      attempt.published = true;
      if (attempt.oldValue != 0L && context.sampleNextReplacement()) {
        worker.recordResidenceSample(
            ValueBlock.createdAtMillis(attempt.oldValue), ticker.currentTimeMillis());
      }
      boolean maintenanceRequired =
          attempt.oldValue == 0L
              || oldAllocation != attempt.replacementAllocation
              || (attempt.oldValue != 0L
                  && replacementTimerMaintenanceRequired(
                      context,
                      entry,
                      attempt.expectedTaggedValue,
                      oldAllocation,
                      attempt.replacementAllocation,
                      attempt.deadlineNanos));
      if (maintenanceRequired) {
        attempt.mutationPrepared = worker.prepareMutation(entry, Entry.PENDING_UPDATE);
        if (attempt.mutationPrepared) {
          attempt.mutationValueAllocation = attempt.replacementAllocation;
          attempt.mutationVersion = entry.mutationVersion();
        }
      }
      if (attempt.chargeReserved) {
        logicalAdmission.completeReplacement(entry);
        maybeFailPostCharge(PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
      }
      if (attempt.retirementPrepared) {
        if (attempt.chargeReserved) {
          maybeFailPostCharge(PostChargeFailurePoint.BEFORE_RETIREMENT_COMMIT);
        }
        retirementLane.commit(context.retirementReservation());
        attempt.retirementPrepared = false;
      }
      if (attempt.chargeReserved) {
        maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
      }
      attempt.writerHeld = false;
      releaseWriter(entry, false);
      attempt.replacementAddress = 0L;
      attempt.committed = true;
      attempt.result = attempt.replacement;
    } catch (Throwable failure) {
      operationFailure = failure;
      throwUnchecked(failure);
    } finally {
      Throwable cleanupFailure = null;
      if (attempt.retirementPrepared) {
        try {
          if (published) {
            retirementLane.commit(context.retirementReservation());
          } else {
            retirementLane.cancel(context.retirementReservation());
          }
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        } finally {
          attempt.retirementPrepared = false;
        }
      }
      if (!published && allocationUpdated) {
        try {
          entry.currentValueAllocation(oldAllocation);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (!published && attempt.replacementAddress != 0L) {
        try {
          freeBlock(attempt.replacementAddress, attempt.replacementAllocation);
          attempt.replacementAddress = 0L;
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (attempt.writerHeld) {
        try {
          attempt.writerHeld = false;
          releaseWriter(entry, false);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      try {
        if (attempt.mutationPrepared || claimMutationRetryProtected(context, entry)) {
          enqueueWriterMutationHint(
              context,
              entry,
              attempt.mutationKeyHash,
              attempt.mutationPrepared ? attempt.mutationValueAllocation : 0L,
              attempt.mutationPrepared
                  ? attempt.mutationVersion
                  : WriterLifecycleLane.UNSEEDED_MUTATION_VERSION,
              false);
        }
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
      if (cleanupFailure != null && operationFailure != null) {
        operationFailure.addSuppressed(cleanupFailure);
      }
      if (published && operationFailure != null) {
        worker.recordTerminalFailure(operationFailure);
      } else if (!published && attempt.chargeReserved) {
        try {
          logicalAdmission.rollbackUnpublishedDelta(chargeDelta);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (attempt.chargeReserved) {
        if (cleanupFailure != null) {
          worker.recordTerminalFailure(cleanupFailure);
        }
        attempt.chargeReserved = false;
      }
      if (cleanupFailure != null && operationFailure == null) {
        throwUnchecked(cleanupFailure);
      }
    }
  }

  private void discardPreparedComputeAttempt(
      ThreadContext context, ComputeAttempt<V> attempt, Entry probe) {
    discardPreparedComputeAttempt(context, attempt, probe, null);
  }

  private void discardPreparedComputeAttempt(
      ThreadContext context, ComputeAttempt<V> attempt, Entry probe, Throwable operationFailure) {
    Throwable cleanupFailure = null;
    if (attempt.retirementPrepared) {
      try {
        attempt.retirementLane.cancel(context.retirementReservation());
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      } finally {
        attempt.retirementPrepared = false;
      }
    }
    if (!attempt.published && attempt.replacementAddress != 0L) {
      try {
        freeBlock(attempt.replacementAddress, attempt.replacementAllocation);
        attempt.replacementAddress = 0L;
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (attempt.insertion && !attempt.probeInserted) {
      long value = Entry.rawValueAddress(probe.valueAddress);
      if (value != 0L) {
        try {
          freeBlock(value, probe.currentValueAllocation());
          probe.clearValue();
          probe.currentValueAllocation(0L);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
    }
    if (attempt.chargeReserved) {
      boolean publishedMapping =
          attempt.published
              || (attempt.insertion && attempt.probeInserted && !probe.isLogicallyAbsent());
      if (publishedMapping) {
        Throwable failure =
            operationFailure != null
                ? operationFailure
                : cleanupFailure != null
                    ? cleanupFailure
                    : new IllegalStateException("logical charge failed after publication");
        worker.recordTerminalFailure(failure);
      } else {
        try {
          logicalAdmission.rollbackUnpublishedDelta(attempt.chargeDelta);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      attempt.chargeReserved = false;
    }
    if (cleanupFailure != null) {
      if (operationFailure != null) {
        operationFailure.addSuppressed(cleanupFailure);
      } else {
        throwUnchecked(cleanupFailure);
      }
    }
  }

  private void prepareComputeRemoval(
      ThreadContext context, Entry entry, ComputeAttempt<V> attempt) {
    WriterLifecycleLane lane = context.lifecycleLane();
    long value = Entry.rawValueAddress(entry.valueAddress);
    long valueAllocation = currentValueAllocation(entry);
    long sequence = lane.reserve();
    try {
      lane.writeRemoval(sequence, entry, value, valueAllocation, entry.generation(), null);
      entry.markRetired();
      attempt.lifecycleLane = lane;
      attempt.lifecycleSequence = sequence;
      attempt.removalPrepared = true;
    } catch (Throwable failure) {
      try {
        worker.cancelWriterLifecycle(lane, sequence, false);
      } catch (Throwable cancelFailure) {
        failure.addSuppressed(cancelFailure);
      }
      throw failure;
    }
  }

  private void finishComputeAttempt(ThreadContext context, ComputeAttempt<V> attempt) {
    if (attempt.removalPrepared) {
      markLogicallyAbsent(attempt.entry);
      clearValue(attempt.entry);
      attempt.writerHeld = false;
      releaseWriter(attempt.entry);
      attempt.removalPrepared = false;
      worker.commitWriterLifecycle(attempt.lifecycleLane, attempt.lifecycleSequence, true);
      worker.throwIfUnavailable();
      return;
    }
    if (attempt.writerHeld) {
      maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
      attempt.writerHeld = false;
      releaseWriter(attempt.entry);
    }
    // Insertion releases the writer before publishing its captured mutation hint.
    attempt.chargeReserved = false;
  }

  private void abortComputeAttempt(ThreadContext context, ComputeAttempt<V> attempt) {
    if (attempt.removalPrepared) {
      attempt.entry.restoreAlive();
      markLogicallyPresent(attempt.entry);
      worker.cancelWriterLifecycle(attempt.lifecycleLane, attempt.lifecycleSequence, true);
      attempt.removalPrepared = false;
    }
    if (attempt.writerHeld) {
      attempt.writerHeld = false;
      releaseWriter(attempt.entry);
    }
  }

  private void releaseComputeReader(ThreadContext context, ComputeAttempt<V> attempt) {
    if (attempt.readerEntered) {
      exit(context);
      attempt.readerEntered = false;
    }
  }

  private void reacquireComputeReader(ThreadContext context, ComputeAttempt<V> attempt) {
    if (!attempt.readerEntered) {
      enterWriterValueReader(context);
      attempt.readerEntered = true;
    }
  }

  private boolean removeStaleLookupEntry(ThreadContext context, Entry entry) {
    if (entry == null || entry.isAlive() || entry.isWriterLocked()) {
      return false;
    }
    return context.removeEntryIfSame(data, entry);
  }

  private void putInternal(K key, V value, long expireAtMillis) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      putOne(context, key, value, expireAtMillis, false);
    } finally {
      exitWriter(context);
    }
  }

  private void putOne(
      ThreadContext context, K key, V value, long expireAtMillis, boolean deferMaintenanceWake) {
    int keyLength = KeyEncoder.encode(keySerializer, key, context);
    putSerialized(
        context,
        context.lookupKey,
        context.keyBytes,
        keyLength,
        value,
        expireAtMillis,
        deferMaintenanceWake);
  }

  private static long keyAllocationLength(int keyLength) {
    return Entry.keyPhysicalAllocationLengthForKeyLength(keyLength);
  }

  private static long logicalKeyAllocationLength(int keyLength) {
    return Entry.keyAllocationLengthForKeyLength(keyLength);
  }

  private void putSerialized(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      long requestedExpiry,
      boolean deferMaintenanceWake) {
    // These values are deterministic for one public put. Compute them once before entering the
    // retry loop; retries reuse the encoded key/value metadata while preparing a fresh candidate.
    int valueLength;
    long keyAllocation;
    long valueAllocation;
    long deadlineNanos;
    long charge;
    int hash;
    valueLength = serializedSize(valueSerializer, value);
    keyAllocation = keyAllocationLength(keyLength);
    valueAllocation = ValueBlock.allocationLength(valueLength);
    deadlineNanos = resolveDeadline(context, requestedExpiry);
    charge = logicalCharge(logicalKeyAllocationLength(keyLength), valueAllocation);
    hash = lookup.hash();
    while (true) {
      worker.throwIfUnavailable();
      if (putResolvedEntry(
          context,
          lookup,
          keyBytes,
          keyLength,
          value,
          valueLength,
          keyAllocation,
          valueAllocation,
          charge,
          deadlineNanos,
          deferMaintenanceWake,
          hash)) {
        return;
      }
    }
  }

  private boolean putResolvedEntry(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      int valueLength,
      long keyAllocation,
      long valueAllocation,
      long charge,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      int hash) {
    // Get-first: probe with the encoded key before building an insert candidate; a live hit
    // routes straight to the replace transaction. The probe runs under a reader scope like
    // the insert's putIfAbsent — equals reads the resident entry's native key block.
    Entry existing;
    enterWriterReader(context);
    try {
      existing = data.get(lookup);
    } finally {
      exit(context);
    }
    if (existing != null && existing.isAlive()) {
      if (replaceExistingResolvedResult(
          context,
          existing,
          value,
          valueLength,
          valueAllocation,
          deadlineNanos,
          deferMaintenanceWake,
          0L)) {
        return true;
      }
      // The replace revalidation rejected the entry (claimed, retired, or replaced
      // concurrently); the insert path re-resolves from scratch.
    }
    // Prepare the value before the CHM operation and let the insertion decide whether this is
    // a new mapping or a replacement; a collision reuses the prepared value in the existing
    // replacement transaction.
    return insertNewEntry(
        context,
        hash,
        keyBytes,
        keyLength,
        value,
        valueLength,
        keyAllocation,
        valueAllocation,
        charge,
        deadlineNanos,
        deferMaintenanceWake);
  }

  private boolean replaceExistingResolvedResult(
      ThreadContext context,
      Entry existing,
      Object value,
      int valueLength,
      long newAllocation,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      long preparedReplacement) {
    int result =
        replaceExisting(
            context,
            existing,
            value,
            valueLength,
            deadlineNanos,
            deferMaintenanceWake,
            newAllocation,
            preparedReplacement);
    if (result < 0) {
      throw new IllegalStateException("cache write failed");
    }
    return result > 0;
  }

  private boolean insertNewEntry(
      ThreadContext context,
      int hash,
      byte[] keyBytes,
      int keyLength,
      Object value,
      int valueLength,
      long keyAllocation,
      long valueAllocation,
      long charge,
      long deadlineNanos,
      boolean deferMaintenanceWake) {
    Entry candidate =
        allocateEntry(
            context,
            hash,
            keyBytes,
            keyLength,
            value,
            valueLength,
            deadlineNanos,
            keyAllocation,
            valueAllocation);
    if (candidate == null) {
      throw new IllegalStateException("native allocation failed while preparing cache entry");
    }
    return insertFirstCandidate(
        context,
        candidate,
        hash,
        value,
        valueLength,
        valueAllocation,
        charge,
        deadlineNanos,
        deferMaintenanceWake);
  }

  /** Insert-first publication boundary. The candidate is prepared before the CHM operation. */
  private boolean insertFirstCandidate(
      ThreadContext context,
      Entry candidate,
      int keyHash,
      Object value,
      int valueLength,
      long valueAllocation,
      long charge,
      long deadlineNanos,
      boolean deferMaintenanceWake) {
    return insertFirstCandidateTransaction(
        context,
        candidate,
        keyHash,
        value,
        valueLength,
        valueAllocation,
        charge,
        deadlineNanos,
        deferMaintenanceWake);
  }

  /** Owns the exceptional cleanup state for the insert-first transaction. */
  private boolean insertFirstCandidateTransaction(
      ThreadContext context,
      Entry candidate,
      int keyHash,
      Object value,
      int valueLength,
      long valueAllocation,
      long charge,
      long deadlineNanos,
      boolean deferMaintenanceWake) {
    boolean chargeReserved = false;
    boolean candidateOwned = true;
    boolean candidateWriterHeld = false;
    boolean handoffCompleted = false;
    try {
      // Publish a logically-absent placeholder first. Only the CHM winner may reserve its charge;
      // a collision therefore never owns a victim transaction that needs to be rolled back.
      Entry winner = insertCandidateUnderReader(context, candidate);
      if (winner == null) {
        // insertCandidateUnderReader transfers the candidate's writer claim to this method for
        // the charge/publish handoff. Track that ownership explicitly; observing the lock bit
        // in finally is racy with the next writer that may claim the now-present entry.
        candidateWriterHeld = true;
        chargeReserved = reserveLogicalCharge(charge);
        if (!chargeReserved) {
          candidateWriterHeld = false;
          removeUnadmittedCandidate(context, candidate);
          throw new IllegalStateException("logical charge reservation failed");
        }
        logicalAdmission.markPresentAfterCharge(candidate);
        maybeFailPostCharge(PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
        maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
        boolean mutationPrepared = worker.prepareMutation(candidate, Entry.PENDING_ADD);
        long mutationVersion =
            mutationPrepared
                ? candidate.mutationVersion()
                : WriterLifecycleLane.UNSEEDED_MUTATION_VERSION;
        candidateWriterHeld = false;
        releaseWriter(candidate, deferMaintenanceWake, false);
        publishNewCandidateFast(
            context,
            candidate,
            keyHash,
            valueAllocation,
            mutationVersion,
            mutationPrepared,
            deferMaintenanceWake);
        candidateOwned = false;
        handoffCompleted = true;
        chargeReserved = false;
        return true;
      }
      if (!winner.isAlive()) {
        candidateOwned = handleStaleWinner(context, candidate, winner, valueAllocation);
        return false;
      }
      long preparedReplacement = handleInsertCollision(candidate);
      candidateOwned = false;
      return replaceExistingResolvedResult(
          context,
          winner,
          value,
          valueLength,
          valueAllocation,
          deadlineNanos,
          deferMaintenanceWake,
          preparedReplacement);
    } finally {
      cleanupInsertFirstCandidate(
          context,
          candidate,
          valueAllocation,
          deferMaintenanceWake,
          charge,
          chargeReserved,
          candidateOwned,
          candidateWriterHeld,
          handoffCompleted);
    }
  }

  private void cleanupInsertFirstCandidate(
      ThreadContext context,
      Entry candidate,
      long valueAllocation,
      boolean deferMaintenanceWake,
      long charge,
      boolean chargeReserved,
      boolean candidateOwned,
      boolean candidateWriterHeld,
      boolean handoffCompleted) {
    if (candidate != null && (candidateOwned || candidateWriterHeld || chargeReserved)) {
      cleanupNewEntryAfterFailure(
          context,
          candidate,
          valueAllocation,
          deferMaintenanceWake,
          charge,
          chargeReserved,
          candidateOwned,
          candidateWriterHeld,
          handoffCompleted);
    }
  }

  /**
   * The uncontended new-key handoff: reserve, publish logical presence, then release the writer.
   */
  private void publishNewCandidateFast(
      ThreadContext context,
      Entry candidate,
      int keyHash,
      long valueAllocation,
      long mutationVersion,
      boolean mutationPrepared,
      boolean deferMaintenanceWake) {
    publishInsertedCandidateSafely(
        context,
        candidate,
        keyHash,
        valueAllocation,
        mutationVersion,
        mutationPrepared,
        deferMaintenanceWake);
  }

  /** Cold stale-winner path. The candidate remains private and is freed after the stale mapping. */
  private boolean handleStaleWinner(
      ThreadContext context, Entry candidate, Entry winner, long valueAllocation) {
    boolean staleRemovalPending;
    enterWriterReader(context);
    try {
      // The insertion guard has ended, so winner's native key may already be reclaimed.
      // Revalidate its identity using the private candidate before any native winner access.
      staleRemovalPending =
          data.get(candidate) == winner && !removeStaleLookupEntry(context, winner);
    } finally {
      exit(context);
    }
    if (staleRemovalPending) {
      awaitWriterRelease(winner);
    }
    candidate.markDead();
    freeEntry(candidate, valueAllocation);
    return false;
  }

  /** Cold same-key collision path; the candidate supplies the already serialized replacement. */
  private long handleInsertCollision(Entry candidate) {
    candidate.markDead();
    long preparedReplacement = Entry.rawValueAddress(candidate.valueAddress);
    freeCandidateKey(candidate);
    return preparedReplacement;
  }

  /** Cold rollback path for a failed new-entry publication; the successful path never enters it. */
  private void cleanupNewEntryAfterFailure(
      ThreadContext context,
      Entry candidate,
      long valueAllocation,
      boolean deferMaintenanceWake,
      long charge,
      boolean chargeReserved,
      boolean candidateOwned,
      boolean candidateWriterHeld,
      boolean handoffCompleted) {
    Throwable cleanupFailure = null;
    if (chargeReserved
        && candidateWriterHeld
        && candidate.isLogicallyAbsent()
        && isMappedEntry(candidate)) {
      try {
        // removeUnadmittedCandidate owns the writer release on this path. Transfer that
        // ownership before calling it so an exception cannot make the cleanup release the same
        // Entry a second time.
        candidateWriterHeld = false;
        removeUnadmittedCandidate(context, candidate);
        candidateOwned = false;
        logicalAdmission.rollbackUnpublishedDelta(charge);
        chargeReserved = false;
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (candidateOwned) {
      // A CHM insertion can become visible before a failure is reported by the insertion or
      // maintenance handoff. Never free native blocks while the candidate is still the map's
      // key/value. A retired candidate is owned by its lifecycle record, not this cleanup path.
      boolean candidateVisible = isMappedEntry(candidate);
      if (candidateVisible || !candidate.isAlive()) {
        candidateOwned = false;
      }
    }
    if (candidateWriterHeld) {
      candidateWriterHeld = false;
      try {
        releaseWriter(candidate, deferMaintenanceWake, false);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (chargeReserved && (!handoffCompleted || cleanupFailure != null)) {
      worker.recordTerminalFailure(
          cleanupFailure != null
              ? cleanupFailure
              : new IllegalStateException("new-key charge failed after publication"));
    }
    if (candidateOwned) {
      try {
        freeEntry(candidate, valueAllocation);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (cleanupFailure != null) {
      throwUnchecked(cleanupFailure);
    }
  }

  /** Publishes a logically-absent candidate and returns the CHM collision, if any. */
  private Entry insertCandidateUnderReader(ThreadContext context, Entry candidate) {
    Entry winner;
    boolean candidateWriterHeld = false;
    boolean readerEntered = false;
    try {
      enterWriterReader(context);
      readerEntered = true;
      try {
        // CHM publishes the candidate before the logical-present handoff below. Keep the private
        // entry writer-claimed across that interval so remove/TTL cannot unlink and retire it
        // before the charge handoff completes.
        if (!candidate.claimWriter()) {
          throw new IllegalStateException("new cache entry writer claim failed");
        }
        candidateWriterHeld = true;
        winner = data.putIfAbsent(candidate, candidate);
      } catch (Throwable failure) {
        if (isMappedEntry(candidate)) {
          // The CHM node is already visible. Keep the candidate for terminal shutdown instead of
          // unlinking it after a publication-side exception; only an unpublished private block
          // may be rolled back by the writer.
          if (candidateWriterHeld) {
            candidateWriterHeld = false;
            try {
              releaseWriter(candidate, false, false);
            } catch (Throwable releaseFailure) {
              failure.addSuppressed(releaseFailure);
            }
          }
          worker.recordTerminalFailure(failure);
        }
        throw failure;
      }
      if (candidateWriterHeld) {
        candidateWriterHeld = false;
        if (winner != null) {
          releaseWriter(candidate, false, false);
        }
      }
      return winner;
    } finally {
      if (readerEntered) {
        exit(context);
        readerEntered = false;
      }
      if (candidateWriterHeld) {
        candidateWriterHeld = false;
        releaseWriter(candidate, false, false);
      }
    }
  }

  private void removeUnadmittedCandidate(ThreadContext context, Entry candidate) {
    boolean readerEntered = false;
    boolean removed = false;
    try {
      candidate.markRetired();
      enterWriterReader(context);
      readerEntered = true;
      removed = context.removeEntryIfSame(data, candidate);
      if (!removed) {
        candidate.restoreAlive();
        throw new IllegalStateException("unadmitted candidate was not current");
      }
    } catch (Throwable failure) {
      // A CHM implementation may unlink the candidate and then throw. Treat that as an
      // unlinked candidate so the failed charge handoff cannot leak or be freed twice. If the
      // mapping is still current, keep it alive until terminal shutdown can reclaim it.
      if (!removed && !isMappedEntry(candidate)) {
        removed = true;
      }
      if (!removed && isMappedEntry(candidate)) {
        try {
          candidate.restoreAlive();
        } catch (Throwable restoreFailure) {
          failure.addSuppressed(restoreFailure);
        }
      }
      worker.recordTerminalFailure(failure);
      throw failure;
    } finally {
      if (readerEntered) {
        exit(context);
      }
      if (candidate.isWriterLocked()) {
        releaseWriter(candidate, false, false);
      }
      if (removed) {
        candidate.markDead();
        freeEntry(candidate, candidate.currentValueAllocation());
      }
    }
  }

  /** Returns CHM identity without invoking Entry.hashCode() or reading the native key hash. */
  private boolean isMappedEntry(Entry entry) {
    if (entry == null) {
      return false;
    }
    try {
      for (Entry mapped : data.values()) {
        if (mapped == entry) {
          return true;
        }
      }
      return false;
    } catch (Throwable ignored) {
      return true;
    }
  }

  private void publishInsertedCandidateSafely(
      ThreadContext context,
      Entry candidate,
      int keyHash,
      long valueAllocation,
      long mutationVersion,
      boolean mutationPrepared,
      boolean deferMaintenanceWake) {
    try {
      publishInsertedCandidate(
          context,
          candidate,
          keyHash,
          valueAllocation,
          mutationVersion,
          mutationPrepared,
          deferMaintenanceWake);
    } catch (Throwable failure) {
      worker.recordTerminalFailure(failure);
      throwUnchecked(failure);
    }
  }

  private void publishInsertedCandidate(
      ThreadContext context,
      Entry candidate,
      int keyHash,
      long valueAllocation,
      long mutationVersion,
      boolean mutationPrepared,
      boolean deferMaintenanceWake) {
    if (mutationPrepared) {
      enqueueWriterMutationHint(
          context, candidate, keyHash, valueAllocation, mutationVersion, !deferMaintenanceWake);
    }
    if (!deferMaintenanceWake && logicalAdmission.isOverTarget()) {
      worker.requestCapacityMaintenance();
    }
  }

  private void enqueueWriterMutationHint(
      ThreadContext context,
      Entry entry,
      int keyHash,
      long valueAllocation,
      long mutationVersion,
      boolean wake) {
    worker.enqueueWriterMutationHint(
        context.lifecycleLane(), entry, keyHash, valueAllocation, mutationVersion, wake);
  }

  private int replaceExisting(
      ThreadContext context,
      Entry entry,
      Object value,
      int valueLength,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      long newAllocation,
      long preparedReplacement) {
    return replaceExistingOnce(
        context,
        entry,
        value,
        valueLength,
        deadlineNanos,
        deferMaintenanceWake,
        newAllocation,
        preparedReplacement,
        null,
        0L,
        -1L);
  }

  private int replaceExistingVersioned(
      ThreadContext context,
      Entry entry,
      Object value,
      int valueLength,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      long newAllocation,
      long preparedReplacement,
      long expectedTaggedValue,
      long expectedGeneration) {
    return replaceExistingOnce(
        context,
        entry,
        value,
        valueLength,
        deadlineNanos,
        deferMaintenanceWake,
        newAllocation,
        preparedReplacement,
        null,
        expectedTaggedValue,
        expectedGeneration);
  }

  private int replaceExistingReturningPreviousVersioned(
      ThreadContext context,
      Entry entry,
      Object value,
      int valueLength,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      long newAllocation,
      long preparedReplacement,
      long expectedTaggedValue,
      long expectedGeneration,
      PreviousValue<Object> previous) {
    return replaceExistingOnce(
        context,
        entry,
        value,
        valueLength,
        deadlineNanos,
        deferMaintenanceWake,
        newAllocation,
        preparedReplacement,
        previous,
        expectedTaggedValue,
        expectedGeneration);
  }

  private int replaceExistingOnce(
      ThreadContext context,
      Entry entry,
      Object value,
      int valueLength,
      long deadlineNanos,
      boolean deferMaintenanceWake,
      long newAllocation,
      long preparedReplacement,
      PreviousValue<Object> previous,
      long expectedTaggedValue,
      long expectedGeneration) {
    worker.throwIfUnavailable();
    long replacementLogicalCharge = logicalCharge(entry.keyAllocationLength(), newAllocation);
    long replacement = preparedReplacement;
    boolean writerHeld = false;
    boolean replacementTransferred = false;
    long oldAllocation = 0L;
    Throwable operationFailure = null;
    RetirementJournal.Lane retirementLane = context.retirementLane();
    RetirementSegment.Reservation retirementReservation = context.retirementReservation();
    boolean versioned = expectedTaggedValue != 0L;
    long chargeDelta = 0L;
    try {
      replacement =
          prepareReplacementValue(
              context, value, valueLength, deadlineNanos, newAllocation, replacement);

      long newTaggedValue =
          Entry.tagValueAddress(replacement, deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE);
      long claimedStateWord = awaitWriter(entry);
      if (claimedStateWord == 0L) {
        return 0;
      }
      writerHeld = true;

      // The Entry writer claim prevents every path that can unlink, retire, or replace this Entry
      // from changing its value address or generation. Read the publication state once while the
      // claim is held; no second ReaderGuard is needed here.
      long finalTaggedValue = entry.valueAddress;
      long finalAddress = Entry.rawValueAddress(finalTaggedValue);
      long finalGeneration = Entry.generationOfStateWord(claimedStateWord);
      long finalAllocation = entry.currentValueAllocation();
      if (finalAddress != 0L && finalAllocation == 0L) {
        finalAllocation = ValueBlock.allocationLength(ValueBlock.length(finalAddress));
      }
      boolean logicallyAbsent = Entry.isAbsentTaggedValue(finalTaggedValue);
      if (!Entry.isAliveTagged(finalTaggedValue)
          || (versioned
              && (finalTaggedValue != expectedTaggedValue
                  || (expectedGeneration >= 0L && finalGeneration != expectedGeneration)))) {
        return 0;
      }

      if (expectedTaggedValue != 0L && valueIfLiveWhileWriterHeld(entry) == 0L) {
        return 0;
      }
      long oldTagged = finalTaggedValue;
      long old = finalAddress;
      oldAllocation = finalAllocation;
      if (previous != null) {
        previous.value = snapshotValueWhileWriterHeld(context, entry, oldTagged);
      }

      boolean maintenanceRequired =
          old == 0L
              || oldAllocation != newAllocation
              || (old != 0L
                  && replacementTimerMaintenanceRequired(
                      context, entry, oldTagged, oldAllocation, newAllocation, deadlineNanos));
      boolean sameChargeReplacement =
          !versioned && previous == null && old != 0L && !logicallyAbsent && !maintenanceRequired;
      if (sameChargeReplacement) {
        // The common replacement shape needs neither a logical ledger transition nor timer
        // maintenance. Transfer the writer ownership to the small fast helper; it owns the
        // retirement record and its rollback once entered.
        writerHeld = false;
        // The helper also owns replacement cleanup on failure. Mark the outer transaction as
        // transferred so its finally block cannot free that native block a second time.
        replacementTransferred = true;
        return replaceSameShapeFast(
            context,
            entry,
            replacement,
            newAllocation,
            newTaggedValue,
            old,
            oldAllocation,
            deferMaintenanceWake);
      }
      chargeDelta = 0L;
      long currentLogicalCharge =
          logicallyAbsent ? 0L : logicalCharge(entry.keyAllocationLength(), finalAllocation);
      chargeDelta = replacementLogicalCharge - currentLogicalCharge;
      // Transfer the claimed Entry and the prepared native replacement to the maintenance-aware
      // helper. Its rollback method owns every state after this point, including charge and
      // retirement reservations; the outer finally therefore stays small and cold.
      writerHeld = false;
      replacementTransferred = true;
      return replaceWithMaintenance(
          context,
          entry,
          replacement,
          newAllocation,
          newTaggedValue,
          old,
          oldAllocation,
          deferMaintenanceWake,
          maintenanceRequired,
          chargeDelta,
          retirementLane,
          retirementReservation);
    } catch (Throwable failure) {
      operationFailure = failure;
      throwUnchecked(failure);
      return -1;
    } finally {
      Throwable cleanupFailure = null;
      if (writerHeld) {
        try {
          writerHeld = false;
          releaseWriter(entry, deferMaintenanceWake);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (!replacementTransferred) {
        try {
          rollbackReplacement(replacement, newAllocation);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (cleanupFailure != null && operationFailure != null) {
        operationFailure.addSuppressed(cleanupFailure);
      }
      if (cleanupFailure != null && operationFailure == null) {
        throwUnchecked(cleanupFailure);
      }
    }
  }

  /** Prepares the replacement value once; collision retries reuse the caller-owned block. */
  private long prepareReplacementValue(
      ThreadContext context,
      Object value,
      int valueLength,
      long deadlineNanos,
      long newAllocation,
      long preparedReplacement) {
    if (preparedReplacement != 0L) {
      return preparedReplacement;
    }
    long replacement =
        allocateReplacement(context, value, valueLength, deadlineNanos, newAllocation);
    if (replacement == 0L) {
      nativeAllocationRejected();
      throw new IllegalStateException("native allocation failed while replacing cache entry");
    }
    return replacement;
  }

  /** Cold replacement path for timer, ledger, mutation and retirement maintenance. */
  private int replaceWithMaintenance(
      ThreadContext context,
      Entry entry,
      long replacement,
      long newAllocation,
      long newTaggedValue,
      long old,
      long oldAllocation,
      boolean deferMaintenanceWake,
      boolean maintenanceRequired,
      long chargeDelta,
      RetirementJournal.Lane retirementLane,
      RetirementSegment.Reservation retirementReservation) {
    return replaceWithMaintenanceTransaction(
        context,
        entry,
        replacement,
        newAllocation,
        newTaggedValue,
        old,
        oldAllocation,
        deferMaintenanceWake,
        maintenanceRequired,
        chargeDelta,
        retirementLane,
        retirementReservation);
  }

  /** Owns the cold replacement transaction and its rollback state. */
  private int replaceWithMaintenanceTransaction(
      ThreadContext context,
      Entry entry,
      long replacement,
      long newAllocation,
      long newTaggedValue,
      long old,
      long oldAllocation,
      boolean deferMaintenanceWake,
      boolean maintenanceRequired,
      long chargeDelta,
      RetirementJournal.Lane retirementLane,
      RetirementSegment.Reservation retirementReservation) {
    boolean chargeReserved = false;
    boolean retirementPrepared = false;
    boolean published = false;
    boolean allocationUpdated = false;
    boolean mutationPrepared = false;
    boolean publicationChanged = false;
    boolean writerHeld = true;
    boolean residenceSampled = false;
    long residenceCreatedAtMillis = 0L;
    int mutationKeyHash = 0;
    long mutationValueAllocation = 0L;
    long mutationVersion = WriterLifecycleLane.UNSEEDED_MUTATION_VERSION;
    Throwable operationFailure = null;
    try {
      if (chargeDelta != 0L) {
        chargeReserved = reserveLogicalCharge(chargeDelta);
        if (!chargeReserved) {
          throw new IllegalStateException("logical charge reservation failed");
        }
      }
      if (old != 0L) {
        if (!retirementLane.reserve(retirementReservation)) {
          throw new IllegalStateException("retirement journal is closed");
        }
        retirementPrepared = true;
        retirementLane.write(retirementReservation, old, oldAllocation);
      }
      if (old != 0L && context.sampleNextReplacement()) {
        residenceSampled = true;
        residenceCreatedAtMillis = ValueBlock.createdAtMillis(old);
      }
      entry.currentValueAllocation(newAllocation);
      allocationUpdated = true;
      publishValue(entry, newTaggedValue);
      published = true;
      publicationChanged = true;
      if (maintenanceRequired) {
        mutationPrepared = worker.prepareMutation(entry, Entry.PENDING_UPDATE);
        if (mutationPrepared) {
          mutationKeyHash = context.lookupKey.hash();
          mutationValueAllocation = newAllocation;
          mutationVersion = entry.mutationVersion();
        }
      }
      if (chargeReserved) {
        logicalAdmission.completeReplacement(entry);
        maybeFailPostCharge(PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
      }
      if (retirementPrepared) {
        if (chargeReserved) {
          maybeFailPostCharge(PostChargeFailurePoint.BEFORE_RETIREMENT_COMMIT);
        }
        retirementLane.commit(retirementReservation);
        retirementPrepared = false;
      }
      if (chargeReserved) {
        maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
      }
      writerHeld = false;
      releaseWriter(entry, deferMaintenanceWake);
      if (residenceSampled) {
        worker.recordResidenceSample(residenceCreatedAtMillis, ticker.currentTimeMillis());
      }
      return 1;
    } catch (Throwable failure) {
      operationFailure = failure;
      throwUnchecked(failure);
      return -1;
    } finally {
      rollbackReplacement(
          context,
          entry,
          replacement,
          newAllocation,
          oldAllocation,
          deferMaintenanceWake,
          chargeDelta,
          chargeReserved,
          retirementLane,
          retirementReservation,
          retirementPrepared,
          published,
          allocationUpdated,
          mutationPrepared,
          mutationKeyHash,
          mutationValueAllocation,
          mutationVersion,
          publicationChanged,
          writerHeld,
          operationFailure);
    }
  }

  /** Rolls back any portion of the maintenance replacement transaction that did not publish. */
  private void rollbackReplacement(
      ThreadContext context,
      Entry entry,
      long replacement,
      long newAllocation,
      long oldAllocation,
      boolean deferMaintenanceWake,
      long chargeDelta,
      boolean chargeReserved,
      RetirementJournal.Lane retirementLane,
      RetirementSegment.Reservation retirementReservation,
      boolean retirementPrepared,
      boolean published,
      boolean allocationUpdated,
      boolean mutationPrepared,
      int mutationKeyHash,
      long mutationValueAllocation,
      long mutationVersion,
      boolean publicationChanged,
      boolean writerHeld,
      Throwable operationFailure) {
    Throwable cleanupFailure = null;
    if (retirementPrepared) {
      try {
        if (publicationChanged) {
          retirementLane.commit(retirementReservation);
        } else {
          retirementLane.cancel(retirementReservation);
        }
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (!published && allocationUpdated) {
      try {
        entry.currentValueAllocation(oldAllocation);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (writerHeld) {
      try {
        releaseWriter(entry, deferMaintenanceWake);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (!published) {
      try {
        rollbackReplacement(replacement, newAllocation);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    try {
      if (mutationPrepared || claimMutationRetryProtected(context, entry)) {
        enqueueWriterMutationHint(
            context,
            entry,
            mutationPrepared ? mutationKeyHash : context.lookupKey.hash(),
            mutationPrepared ? mutationValueAllocation : 0L,
            mutationPrepared ? mutationVersion : WriterLifecycleLane.UNSEEDED_MUTATION_VERSION,
            !deferMaintenanceWake);
      }
    } catch (Throwable failure) {
      cleanupFailure = appendFailure(cleanupFailure, failure);
    }
    if (cleanupFailure != null && operationFailure != null) {
      operationFailure.addSuppressed(cleanupFailure);
    }
    if (published) {
      if (operationFailure != null) {
        worker.recordTerminalFailure(operationFailure);
      }
    } else if (chargeReserved) {
      try {
        logicalAdmission.rollbackUnpublishedDelta(chargeDelta);
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
    }
    if (chargeReserved) {
      if (operationFailure != null) {
        worker.recordTerminalFailure(operationFailure);
      } else if (cleanupFailure != null) {
        worker.recordTerminalFailure(cleanupFailure);
      }
    }
    if (cleanupFailure != null && operationFailure == null) {
      throwUnchecked(cleanupFailure);
    }
  }

  /**
   * Hot replacement path for an already-present value with unchanged logical shape and no timer
   * maintenance. Retirement is still recorded because the native value block changes.
   */
  private int replaceSameShapeFast(
      ThreadContext context,
      Entry entry,
      long replacement,
      long newAllocation,
      long newTaggedValue,
      long old,
      long oldAllocation,
      boolean deferMaintenanceWake) {
    return replaceSameShapeFastTransaction(
        context,
        entry,
        replacement,
        newAllocation,
        newTaggedValue,
        old,
        oldAllocation,
        deferMaintenanceWake);
  }

  /** Owns the native-retirement transaction for the hot replacement path. */
  private int replaceSameShapeFastTransaction(
      ThreadContext context,
      Entry entry,
      long replacement,
      long newAllocation,
      long newTaggedValue,
      long old,
      long oldAllocation,
      boolean deferMaintenanceWake) {
    RetirementJournal.Lane retirementLane = context.retirementLane();
    RetirementSegment.Reservation retirementReservation = context.retirementReservation();
    boolean retirementPrepared = false;
    boolean published = false;
    boolean allocationUpdated = false;
    boolean writerHeld = true;
    boolean residenceSampled = false;
    long residenceCreatedAtMillis = 0L;
    long releasedStateWord = 0L;
    Throwable operationFailure = null;
    try {
      if (!retirementLane.reserve(retirementReservation)) {
        throw new IllegalStateException("retirement journal is closed");
      }
      retirementPrepared = true;
      retirementLane.write(retirementReservation, old, oldAllocation);
      if (context.sampleNextReplacement()) {
        residenceSampled = true;
        residenceCreatedAtMillis = ValueBlock.createdAtMillis(old);
      }
      entry.currentValueAllocation(newAllocation);
      allocationUpdated = true;
      publishValue(entry, newTaggedValue);
      published = true;
      retirementLane.commit(retirementReservation);
      retirementPrepared = false;
      releasedStateWord = releaseWriterReturningState(entry, deferMaintenanceWake);
      writerHeld = false;
      if (residenceSampled) {
        worker.recordResidenceSample(residenceCreatedAtMillis, ticker.currentTimeMillis());
      }
      return 1;
    } catch (Throwable failure) {
      operationFailure = failure;
      throwUnchecked(failure);
      return -1;
    } finally {
      Throwable cleanupFailure = null;
      if (retirementPrepared) {
        try {
          if (published) {
            retirementLane.commit(retirementReservation);
          } else {
            retirementLane.cancel(retirementReservation);
          }
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (!published && allocationUpdated) {
        try {
          entry.currentValueAllocation(oldAllocation);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (writerHeld) {
        try {
          releaseWriter(entry, deferMaintenanceWake);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      try {
        boolean retryRequested =
            (releasedStateWord == 0L
                    || Entry.mutationRetryRequestedInReleasedWord(releasedStateWord))
                && claimMutationRetryProtected(context, entry);
        if (retryRequested) {
          enqueueWriterMutationHint(
              context,
              entry,
              context.lookupKey.hash(),
              0L,
              WriterLifecycleLane.UNSEEDED_MUTATION_VERSION,
              !deferMaintenanceWake);
        }
      } catch (Throwable failure) {
        cleanupFailure = appendFailure(cleanupFailure, failure);
      }
      if (!published) {
        try {
          rollbackReplacement(replacement, newAllocation);
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (cleanupFailure != null && operationFailure != null) {
        operationFailure.addSuppressed(cleanupFailure);
      }
      if (cleanupFailure != null && operationFailure == null) {
        throwUnchecked(cleanupFailure);
      }
      if (operationFailure != null && published) {
        worker.recordTerminalFailure(operationFailure);
      }
    }
  }

  private long allocateReplacement(
      ThreadContext context, Object value, int valueLength, long deadlineNanos, long allocation) {
    long replacement = 0L;
    try {
      replacement = allocateNative(context, allocation);
      if (replacement == 0L) {
        return 0L;
      }
      initializeValueBlock(context, replacement, deadlineNanos, valueLength);
      writeValue(context, ValueBlock.payloadAddress(replacement), value, valueLength);
      return replacement;
    } catch (Throwable failure) {
      if (replacement != 0L) {
        freeBlock(replacement, allocation);
      }
      throw failure;
    }
  }

  private void rollbackReplacement(long replacement, long newAllocation) {
    if (replacement != 0L) {
      freeBlock(replacement, newAllocation);
    }
  }

  private Entry allocateEntry(
      ThreadContext context,
      int hash,
      byte[] keyBytes,
      int keyLength,
      Object value,
      int valueLength,
      long deadlineNanos,
      long keyAllocation,
      long valueAllocation) {
    worker.throwIfUnavailable();
    return allocateEntryAfterReserve(
        context,
        hash,
        keyBytes,
        keyLength,
        value,
        valueLength,
        deadlineNanos,
        keyAllocation,
        valueAllocation);
  }

  private Entry allocateEntryAfterReserve(
      ThreadContext context,
      int hash,
      byte[] keyBytes,
      int keyLength,
      Object value,
      int valueLength,
      long deadlineNanos,
      long keyAllocation,
      long valueAllocation) {
    long keyAddress = 0L;
    long valueAddress = 0L;
    try {
      keyAddress = allocateNative(context, keyAllocation);
      if (keyAddress == 0L) {
        nativeAllocationRejected();
        return null;
      }
      NativeMemory.copy(keyBytes, 0, keyAddress, keyLength);
      valueAddress = allocateNative(context, valueAllocation);
      if (valueAddress == 0L) {
        nativeAllocationRejected();
        freeBlock(keyAddress, keyAllocation);
        keyAddress = 0L;
        return null;
      }
      initializeValueBlock(context, valueAddress, deadlineNanos, valueLength);
      writeValue(context, ValueBlock.payloadAddress(valueAddress), value, valueLength);
      Entry entry =
          new Entry(
              keyAddress,
              keyLength,
              Entry.absentTaggedValue(
                  valueAddress, deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE));
      entry.initializeKeyHash(hash);
      entry.initializeNativeMetadata();
      entry.currentValueAllocation(valueAllocation);
      return entry;
    } catch (Throwable failure) {
      if (valueAddress != 0L) {
        freeBlock(valueAddress, valueAllocation);
      }
      if (keyAddress != 0L) {
        freeBlock(keyAddress, keyAllocation);
      }
      throw failure;
    }
  }

  @Override
  public void remove(Object key) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      removeOne(context, key, false);
    } finally {
      exitWriter(context);
    }
  }

  /** Internal removal that reports whether a live mapping was removed. */
  boolean removeIfPresent(Object key) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    try {
      return removeOne(context, key, false);
    } finally {
      exitWriter(context);
    }
  }

  private boolean removeOne(ThreadContext context, Object key, boolean deferMaintenanceWake) {
    return removeOneInternal(context, key, deferMaintenanceWake, null);
  }

  private boolean removeOneInternal(
      ThreadContext context,
      Object key,
      boolean deferMaintenanceWake,
      PreviousValue<Object> previous) {
    KeyEncoder.encode(keySerializer, key, context);
    enterWriterReader(context);
    Entry entry;
    try {
      entry = data.get(context.lookupKey);
    } finally {
      exit(context);
    }
    return entry != null && removeEntryByIdentity(context, entry, deferMaintenanceWake, previous);
  }

  private boolean removeEntryByIdentity(
      ThreadContext context, Entry entry, boolean deferMaintenanceWake) {
    return removeEntryByIdentity(context, entry, deferMaintenanceWake, null, 0L, -1L, true);
  }

  private boolean removeEntryByIdentity(
      ThreadContext context,
      Entry entry,
      boolean deferMaintenanceWake,
      PreviousValue<Object> previous) {
    return removeEntryByIdentity(context, entry, deferMaintenanceWake, previous, 0L, -1L, true);
  }

  private boolean removeEntryByIdentity(
      ThreadContext context,
      Entry entry,
      boolean deferMaintenanceWake,
      long expectedTaggedValue,
      long expectedGeneration) {
    return removeEntryByIdentity(
        context, entry, deferMaintenanceWake, null, expectedTaggedValue, expectedGeneration, true);
  }

  private boolean removeEntryByIdentity(
      ThreadContext context,
      Entry entry,
      boolean deferMaintenanceWake,
      PreviousValue<Object> previous,
      long expectedTaggedValue,
      long expectedGeneration,
      boolean requireLiveMapping) {
    enterWriterValueReader(context);
    try {
      if (!entry.isAlive()
          || data.get(entry) != entry
          || (expectedTaggedValue != 0L
              && (!Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)
                  || entry.generation() != expectedGeneration))) {
        return false;
      }
    } finally {
      exit(context);
    }

    if (awaitWriter(entry) == 0L) {
      return false;
    }
    boolean removeAttemptStarted = false;
    boolean readerEntered = false;
    try {
      enterWriterValueReader(context);
      readerEntered = true;
      // A mapping-state change completed while this thread waited for the writer claim is
      // caught by removeEntryIfSame's identity gate inside the removal transaction; the
      // per-entry claim already serializes every unlink, retire, and replace.
      if (expectedTaggedValue != 0L
          && (!Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)
              || entry.generation() != expectedGeneration)) {
        return false;
      }
      // Expired mappings are logically absent. Do not report a successful user removal for one;
      // the maintenance actor will perform the physical expiry retirement normally.
      if (requireLiveMapping && valueIfLiveWhileWriterHeld(entry) == 0L) {
        return false;
      }
      // The helper owns the writer release on every return/exception. Mark the handoff before
      // entering it so this finally block cannot accidentally release a writer claimed by the
      // next operation.
      removeAttemptStarted = true;
      return removeClaimedEntryOutsideGate(context, entry, deferMaintenanceWake, previous);
    } finally {
      if (readerEntered) {
        exit(context);
      }
      if (!removeAttemptStarted && entry.isWriterLocked()) {
        releaseWriter(entry);
      }
    }
  }

  private boolean clearEntryByIdentity(ThreadContext context, Entry entry) {
    return removeEntryByIdentity(context, entry, true, null, 0L, -1L, false);
  }

  /** Removes a claimed entry without holding a cache-wide gate across the CHM transition. */
  private boolean removeClaimedEntryOutsideGate(
      ThreadContext context,
      Entry entry,
      boolean deferMaintenanceWake,
      PreviousValue<Object> previous) {
    boolean lifecyclePrepared = false;
    boolean removedFromMap = false;
    boolean writerHeld = true;
    long lifecycleSequence = 0L;
    WriterLifecycleLane lane = context.lifecycleLane();
    try {
      if (previous != null) {
        previous.value = snapshotValueEntered(context, entry, entry.valueAddress);
      }
      long value = Entry.rawValueAddress(entry.valueAddress);
      long valueAllocation = currentValueAllocation(entry);
      lifecycleSequence = lane.reserve();
      lifecyclePrepared = true;
      lane.writeRemoval(lifecycleSequence, entry, value, valueAllocation, entry.generation(), null);
      entry.markRetired();
      if (!context.removeEntryIfSame(data, entry)) {
        entry.restoreAlive();
        writerHeld = false;
        releaseWriter(entry);
        worker.cancelWriterLifecycle(lane, lifecycleSequence, false);
        lifecyclePrepared = false;
        return false;
      }
      removedFromMap = true;
      logicalAdmission.markAbsent(entry);
      clearValue(entry);
      writerHeld = false;
      releaseWriter(entry);
      lifecyclePrepared = false;
      worker.commitWriterLifecycle(lane, lifecycleSequence, !deferMaintenanceWake);
      worker.throwIfUnavailable();
      return true;
    } catch (Throwable failure) {
      if (writerHeld) {
        writerHeld = false;
        releaseWriter(entry);
      }
      if (lifecyclePrepared) {
        if (removedFromMap) {
          lifecyclePrepared = false;
          worker.commitWriterLifecycle(lane, lifecycleSequence, false);
          worker.throwIfUnavailable();
        } else {
          worker.cancelWriterLifecycle(lane, lifecycleSequence, false);
        }
      }
      throw failure;
    }
  }

  @Override
  public V get(Object key) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = contexts.get();
    context.enterKey();
    try {
      KeyEncoder.encode(keySerializer, key, context);
      return getEncoded(context);
    } finally {
      context.exitKey();
    }
  }

  /** Reads the lookup key already installed in the supplied thread context. */
  private V getEncoded(ThreadContext context) {
    return getEncoded(context, true);
  }

  /** Reads an internal lookup without changing the public request counters. */
  private long readDeadlineNanosFor(long observedTaggedValue) {
    return Entry.hasTtl(observedTaggedValue) ? deadlineClock.nowNanos() : 0L;
  }

  private V getEncoded(ThreadContext context, boolean recordStats) {
    enter(context);
    boolean serializedValueEntered = false;
    try {
      Entry entry = data.get(context.lookupKey);
      long observedTaggedValue = entry == null ? 0L : entry.valueAddress;
      long value =
          valueIfLive(
              entry,
              observedTaggedValue,
              preReadValueDeadline(observedTaggedValue),
              readDeadlineNanosFor(observedTaggedValue));
      if (value == 0L) {
        if (recordStats) {
          miss(context);
        }
        return null;
      }
      if (recordStats) {
        hit(context, entry, observedTaggedValue);
      }
      int length = ValueBlock.length(value);
      ByteBuffer serializedValue =
          context.readOnlyValueBuffer(ValueBlock.payloadAddress(value), length);
      serializedValueEntered = true;
      return valueSerializer.deserialize(serializedValue);
    } finally {
      if (serializedValueEntered) {
        context.releaseReadOnlyValueBuffer();
      }
      exit(context);
    }
  }

  /**
   * Materializes a value while the caller holds a reader admission. The returned object is a heap
   * snapshot; it never retains a native address or a callback-scoped buffer.
   */
  private V snapshotValueEntered(ThreadContext context, Entry entry, long taggedValue) {
    long value = valueIfLive(entry, taggedValue, deadlineClock.nowNanos());
    if (value == 0L) {
      return null;
    }
    V result =
        deserialize(
            context, valueSerializer, ValueBlock.payloadAddress(value), ValueBlock.length(value));
    return result;
  }

  /** Materializes the old value while the Entry writer claim prevents retirement or replacement. */
  private V snapshotValueWhileWriterHeld(ThreadContext context, Entry entry, long taggedValue) {
    if (entry.valueAddress != taggedValue
        || !isAliveTaggedValue(taggedValue)
        || taggedValue == 0L) {
      return null;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (Entry.hasTtl(taggedValue) && ValueBlock.expired(value, deadlineClock.nowNanos())) {
      return null;
    }
    V result =
        deserialize(
            context, valueSerializer, ValueBlock.payloadAddress(value), ValueBlock.length(value));
    return result;
  }

  Map.Entry<K, V> snapshotEntryForView(Entry entry) {
    ThreadContext context = contexts.get();
    enter(context);
    try {
      if (!entry.isAlive() || data.get(entry) != entry || valueIfLive(entry) == 0L) {
        return null;
      }
      K key = deserialize(context, keySerializer, entry.nativeKeyBytesAddress(), entry.keyLength());
      V value = snapshotValueEntered(context, entry, entry.valueAddress);
      return value == null ? null : new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
    } finally {
      exit(context);
    }
  }

  Map.Entry<K, V> nextViewEntry(Iterator<Map.Entry<Entry, Entry>> iterator) {
    ThreadContext context = contexts.get();
    enter(context);
    try {
      while (iterator.hasNext()) {
        Map.Entry<Entry, Entry> node = iterator.next();
        Entry entry = node.getKey();
        if (!entry.isAlive() || data.get(entry) != entry || valueIfLive(entry) == 0L) {
          continue;
        }
        K key =
            deserialize(context, keySerializer, entry.nativeKeyBytesAddress(), entry.keyLength());
        V value = snapshotValueEntered(context, entry, entry.valueAddress);
        if (value != null) {
          return new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
        }
      }
      return null;
    } finally {
      exit(context);
    }
  }

  private Map.Entry<K, V> snapshotEntryForBulk(Entry entry) {
    return snapshotEntryForView(entry);
  }

  boolean containsValueForView(Object value) {
    Objects.requireNonNull(value, "value");
    final boolean[] found = {false};
    data.forEach(
        (ignored, entry) -> {
          if (!found[0]) {
            V snapshot = snapshotValueForBulk(entry);
            if (snapshot != null && Objects.equals(value, snapshot)) {
              found[0] = true;
            }
          }
        });
    return found[0];
  }

  private K snapshotKeyForBulk(Entry entry) {
    ThreadContext context = contexts.get();
    enter(context);
    try {
      if (!entry.isAlive() || data.get(entry) != entry || valueIfLive(entry) == 0L) {
        return null;
      }
      return deserialize(context, keySerializer, entry.nativeKeyBytesAddress(), entry.keyLength());
    } finally {
      exit(context);
    }
  }

  private V snapshotValueForBulk(Entry entry) {
    ThreadContext context = contexts.get();
    enter(context);
    try {
      if (!entry.isAlive() || data.get(entry) != entry || valueIfLive(entry) == 0L) {
        return null;
      }
      return snapshotValueEntered(context, entry, entry.valueAddress);
    } finally {
      exit(context);
    }
  }

  @Override
  public boolean containsKey(Object key) {
    Objects.requireNonNull(key, "key");
    ThreadContext context = contexts.get();
    context.enterKey();
    try {
      KeyEncoder.encode(keySerializer, key, context);
      return containsKeyEncoded(context);
    } finally {
      context.exitKey();
    }
  }

  private boolean containsKeyEncoded(ThreadContext context) {
    enter(context);
    Entry expiredEntry = null;
    long expiredTaggedValue = 0L;
    boolean present;
    try {
      Entry entry = data.get(context.lookupKey);
      long observedTaggedValue = entry == null ? 0L : entry.valueAddress;
      long observedNowNanos = Entry.hasTtl(observedTaggedValue) ? deadlineClock.nowNanos() : 0L;
      long value = valueIfLive(entry, observedTaggedValue, observedNowNanos);
      if (value == 0L) {
        if (entry != null) {
          long rawValue = Entry.rawValueAddress(observedTaggedValue);
          if (Entry.hasTtl(observedTaggedValue)
              && rawValue != 0L
              && ValueBlock.expired(rawValue, observedNowNanos)) {
            expiredEntry = entry;
            expiredTaggedValue = observedTaggedValue;
          }
        }
        miss(context);
        present = false;
      } else {
        hit(context, entry, observedTaggedValue);
        present = true;
      }
    } finally {
      exit(context);
    }
    if (!present) {
      completeExpiredReadAfterReader(expiredEntry, expiredTaggedValue);
    }
    return present;
  }

  @Override
  public boolean containsValue(Object value) {
    return mapViews.containsValue(value);
  }

  @Override
  public Set<K> keySet() {
    return mapViews.keySet();
  }

  @Override
  public Collection<V> values() {
    return mapViews.values();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return mapViews.entrySet();
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    Objects.requireNonNull(key, "key");
    V value = get(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public void forEach(java.util.function.BiConsumer<? super K, ? super V> action) {
    Objects.requireNonNull(action, "action");
    data.forEach(
        (ignored, entry) -> {
          Map.Entry<K, V> snapshot = snapshotEntryForView(entry);
          if (snapshot != null) {
            invokeUserCallback(() -> action.accept(snapshot.getKey(), snapshot.getValue()));
          }
        });
  }

  @Override
  public void replaceAll(
      java.util.function.BiFunction<? super K, ? super V, ? extends V> function) {
    Objects.requireNonNull(function, "function");
    for (Map.Entry<K, V> snapshot : entrySet()) {
      while (true) {
        Map.Entry<K, V> currentSnapshot = snapshot;
        V replacement =
            Objects.requireNonNull(
                invokeUserCallback(
                    () -> function.apply(currentSnapshot.getKey(), currentSnapshot.getValue())),
                "function result");
        if (replace(currentSnapshot.getKey(), currentSnapshot.getValue(), replacement)) {
          break;
        }
        V current = get(currentSnapshot.getKey());
        if (current == null) {
          break;
        }
        snapshot =
            new java.util.AbstractMap.SimpleImmutableEntry<>(currentSnapshot.getKey(), current);
      }
    }
  }

  @Override
  public void clear() {
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    int removed = 0;
    try {
      Iterator<Map.Entry<Entry, Entry>> iterator = data.entrySet().iterator();
      while (true) {
        Entry entry;
        enterWriterReader(context);
        try {
          try {
            entry = iterator.next().getKey();
          } catch (java.util.NoSuchElementException end) {
            break;
          }
        } finally {
          exit(context);
        }
        if (clearEntryByIdentity(context, entry)) {
          removed++;
        }
      }
    } finally {
      if (removed != 0) {
        worker.requestMutationMaintenance();
      }
      exitWriter(context);
    }
  }

  @Override
  public boolean getDirect(K key, DirectValueConsumer consumer) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(consumer, "consumer");
    ThreadContext context = contexts.get();
    context.enterKey();
    try {
      KeyEncoder.encode(keySerializer, key, context);
      return getDirectInContext(context, consumer);
    } finally {
      context.exitKey();
    }
  }

  private boolean getDirectInContext(ThreadContext context, DirectValueConsumer consumer) {
    enter(context);
    try {
      return getDirectEntered(context, data.get(context.lookupKey), consumer);
    } finally {
      exit(context);
    }
  }

  private boolean getDirectEntered(
      ThreadContext context, Entry entry, DirectValueConsumer consumer) {
    long observedTaggedValue = entry == null ? 0L : entry.valueAddress;
    long value =
        valueIfLive(
            entry,
            observedTaggedValue,
            preReadValueDeadline(observedTaggedValue),
            Entry.hasTtl(observedTaggedValue) ? deadlineClock.nowNanos() : 0L);
    if (value == 0L) {
      miss(context);
      return false;
    }
    hit(context, entry, observedTaggedValue);
    DirectValueView view =
        context.pushDirectView(ValueBlock.payloadAddress(value), ValueBlock.length(value));
    try {
      enterUserCallback(context);
      try {
        consumer.accept(view);
      } finally {
        exitUserCallback(context);
      }
      return true;
    } finally {
      context.popDirectView();
    }
  }

  @Override
  public int getDirectAll(Collection<? extends K> keys, DirectEntryConsumer<K> consumer) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(consumer, "consumer");
    if (keys.isEmpty()) {
      return 0;
    }

    ThreadContext context = contexts.get();
    int hits = 0;
    int expected = keys.size();
    Set<Entry> uniqueEntries = context.acquireBulkEntries(expected);
    try {
      context.enterKey();
      try {
        Iterator<? extends K> iterator = keys.iterator();
        while (iterator.hasNext()) {
          enter(context);
          context.beginBulkRead();
          try {
            int processed = 0;
            long bulkNowNanos = 0L;
            boolean bulkClockRead = false;
            while (processed < BULK_READ_CHUNK_SIZE && iterator.hasNext()) {
              processed++;
              K key = Objects.requireNonNull(iterator.next(), "key");
              KeyEncoder.encode(keySerializer, key, context);
              Entry entry = data.get(context.lookupKey);
              long observedTaggedValue = entry == null ? 0L : entry.valueAddress;
              long value;
              if (Entry.hasTtl(observedTaggedValue)) {
                if (!bulkClockRead) {
                  bulkNowNanos = deadlineClock.nowNanos();
                  bulkClockRead = true;
                }
              }
              value = valueIfLive(entry, observedTaggedValue, bulkNowNanos);
              if (value == 0L) {
                context.bulkMiss();
              } else {
                if (!uniqueEntries.add(entry)) {
                  continue;
                }
                context.bulkHit(entry);
                DirectValueView view =
                    context.pushDirectView(
                        ValueBlock.payloadAddress(value), ValueBlock.length(value));
                try {
                  enterUserCallback(context);
                  try {
                    consumer.accept(key, view);
                  } finally {
                    exitUserCallback(context);
                  }
                  hits++;
                } finally {
                  context.popDirectView();
                }
              }
            }
          } finally {
            context.finishBulkRead();
            exit(context);
          }
        }
      } finally {
        context.exitKey();
      }
    } finally {
      context.releaseBulkEntries(uniqueEntries);
    }
    return hits;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) {
    Objects.requireNonNull(entries, "entries");
    if (entries.isEmpty()) {
      return;
    }
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    Throwable operationFailure = null;
    try {
      Iterator<? extends Map.Entry<? extends K, ? extends V>> iterator =
          entries.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<? extends K, ? extends V> entry = iterator.next();
        K key = Objects.requireNonNull(entry.getKey(), "key");
        V value = Objects.requireNonNull(entry.getValue(), "value");
        putOne(context, key, value, DEFAULT_TTL, true);
      }
    } catch (Throwable failure) {
      operationFailure = failure;
      throw failure;
    } finally {
      try {
        worker.requestMutationMaintenance();
      } catch (Throwable cleanupFailure) {
        if (operationFailure == null) {
          throw cleanupFailure;
        }
        operationFailure.addSuppressed(cleanupFailure);
      } finally {
        exitWriter(context);
      }
    }
  }

  @Override
  public Map<K, V> getAll(Collection<? extends K> keys) {
    Objects.requireNonNull(keys, "keys");
    if (keys.isEmpty()) {
      return new HashMap<>();
    }

    int expected = keys.size();
    Map<K, V> result = new HashMap<>(resultCapacity(expected));
    ThreadContext context = contexts.get();
    Set<Entry> uniqueEntries = context.acquireBulkEntries(expected);
    try {
      context.enterKey();
      try {
        Iterator<? extends K> iterator = keys.iterator();
        while (iterator.hasNext()) {
          enter(context);
          context.beginBulkRead();
          try {
            int processed = 0;
            long bulkNowNanos = 0L;
            boolean bulkClockRead = false;
            while (processed < BULK_READ_CHUNK_SIZE && iterator.hasNext()) {
              processed++;
              K key = Objects.requireNonNull(iterator.next(), "key");
              KeyEncoder.encode(keySerializer, key, context);
              Entry entry = data.get(context.lookupKey);
              long observedTaggedValue = entry == null ? 0L : entry.valueAddress;
              long value;
              if (Entry.hasTtl(observedTaggedValue)) {
                if (!bulkClockRead) {
                  bulkNowNanos = deadlineClock.nowNanos();
                  bulkClockRead = true;
                }
              }
              value = valueIfLive(entry, observedTaggedValue, bulkNowNanos);
              if (value == 0L) {
                context.bulkMiss();
              } else {
                if (!uniqueEntries.add(entry)) {
                  continue;
                }
                context.bulkHit(entry);
                int length = ValueBlock.length(value);
                ByteBuffer serializedValue =
                    context.readOnlyValueBuffer(ValueBlock.payloadAddress(value), length);
                try {
                  result.put(key, valueSerializer.deserialize(serializedValue));
                } finally {
                  context.releaseReadOnlyValueBuffer();
                }
              }
            }
          } finally {
            context.finishBulkRead();
            exit(context);
          }
        }
      } finally {
        context.exitKey();
      }
    } finally {
      context.releaseBulkEntries(uniqueEntries);
    }
    return result;
  }

  private static int resultCapacity(int requestedEntries) {
    int boundedEntries = Math.min(requestedEntries, BULK_READ_CHUNK_SIZE);
    if (boundedEntries < 3) {
      return boundedEntries + 1;
    }
    long requested = ((long) boundedEntries * 4L + 2L) / 3L + 1L;
    return (int) Math.min(1L << 30, requested);
  }

  @Override
  public int removeAll(Collection<? extends K> keys) {
    Objects.requireNonNull(keys, "keys");
    if (keys.isEmpty()) {
      return 0;
    }
    ThreadContext context = enterWriter();
    if (context == null) {
      throw new IllegalStateException("reentrant cache write is not supported");
    }
    int removed = 0;
    try {
      for (K key : keys) {
        Objects.requireNonNull(key, "key");
        if (removeOne(context, key, true)) {
          removed++;
        }
      }
    } finally {
      if (removed != 0) {
        worker.requestMutationMaintenance();
      }
      exitWriter(context);
    }
    return removed;
  }

  private boolean putIfAbsentValue(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      long requestedExpiry) {
    return putIfAbsentValueResolved(
        context, lookup, keyBytes, keyLength, value, resolveDeadline(context, requestedExpiry));
  }

  private V putIfAbsentValueReturningPrevious(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      long requestedExpiry) {
    PreviousValue<Object> previous = new PreviousValue<>();
    boolean inserted =
        putIfAbsentValueResolved(
            context,
            lookup,
            keyBytes,
            keyLength,
            value,
            resolveDeadline(context, requestedExpiry),
            previous);
    if (inserted) {
      return null;
    }
    @SuppressWarnings("unchecked")
    V previousValue = (V) previous.value;
    return previousValue;
  }

  private boolean putIfAbsentValueResolved(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      long deadlineNanos) {
    return putIfAbsentValueResolved(
        context, lookup, keyBytes, keyLength, value, deadlineNanos, null);
  }

  private boolean putIfAbsentValueResolved(
      ThreadContext context,
      LookupKey lookup,
      byte[] keyBytes,
      int keyLength,
      Object value,
      long deadlineNanos,
      PreviousValue<Object> previous) {
    enterWriterValueReader(context);
    try {
      Entry current = data.get(lookup);
      if (valueIfLive(current) != 0L) {
        if (previous != null) {
          V observed = snapshotValueEntered(context, current, current.valueAddress);
          if (observed == null && valueIfLive(current) == 0L) {
            // The live check and value materialization crossed the TTL boundary. Fall through to
            // the insertion path instead of reporting a false existing mapping.
          } else {
            previous.value = observed;
            return false;
          }
        } else {
          return false;
        }
      }
    } finally {
      exit(context);
    }
    int valueLength = serializedSize(valueSerializer, value);
    if (deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE
        && context.writeCreatedAtMillis() == Long.MIN_VALUE) {
      context.writeCreatedAtMillis(deadlineClock.wallMillisAt(deadlineClock.nowNanos()));
    }
    long keyAllocation = keyAllocationLength(keyLength);
    long valueAllocation = ValueBlock.allocationLength(valueLength);
    int keyHash = lookup.hash();
    long charge = logicalCharge(logicalKeyAllocationLength(keyLength), valueAllocation);
    Entry candidate = null;
    boolean chargeReserved = false;
    boolean candidateOwned = false;
    boolean candidateWriterHeld = false;
    boolean handoffCompleted = false;
    try {
      while (true) {
        candidate =
            allocateEntry(
                context,
                lookup.hash(),
                keyBytes,
                keyLength,
                value,
                valueLength,
                deadlineNanos,
                keyAllocation,
                valueAllocation);
        if (candidate == null) {
          throw new IllegalStateException("native allocation failed while preparing cache entry");
        }
        candidateOwned = true;

        // Link an absent placeholder before charging. A CHM collision is therefore resolved
        // without reserving capacity or claiming a victim. Only the winner reserves its own
        // logical charge.
        Entry winner = insertCandidateUnderReader(context, candidate);
        if (winner == null) {
          // The insertion helper transfers the candidate writer claim only for the winning new-key
          // path. Keep ownership explicit so a concurrent successor cannot be released by cleanup.
          candidateWriterHeld = true;
          chargeReserved = reserveLogicalCharge(charge);
          if (!chargeReserved) {
            candidateWriterHeld = false;
            removeUnadmittedCandidate(context, candidate);
            candidateOwned = false;
            throw new IllegalStateException("logical charge reservation failed");
          }
          logicalAdmission.markPresentAfterCharge(candidate);
          maybeFailPostCharge(PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
          maybeFailPostCharge(PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
          boolean mutationPrepared = worker.prepareMutation(candidate, Entry.PENDING_ADD);
          long mutationVersion =
              mutationPrepared
                  ? candidate.mutationVersion()
                  : WriterLifecycleLane.UNSEEDED_MUTATION_VERSION;
          candidateWriterHeld = false;
          releaseWriter(candidate, false, false);
          publishInsertedCandidateSafely(
              context,
              candidate,
              keyHash,
              valueAllocation,
              mutationVersion,
              mutationPrepared,
              false);
          candidateOwned = false;
          handoffCompleted = true;
          return true;
        }

        // The candidate lost the CHM race and never entered the logical ledger.
        candidate.markDead();
        freeEntry(candidate, valueAllocation);
        candidateOwned = false;
        candidate = null;

        if (!winner.isAlive()) {
          boolean staleRemovalPending;
          enterWriterReader(context);
          try {
            staleRemovalPending =
                data.get(lookup) == winner && !removeStaleLookupEntry(context, winner);
          } finally {
            exit(context);
          }
          if (staleRemovalPending) {
            awaitWriterRelease(winner);
          }
          continue;
        }

        enterWriterValueReader(context);
        long taggedValue;
        long generation;
        boolean writerBusy;
        boolean expired;
        boolean empty;
        try {
          if (!winner.isAlive()) {
            continue;
          }
          taggedValue = winner.valueAddress;
          generation = winner.generation();
          long nowNanos = deadlineClock.nowNanos();
          long liveValue = valueIfLive(winner, taggedValue, nowNanos);
          if (liveValue != 0L) {
            if (previous != null) {
              previous.value = snapshotValueEntered(context, winner, taggedValue);
            }
            return false;
          }
          long rawValue = Entry.rawValueAddress(taggedValue);
          writerBusy = winner.isWriterLocked();
          empty = rawValue == 0L;
          expired =
              !empty && Entry.hasTtl(taggedValue) && ValueBlock.expired(rawValue, nowNanos);
        } finally {
          exit(context);
        }

        if (writerBusy) {
          awaitWriterRelease(winner);
        } else if (expired) {
          int expiredResult = removeExpiredEntry(context, winner, generation, taggedValue);
          if (expiredResult == EXPIRED_WRITER_BUSY) {
            awaitWriterRelease(winner);
          }
        } else if (empty) {
          if (!removeEntryByIdentity(
              context, winner, false, null, taggedValue, generation, false)) {
            awaitWriterRelease(winner);
          }
        }
      }
    } finally {
      Throwable cleanupFailure = null;
      if (chargeReserved
          && candidateWriterHeld
          && candidate != null
          && candidate.isLogicallyAbsent()
          && isMappedEntry(candidate)) {
        try {
          // removeUnadmittedCandidate owns the writer release on this path. Transfer that
          // ownership before calling it so the candidate cleanup below cannot release it twice.
          candidateWriterHeld = false;
          removeUnadmittedCandidate(context, candidate);
          candidateOwned = false;
          logicalAdmission.rollbackUnpublishedDelta(charge);
          chargeReserved = false;
        } catch (Throwable failure) {
          cleanupFailure = appendFailure(cleanupFailure, failure);
        }
      }
      if (candidateOwned && candidate != null) {
        if (candidateWriterHeld) {
          candidateWriterHeld = false;
          try {
            releaseWriter(candidate);
          } catch (Throwable failure) {
            cleanupFailure = appendFailure(cleanupFailure, failure);
          }
        }
        if (!isMappedEntry(candidate)) {
          try {
            candidate.markDead();
            freeEntry(candidate, valueAllocation);
          } catch (Throwable failure) {
            cleanupFailure = appendFailure(cleanupFailure, failure);
          }
        } else {
          worker.recordTerminalFailure(
              new IllegalStateException("candidate remained mapped after putIfAbsent failure"));
        }
      }
      if (chargeReserved && (!handoffCompleted || cleanupFailure != null)) {
        worker.recordTerminalFailure(
            cleanupFailure != null
                ? cleanupFailure
                : new IllegalStateException("new-key charge failed after publication"));
      }
      if (chargeReserved) {
        chargeReserved = false;
      }
      if (cleanupFailure != null) {
        throwUnchecked(cleanupFailure);
      }
    }
  }

  private int removeExpiredEntry(
      ThreadContext context, Entry entry, long expectedGeneration, long expectedValueAddress) {
    boolean lifecyclePrepared = false;
    long lifecycleSequence = 0L;
    boolean removedFromMap = false;
    boolean readerEntered = false;
    boolean writerHeld = false;
    WriterLifecycleLane lane = context.lifecycleLane();
    try {
      if (!entry.isAlive()) {
        return EXPIRED_NOOP;
      }
      if (!claimWriter(entry)) {
        if (!entry.isAlive()) {
          return EXPIRED_NOOP;
        }
        return writerLockedProtected(context, entry) ? EXPIRED_WRITER_BUSY : EXPIRED_NOOP;
      }
      writerHeld = true;

      enterWriterValueReader(context);
      readerEntered = true;
      long taggedValue = entry.valueAddress;
      long value = Entry.rawValueAddress(taggedValue);
      if (entry.generation() != expectedGeneration
          || taggedValue != expectedValueAddress
          || value == 0L
          || !Entry.hasTtl(taggedValue)
          || !ValueBlock.expired(value, deadlineClock.nowNanos())) {
        writerHeld = false;
        releaseWriter(entry);
        return EXPIRED_NOOP;
      }
      long valueAllocation = currentValueAllocation(entry);
      lifecycleSequence = lane.reserve();
      lifecyclePrepared = true;
      lane.writeRemoval(
          lifecycleSequence,
          entry,
          value,
          valueAllocation,
          entry.generation(),
          RemovalCause.EXPIRED);
      entry.markRetired();
      if (!context.removeEntryIfSame(data, entry)) {
        entry.restoreAlive();
        writerHeld = false;
        releaseWriter(entry);
        worker.cancelWriterLifecycle(lane, lifecycleSequence, false);
        lifecyclePrepared = false;
        return EXPIRED_NOOP;
      }
      removedFromMap = true;
      logicalAdmission.markAbsent(entry);
      clearValue(entry);
      writerHeld = false;
      releaseWriter(entry);
      exit(context);
      readerEntered = false;
      lifecyclePrepared = false;
      worker.commitWriterLifecycle(lane, lifecycleSequence, true);
      worker.throwIfUnavailable();
      return EXPIRED_REMOVED;
    } catch (Throwable failure) {
      if (writerHeld) {
        writerHeld = false;
        releaseWriter(entry);
      }
      if (lifecyclePrepared) {
        if (removedFromMap) {
          lifecyclePrepared = false;
          worker.commitWriterLifecycle(lane, lifecycleSequence, false);
          worker.throwIfUnavailable();
        } else {
          worker.cancelWriterLifecycle(lane, lifecycleSequence, false);
        }
      }
      throw failure;
    } finally {
      if (readerEntered) {
        exit(context);
      }
    }
  }

  @Override
  public CompletableFuture<Void> flushAsync() {
    if (worker.thread() == Thread.currentThread()) {
      throw new IllegalStateException("flushAsync cannot be called from the maintenance actor");
    }
    synchronized (lifecycleLock) {
      Runnable hook = flushLifecycleHookForTest;
      if (hook != null) {
        hook.run();
      }
      ThreadContext context = contexts.get();
      if (context.isRegistered()) {
        context.flushRead();
      }
      return independentWaiter(worker.flush());
    }
  }

  void setFlushLifecycleHookForTest(Runnable hook) {
    flushLifecycleHookForTest = hook;
  }

  void setPostChargeFailurePointForTest(PostChargeFailurePoint point) {
    postChargeFailurePointForTest = point;
  }

  private void maybeFailPostCharge(PostChargeFailurePoint point) {
    if (postChargeFailurePointForTest == point) {
      throw new IllegalStateException("injected post-charge failure: " + point);
    }
  }

  @Override
  public int size() {
    long count = logicalAdmission.logicalMappingCount();
    return (int) Math.min(Integer.MAX_VALUE, Math.max(0L, count));
  }

  @Override
  public long mappingCount() {
    return data.mappingCount();
  }

  @Override
  public boolean isEmpty() {
    return logicalAdmission.logicalMappingCount() == 0L;
  }

  @Override
  public boolean equals(Object object) {
    if (object == this) {
      return true;
    }
    // OHCache intentionally is not a java.util.Map subtype. Restrict equality to the same
    // interface family so HashMap.equals(cache) and cache.equals(HashMap) are both false.
    if (!(object instanceof OHCache)) {
      return false;
    }
    OHCache<?, ?> other = (OHCache<?, ?>) object;
    int matched = 0;
    try {
      for (Map.Entry<K, V> entry : entrySet()) {
        Object otherValue = other.get(entry.getKey());
        if (!Objects.equals(entry.getValue(), otherValue)
            || (otherValue == null && !other.containsKey(entry.getKey()))) {
          return false;
        }
        matched++;
      }
      // The entry view performs the live/TTL check while it is traversed. Compare the number of
      // entries after that traversal so an expired-but-not-yet-unlinked node cannot make equals
      // return true merely because the initial physical count matched the other map.
      return matched == other.size();
    } catch (ClassCastException | NullPointerException ignored) {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (Map.Entry<K, V> entry : entrySet()) {
      hash += entry.hashCode();
    }
    return hash;
  }

  @Override
  public String toString() {
    Iterator<Map.Entry<K, V>> iterator = entrySet().iterator();
    if (!iterator.hasNext()) {
      return "{}";
    }
    StringBuilder builder = new StringBuilder("{");
    while (true) {
      Map.Entry<K, V> entry = iterator.next();
      builder.append(entry.getKey()).append('=').append(entry.getValue());
      if (!iterator.hasNext()) {
        return builder.append('}').toString();
      }
      builder.append(", ");
    }
  }

  @Override
  public long capacity() {
    return capacity;
  }

  @Override
  public long totalAllocatedBytes() {
    return memory.allocated();
  }

  ConcurrentHashMap<Entry, Entry> dataForTest() {
    return data;
  }

  @Override
  public OHCacheStats stats() {
    MaintenanceEventLoop.Snapshot snapshot = worker.snapshot();
    return new OHCacheStats(
        snapshot.hits,
        snapshot.misses,
        snapshot.evictionCount,
        snapshot.evictionWeight,
        snapshot.expirationCount,
        snapshot.residenceSampleCount,
        snapshot.residenceSampleRate,
        snapshot.sampledAverageResidenceTimeMillis,
        size(),
        snapshot.liveWeight,
        memory.allocated(),
        snapshot.unhealthy,
        snapshot.timeoutLagMillis,
        snapshot.ttlBacklog,
        snapshot.nativeAllocationFailureCount,
        memory.smallAllocationFallbackCount(),
        memory.directEntryAllocationCount(),
        snapshot.maintenancePassWorkNanos,
        snapshot.maintenanceActiveNanosTotal,
        snapshot.maintenanceParkNanosTotal,
        snapshot.maintenanceImmediateContinuationCount,
        snapshot.retirementSealScannedLanes,
        snapshot.retirementSealSealedLanes,
        snapshot.retirementSealRecords,
        snapshot.retirementSealRecordsTotal,
        snapshot.retirementReclaimRecordsTotal,
        snapshot.retirementSealScannedLanesTotal,
        snapshot.retirementReclaimBlockedCount,
        snapshot.retirementReclaimBlockedNanos,
        snapshot.maintenancePassCount,
        snapshot.maintenanceWakeCount,
        snapshot.maintenanceCollectedRecordsTotal,
        snapshot.activeReaderCount,
        snapshot.accessRingDroppedCount,
        snapshot.retirementQueueDepth,
        snapshot.retirementPublishedRecordsTotal,
        snapshot.retirementCompletedRecordsTotal,
        snapshot.retirementLagRecords,
        snapshot.retirementUnsafeRecords,
        snapshot.retirementUnsafeBytes,
        snapshot.retirementSafeRecords,
        snapshot.retirementSafeBytes,
        snapshot.retirementClaimedRecords,
        snapshot.retirementClaimedBytes,
        snapshot.retirementActorReclaimedRecords,
        snapshot.retirementAllocatedSegments,
        snapshot.retirementReusedSegments,
        snapshot.retirementTrimmedSegments,
        snapshot.ghostNativeBytes,
        snapshot.ghostAllocationTrimCount,
        snapshot.ghostAllocationDropCount,
        snapshot.ghostRehashPending,
        snapshot.lifecycleJournalPublishedRecords,
        snapshot.lifecycleJournalCompletedRecords,
        snapshot.lifecycleJournalLagRecords,
        snapshot.lifecycleJournalAllocatedSegments,
        snapshot.lifecycleJournalHeadOfLineStopCount,
        snapshot.allocatorPageAllocatedCount,
        snapshot.allocatorPageReusedCount,
        snapshot.allocatorPageReadyCount,
        snapshot.allocatorPageTrimmedCount,
        snapshot.writerResourceActiveCount,
        snapshot.writerResourceRetiringCount,
        snapshot.writerResourcePooledCount,
        snapshot.retirementGeneratedBytesTotal,
        snapshot.retirementCompletedBytesTotal,
        snapshot.retirementGeneratedBytesPerSecond,
        snapshot.retirementCompletedBytesPerSecond,
        snapshot.nativeDebtBudgetBytes,
        snapshot.nativeDebtHeadroomBytes,
        snapshot.retirementSafeSegmentCount,
        snapshot.retirementReclaimBatchCount,
        snapshot.retirementOldestSafeWaitNanos,
        snapshot.allocatorReadyPagesByClass,
        snapshot.allocatorPagesInUseByClass,
        snapshot.allocatorPageOccupancyByClass,
        snapshot.allocatorRetainedPagesCurrent,
        snapshot.allocatorPooledPageCount,
        snapshot.allocatorTrimmedBytesTotal);
  }

  /** Gives each flush caller its own cancellation state while sharing actor completion. */
  private static <T> CompletableFuture<T> independentWaiter(CompletableFuture<T> shared) {
    CompletableFuture<T> waiter = new CompletableFuture<>();
    shared.whenComplete(
        (value, failure) -> {
          if (failure != null) {
            waiter.completeExceptionally(failure);
          } else {
            waiter.complete(value);
          }
        });
    return waiter;
  }

  private void enter(ThreadContext context) {
    readerGuard.enter(context);
  }

  private void enterWriterReader(ThreadContext context) {
    readerGuard.enterLookupAfterAdmission(context);
  }

  private void enterWriterValueReader(ThreadContext context) {
    readerGuard.enterAfterAdmission(context);
  }

  private void exit(ThreadContext context) {
    readerGuard.exit(context);
  }

  private void enterUserCallback(ThreadContext context) {
    context.enterUserCallback();
  }

  private void exitUserCallback(ThreadContext context) {
    context.exitUserCallback();
  }

  private <T> T invokeUserCallback(java.util.function.Supplier<? extends T> callback) {
    ThreadContext context = contexts.get();
    enterUserCallback(context);
    try {
      return callback.get();
    } finally {
      exitUserCallback(context);
    }
  }

  private void invokeUserCallback(Runnable callback) {
    ThreadContext context = contexts.get();
    enterUserCallback(context);
    try {
      callback.run();
    } finally {
      exitUserCallback(context);
    }
  }

  private void markLogicallyPresent(Entry entry) {
    logicalAdmission.markPresent(entry);
  }

  private long logicalCharge(long keyAllocation, long valueAllocation) {
    return countBounded ? 1L : CacheMath.logicalEntryBytes(keyAllocation, valueAllocation);
  }

  private void markLogicallyAbsent(Entry entry) {
    logicalAdmission.markAbsent(entry);
  }

  /**
   * Applies reader/Entry-writer expiry accounting without waiting for the cache mutation gate. The
   * caller may still be inside a reader scope; a gate owner can be waiting for that scope to drain,
   * so blocking here would invert the gate/read lock order. The maintenance actor retries the
   * logical transition when this short race is lost.
   */
  private boolean tryMarkLogicallyAbsent(Entry entry) {
    if (entry == null || !entry.isAlive()) {
      return false;
    }
    logicalAdmission.markAbsent(entry);
    return true;
  }

  /** Completes an expiry debit after the reader scope has been released. */
  private void completeExpiredReadAfterReader(Entry entry, long expectedTaggedValue) {
    if (entry == null || expectedTaggedValue == 0L) {
      return;
    }
    LogicalAdmission admission = logicalAdmission;
    if (admission == null) {
      return;
    }
    boolean publishMutation = false;
    ThreadContext context = contexts.get();
    enter(context);
    try {
      boolean writerHeld = false;
      try {
        if (!entry.isAlive()
            || !Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)) {
          return;
        }
        if (!claimWriter(entry)) {
          return;
        }
        writerHeld = true;
        if (!Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)) {
          return;
        }
        long value = Entry.rawValueAddress(expectedTaggedValue);
        if (Entry.hasTtl(expectedTaggedValue)
            && value != 0L
            && ValueBlock.expired(value, deadlineClock.nowNanos())) {
          admission.markAbsent(entry);
          publishMutation = true;
        }
      } finally {
        if (writerHeld) {
          writerHeld = false;
          releaseWriter(entry);
        }
      }

    } finally {
      exit(context);
    }
    if (publishMutation) {
      publishReaderMutation(entry);
    }
  }

  /** Reserves a logical charge and leaves all physical capacity work to the actor. */
  private boolean reserveLogicalCharge(long delta) {
    try {
      if (!logicalAdmission.tryChargeDelta(delta)) {
        throw new IllegalStateException("logical charge overflow or underflow");
      }
      return delta != 0L;
    } catch (Throwable failure) {
      worker.recordTerminalFailure(failure);
      throwUnchecked(failure);
      return false;
    }
  }

  /**
   * Acquires an Entry writer with an event-driven slow path. The native writer bit remains the
   * ownership and generation authority; the Java monitor only closes the failed-claim to wake-up
   * race without adding a field to Entry. Returns the post-claim state word, or 0 on failure.
   */
  private long awaitWriter(Entry entry) {
    if (entry == null) {
      return 0L;
    }
    ThreadContext context = contexts.get();
    long claimed = claimWriterStateWordProtected(context, entry);
    if (claimed != 0L || !entry.isAlive()) {
      return claimed;
    }
    boolean interrupted = false;
    try {
      synchronized (entry) {
        while (true) {
          boolean wait;
          enterWriterReader(context);
          try {
            if (!entry.isAlive()) {
              return 0L;
            }
            claimed = entry.claimWriterStateWord();
            if (claimed != 0L) {
              return claimed;
            }
            wait = entry.markWriterWaiter();
          } finally {
            exit(context);
          }
          if (wait) {
            try {
              entry.wait();
            } catch (InterruptedException interruption) {
              interrupted = true;
            }
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Waits for the current writer without retaining native lifetime protection while parked. */
  private void awaitWriterRelease(Entry entry) {
    if (entry == null) {
      return;
    }
    ThreadContext context = contexts.get();
    boolean interrupted = false;
    try {
      synchronized (entry) {
        while (true) {
          boolean wait;
          enterWriterReader(context);
          try {
            if (!entry.isAlive() || !entry.isWriterLocked()) {
              return;
            }
            wait = entry.markWriterWaiter();
          } finally {
            exit(context);
          }
          if (wait) {
            try {
              entry.wait();
            } catch (InterruptedException interruption) {
              interrupted = true;
            }
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private long claimWriterStateWordProtected(ThreadContext context, Entry entry) {
    enterWriterReader(context);
    try {
      return entry.isAlive() ? entry.claimWriterStateWord() : 0L;
    } finally {
      exit(context);
    }
  }

  private boolean claimMutationRetryProtected(ThreadContext context, Entry entry) {
    enterWriterReader(context);
    try {
      return entry.isAlive() && entry.claimMutationRetry();
    } finally {
      exit(context);
    }
  }

  private boolean writerLockedProtected(ThreadContext context, Entry entry) {
    enterWriterReader(context);
    try {
      return entry.isAlive() && entry.isWriterLocked();
    } finally {
      exit(context);
    }
  }

  /** Centralizes the release seam used by tests and all cache-owned writer paths. */
  private void releaseWriter(Entry entry) {
    releaseWriter(entry, false, true);
  }

  /** Releases a writer and optionally leaves the capacity wake to the end of a write batch. */
  private void releaseWriter(Entry entry, boolean deferMaintenanceWake) {
    releaseWriter(entry, deferMaintenanceWake, true);
  }

  /** Releases a writer and reports the released state word for the retry-bit epilogue. */
  private long releaseWriterReturningState(Entry entry, boolean deferMaintenanceWake) {
    long released = entry.finishWriterReturningStateWord();
    if (!deferMaintenanceWake) {
      // The actor snapshot is the only writer-side capacity signal; the ledger sum stays on the
      // actor. A capacity pass blocked on this writer is the exceptional case and needs a wake.
      if (logicalAdmission.isOverTarget()) {
        worker.requestCapacityMaintenance();
      } else {
        worker.requestCapacityMaintenanceIfBlocked(entry);
      }
    }
    return released;
  }

  /** Releases a writer with an explicit capacity-wake decision for coalesced publish paths. */
  private void releaseWriter(
      Entry entry, boolean deferMaintenanceWake, boolean requestCapacityWake) {
    entry.finishWriter();
    if (!deferMaintenanceWake) {
      // The actor snapshot is the only writer-side capacity signal; the ledger sum stays on the
      // actor. A capacity pass blocked on this writer is the exceptional case and needs a wake.
      boolean capacityWake = requestCapacityWake && logicalAdmission.isOverTarget();
      if (capacityWake) {
        worker.requestCapacityMaintenance();
      } else {
        worker.requestCapacityMaintenanceIfBlocked(entry);
      }
    }
  }

  /**
   * Publishes TTL absence only while owning the same Entry version lock used by value writers. The
   * pointer check and logical transition must not straddle a replacement publication.
   */
  private void markLogicallyAbsentIfCurrent(Entry entry, long expectedTaggedValue) {
    if (!entry.isAlive()) {
      return;
    }
    // A reader can observe an expired value while the entry is still in the insertion handoff,
    // before finishInsertion flips the logical tag to present. In that case there is no writer
    // claim to acquire, but the actor still needs a mutation hint so it cannot retain a stale
    // policy node after the handoff completes.
    if (entry.isLogicallyAbsent()) {
      publishReaderMutation(entry);
      return;
    }
    if (!Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)) {
      return;
    }
    if (!claimWriter(entry)) {
      return;
    }
    boolean publishMutation = false;
    try {
      if (Entry.samePublishedValue(entry.valueAddress, expectedTaggedValue)) {
        markLogicallyAbsent(entry);
        publishMutation = true;
      }
    } finally {
      releaseWriter(entry);
    }
    if (publishMutation) {
      // Publish only after releasing the entry writer lock. Otherwise the actor can consume this
      // hint as a locked retry, while this read-side path has no writer epilogue to requeue it.
      // Coalesce with any pending hint. The first publication lazily acquires this reader's lane.
      publishReaderMutation(entry);
    }
  }

  private boolean claimWriter(Entry entry) {
    return claimWriterStateWordProtected(contexts.get(), entry) != 0L;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
    throw (T) failure;
  }

  private static Throwable appendFailure(Throwable first, Throwable additional) {
    if (first == null) {
      return additional;
    }
    first.addSuppressed(additional);
    return first;
  }

  private ThreadContext enterWriter() {
    worker.throwIfUnavailable();
    ThreadContext context = contexts.get();
    if (!context.isRegistered()) {
      worker.registerReader(context);
    }
    if (context.isWriterEntered()) {
      return null;
    }
    ensureWriterResources(context);
    context.enterKey();
    if (context.tryEnterWriter()) {
      return context;
    }
    context.exitKey();
    return null;
  }

  private void ensureWriterResources(ThreadContext context) {
    if (!context.hasWriterResources()) {
      try {
        WriterResource resource = writerResources.acquire();
        context.bindWriterResource(resource);
        readers.attachWriterResource(context.slot, resource);
      } catch (Throwable failure) {
        worker.recordTerminalFailure(failure);
        throw failure;
      }
    }
  }

  private void publishReaderMutation(Entry entry) {
    if (!entry.isAlive()) {
      return;
    }
    ThreadContext context = contexts.get();
    ensureWriterResources(context);
    // Resource acquisition can pause before the publishing responsibility is claimed. Protect
    // native key metadata and recheck the heap lifecycle after that cold path completes.
    enterWriterReader(context);
    try {
      if (entry.isAlive() && worker.prepareMutation(entry, Entry.PENDING_UPDATE)) {
        worker.enqueueWriterMutationHint(
            context.lifecycleLane(),
            entry,
            0,
            0L,
            WriterLifecycleLane.UNSEEDED_MUTATION_VERSION,
            true);
      }
    } finally {
      exit(context);
    }
  }

  private void exitWriter(ThreadContext context) {
    context.exitWriter();
    context.exitKey();
  }

  private static void hit(ThreadContext context, Entry entry, long observedValueAddress) {
    long sequence = context.hit();
    context.access(entry, observedValueAddress);
    context.finishRead(sequence);
  }

  private static void miss(ThreadContext context) {
    long sequence = context.miss();
    context.finishRead(sequence);
  }

  private static boolean isAliveTaggedValue(long taggedValue) {
    return (taggedValue & VALUE_LIFECYCLE_MASK) == 0L;
  }

  /** Eager value-header read issued while the equals and absence-word misses are in flight. */
  private static long preReadValueDeadline(long taggedValue) {
    if (taggedValue == 0L || !isAliveTaggedValue(taggedValue)) {
      return 0L;
    }
    return ValueBlock.deadlineNanos(Entry.rawValueAddress(taggedValue));
  }

  private long valueIfLive(Entry entry) {
    if (entry == null) {
      return 0L;
    }
    long taggedValue = entry.valueAddress;
    if (!isAliveTaggedValue(taggedValue)
        || taggedValue == 0L
        || Entry.isAbsentTaggedValue(taggedValue)) {
      return 0L;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (!Entry.hasTtl(taggedValue)) {
      return value;
    }
    if (ValueBlock.expired(value, deadlineClock.nowNanos())) {
      markLogicallyAbsentIfCurrent(entry, taggedValue);
      return 0L;
    }
    return value;
  }

  private long valueIfLive(Entry entry, long taggedValue, long nowNanos) {
    if (entry == null
        || taggedValue == 0L
        || !isAliveTaggedValue(taggedValue)
        || Entry.isAbsentTaggedValue(taggedValue)) {
      return 0L;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (!Entry.hasTtl(taggedValue)) {
      return value;
    }
    if (ValueBlock.expired(value, nowNanos)) {
      markLogicallyAbsentIfCurrent(entry, taggedValue);
      return 0L;
    }
    return value;
  }

  /**
   * Hot-path liveness with the value deadline pre-read so its cache miss overlaps the equals walk.
   * The logical-absence test runs on the already-loaded tagged word, so the read path never
   * dereferences the native metadata prefix.
   */
  private long valueIfLive(Entry entry, long taggedValue, long preReadDeadline, long nowNanos) {
    if (entry == null
        || taggedValue == 0L
        || !isAliveTaggedValue(taggedValue)
        || Entry.isAbsentTaggedValue(taggedValue)) {
      return 0L;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (!Entry.hasTtl(taggedValue)) {
      return value;
    }
    if (ValueBlock.expiredByDeadline(preReadDeadline, nowNanos)) {
      markLogicallyAbsentIfCurrent(entry, taggedValue);
      return 0L;
    }
    return value;
  }

  /** Returns the live value while the caller owns the Entry writer claim. */
  private long valueIfLiveWhileWriterHeld(Entry entry) {
    long taggedValue = entry.valueAddress;
    if (taggedValue == 0L
        || !isAliveTaggedValue(taggedValue)
        || Entry.isAbsentTaggedValue(taggedValue)) {
      return 0L;
    }
    long value = Entry.rawValueAddress(taggedValue);
    if (!Entry.hasTtl(taggedValue)) {
      return value;
    }
    if (ValueBlock.expired(value, deadlineClock.nowNanos())) {
      tryMarkLogicallyAbsent(entry);
      // This path already owns the Entry writer claim, so the reader-side helper cannot publish
      // the absence after releasing it. Keep the actor's policy mirror from retaining the
      // logically absent mapping by publishing an advisory update while the claim is held; the
      // actor will either apply it after the claim clears or leave its normal retry marker.
      publishReaderMutation(entry);
      return 0L;
    }
    return value;
  }

  private long resolveDeadline(ThreadContext context, long requestedExpiry) {
    if (requestedExpiry == DEFAULT_TTL) {
      if (!ttlEnabled) {
        context.writeCreatedAtMillis(Long.MIN_VALUE);
        context.writeMonotonicNowNanos(Long.MIN_VALUE);
        return MonotonicDeadlineClock.NO_DEADLINE;
      }
      long monotonicNowNanos = deadlineClock.nowNanos();
      context.writeCreatedAtMillis(deadlineClock.wallMillisAt(monotonicNowNanos));
      context.writeMonotonicNowNanos(monotonicNowNanos);
      return deadlineClock.deadlineAfterMillis(defaultTtlMillis, monotonicNowNanos);
    }
    if (requestedExpiry <= 0L || (!ttlEnabled && requestedExpiry == DEFAULT_TTL)) {
      context.writeCreatedAtMillis(Long.MIN_VALUE);
      context.writeMonotonicNowNanos(Long.MIN_VALUE);
      return MonotonicDeadlineClock.NO_DEADLINE;
    }
    long monotonicNowNanos = deadlineClock.nowNanos();
    long wallNowMillis = ticker.currentTimeMillis();
    context.writeCreatedAtMillis(wallNowMillis);
    context.writeMonotonicNowNanos(monotonicNowNanos);
    return deadlineClock.deadlineFromEpochMillis(requestedExpiry, wallNowMillis, monotonicNowNanos);
  }

  private boolean replacementTimerMaintenanceRequired(
      ThreadContext context,
      Entry entry,
      long oldTaggedValue,
      long oldAllocation,
      long newAllocation,
      long newDeadlineNanos) {
    long oldValue = Entry.rawValueAddress(oldTaggedValue);
    if (oldValue == 0L || oldAllocation != newAllocation) {
      return true;
    }
    if (!Entry.hasTtl(oldTaggedValue) && newDeadlineNanos == MonotonicDeadlineClock.NO_DEADLINE) {
      return false;
    }
    long oldDeadlineNanos = ValueBlock.deadlineNanos(oldValue);
    return oldDeadlineNanos != newDeadlineNanos
        && !canExtendTimerDeadline(
            entry, oldDeadlineNanos, newDeadlineNanos, context.writeMonotonicNowNanos());
  }

  /**
   * Pure extensions of a live deadline are applied by the writer under its claim: the wheel
   * self-heals a stale link by re-adding with the current deadline when the old slot fires.
   * Shrinks, TTL transitions, and first schedules still need the maintenance path.
   */
  private boolean canExtendTimerDeadline(
      Entry entry, long oldDeadlineNanos, long newDeadlineNanos, long writerNowNanos) {
    if (entry == null
        || oldDeadlineNanos == MonotonicDeadlineClock.NO_DEADLINE
        || newDeadlineNanos == MonotonicDeadlineClock.NO_DEADLINE
        || newDeadlineNanos < oldDeadlineNanos
        || writerNowNanos == Long.MIN_VALUE
        || oldDeadlineNanos <= writerNowNanos
        || !entry.timerScheduled()) {
      return false;
    }
    entry.extendTimerDeadlineTickAfterWriterClaim(TimerWheel.ceilTick(newDeadlineNanos));
    return true;
  }

  private long resolveDeadline(long requestedExpiry) {
    if (requestedExpiry == DEFAULT_TTL) {
      return ttlEnabled
          ? deadlineClock.deadlineAfterMillis(defaultTtlMillis)
          : MonotonicDeadlineClock.NO_DEADLINE;
    }
    return requestedExpiry <= 0L
        ? MonotonicDeadlineClock.NO_DEADLINE
        : deadlineClock.deadlineFromEpochMillis(requestedExpiry);
  }

  private boolean deadlineExpired(long deadlineNanos) {
    return deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE
        && MonotonicDeadlineClock.expired(deadlineNanos, deadlineClock.nowNanos());
  }

  private void publishValue(Entry entry, long taggedValueAddress) {
    // A replacement must keep the logical-absence tag: completeReplacement counts on it.
    entry.valueAddress = taggedValueAddress | (entry.valueAddress & Entry.VALUE_LOGICALLY_ABSENT);
  }

  private void clearValue(Entry entry) {
    entry.clearValue();
    entry.currentValueAllocation(0L);
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
  private void writeValue(ThreadContext context, long payloadAddress, Object value, int length) {
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

  private void initializeValueBlock(
      ThreadContext context, long address, long deadlineNanos, int valueLength) {
    ValueBlock.initialize(
        address,
        deadlineNanos,
        valueLength,
        deadlineNanos != MonotonicDeadlineClock.NO_DEADLINE
            ? context.writeCreatedAtMillis()
            : worker.nowMillis());
  }

  private long allocateNative(ThreadContext context, long allocation) {
    try {
      return context.writer().allocate(allocation);
    } catch (OutOfMemoryError failure) {
      nativeAllocationRejected(failure);
      throw failure;
    }
  }

  private void nativeAllocationRejected() {
    nativeAllocationRejected(new OutOfMemoryError("native allocation failed"));
  }

  private void nativeAllocationRejected(Throwable failure) {
    worker.recordNativeAllocationFailure();
    worker.recordTerminalFailure(failure);
  }

  private void freeEntry(Entry entry, long valueAllocation) {
    freeBlock(Entry.rawValueAddress(entry.valueAddress), valueAllocation);
    freeBlock(entry.nativeKeyAddress, entry.nativeKeyAllocationLength());
  }

  private void freeCandidateKey(Entry entry) {
    freeBlock(entry.nativeKeyAddress, entry.nativeKeyAllocationLength());
  }

  private void freeBlock(long address, long allocation) {
    if (address != 0L) {
      memory.releaseEntry(address, allocation);
    }
  }

  private static long currentValueAllocation(Entry entry) {
    long allocation = entry.currentValueAllocation();
    if (allocation != 0L) {
      return allocation;
    }
    long address = Entry.rawValueAddress(entry.valueAddress);
    return address == 0L ? 0L : ValueBlock.allocationLength(ValueBlock.length(address));
  }
}
