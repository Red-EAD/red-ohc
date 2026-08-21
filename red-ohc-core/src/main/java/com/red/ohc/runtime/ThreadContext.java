package com.red.ohc.runtime;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.ReliableRemovalQueue;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
  private static final int MAX_REUSABLE_BULK_KEYS = 4_096;
  public static final int MAX_THREAD_LOCAL_FINGERPRINT_BYTES = 64 * 1024;
  public static final int MAX_FINGERPRINT_BYTES = FingerprintScratchPool.MAX_FINGERPRINT_BYTES;
  public byte[] keyBytes = new byte[64];
  public ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
  public LookupKey lookupKey = new LookupKey();
  public final ReaderSlot slot = new ReaderSlot();
  private DirectValueView[] directViews;
  private int directViewDepth;
  private int readerDepth;
  private ByteBuffer writableValueBuffer;
  private ByteBuffer[] readOnlyValueBuffers;
  private int readOnlyValueDepth;
  private FingerprintScratch[] fingerprintScratches;
  private boolean[] pooledFingerprintScratches;
  private int fingerprintScratchDepth;
  private Set<Entry>[] bulkEntrySets;
  private int[] bulkEntrySetCapacities;
  private boolean[] reusableBulkEntrySets;
  private int bulkEntrySetDepth;

  private WriterState writerState;
  private long readSequence;
  private long accessSequence;
  private int bulkReadDepth;
  private boolean bulkReadChanged;
  private final FingerprintScratchPool fingerprintScratchPool;

  boolean registered;

  public ThreadContext(WriterArena writerArena) {
    this(writerArena, null);
  }

  public ThreadContext(WriterArena writerArena, FingerprintScratchPool fingerprintScratchPool) {
    this.fingerprintScratchPool = fingerprintScratchPool;
    if (writerArena != null) {
      this.writerState = new WriterState(writerArena);
    }
  }

  private static final class WriterState {
    private final RetirementQueue.Reservation retirement = new RetirementQueue.Reservation();
    private final ReliableRemovalQueue.Reservation reliableRemoval =
        new ReliableRemovalQueue.Reservation();
    private WriterArena writerArena;
    private int budgetStripeIndex = -1;
    private boolean writerEntered;
    private boolean retirementPublished;

    private WriterState(WriterArena writerArena) {
      this.writerArena = writerArena;
    }
  }




  public static void verifyNativeByteBufferSupported() {
    NativeByteBuffer.verifySupported();
  }

  public void ensureKey(int length) {
    if (keyBytes.length < length) {
      keyBytes = new byte[round(length)];
      keyBuffer = ByteBuffer.wrap(keyBytes);
    }
  }




  public ByteBuffer keyBuffer(int length) {
    keyBuffer.clear();
    keyBuffer.limit(length);
    return keyBuffer;
  }

  /** Reuses the direct-buffer shell while each invocation still invalidates it before returning. */
  public ByteBuffer writableValueBuffer(long address, int length) {
    if (writableValueBuffer == null) {
      writableValueBuffer = NativeByteBuffer.writable(address, length);
    } else {
      NativeByteBuffer.writable(writableValueBuffer, address, length);
    }
    return writableValueBuffer;
  }

  /** Reuses read-only native views while preserving nested deserialize calls by depth. */
  public ByteBuffer readOnlyValueBuffer(long address, int length) {
    if (readOnlyValueBuffers == null) {
      readOnlyValueBuffers = new ByteBuffer[4];
    }
    if (readOnlyValueDepth == readOnlyValueBuffers.length) {
      ByteBuffer[] expanded = new ByteBuffer[readOnlyValueBuffers.length << 1];
      System.arraycopy(readOnlyValueBuffers, 0, expanded, 0, readOnlyValueBuffers.length);
      readOnlyValueBuffers = expanded;
    }
    ByteBuffer buffer = readOnlyValueBuffers[readOnlyValueDepth];
    if (buffer == null) {
      buffer = NativeByteBuffer.readOnly(address, length);
      readOnlyValueBuffers[readOnlyValueDepth] = buffer;
    } else {
      NativeByteBuffer.readOnly(buffer, address, length);
    }
    readOnlyValueDepth++;
    return buffer;
  }

  public void releaseReadOnlyValueBuffer() {
    if (readOnlyValueDepth <= 0) {
      throw new IllegalStateException("read-only value buffer is not entered");
    }
    ByteBuffer buffer = readOnlyValueBuffers[--readOnlyValueDepth];
    NativeByteBuffer.invalidate(buffer);
  }

  public void invalidateWritableValueBuffer(ByteBuffer buffer) {
    NativeByteBuffer.invalidate(buffer);
  }

  /** Enters a per-depth direct scratch buffer for weak-value fingerprint validation. */
  public ByteBuffer enterFingerprintScratch(int length) {
    if (length < 0 || length > MAX_FINGERPRINT_BYTES) {
      return null;
    }
    FingerprintScratch scratch = null;
    boolean pooled = false;
    try {
      ensureFingerprintDepth();
      int depth = fingerprintScratchDepth;
      if (length <= MAX_THREAD_LOCAL_FINGERPRINT_BYTES) {
        scratch = fingerprintScratches[depth];
        if (scratch == null) {
          scratch = new FingerprintScratch();
          fingerprintScratches[depth] = scratch;
        }
        if (!scratch.ensureCapacity(length)) {
          return null;
        }
      } else {
        if (fingerprintScratchPool == null) {
          return null;
        }
        scratch = fingerprintScratchPool.acquire(length);
        if (scratch == null) {
          return null;
        }
        pooled = true;
        fingerprintScratches[depth] = scratch;
      }
      pooledFingerprintScratches[depth] = pooled;
      ByteBuffer prepared = scratch.prepare(length);
      fingerprintScratchDepth++;
      return prepared;
    } catch (OutOfMemoryError ignored) {
      if (pooled && scratch != null) {
        fingerprintScratchPool.release(scratch);
        fingerprintScratches[fingerprintScratchDepth] = null;
        pooledFingerprintScratches[fingerprintScratchDepth] = false;
      }
      return null;
    }
  }

  /** Computes a CRC32C or Adler32 fingerprint using the currently entered scratch state. */
  public long fingerprint(ByteBuffer buffer, int length) {
    if (fingerprintScratchDepth <= 0) {
      throw new IllegalStateException("fingerprint scratch is not entered");
    }
    return fingerprintScratches[fingerprintScratchDepth - 1].checksum(buffer, length);
  }

  public void exitFingerprintScratch() {
    if (fingerprintScratchDepth <= 0) {
      throw new IllegalStateException("fingerprint scratch is not entered");
    }
    int depth = --fingerprintScratchDepth;
    if (pooledFingerprintScratches[depth]) {
      fingerprintScratchPool.release(fingerprintScratches[depth]);
      fingerprintScratches[depth] = null;
      pooledFingerprintScratches[depth] = false;
    }
  }

  public WriterArena writer() {
    WriterState state = writerState;
    if (state == null || state.writerArena == null) {
      throw new IllegalStateException("writer resources are not bound");
    }
    return state.writerArena;
  }

  public boolean hasWriterResources() {
    WriterState state = writerState;
    return state != null && state.writerArena != null;
  }

  public void bindWriterResources(WriterArena writerArena, int budgetStripeIndex) {
    if (writerArena == null) {
      throw new IllegalArgumentException("writer arena is required");
    }
    if (budgetStripeIndex < 0) {
      throw new IllegalArgumentException("budget stripe index must be non-negative");
    }
    WriterState state = ensureWriterState();
    if (state.writerArena != null) {
      if (state.writerArena != writerArena) {
        throw new IllegalStateException("writer resources are already bound");
      }
      return;
    }
    state.writerArena = writerArena;
    state.budgetStripeIndex = budgetStripeIndex;
  }

  public boolean tryEnterWriter() {
    WriterState state = ensureWriterState();
    if (state.writerEntered) {
      return false;
    }
    state.writerEntered = true;
    return true;
  }

  public void exitWriter() {
    WriterState state = writerState;
    if (state == null || !state.writerEntered) {
      throw new IllegalStateException("writer is not entered");
    }
    state.writerEntered = false;
  }

  public boolean isWriterEntered() {
    WriterState state = writerState;
    return state != null && state.writerEntered;
  }

  public RetirementQueue.Reservation retirement() {
    return ensureWriterState().retirement;
  }

  public ReliableRemovalQueue.Reservation reliableRemoval() {
    return ensureWriterState().reliableRemoval;
  }

  public int budgetStripeIndex() {
    WriterState state = writerState;
    if (state == null || state.writerArena == null) {
      throw new IllegalStateException("writer resources are not bound");
    }
    return state.budgetStripeIndex;
  }

  public boolean isRegistered() {
    return registered;
  }

  public int readerDepth() {
    return readerDepth;
  }

  public void enterReader() {
    readerDepth++;
  }

  public boolean exitReader() {
    if (readerDepth <= 0) {
      throw new IllegalStateException("reader guard is not entered");
    }
    return --readerDepth == 0;
  }

  public DirectValueView pushDirectView(long address, int length) {
    if (directViews == null) {
      directViews = new DirectValueView[4];
    }
    if (directViewDepth == directViews.length) {
      DirectValueView[] expanded = new DirectValueView[directViews.length << 1];
      System.arraycopy(directViews, 0, expanded, 0, directViews.length);
      directViews = expanded;
    }
    DirectValueView view = directViews[directViewDepth];
    if (view == null) {
      view = new DirectValueView();
      directViews[directViewDepth] = view;
    }
    directViewDepth++;
    view.reset(address, length, this);
    return view;
  }

  public void popDirectView() {
    if (directViewDepth <= 0) {
      throw new IllegalStateException("direct view is not entered");
    }
    DirectValueView view = directViews[--directViewDepth];
    try {
      if (view.hasMaterializedByteBuffer()) {
        releaseReadOnlyValueBuffer();
      }
    } finally {
      view.reset(0L, 0, null);
    }
  }

  /** Acquires per-depth hit-Entry identity tracking without retaining a huge table. */
  public Set<Entry> acquireBulkEntries(int expected) {
    if (expected < 0) {
      throw new IllegalArgumentException("negative bulk entry count");
    }
    ensureBulkEntryDepth();
    int depth = bulkEntrySetDepth++;
    boolean reusable = expected <= MAX_REUSABLE_BULK_KEYS;
    Set<Entry> entries = bulkEntrySets[depth];
    if (!reusable || entries == null || bulkEntrySetCapacities[depth] < expected) {
      entries =
          Collections.newSetFromMap(
              new IdentityHashMap<>(Math.min(expected, MAX_REUSABLE_BULK_KEYS)));
      bulkEntrySets[depth] = entries;
      bulkEntrySetCapacities[depth] = reusable ? expected : 0;
    } else {
      entries.clear();
    }
    reusableBulkEntrySets[depth] = reusable;
    return entries;
  }

  /** Releases a duplicate table while preserving nested bulk-call isolation. */
  public void releaseBulkEntries(Set<Entry> entries) {
    if (bulkEntrySetDepth <= 0) {
      throw new IllegalStateException("bulk entry set is not acquired");
    }
    int depth = --bulkEntrySetDepth;
    if (bulkEntrySets[depth] != entries) {
      throw new IllegalStateException("bulk entry set release order mismatch");
    }
    entries.clear();
    if (!reusableBulkEntrySets[depth]) {
      bulkEntrySets[depth] = null;
      bulkEntrySetCapacities[depth] = 0;
    }
    reusableBulkEntrySets[depth] = false;
  }

  private void ensureBulkEntryDepth() {
    if (bulkEntrySets == null) {
      bulkEntrySets = new Set[2];
      bulkEntrySetCapacities = new int[2];
      reusableBulkEntrySets = new boolean[2];
    }
    if (bulkEntrySetDepth < bulkEntrySets.length) {
      return;
    }
    int expandedLength = bulkEntrySets.length << 1;
    Set<Entry>[] expanded = new Set[expandedLength];
    int[] expandedCapacities = new int[expandedLength];
    boolean[] expandedReusable = new boolean[expandedLength];
    System.arraycopy(bulkEntrySets, 0, expanded, 0, bulkEntrySets.length);
    System.arraycopy(bulkEntrySetCapacities, 0, expandedCapacities, 0, bulkEntrySetCapacities.length);
    System.arraycopy(reusableBulkEntrySets, 0, expandedReusable, 0, reusableBulkEntrySets.length);
    bulkEntrySets = expanded;
    bulkEntrySetCapacities = expandedCapacities;
    reusableBulkEntrySets = expandedReusable;
  }

  private void ensureFingerprintDepth() {
    if (fingerprintScratches == null) {
      fingerprintScratches = new FingerprintScratch[2];
      pooledFingerprintScratches = new boolean[2];
    }
    if (fingerprintScratchDepth < fingerprintScratches.length) {
      return;
    }
    FingerprintScratch[] expanded = new FingerprintScratch[fingerprintScratches.length << 1];
    boolean[] expandedPooled = new boolean[pooledFingerprintScratches.length << 1];
    System.arraycopy(fingerprintScratches, 0, expanded, 0, fingerprintScratches.length);
    System.arraycopy(pooledFingerprintScratches, 0, expandedPooled, 0, pooledFingerprintScratches.length);
    fingerprintScratches = expanded;
    pooledFingerprintScratches = expandedPooled;
  }

  public void markRegistered() {
    registered = true;
  }

  public long hit() {
    slot.localHits++;
    return ++readSequence;
  }

  public long miss() {
    slot.localMisses++;
    return ++readSequence;
  }

  public void beginBulkRead() {
    if (bulkReadDepth == 0) {
      bulkReadChanged = false;
    }
    bulkReadDepth++;
  }

  public void bulkHit(Entry entry) {
    slot.localHits++;
    readSequence++;
    if ((++accessSequence & 15L) == 0L) {
      accessRing().offer(entry, entry.generation());
    }
    bulkReadChanged = true;
  }

  public void bulkMiss() {
    slot.localMisses++;
    readSequence++;
    bulkReadChanged = true;
  }

  /** Publishes one batch of read counters instead of one per key. */
  public void finishBulkRead() {
    if (bulkReadDepth == 0) {
      return;
    }
    if (--bulkReadDepth != 0) {
      return;
    }
    if (bulkReadChanged) {
      publish();
    }
    bulkReadChanged = false;
  }

  /** Delivers every sampled hit to this thread's actor-owned access ring. */
  public void access(Entry entry) {
    if ((++accessSequence & 15L) != 0L) {
      return;
    }
    publishSampledAccess(entry);
  }

  private void publishSampledAccess(Entry entry) {
    accessRing().offer(entry, entry.generation());
  }

  public void finishRead(long sequence) {
    if ((sequence & 1023L) != 0L) {
      return;
    }
    publishReadCounters();
  }

  private void publishReadCounters() {
    publish();
  }

  /** Flushes sub-threshold read counters when a control-plane barrier is requested. */
  public void flushRead() {
    publish();
  }

  public void markRetirementPublished() {
    ensureWriterState().retirementPublished = true;
  }

  public boolean consumeRetirementPublished() {
    WriterState state = writerState;
    if (state == null) {
      return false;
    }
    boolean published = state.retirementPublished;
    state.retirementPublished = false;
    return published;
  }

  public void publish() {
    slot.publishedHits = slot.localHits;
    slot.publishedMisses = slot.localMisses;
  }

  private AccessRing accessRing() {
    AccessRing ring = slot.access;
    if (ring == null) {
      ring = new AccessRing();
      slot.access = ring;
    }
    return ring;
  }

  private WriterState ensureWriterState() {
    WriterState state = writerState;
    if (state == null) {
      state = new WriterState(null);
      writerState = state;
    }
    return state;
  }

  private static int round(int value) {
    if (value < 0 || value > (1 << 30)) {
      throw new IllegalArgumentException("serialized key is too large: " + value);
    }
    long size = 64L;
    while (size < value) {
      size <<= 1;
    }
    return (int) size;
  }

}
