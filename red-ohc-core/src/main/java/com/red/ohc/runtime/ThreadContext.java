package com.red.ohc.runtime;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.ReliableRemovalQueue;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
  private static final int MAX_REUSABLE_BULK_KEYS = 4_096;
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
  private Set<Entry>[] bulkEntrySets;
  private int[] bulkEntrySetCapacities;
  private boolean[] reusableBulkEntrySets;
  private int bulkEntrySetDepth;

  private WriterState writerState;
  private long readSequence;
  private long accessSequence;
  private MaintenanceEventLoop maintenance;
  private int bulkReadDepth;
  private boolean bulkReadChanged;

  boolean registered;

  public ThreadContext(WriterArena writerArena) {
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
    private long maintenanceWakeGeneration = Long.MIN_VALUE;

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
    ByteBuffer buffer = readOnlyValueBuffer(address, length);
    try {
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
      view.reset(address, length, buffer);
      return view;
    } catch (Throwable failure) {
      releaseReadOnlyValueBuffer();
      throw failure;
    }
  }

  public void popDirectView() {
    if (directViewDepth <= 0) {
      throw new IllegalStateException("direct view is not entered");
    }
    DirectValueView view = directViews[--directViewDepth];
    try {
      view.reset(0L, 0, null);
    } finally {
      releaseReadOnlyValueBuffer();
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
      entries = Collections.newSetFromMap(new IdentityHashMap<>(expected));
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

  /** Publishes one batch of read counters and one worker hint instead of one per key. */
  public void finishBulkRead() {
    if (bulkReadDepth == 0) {
      return;
    }
    if (--bulkReadDepth != 0) {
      return;
    }
    if (bulkReadChanged) {
      publish();
      MaintenanceEventLoop loop = maintenance;
      if (loop != null) {
        loop.signalAccess(slot);
      }
    }
    bulkReadChanged = false;
  }

  /** Delivers every hit to the policy stream; only an empty-to-nonempty transition wakes it. */
  public void access(Entry entry) {
    if ((++accessSequence & 15L) != 0L) {
      return;
    }
    if (!accessRing().offer(entry, entry.generation())) {
      return;
    }
    MaintenanceEventLoop loop = maintenance;
    if (loop != null) {
      loop.signalAccess(slot);
    }
  }

  public void finishRead(long sequence) {
    if ((sequence & 1023L) == 0L) {
      publish();
      // Global counters are deliberately decoupled from the policy stream. This bounded
      // stats publication may wake the actor even when an earlier access burst was already
      // drained; hit delivery itself still signals only on an empty-to-nonempty ring edge.
      MaintenanceEventLoop loop = maintenance;
      if (loop != null) {
        loop.signalAccess(slot);
      }
    }
  }

  /** Flushes sub-threshold read counters when a control-plane barrier is requested. */
  public void flushRead() {
    publish();
    MaintenanceEventLoop loop = maintenance;
    if (loop != null) {
      loop.signalAccess(slot);
    }
  }

  public void bindMaintenance(MaintenanceEventLoop maintenance) {
    this.maintenance = maintenance;
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

  /**
   * The maintenance actor increments its idle generation only as it commits to an idle park. A
   * writer signals at most once per such generation, removing a global WakeGate access from steady
   * replacement traffic while retaining the park-before-publish handshake.
   */
  public boolean needsMaintenanceWake(long idleGeneration) {
    WriterState state = ensureWriterState();
    if (state.maintenanceWakeGeneration == idleGeneration) {
      return false;
    }
    state.maintenanceWakeGeneration = idleGeneration;
    return true;
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
