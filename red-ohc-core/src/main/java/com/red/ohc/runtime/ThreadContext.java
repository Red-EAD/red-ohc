package com.red.ohc.runtime;

import java.nio.ByteBuffer;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;

import com.red.ohc.codec.LookupKey;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.ReliableRemovalQueue;
import com.red.ohc.maintenance.RetirementQueue;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.WriterArena;

public final class ThreadContext {
  private static final int MAX_REUSABLE_BULK_KEYS = 4_096;
  public byte[] keyBytes = new byte[64];
  public ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
  public final LookupKey lookupKey = new LookupKey();
  public final ReaderSlot slot = new ReaderSlot();
  private DirectValueView[] directViews = new DirectValueView[4];
  private int directViewDepth;
  private int readerDepth;
  private ByteBuffer writableValueBuffer;
  private ByteBuffer[] readOnlyValueBuffers = new ByteBuffer[4];
  private int readOnlyValueDepth;
  private ReferenceOpenHashSet<Entry>[] bulkEntrySets = new ReferenceOpenHashSet[2];
  private boolean[] reusableBulkEntrySets = new boolean[2];
  private int bulkEntrySetDepth;

  /**
   * Reusable native-retirement reservation; it is active only across one writer critical section.
   */
  public final RetirementQueue.Reservation retirement = new RetirementQueue.Reservation();

  public final ReliableRemovalQueue.Reservation reliableRemoval =
      new ReliableRemovalQueue.Reservation();
  private WriterArena writerArena;
  private Budget.Lease budgetLease;
  private long readSequence;
  private long accessSequence;
  private MaintenanceEventLoop maintenance;
  private int bulkReadDepth;
  private boolean bulkReadChanged;
  private boolean retirementPublished;

  /** Last actor idle generation this writer has already signalled. */
  private long maintenanceWakeGeneration = Long.MIN_VALUE;

  boolean registered;

  public ThreadContext(WriterArena writerArena, Budget.Lease budgetLease) {
    this.writerArena = writerArena;
    this.budgetLease = budgetLease;
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
    return writerArena;
  }

  public Budget.Lease budgetLease() {
    return budgetLease;
  }

  public boolean hasWriterResources() {
    return writerArena != null;
  }

  public void bindWriterResources(WriterArena writerArena, Budget.Lease budgetLease) {
    if (writerArena == null || budgetLease == null) {
      throw new IllegalArgumentException("writer arena and budget lease are required");
    }
    if (this.writerArena != null || this.budgetLease != null) {
      if (this.writerArena != writerArena || this.budgetLease != budgetLease) {
        throw new IllegalStateException("writer resources are already bound");
      }
      return;
    }
    this.writerArena = writerArena;
    this.budgetLease = budgetLease;
  }

  public boolean tryActivateWriter() {
    return budgetLease.tryActivate();
  }

  public void deactivateWriter() {
    budgetLease.deactivate();
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
  public ReferenceOpenHashSet<Entry> acquireBulkEntries(int expected) {
    if (expected < 0) {
      throw new IllegalArgumentException("negative bulk entry count");
    }
    ensureBulkEntryDepth();
    int depth = bulkEntrySetDepth++;
    boolean reusable = expected <= MAX_REUSABLE_BULK_KEYS;
    ReferenceOpenHashSet<Entry> entries = bulkEntrySets[depth];
    if (!reusable || entries == null) {
      entries = new ReferenceOpenHashSet<>(expected);
      bulkEntrySets[depth] = entries;
    } else {
      entries.clear();
      entries.ensureCapacity(expected);
    }
    reusableBulkEntrySets[depth] = reusable;
    return entries;
  }

  /** Releases a duplicate table while preserving nested bulk-call isolation. */
  public void releaseBulkEntries(ReferenceOpenHashSet<Entry> entries) {
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
    }
    reusableBulkEntrySets[depth] = false;
  }

  private void ensureBulkEntryDepth() {
    if (bulkEntrySetDepth < bulkEntrySets.length) {
      return;
    }
    int expandedLength = bulkEntrySets.length << 1;
    ReferenceOpenHashSet<Entry>[] expanded = new ReferenceOpenHashSet[expandedLength];
    boolean[] expandedReusable = new boolean[expandedLength];
    System.arraycopy(bulkEntrySets, 0, expanded, 0, bulkEntrySets.length);
    System.arraycopy(
        reusableBulkEntrySets, 0, expandedReusable, 0, reusableBulkEntrySets.length);
    bulkEntrySets = expanded;
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
      slot.access.offer(entry, entry.generation());
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
      if (loop != null && slot.markAccessPending()) {
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
    if (!slot.access.offer(entry, entry.generation())) {
      return;
    }
    MaintenanceEventLoop loop = maintenance;
    if (loop != null && slot.markAccessPending()) {
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
      if (loop != null && slot.markAccessPending()) {
        loop.signalAccess(slot);
      }
    }
  }

  /** Flushes sub-threshold read counters when a control-plane barrier is requested. */
  public void flushRead() {
    publish();
    MaintenanceEventLoop loop = maintenance;
    if (loop != null && slot.markAccessPending()) {
      loop.signalAccess(slot);
    }
  }

  public void bindMaintenance(MaintenanceEventLoop maintenance) {
    this.maintenance = maintenance;
  }

  public void markRetirementPublished() {
    retirementPublished = true;
  }

  public boolean consumeRetirementPublished() {
    boolean published = retirementPublished;
    retirementPublished = false;
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
    if (maintenanceWakeGeneration == idleGeneration) {
      return false;
    }
    maintenanceWakeGeneration = idleGeneration;
    return true;
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
