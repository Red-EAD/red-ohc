package com.red.ohc.runtime;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache-local weak reader registry. A ThreadContext belongs to its thread, not the cache: once a
 * thread disappears the registry must not keep its ReaderSlot, scratch arrays, arena binding and
 * ThreadLocal graph alive indefinitely.
 */
public final class ReaderRegistry {
  private static final int REGISTER_CLEANUP_LIMIT = 4;
  @SuppressWarnings("unchecked")
  private static final WeakReference<ReaderSlot>[] EMPTY_SCAN_SNAPSHOT = new WeakReference[0];

  private final ReferenceQueue<ReaderSlot> collectedReaders = new ReferenceQueue<>();
  private final Set<WeakReference<ReaderSlot>> slots = ConcurrentHashMap.newKeySet();
  private final Object snapshotLock = new Object();
  private volatile WeakReference<ReaderSlot>[] accessScanSnapshot = EMPTY_SCAN_SNAPSHOT;

  public void register(ReaderSlot slot) {
    synchronized (snapshotLock) {
      cleanupCollectedLocked(REGISTER_CLEANUP_LIMIT);
      slots.add(new IdentityWeakReference(slot, collectedReaders));
      rebuildAccessScanSnapshotLocked();
    }
  }

  /** Actor-owned access scan view; the array and its elements retain only weak reader references. */
  public WeakReference<ReaderSlot>[] accessScanSnapshot() {
    return accessScanSnapshot;
  }

  /** Weakly consistent live view for actor and close-side scans. */
  public Iterable<WeakReference<ReaderSlot>> references() {
    return slots;
  }

  /** Removes at most {@code limit} reader identities reported by the ReferenceQueue. */
  public int cleanupCollected(int limit) {
    if (limit <= 0) {
      return 0;
    }
    synchronized (snapshotLock) {
      int removed = cleanupCollectedLocked(limit);
      if (removed != 0) {
        rebuildAccessScanSnapshotLocked();
      }
      return removed;
    }
  }

  private int cleanupCollectedLocked(int limit) {
    int removed = 0;
    for (int processed = 0; processed < limit; processed++) {
      Reference<? extends ReaderSlot> reference = collectedReaders.poll();
      if (reference == null) {
        break;
      }
      if (slots.remove(reference)) {
        removed++;
      }
    }
    return removed;
  }

  public boolean hasActiveReader() {
    for (WeakReference<ReaderSlot> reference : slots) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.epoch != 0L) {
        return true;
      }
    }
    return false;
  }

  /** Returns the oldest active reader epoch after one weak-registry cleanup and scan. */
  public long minActiveEpoch() {
    cleanupCollected(4_096);
    long minimum = Long.MAX_VALUE;
    for (WeakReference<ReaderSlot> reference : slots) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.epoch != 0L && slot.epoch < minimum) {
        minimum = slot.epoch;
      }
    }
    return minimum;
  }

  /**
   * Close-side scan of distributed writer admission flags; normal writers never share a counter.
   */
  public boolean hasActiveWriter() {
    for (WeakReference<ReaderSlot> reference : slots) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.writerActive) {
        return true;
      }
    }
    return false;
  }

  public int activeWriterCount() {
    int count = 0;
    for (WeakReference<ReaderSlot> reference : slots) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.writerActive) {
        count++;
      }
    }
    return count;
  }

  public void clear() {
    synchronized (snapshotLock) {
      for (WeakReference<ReaderSlot> reference : slots) {
        reference.clear();
      }
      slots.clear();
      while (collectedReaders.poll() != null) {
        // Drain references already queued before shutdown.
      }
      accessScanSnapshot = EMPTY_SCAN_SNAPSHOT;
    }
  }

  private void rebuildAccessScanSnapshotLocked() {
    @SuppressWarnings("unchecked")
    WeakReference<ReaderSlot>[] snapshot = new WeakReference[slots.size()];
    int index = 0;
    for (WeakReference<ReaderSlot> reference : slots) {
      snapshot[index++] = reference;
    }
    if (index != snapshot.length) {
      @SuppressWarnings("unchecked")
      WeakReference<ReaderSlot>[] compacted = new WeakReference[index];
      System.arraycopy(snapshot, 0, compacted, 0, index);
      snapshot = compacted;
    }
    accessScanSnapshot = snapshot;
  }

  int registeredCount() {
    return slots.size();
  }

  private static final class IdentityWeakReference extends WeakReference<ReaderSlot> {
    private final int identityHash;

    private IdentityWeakReference(
        ReaderSlot reader, ReferenceQueue<? super ReaderSlot> collectedReaders) {
      super(reader, collectedReaders);
      this.identityHash = System.identityHashCode(reader);
    }

    @Override
    public int hashCode() {
      return identityHash;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof IdentityWeakReference)) {
        return false;
      }
      ReaderSlot reader = get();
      return reader != null && reader == ((IdentityWeakReference) other).get();
    }
  }
}
