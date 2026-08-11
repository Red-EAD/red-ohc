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

  private final ReferenceQueue<ReaderSlot> collectedReaders = new ReferenceQueue<>();
  private final Set<WeakReference<ReaderSlot>> slots = ConcurrentHashMap.newKeySet();

  public void register(ReaderSlot slot) {
    cleanupCollected(REGISTER_CLEANUP_LIMIT);
    slots.add(new IdentityWeakReference(slot, collectedReaders));
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
    for (WeakReference<ReaderSlot> reference : slots) {
      reference.clear();
    }
    slots.clear();
    while (collectedReaders.poll() != null) {
      // Drain references already queued before shutdown.
    }
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
