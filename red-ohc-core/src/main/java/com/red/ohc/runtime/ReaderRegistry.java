package com.red.ohc.runtime;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache-local weak reader registry. A ThreadContext belongs to its thread, not the cache: once a
 * thread disappears the registry must not keep its ReaderSlot, scratch arrays, arena binding and
 * ThreadLocal graph alive indefinitely.
 */
public final class ReaderRegistry {
  @SuppressWarnings("unchecked")
  private static final WeakReference<ReaderSlot>[] EMPTY = new WeakReference[0];

  private final AtomicReference<WeakReference<ReaderSlot>[]> slots = new AtomicReference<>(EMPTY);

  public void register(ReaderSlot slot) {
    while (true) {
      WeakReference<ReaderSlot>[] current = slots.get();
      int live = 0;
      for (WeakReference<ReaderSlot> reference : current) {
        ReaderSlot existing = reference.get();
        if (existing == slot) {
          return;
        }
        if (existing != null) {
          live++;
        }
      }
      @SuppressWarnings("unchecked")
      WeakReference<ReaderSlot>[] updated = new WeakReference[live + 1];
      int index = 0;
      for (WeakReference<ReaderSlot> reference : current) {
        if (reference.get() != null) {
          updated[index++] = reference;
        }
      }
      updated[index] = new WeakReference<>(slot);
      if (slots.compareAndSet(current, updated)) {
        return;
      }
    }
  }

  /**
   * Actor-side snapshot. Cleared weak references are compacted only when registration or scan runs.
   */
  public WeakReference<ReaderSlot>[] snapshot() {
    compactCleared();
    return slots.get();
  }

  public boolean hasActiveReader() {
    for (WeakReference<ReaderSlot> reference : snapshot()) {
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
    for (WeakReference<ReaderSlot> reference : snapshot()) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.writerActive) {
        return true;
      }
    }
    return false;
  }

  public int activeWriterCount() {
    int count = 0;
    for (WeakReference<ReaderSlot> reference : snapshot()) {
      ReaderSlot slot = reference.get();
      if (slot != null && slot.writerActive) {
        count++;
      }
    }
    return count;
  }

  private void compactCleared() {
    while (true) {
      WeakReference<ReaderSlot>[] current = slots.get();
      int live = 0;
      for (WeakReference<ReaderSlot> reference : current) {
        if (reference.get() != null) {
          live++;
        }
      }
      if (live == current.length) {
        return;
      }
      @SuppressWarnings("unchecked")
      WeakReference<ReaderSlot>[] updated = new WeakReference[live];
      int index = 0;
      for (WeakReference<ReaderSlot> reference : current) {
        if (reference.get() != null) {
          updated[index++] = reference;
        }
      }
      if (slots.compareAndSet(current, updated)) {
        return;
      }
    }
  }
}
