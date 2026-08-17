package com.red.ohc.runtime;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache-local, bounded pool for fingerprint buffers larger than the per-thread scratch limit.
 * Acquisition is non-blocking: resource pressure falls back to native deserialization.
 */
public final class FingerprintScratchPool {
  public static final int MAX_FINGERPRINT_BYTES = 4 * 1024 * 1024;
  private static final int MAX_POOL_BYTES = 4 * 1024 * 1024;
  private static final int MIN_POOLED_BYTES = 128 * 1024;
  private static final int[] SIZE_CLASSES = {
    MIN_POOLED_BYTES,
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    MAX_FINGERPRINT_BYTES
  };

  @SuppressWarnings("unchecked")
  private final ConcurrentLinkedQueue<FingerprintScratch>[] available =
      (ConcurrentLinkedQueue<FingerprintScratch>[]) new ConcurrentLinkedQueue<?>[SIZE_CLASSES.length];
  private final AtomicInteger allocatedBytes = new AtomicInteger();

  public FingerprintScratchPool() {
    for (int index = 0; index < available.length; index++) {
      available[index] = new ConcurrentLinkedQueue<>();
    }
  }

  FingerprintScratch acquire(int length) {
    if (length <= ThreadContext.MAX_THREAD_LOCAL_FINGERPRINT_BYTES
        || length > MAX_FINGERPRINT_BYTES) {
      return null;
    }
    int classIndex = sizeClassIndex(length);
    for (int index = classIndex; index < SIZE_CLASSES.length; index++) {
      FingerprintScratch scratch = available[index].poll();
      if (scratch != null) {
        return scratch;
      }
    }

    int capacity = SIZE_CLASSES[classIndex];
    if (!reserve(capacity)) {
      reclaimIdle(capacity);
      if (!reserve(capacity)) {
        return null;
      }
    }
    try {
      return new FingerprintScratch(capacity);
    } catch (OutOfMemoryError | RuntimeException failure) {
      allocatedBytes.addAndGet(-capacity);
      return null;
    }
  }

  void release(FingerprintScratch scratch) {
    if (scratch == null) {
      return;
    }
    int index = sizeClassIndex(scratch.capacity());
    available[index].offer(scratch);
  }

  /** Releases idle direct buffers when the owning cache is closed. */
  public void clear() {
    for (ConcurrentLinkedQueue<FingerprintScratch> queue : available) {
      FingerprintScratch scratch;
      while ((scratch = queue.poll()) != null) {
        allocatedBytes.addAndGet(-scratch.capacity());
        scratch.discard();
      }
    }
    allocatedBytes.set(0);
  }

  private boolean reserve(int capacity) {
    while (true) {
      int current = allocatedBytes.get();
      if (current > MAX_POOL_BYTES - capacity) {
        return false;
      }
      if (allocatedBytes.compareAndSet(current, current + capacity)) {
        return true;
      }
    }
  }

  private void reclaimIdle(int requiredCapacity) {
    for (ConcurrentLinkedQueue<FingerprintScratch> queue : available) {
      FingerprintScratch scratch;
      while (allocatedBytes.get() + requiredCapacity > MAX_POOL_BYTES
          && (scratch = queue.poll()) != null) {
        allocatedBytes.addAndGet(-scratch.capacity());
        scratch.discard();
      }
      if (allocatedBytes.get() + requiredCapacity <= MAX_POOL_BYTES) {
        return;
      }
    }
  }

  private static int sizeClassIndex(int length) {
    for (int index = 0; index < SIZE_CLASSES.length; index++) {
      if (length <= SIZE_CLASSES[index]) {
        return index;
      }
    }
    return SIZE_CLASSES.length - 1;
  }
}
