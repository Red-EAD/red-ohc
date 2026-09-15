package com.red.ohc.runtime;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.WriterLifecycleJournal;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/** Dynamic high-water registry for writer-exclusive allocator and journal resources. */
public final class WriterResourceRegistry {
  private static final int RESOURCE_CHUNK_SHIFT = 6;
  private static final int RESOURCE_CHUNK_SIZE = 1 << RESOURCE_CHUNK_SHIFT;
  private static final int RESOURCE_CHUNK_MASK = RESOURCE_CHUNK_SIZE - 1;
  private static final int QUEUE_CHUNK_SIZE = 16;

  private final NativeMemory.Memory memory;
  private final WriterLifecycleJournal lifecycleJournal;
  private final RetirementJournal retirementJournal;
  private final Object registrationLock = new Object();
  private final MpmcUnboundedXaddArrayQueue<WriterResource> pooledResources =
      new MpmcUnboundedXaddArrayQueue<>(QUEUE_CHUNK_SIZE);
  private final MpscUnboundedArrayQueue<WriterResource> retirementRequests =
      new MpscUnboundedArrayQueue<>(QUEUE_CHUNK_SIZE);
  private final ArrayDeque<WriterResource> retiringResources = new ArrayDeque<>();
  private int retirementScanRemaining;
  private final AtomicInteger activeCount = new AtomicInteger();
  private final AtomicInteger retiringCount = new AtomicInteger();
  private final AtomicInteger pooledCount = new AtomicInteger();
  private volatile WriterResource[][] resourceChunks = new WriterResource[1][];
  private volatile int resourceCount;
  private volatile long registrationVersion;
  private volatile Runnable readySignal;

  public WriterResourceRegistry(
      NativeMemory.Memory memory,
      WriterLifecycleJournal lifecycleJournal,
      RetirementJournal retirementJournal) {
    if (memory == null || lifecycleJournal == null || retirementJournal == null) {
      throw new NullPointerException("writer resource dependencies");
    }
    this.memory = memory;
    this.lifecycleJournal = lifecycleJournal;
    this.retirementJournal = retirementJournal;
  }

  /** First-write cold path. The returned bundle remains exclusive until its ReaderSlot is gone. */
  public WriterResource acquire() {
    WriterResource resource = pooledResources.poll();
    synchronized (registrationLock) {
      if (resource == null) {
        resource = pooledResources.poll();
      }
      if (resource == null) {
        resource = createResourceLocked();
      } else {
        pooledCount.decrementAndGet();
      }
      resource.activate(++registrationVersion);
      activeCount.incrementAndGet();
      return resource;
    }
  }

  /** Terminated-reader cleanup publishes the transition; the actor performs cut and drain. */
  public void requestRetirement(WriterResource resource) {
    if (resource == null || !resource.beginRetiring()) {
      return;
    }
    activeCount.decrementAndGet();
    retiringCount.incrementAndGet();
    if (!retirementRequests.offer(resource)) {
      throw new IllegalStateException("unbounded writer retirement queue rejected a resource");
    }
    Runnable signal = readySignal;
    if (signal != null) {
      signal.run();
    }
  }

  /** Actor-owned fixed snapshot: every pending resource is visited at most once per turn. */
  public int processRetirements() {
    return processRetirements(Long.MAX_VALUE, Integer.MAX_VALUE);
  }

  /** Processes only resources registered no later than the actor turn's captured version. */
  public int processRetirements(long maximumRegistrationVersion) {
    return processRetirements(maximumRegistrationVersion, Integer.MAX_VALUE);
  }

  /** Processes at most the requested number of resources from the actor's fixed snapshot. */
  public int processRetirements(long maximumRegistrationVersion, int maximumResources) {
    if (maximumResources <= 0) {
      return 0;
    }
    boolean admitted = false;
    int requestBoundary = Math.min(retirementRequests.size(), maximumResources);
    for (int index = 0; index < requestBoundary; index++) {
      WriterResource requested = retirementRequests.poll();
      if (requested == null) {
        break;
      }
      if (requested.registrationVersion() > maximumRegistrationVersion) {
        retirementRequests.offer(requested);
        continue;
      }
      requested.captureRetirementBoundaries();
      retiringResources.addLast(requested);
      admitted = true;
    }
    int work = 0;
    if (admitted || retirementScanRemaining == 0) {
      requestRetirementScan();
    }
    int scanLimit = Math.min(retirementScanRemaining, maximumResources);
    for (int index = 0; index < scanLimit; index++) {
      WriterResource resource = retiringResources.pollFirst();
      if (resource == null) {
        break;
      }
      retirementScanRemaining--;
      if (!resource.retirementComplete()) {
        retiringResources.addLast(resource);
        continue;
      }
      resource.pool();
      retiringCount.decrementAndGet();
      pooledCount.incrementAndGet();
      if (!pooledResources.offer(resource)) {
        throw new IllegalStateException("unbounded writer resource pool rejected a resource");
      }
      work++;
    }
    return work;
  }

  /** Actor-owned continuation includes unvisited resources, but not an entirely blocked sweep. */
  public boolean hasRetirementWork() {
    return retirementScanRemaining != 0 || !retirementRequests.isEmpty();
  }

  /** Actor-owned watermark progress invalidates checks made earlier in a bounded sweep. */
  public void requestRetirementScan() {
    retirementScanRemaining = retiringResources.size();
  }

  public boolean hasRetiringResources() {
    return retiringCount.get() != 0;
  }

  public int activeCount() {
    return activeCount.get();
  }

  public int retiringCount() {
    return retiringCount.get();
  }

  public int pooledCount() {
    return pooledCount.get();
  }

  public int resourceCount() {
    return resourceCount;
  }

  public long captureRegistrationVersion() {
    return registrationVersion;
  }

  public void bindReadySignal(Runnable signal) {
    if (signal == null) {
      throw new NullPointerException("signal");
    }
    readySignal = signal;
  }

  /** Close-side detach after writer admission reaches zero. */
  public void detachAll() {
    int count = resourceCount;
    WriterResource[][] chunks = resourceChunks;
    for (int index = 0; index < count; index++) {
      WriterResource[] chunk = chunks[index >>> RESOURCE_CHUNK_SHIFT];
      if (chunk != null) {
        chunk[index & RESOURCE_CHUNK_MASK].arena().detach();
      }
    }
  }

  private WriterResource createResourceLocked() {
    int index = resourceCount;
    WriterArena arena = memory.newWriterArena();
    WriterLifecycleLane lifecycleLane = lifecycleJournal.createLane();
    RetirementJournal.Lane retirementLane = retirementJournal.createLane();
    WriterResource resource =
        new WriterResource(index + 1, arena, lifecycleLane, retirementLane);
    int chunkIndex = index >>> RESOURCE_CHUNK_SHIFT;
    WriterResource[][] chunks = resourceChunks;
    if (chunkIndex >= chunks.length) {
      chunks = Arrays.copyOf(chunks, Math.max(chunkIndex + 1, chunks.length << 1));
      resourceChunks = chunks;
    }
    WriterResource[] chunk = chunks[chunkIndex];
    if (chunk == null) {
      chunk = new WriterResource[RESOURCE_CHUNK_SIZE];
      chunks[chunkIndex] = chunk;
    }
    chunk[index & RESOURCE_CHUNK_MASK] = resource;
    resourceCount = index + 1;
    return resource;
  }
}
