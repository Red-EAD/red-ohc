package com.red.ohc.storage;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exact native-resident admission budget. A normal writer consumes its thread-local lease; the
 * globally contended balance is touched only when a lease refills or the maintenance actor releases
 * reclaimed resident memory.
 */
public final class Budget {
  private static final long MAX_REFILL_BYTES = 64L << 10;

  private final long capacity;

  /** Capacity that is neither resident nor temporarily assigned to a writer stripe. */
  private final AtomicLong available;

  private final ReferenceQueue<Thread> collectedOwners = new ReferenceQueue<>();
  private final Set<Lease> leases = ConcurrentHashMap.newKeySet();

  public Budget(long capacity) {
    if (capacity <= 0L) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.capacity = capacity;
    this.available = new AtomicLong(capacity);
  }

  /** Creates the cache-local lease used by a writer thread for small allocations. */
  public Lease leaseForCurrentThread() {
    Lease lease = new Lease(this, Thread.currentThread(), collectedOwners);
    leases.add(lease);
    return lease;
  }

  /**
   * Attempts one producer-side reservation without reclaiming another writer's lease or waiting
   * for the maintenance actor.
   */
  public boolean tryReserve(Lease lease, long bytes) {
    if (lease == null || lease.owner != this) {
      throw new IllegalArgumentException("foreign budget lease");
    }
    if (bytes <= 0L || bytes > capacity) {
      return false;
    }
    long credit = lease.credit;
    if (credit >= bytes) {
      lease.credit = credit - bytes;
      return true;
    }
    long needed = bytes - credit;
    while (true) {
      long free = available.get();
      if (free < needed) {
        return false;
      }
      long request = Math.max(needed, Math.min(MAX_REFILL_BYTES, capacity));
      long grant = Math.min(free, request);
      if (available.compareAndSet(free, free - grant)) {
        lease.credit = credit + grant - bytes;
        return true;
      }
      Thread.onSpinWait();
    }
  }

  public void refund(Lease lease, long bytes) {
    if (bytes <= 0L) {
      return;
    }
    if (lease == null || lease.owner != this) {
      throw new IllegalArgumentException("foreign budget lease");
    }
    lease.credit += bytes;
  }

  /**
   * The actor is the normal caller, so returning reclaimed bytes never creates a producer hotspot.
   */
  public void release(long bytes) {
    if (bytes <= 0L) {
      return;
    }
    long updated = available.addAndGet(bytes);
    if (updated <= capacity) {
      return;
    }
    available.addAndGet(-bytes);
    throw new IllegalStateException("native budget overflow: " + updated + ">" + capacity);
  }

  /**
   * A stats-only snapshot; producers are not stopped, so a concurrently sampled value is
   * approximate.
   */
  public long reserved() {
    long free = available.get();
    for (Lease lease : leases) {
      free += lease.credit;
    }
    long resident = capacity - free;
    return resident <= 0L ? 0L : Math.min(capacity, resident);
  }

  public long capacity() {
    return capacity;
  }

  int leaseCount() {
    return leases.size();
  }

  /** Returns unused writer leases during shutdown; no writer may be active then. */
  public void returnUnusedCredits() {
    for (Lease lease : leases) {
      if (lease.state.compareAndSet(Lease.IDLE, Lease.RECLAIMING)) {
        returnCredit(lease);
        lease.state.set(Lease.IDLE);
      }
    }
  }

  /** Reclaims collected writer leases without scanning live threads. */
  public int reclaimCollectedLeases(int limit) {
    if (limit <= 0) {
      return 0;
    }
    int reclaimed = 0;
    for (int processed = 0; processed < limit; processed++) {
      OwnerReference owner = (OwnerReference) collectedOwners.poll();
      if (owner == null) {
        break;
      }
      Lease lease = owner.lease();
      owner.clearLease();
      if (lease == null || !leases.remove(lease)) {
        continue;
      }
      if (!lease.state.compareAndSet(Lease.IDLE, Lease.RECLAIMING)) {
        throw new IllegalStateException("collected owner still has an active budget lease");
      }
      returnCredit(lease);
      lease.state.set(Lease.IDLE);
      reclaimed++;
    }
    return reclaimed;
  }

  /** Returns credit from any lease that is not currently owned by a writer. */
  public void reclaimIdleLeases() {
    for (Lease lease : leases) {
      if (lease.state.get() != Lease.IDLE) {
        continue;
      }
      if (lease.state.compareAndSet(Lease.IDLE, Lease.RECLAIMING)) {
        boolean dead = !lease.ownerAlive();
        returnCredit(lease);
        lease.state.set(Lease.IDLE);
        if (dead) {
          leases.remove(lease);
        }
      }
    }
  }

  public void clear() {
    for (Lease lease : leases) {
      if (lease.state.get() == Lease.ACTIVE) {
        throw new IllegalStateException("cannot clear an active budget lease");
      }
      lease.credit = 0L;
      lease.ownerThread.clear();
      lease.ownerThread.clearLease();
    }
    leases.clear();
    while (collectedOwners.poll() != null) {
      // Drain references already queued before shutdown.
    }
    available.set(capacity);
  }

  private void returnCredit(Lease lease) {
    long credit = lease.credit;
    lease.credit = 0L;
    if (credit != 0L) {
      release(credit);
    }
  }

  public static final class Lease {
    private final Budget owner;
    private final OwnerReference ownerThread;
    private static final int IDLE = 0;
    private static final int ACTIVE = 1;
    private static final int RECLAIMING = 2;
    private final AtomicInteger state = new AtomicInteger(IDLE);
    private long credit;

    private Lease(
        Budget owner, Thread ownerThread, ReferenceQueue<? super Thread> collectedOwners) {
      this.owner = owner;
      this.ownerThread = new OwnerReference(ownerThread, collectedOwners, this);
    }

    public long credit() {
      return credit;
    }

    private static final int MAX_ACTIVATION_ATTEMPTS = 4;

    /** Activates this lease with a bounded retry across a maintenance reclaim. */
    public boolean tryActivate() {
      for (int attempt = 0; attempt < MAX_ACTIVATION_ATTEMPTS; attempt++) {
        int current = state.get();
        if (current == ACTIVE) {
          return false;
        }
        if (current == IDLE && state.compareAndSet(IDLE, ACTIVE)) {
          return true;
        }
        Thread.onSpinWait();
      }
      return false;
    }

    public void deactivate() {
      if (!state.compareAndSet(ACTIVE, IDLE)) {
        throw new IllegalStateException("budget lease is not active");
      }
    }

    public boolean ownerAlive() {
      Thread owner = ownerThread.get();
      return owner != null && owner.isAlive();
    }
  }

  private static final class OwnerReference extends WeakReference<Thread> {
    private Lease lease;

    private OwnerReference(
        Thread owner, ReferenceQueue<? super Thread> collectedOwners, Lease lease) {
      super(owner, collectedOwners);
      this.lease = lease;
    }

    private Lease lease() {
      return lease;
    }

    private void clearLease() {
      lease = null;
    }
  }
}
