package com.red.ohc.storage;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
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

  private final ConcurrentLinkedQueue<Lease> leases = new ConcurrentLinkedQueue<>();

  public Budget(long capacity) {
    if (capacity <= 0L) {
      throw new IllegalArgumentException("capacity must be positive");
    }
    this.capacity = capacity;
    this.available = new AtomicLong(capacity);
  }

  /** Creates the cache-local lease used by a writer thread for small allocations. */
  public Lease leaseForCurrentThread() {
    Lease lease = new Lease(this, Thread.currentThread());
    leases.add(lease);
    return lease;
  }

  public boolean reserve(Lease lease, long bytes) {
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
    long request = Math.max(needed, Math.min(MAX_REFILL_BYTES, capacity));
    while (true) {
      long free = available.get();
      if (free < needed) {
        reclaimDeadLeases();
        free = available.get();
        if (free < needed) {
          return false;
        }
      }
      long grant = Math.min(free, request);
      if (!available.compareAndSet(free, free - grant)) {
        continue;
      }
      lease.credit = credit + grant - bytes;
      return true;
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

  /** Returns unused writer leases during shutdown; no writer may be active then. */
  public void returnUnusedCredits() {
    for (Lease lease : leases) {
      if (lease.state.compareAndSet(Lease.IDLE, Lease.RECLAIMING)) {
        returnCredit(lease);
        lease.state.set(Lease.IDLE);
      }
    }
  }

  /** Reclaims leases whose owning thread has exited; called by the cache worker. */
  public void reclaimDeadLeases() {
    reclaimIdleLeases();
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
    private final WeakReference<Thread> ownerThread;
    private static final int IDLE = 0;
    private static final int ACTIVE = 1;
    private static final int RECLAIMING = 2;
    private final AtomicInteger state = new AtomicInteger(IDLE);
    private long credit;

    private Lease(Budget owner, Thread ownerThread) {
      this.owner = owner;
      this.ownerThread = new WeakReference<>(ownerThread);
    }

    public long credit() {
      return credit;
    }

    public void activate() {
      while (true) {
        int current = state.get();
        if (current == RECLAIMING) {
          // The worker owns this short transition only while it transfers the unused
          // credit back to the global balance. A writer racing that transition must
          // retry, otherwise an ordinary back-to-back put is reported as a failure.
          Thread.onSpinWait();
          continue;
        }
        if (current == ACTIVE) {
          throw new IllegalStateException("budget lease is already active");
        }
        if (state.compareAndSet(IDLE, ACTIVE)) {
          return;
        }
      }
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
}
