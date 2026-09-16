package com.red.ohc.maintenance;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.CacheMath;

/**
 * Logical occupancy ledger shared by synchronous writers and the single maintenance actor.
 *
 * <p>Writers only add their signed deltas. The actor refreshes a stable-enough snapshot before
 * making capacity decisions; exact reconciliation is deliberately kept on the cold flush/close
 * path instead of putting a global CAS in every write.
 */
public final class LogicalAdmission {
  private final long target;
  private final boolean countBounded;
  private final LongAdder logicalCharge = new LongAdder();
  private final LongAdder logicalMappingCount = new LongAdder();
  /** External reductions invalidate the actor's running charge; actor removals do not set this. */
  private final AtomicBoolean externalChargeReduced = new AtomicBoolean();
  /** Actor-local running charge for one bounded capacity pass. Only the maintenance actor writes it. */
  private long actorCharge;
  private volatile boolean actorOverTarget;

  public LogicalAdmission(long target, boolean countBounded) {
    if (target <= 0L) {
      throw new IllegalArgumentException("target must be positive");
    }
    this.target = target;
    this.countBounded = countBounded;
  }

  public long logicalCharge() {
    return logicalCharge.sum();
  }

  public long logicalMappingCount() {
    return logicalMappingCount.sum();
  }

  /** Returns the configured steady-state eviction target. */
  public long target() {
    return target;
  }

  /** Returns the last actor-published capacity decision. */
  public boolean isOverTarget() {
    return actorOverTarget;
  }

  /** Writer-side hint only; it does not participate in admission or capacity correctness. */
  public boolean needsCapacityWake() {
    // Over-target is exactly when the actor most needs the wake: it may be parked in the
    // capacity-retry backoff window after a locked victim, and a writer's unblock (or mere
    // continued pressure) should break that wait immediately. The suppression used to keep
    // writers silent during the deepest divergence window.
    if (actorOverTarget) {
      return true;
    }
    return logicalCharge.sum() > target;
  }

  /** Refreshes the actor-owned decision snapshot immediately before a capacity pass. */
  public void refreshActorSnapshot() {
    long charge = logicalCharge.sum();
    actorCharge = charge;
    actorOverTarget = charge > target;
  }

  /** Starts a capacity pass with one exact ledger sample. Actor-thread only. */
  long actorBeginCapacityPass() {
    // Clear before sampling. A reduction published after the exchange remains visible to the
    // next victim check; reductions consumed by the exchange happen-before the ledger sample.
    externalChargeReduced.getAndSet(false);
    long charge = logicalCharge.sum();
    actorCharge = charge;
    actorOverTarget = charge > target;
    return charge;
  }

  /** Applies a successful actor-owned removal to the running capacity sample. */
  void actorSubtractReleasedCharge(long charge) {
    if (charge < 0L) {
      throw new IllegalArgumentException("charge must be non-negative");
    }
    if (charge != 0L) {
      actorCharge = Math.max(0L, actorCharge - charge);
    }
    actorOverTarget = actorCharge > target;
  }

  /** Reconciles the actor running sample with the shared ledger. Actor-thread only. */
  void actorReconcileCapacity() {
    actorBeginCapacityPass();
  }

  /** Rechecks occupancy after a writer removal, shrink, expiry or reservation rollback. */
  void actorReconcileExternalReductions() {
    if (externalChargeReduced.get()) {
      actorReconcileCapacity();
    }
  }

  private void addExternalCharge(long delta) {
    if (delta != 0L) {
      logicalCharge.add(delta);
      if (delta < 0L) {
        externalChargeReduced.set(true);
      }
    }
  }

  /** Applies a signed logical charge delta without imposing the configured eviction target. */
  public boolean tryChargeDelta(long delta) {
    if (delta == Long.MIN_VALUE) {
      throw new IllegalArgumentException("invalid logical charge delta");
    }
    addExternalCharge(delta);
    return true;
  }

  /** Releases a reservation only for a candidate that never became a mapping. */
  public void releaseUnpublishedCharge(long charge) {
    if (charge < 0L) {
      throw new IllegalArgumentException("charge must be non-negative");
    }
    addExternalCharge(-charge);
  }

  /** Rolls back a signed reservation before its mapping was published. */
  public void rollbackUnpublishedDelta(long delta) {
    if (delta == Long.MIN_VALUE) {
      throw new IllegalArgumentException("invalid logical charge delta");
    }
    addExternalCharge(-delta);
  }

  /** Marks a mapping present after its charge was reserved by the caller. */
  public boolean markPresentAfterCharge(Entry entry) {
    if (entry == null || !entry.markLogicallyPresent()) {
      return false;
    }
    logicalMappingCount.increment();
    return true;
  }

  /** Charges and marks an existing logically-absent mapping present. */
  public void markPresent(Entry entry) {
    if (entry == null || !entry.isLogicallyAbsent()) {
      return;
    }
    long charge = chargeOf(entry);
    logicalCharge.add(charge);
    try {
      markPresentAfterCharge(entry);
    } catch (Throwable failure) {
      addExternalCharge(-charge);
      throw failure;
    }
  }

  /** Removes a mapping from the logical ledger after its CHM unlink is durable. */
  public void markAbsent(Entry entry) {
    if (entry == null || !entry.markLogicallyAbsent()) {
      return;
    }
    // Removal callers clear the value pointer only after this transition; the published native
    // allocation is therefore still the authoritative byte-bounded charge at this point.
    long charge = chargeOf(entry);
    logicalMappingCount.decrement();
    addExternalCharge(-charge);
  }

  /** Marks a physically unlinked entry absent while the maintenance actor owns its writer claim. */
  long markAbsentByActor(Entry entry) {
    if (entry == null || !entry.markLogicallyAbsentAfterWriterClaim()) {
      return 0L;
    }
    long charge = chargeOf(entry);
    logicalMappingCount.decrement();
    logicalCharge.add(-charge);
    return charge;
  }

  /** Completes a replacement after the new value pointer has been published. */
  public void completeReplacement(Entry entry) {
    if (entry != null && entry.markLogicallyPresent()) {
      logicalMappingCount.increment();
    }
  }

  public boolean isCountBounded() {
    return countBounded;
  }

  /** Cold consistency check used after actor barriers and during shutdown. */
  public void assertStable(ConcurrentHashMap<Entry, Entry> mappings) {
    if (!assertStableIfQuiescent(mappings, () -> true)) {
      throw new IllegalStateException("logical admission stability check was not sampled");
    }
  }

  /**
   * Performs the same check only when the caller still owns a quiescent boundary.
   *
   * <p>A concurrent writer makes the sample inconclusive, not corrupt. Returning {@code false}
   * leaves the retry decision to the caller instead of turning normal concurrency into a terminal
   * cache failure.
   */
  public boolean assertStableIfQuiescent(
      ConcurrentHashMap<Entry, Entry> mappings, BooleanSupplier quiescent) {
    if (mappings == null) {
      throw new NullPointerException("mappings");
    }
    if (quiescent == null) {
      throw new NullPointerException("quiescent");
    }
    // LongAdder deliberately has no single linearization point. A writer can therefore finish its
    // native publication between the first ledger sample and the CHM scan even when the actor is
    // already returning to idle. Take one complete diagnostic probe; a mismatch is handed back to
    // the caller, which owns the next actor turn and can sample again after the writer settles.
    if (!quiescent.getAsBoolean()) {
      return false;
    }
    long charge = logicalCharge.sum();
    long count = logicalMappingCount.sum();
    if (charge < 0L || count < 0L) {
      throw new IllegalStateException(
          "logical admission became negative: charge=" + charge + ", count=" + count);
    }
    long actualCharge = 0L;
    long actualCount = 0L;
    for (Entry entry : mappings.keySet()) {
      if (!entry.isAlive()
          || entry.isLogicallyAbsent()
          || Entry.rawValueAddress(entry.valueAddress) == 0L) {
        continue;
      }
      actualCount++;
      actualCharge += chargeOf(entry);
    }
    if (!quiescent.getAsBoolean()) {
      return false;
    }
    long endingCharge = logicalCharge.sum();
    long endingCount = logicalMappingCount.sum();
    if (charge == endingCharge
        && count == endingCount
        && actualCount == count
        && actualCharge == charge) {
      refreshActorSnapshot();
      return true;
    }
    // A diagnostic mismatch is not evidence of corruption. The writer may have remained active
    // during this probe; let the caller keep the actor runnable and sample again next time.
    return false;
  }

  /** Derives the mapping charge from immutable key size and the published native value allocation. */
  long chargeOf(Entry entry) {
    return countBounded
        ? 1L
        : CacheMath.logicalEntryBytes(entry.keyAllocationLength(), entry.currentValueAllocation());
  }
}
