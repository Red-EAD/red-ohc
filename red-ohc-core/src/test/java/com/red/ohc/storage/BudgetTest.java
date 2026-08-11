package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

public class BudgetTest {
  @Test
  public void failedReservationReturnsCreditToTheCallingThread() {
    Budget budget = new Budget(64L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.tryReserve(lease, 32L));
    budget.refund(lease, 32L);

    assertTrue(budget.tryReserve(lease, 32L));
    assertTrue(budget.tryReserve(lease, 32L));
  }

  @Test
  public void leasesAreIndependentPerWriterThread() throws Exception {
    Budget budget = new Budget(1 << 20);
    Budget.Lease first = budget.leaseForCurrentThread();
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    java.util.concurrent.Future<Budget.Lease> future =
        executor.submit(budget::leaseForCurrentThread);
    try {
      assertTrue(first != future.get(1L, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      future.cancel(true);
      executor.shutdownNow();
    }
  }

  @Test(timeOut = 2_000L)
  public void collectedOwnerLeaseIsRemovedAndCreditIsReturnedOnce() throws Exception {
    Budget budget = new Budget(1 << 20);
    AtomicReference<Budget.Lease> observed = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              Budget.Lease lease = budget.leaseForCurrentThread();
              assertTrue(budget.tryReserve(lease, 64L));
              observed.set(lease);
            },
            "short-lived-budget-owner");
    owner.start();
    owner.join();

    Budget.Lease lease = observed.get();
    Field ownerField = Budget.Lease.class.getDeclaredField("ownerThread");
    ownerField.setAccessible(true);
    WeakReference<?> ownerReference = (WeakReference<?>) ownerField.get(lease);
    ownerReference.clear();
    assertTrue(
        ownerReference.enqueue(),
        "the owner reference must be attached to a ReferenceQueue for event-driven cleanup");

    assertEquals(reclaimCollectedLeases(budget, 16), 1);
    assertEquals(leaseCount(budget), 0);
    assertEquals(budget.reserved(), 64L, "only the admitted allocation remains resident");

    assertEquals(reclaimCollectedLeases(budget, 16), 0);
    assertEquals(budget.reserved(), 64L, "cleanup must not return the same credit twice");
    budget.release(64L);
    assertEquals(budget.reserved(), 0L);
  }

  @Test
  public void collectedCleanupLimitCountsAlreadyRemovedQueueRecords() throws Exception {
    Budget budget = new Budget(1 << 20);
    Budget.Lease first = budget.leaseForCurrentThread();
    Budget.Lease second = budget.leaseForCurrentThread();
    Set<?> leases = leases(budget);
    assertTrue(leases.remove(first));
    assertTrue(leases.remove(second));
    WeakReference<?> firstOwner = ownerReference(first);
    WeakReference<?> secondOwner = ownerReference(second);
    firstOwner.clear();
    secondOwner.clear();
    assertTrue(firstOwner.enqueue());
    assertTrue(secondOwner.enqueue());

    assertEquals(reclaimCollectedLeases(budget, 1), 0);

    Field queueField = Budget.class.getDeclaredField("collectedOwners");
    queueField.setAccessible(true);
    ReferenceQueue<?> queue = (ReferenceQueue<?>) queueField.get(budget);
    assertTrue(queue.poll() != null, "one stale queue record must remain after a one-record pass");
  }

  @Test
  public void reclaimedResidentWeightRestoresAdmissionWithoutLeavingBatchCreditDebt() {
    Budget budget = new Budget(64L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.tryReserve(lease, 32L));
    budget.refund(lease, 32L);

    assertTrue(
        budget.tryReserve(lease, 64L),
        "the first allocation was fully refunded, so the full budget is admissible again");
  }

  @Test
  public void partialLeaseCreditIsUsedBeforeTouchingTheGlobalBalance() {
    Budget budget = new Budget(128L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.tryReserve(lease, 1L), "the first allocation retains a local refill remainder");
    assertTrue(
        budget.tryReserve(lease, 121L),
        "global free bytes plus the caller's partial lease credit still fit below capacity");
  }

  @Test
  public void leaseBatchesSmallReservationsAndRefundsIntoTheCallingLease() {
    Budget budget = new Budget(1 << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.tryReserve(lease, 8L));
    long afterFirst = lease.credit();
    assertTrue(afterFirst > 0L, "the first small reservation should retain a local lease");
    assertTrue(budget.tryReserve(lease, 8L));
    assertTrue(lease.credit() < afterFirst);

    budget.refund(lease, 8L);
    assertEquals(lease.credit(), afterFirst, "failed/prematurely freed bytes return locally");
    budget.refund(lease, 8L);
    budget.returnUnusedCredits();
    assertEquals(budget.reserved(), 0L);
  }

  @Test
  public void leaseRefillIsBoundedByCapacityWhenCapacityIsSmallerThanTheRefillChunk() {
    Budget budget = new Budget(32L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.tryReserve(lease, 16L));
    assertTrue(budget.tryReserve(lease, 16L));
    assertEquals(budget.reserved(), 32L);
    assertTrue(lease.credit() >= 0L);

    budget.release(32L);
    budget.returnUnusedCredits();
    assertEquals(budget.reserved(), 0L);
  }

  @Test
  public void activeLeaseIsNotReclaimedUntilTheWriterLeaves() {
    Budget budget = new Budget(1 << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(lease.tryActivate());
    assertTrue(budget.tryReserve(lease, 64L));
    long activeCredit = lease.credit();
    budget.reclaimIdleLeases();
    assertEquals(
        lease.credit(),
        activeCredit,
        "a writer-owned lease must not be reclaimed while the writer is active");

    lease.deactivate();
    budget.reclaimIdleLeases();
    assertEquals(
        lease.credit(), 0L, "an idle lease must return unused credit to the global budget");
    assertTrue(lease.tryActivate(), "a lease must be activatable after reclaim returns it to IDLE");
    lease.deactivate();
  }

  @Test(timeOut = 2_000L)
  public void activationReturnsBoundedlyWhileReclaimRemainsInProgress() throws Exception {
    Budget budget = new Budget(1 << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();
    AtomicInteger state = leaseState(lease);
    state.set(2); // Budget.Lease.RECLAIMING

    long started = System.nanoTime();
    assertFalse(lease.tryActivate());
    long elapsedNanos = System.nanoTime() - started;

    assertTrue(
        elapsedNanos < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(100L),
        "persistent reclaim must not make writer admission wait");
    assertEquals(state.get(), 2, "bounded failure must not claim a lease still being reclaimed");

    state.set(0); // Budget.Lease.IDLE
    assertTrue(lease.tryActivate(), "the lease must become usable after reclaim completes");
    lease.deactivate();
    assertEquals(state.get(), 0, "deactivation must return the lease to IDLE");
  }

  @Test(timeOut = 10_000L)
  public void globalReservationDoesNotRejectOnlyBecauseOfCasContention() throws Exception {
    int threads = 64;
    long bytes = 64L << 10;
    Budget budget = new Budget(threads * bytes);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger failures = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>(threads);
    try {
      for (int index = 0; index < threads; index++) {
        futures.add(
            executor.submit(
                () -> {
                  Budget.Lease lease = budget.leaseForCurrentThread();
                  ready.countDown();
                  start.await();
                  for (int attempt = 0; attempt < 1_000; attempt++) {
                    if (!budget.tryReserve(lease, bytes)) {
                      failures.incrementAndGet();
                    } else {
                      budget.release(bytes);
                    }
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(2L, java.util.concurrent.TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
      assertEquals(failures.get(), 0, "CAS contention must not become an admission failure");
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private static AtomicInteger leaseState(Budget.Lease lease) throws Exception {
    Field stateField = lease.getClass().getDeclaredField("state");
    stateField.setAccessible(true);
    return (AtomicInteger) stateField.get(lease);
  }

  private static int reclaimCollectedLeases(Budget budget, int limit) throws Exception {
    Method method = Budget.class.getDeclaredMethod("reclaimCollectedLeases", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(budget, limit);
  }

  private static int leaseCount(Budget budget) throws Exception {
    return budget.leaseCount();
  }

  @SuppressWarnings("unchecked")
  private static Set<Budget.Lease> leases(Budget budget) throws Exception {
    Field leasesField = Budget.class.getDeclaredField("leases");
    leasesField.setAccessible(true);
    return (Set<Budget.Lease>) leasesField.get(budget);
  }

  private static WeakReference<?> ownerReference(Budget.Lease lease) throws Exception {
    Field ownerField = Budget.Lease.class.getDeclaredField("ownerThread");
    ownerField.setAccessible(true);
    return (WeakReference<?>) ownerField.get(lease);
  }

}
