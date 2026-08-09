package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class BudgetTest {
  @Test
  public void failedReservationReturnsCreditToTheCallingThread() {
    Budget budget = new Budget(64L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.reserve(lease, 32L));
    budget.refund(lease, 32L);

    assertTrue(budget.reserve(lease, 32L));
    assertTrue(budget.reserve(lease, 32L));
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
  public void exitedWriterLeaseIsReturnedByMaintenance() throws Exception {
    Budget budget = new Budget(1 << 20);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    java.util.concurrent.Future<Budget.Lease> future =
        executor.submit(
            () -> {
              Budget.Lease lease = budget.leaseForCurrentThread();
              assertTrue(budget.reserve(lease, 64L));
              return lease;
            });
    Budget.Lease lease = future.get(1L, java.util.concurrent.TimeUnit.SECONDS);
    executor.shutdown();
    assertTrue(executor.awaitTermination(1L, java.util.concurrent.TimeUnit.SECONDS));
    budget.reclaimDeadLeases();
    assertEquals(lease.credit(), 0L);
    assertEquals(budget.reserved(), 64L, "the admitted native allocation is still resident");
    budget.release(64L);
    assertEquals(budget.reserved(), 0L);
  }

  @Test
  public void reclaimedResidentWeightRestoresAdmissionWithoutLeavingBatchCreditDebt() {
    Budget budget = new Budget(64L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.reserve(lease, 32L));
    budget.refund(lease, 32L);

    assertTrue(
        budget.reserve(lease, 64L),
        "the first allocation was fully refunded, so the full budget is admissible again");
  }

  @Test
  public void partialLeaseCreditIsUsedBeforeTouchingTheGlobalBalance() {
    Budget budget = new Budget(128L);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.reserve(lease, 1L), "the first allocation retains a local refill remainder");
    assertTrue(
        budget.reserve(lease, 121L),
        "global free bytes plus the caller's partial lease credit still fit below capacity");
  }

  @Test
  public void leaseBatchesSmallReservationsAndRefundsIntoTheCallingLease() {
    Budget budget = new Budget(1 << 20);
    Budget.Lease lease = budget.leaseForCurrentThread();

    assertTrue(budget.reserve(lease, 8L));
    long afterFirst = lease.credit();
    assertTrue(afterFirst > 0L, "the first small reservation should retain a local lease");
    assertTrue(budget.reserve(lease, 8L));
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

    assertTrue(budget.reserve(lease, 16L));
    assertTrue(budget.reserve(lease, 16L));
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

    lease.activate();
    assertTrue(budget.reserve(lease, 64L));
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
  }
}
