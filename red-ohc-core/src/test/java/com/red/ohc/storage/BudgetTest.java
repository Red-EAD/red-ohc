package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.jctools.queues.MpscArrayQueue;
import org.testng.annotations.Test;

public class BudgetTest {
  @Test
  public void boundedCasBudgetUsesTheCachedLogicalCpuCount() throws Exception {
    Field field = Budget.class.getDeclaredField("MAX_RESERVE_RETRIES");
    field.setAccessible(true);
    assertEquals(field.getInt(null), NativeMemory.LOGICAL_CPU_COUNT);
  }

  @Test
  public void rejectsInvalidReservationSizes() {
    Budget budget = new Budget(64L, 1);

    assertFalse(budget.tryReserve(0L, 0));
    assertFalse(budget.tryReserve(-1L, 0));
    assertFalse(budget.tryReserve(65L, 0));
    assertEquals(budget.reserved(), 0L);
  }

  @Test(timeOut = 2_000L)
  public void reserveReturnsFalseWhenRefillStripeRemainsContended() throws Exception {
    Budget budget = new Budget(1_024L, 1);
    AtomicIntegerArray stripeRefills = stripeRefills(budget);
    stripeRefills.set(0, 1);
    AtomicInteger result = new AtomicInteger(1);
    CountDownLatch done = new CountDownLatch(1);
    Thread admission =
        new Thread(
            () -> {
              result.set(budget.tryReserve(64L, 0) ? 1 : 0);
              done.countDown();
            },
            "budget-admission-contention-test");
    admission.setDaemon(true);
    admission.start();
    try {
      assertTrue(
          done.await(500L, TimeUnit.MILLISECONDS),
          "non-blocking admission must not wait for a stalled refiller");
      assertEquals(result.get(), 0);
    } finally {
      stripeRefills.set(0, 0);
      admission.join(500L);
    }
  }

  @Test(timeOut = 10_000L)
  public void sameStripeCompetitionNeverReservesMoreThanCapacity() throws Exception {
    Budget budget = new Budget(1_024L, 1);
    int threads = 32;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch attempted = new CountDownLatch(threads);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger accepted = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>(threads);
    try {
      for (int index = 0; index < threads; index++) {
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  boolean reserved = budget.tryReserve(64L, 0);
                  if (reserved) {
                    accepted.incrementAndGet();
                  }
                  attempted.countDown();
                  release.await();
                  if (reserved) {
                    budget.refund(64L, 0);
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(2L, TimeUnit.SECONDS));
      start.countDown();
      assertTrue(attempted.await(2L, TimeUnit.SECONDS));
      assertTrue(accepted.get() <= 16, "fixed stripe must not oversell the budget");
      release.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
    assertEquals(budget.reserved(), 0L);
  }

  @Test(timeOut = 10_000L)
  public void crossStripeCreditsPreserveTheGlobalConservationLaw() throws Exception {
    Budget budget = new Budget(1 << 20, 4);
    int stripeCount = budget.stripeCount();
    assertEquals(stripeCount, 4);
    int threads = 4;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>(threads);
    Set<Integer> usedStripes = ConcurrentHashMap.newKeySet();
    try {
      for (int index = 0; index < threads; index++) {
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  usedStripes.add(budget.stripeIndex(Thread.currentThread().getId()));
                  assertTrue(budget.tryReserve(128L, budget.stripeIndex(Thread.currentThread().getId())));
                  return null;
                }));
      }
      assertTrue(ready.await(2L, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    assertTrue(usedStripes.size() >= 2, "writers must exercise more than one fixed stripe");
    long free = budget.availableBalance();
    for (int stripe = 0; stripe < stripeCount; stripe++) {
      free += budget.stripeCredit(stripe);
    }
    assertEquals(free + budget.reserved(), budget.capacity());
  }

  @Test
  public void pressureReturnsIdleStripeCreditAndRefundReturnsTheReservation() {
    Budget budget = new Budget(1_024L, 4);

    assertTrue(budget.tryReserve(1L, 0));
    assertEquals(budget.reserved(), 1L);
    assertEquals(budget.reclaimIdleCredits(), 1_023L);
    assertEquals(budget.reserved(), 1L);
    assertEquals(budget.availableBalance(), budget.capacity() - 1L);

    budget.refund(1L, 0);
    assertEquals(budget.reserved(), 0L);
    assertEquals(budget.reclaimIdleCredits(), 1L);
    assertEquals(budget.availableBalance(), budget.capacity());
  }

  @Test
  public void pressureWithoutIdleCreditDoesNotRequestAFullStripeScan() {
    Budget budget = new Budget(1_024L, 4);

    assertFalse(budget.hasIdleCreditHint());
    assertEquals(budget.reclaimIdleCredits(), 0L);
    assertFalse(budget.hasIdleCreditHint());
  }

  @Test
  public void explicitStripeAdmissionPreservesTheSameReservation() {
    Budget budget = new Budget(1_024L, 4);

    assertTrue(budget.tryReserve(128L, 2));
    assertEquals(budget.stripeCredit(2), 896L);
    budget.refund(128L, 2);
    assertEquals(budget.reserved(), 0L);
    assertEquals(budget.reclaimIdleCredits(), 1_024L);
  }

  @Test
  public void clearReturnsAllFixedStripeCredit() {
    Budget budget = new Budget(1_024L, 4);

    assertTrue(budget.tryReserve(64L, 0));
    budget.clear();

    assertEquals(budget.reserved(), 0L);
    assertEquals(budget.availableBalance(), budget.capacity());
  }

  @Test
  public void refundPublishesOneDirtyStripeTokenUntilTheActorConsumesIt() throws Exception {
    Budget budget = new Budget(1_024L, 4);
    Queue<?> dirtyStripes = dirtyStripeQueue(budget);

    assertTrue(budget.tryReserve(1_024L, 2));
    budget.release(912L);
    budget.refund(64L, 2);
    budget.refund(32L, 2);
    budget.refund(16L, 1);

    assertTrue(
        dirtyStripes instanceof MpscArrayQueue,
        "bounded dirty stripe tokens must use reusable array storage");
    assertEquals(dirtyStripes.size(), 2, "one stripe must publish at most one dirty token");
    assertEquals(budget.reclaimIdleCredits(), 112L);
    assertEquals(dirtyStripes.size(), 0);
    assertEquals(budget.availableBalance(), budget.capacity());
  }

  @SuppressWarnings("unchecked")
  private static Queue<?> dirtyStripeQueue(Budget budget) throws Exception {
    Field field = Budget.class.getDeclaredField("dirtyStripeQueue");
    field.setAccessible(true);
    return (Queue<?>) field.get(budget);
  }

  private static AtomicIntegerArray stripeRefills(Budget budget) throws Exception {
    Field field = Budget.class.getDeclaredField("stripeRefills");
    field.setAccessible(true);
    return (AtomicIntegerArray) field.get(budget);
  }
}
