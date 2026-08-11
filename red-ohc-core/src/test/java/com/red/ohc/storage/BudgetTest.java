package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

public class BudgetTest {
  @Test
  public void rejectsInvalidReservationSizes() {
    Budget budget = new Budget(64L, 1);

    assertFalse(budget.tryReserve(0L));
    assertFalse(budget.tryReserve(-1L));
    assertFalse(budget.tryReserve(65L));
    assertEquals(budget.reserved(), 0L);
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
                  boolean reserved = budget.tryReserve(64L);
                  if (reserved) {
                    accepted.incrementAndGet();
                  }
                  attempted.countDown();
                  release.await();
                  if (reserved) {
                    budget.refund(64L);
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
                  assertTrue(budget.tryReserve(128L));
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

    assertTrue(budget.tryReserve(1L));
    assertEquals(budget.reserved(), 1L);
    budget.reclaimIdleCredits();
    assertEquals(budget.reserved(), 1L);
    assertEquals(budget.availableBalance(), budget.capacity() - 1L);

    budget.refund(1L);
    assertEquals(budget.reserved(), 0L);
    budget.reclaimIdleCredits();
    assertEquals(budget.availableBalance(), budget.capacity());
  }

  @Test
  public void clearReturnsAllFixedStripeCredit() {
    Budget budget = new Budget(1_024L, 4);

    assertTrue(budget.tryReserve(64L));
    budget.clear();

    assertEquals(budget.reserved(), 0L);
    assertEquals(budget.availableBalance(), budget.capacity());
  }
}
