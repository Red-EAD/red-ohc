package com.red.ohc.runtime;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public final class FingerprintScratchPoolTest {
  @Test
  public void largeLeasesRespectTheFourMiBPoolBudgetAndReuseAfterRelease() {
    FingerprintScratchPool pool = new FingerprintScratchPool();

    FingerprintScratch first = pool.acquire(3 * 1024 * 1024);
    assertNotNull(first);
    assertNull(pool.acquire(3 * 1024 * 1024));

    pool.release(first);
    FingerprintScratch reused = pool.acquire(3 * 1024 * 1024);
    assertNotNull(reused);
    pool.release(reused);
  }

  @Test
  public void releasingSmallClassAllowsReclaimForALargerClass() {
    FingerprintScratchPool pool = new FingerprintScratchPool();

    FingerprintScratch small = pool.acquire(128 * 1024);
    assertNotNull(small);
    pool.release(small);

    FingerprintScratch large = pool.acquire(3 * 1024 * 1024);
    assertNotNull(large);
    pool.release(large);
  }

  @Test
  public void threadContextReturnsPooledScratchAtScopeExit() {
    FingerprintScratchPool pool = new FingerprintScratchPool();
    ThreadContext context = new ThreadContext(null, pool);

    assertNotNull(context.enterFingerprintScratch(128 * 1024));
    context.exitFingerprintScratch();

    FingerprintScratch reused = pool.acquire(128 * 1024);
    assertNotNull(reused);
    pool.release(reused);
  }

  @Test
  public void clearDiscardsIdleBuffersWithoutBreakingFutureAcquisition() {
    FingerprintScratchPool pool = new FingerprintScratchPool();
    FingerprintScratch scratch = pool.acquire(3 * 1024 * 1024);
    assertNotNull(scratch);
    pool.release(scratch);

    pool.clear();

    FingerprintScratch reacquired = pool.acquire(3 * 1024 * 1024);
    assertNotNull(reacquired);
    assertNotSame(reacquired, scratch);
    pool.release(reacquired);
  }
}
