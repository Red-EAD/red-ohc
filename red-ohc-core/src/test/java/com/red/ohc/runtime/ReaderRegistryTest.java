package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.annotations.Test;

public final class ReaderRegistryTest {
  @Test
  public void registrationPublishesAnActorScanArrayBeforeTheReaderCanEnter() {
    ReaderRegistry registry = new ReaderRegistry();
    ReaderSlot slot = new ReaderSlot();

    registry.register(slot);
    registry.register(slot);

    assertEquals(registry.registeredCount(), 1);
    assertSame(registry.references().iterator().next().get(), slot);
  }

  @Test
  public void collectedReaderIsRemovedFromItsQueuedIdentityReference() throws Exception {
    ReaderRegistry registry = new ReaderRegistry();
    ReaderSlot slot = new ReaderSlot();
    registry.register(slot);
    WeakReference<ReaderSlot> reference = registry.references().iterator().next();

    reference.clear();
    assertTrue(
        reference.enqueue(),
        "registered readers must attach their weak identity key to a ReferenceQueue");

    assertEquals(cleanupCollected(registry, 1), 1);
    assertEquals(registry.registeredCount(), 0);
    assertEquals(cleanupCollected(registry, 1), 0, "queued readers must be removed only once");
  }

  @Test
  public void minimumActiveEpochIsSharedByEveryRetirementStripe() {
    ReaderRegistry registry = new ReaderRegistry();
    ReaderSlot older = new ReaderSlot();
    ReaderSlot newer = new ReaderSlot();
    registry.register(older);
    registry.register(newer);

    older.epoch = 7L;
    newer.epoch = 9L;
    assertEquals(registry.minActiveEpoch(), 7L);

    older.epoch = 0L;
    assertEquals(registry.minActiveEpoch(), 9L);

    newer.epoch = 0L;
    assertEquals(registry.minActiveEpoch(), Long.MAX_VALUE);
  }

  @Test(timeOut = 10_000L)
  public void registrationAndCollectionNeverPublishANullRegistryReference() throws Exception {
    ReaderRegistry registry = new ReaderRegistry();
    ExecutorService executor = Executors.newFixedThreadPool(5);
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int worker = 0; worker < 4; worker++) {
      tasks.add(
          () -> {
            for (int index = 0; index < 2_000; index++) {
              registry.register(new ReaderSlot());
            }
            return null;
          });
    }
    tasks.add(
        () -> {
          for (int scan = 0; scan < 100; scan++) {
            System.gc();
            registry.cleanupCollected(64);
            for (WeakReference<ReaderSlot> reference : registry.references()) {
              assertNotNull(reference);
            }
          }
          return null;
        });
    try {
      List<Future<Void>> results = executor.invokeAll(tasks);
      for (Future<Void> result : results) {
        result.get();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static int cleanupCollected(ReaderRegistry registry, int limit) throws Exception {
    Method method = ReaderRegistry.class.getDeclaredMethod("cleanupCollected", int.class);
    method.setAccessible(true);
    return (Integer) method.invoke(registry, limit);
  }
}
