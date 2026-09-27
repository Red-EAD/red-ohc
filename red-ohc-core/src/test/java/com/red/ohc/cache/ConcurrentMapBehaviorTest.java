package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

public final class ConcurrentMapBehaviorTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(StandardCharsets.UTF_8).length;
        }
      };

  @Test
  public void standardMutationMethodsFollowMapSemantics() {
    try (OHCache<String, String> cache = newCache()) {
      cache.put("one", "1");
      assertEquals(cache.get("one"), "1");
      cache.put("one", "uno");
      assertEquals(cache.get("one"), "uno");
      assertEquals(cache.putIfAbsent("one", "ignored"), "uno");
      assertEquals(cache.putIfAbsent("two", "2"), null);

      assertTrue(cache.replace("one", "uno", "1a"));
      assertFalse(cache.replace("one", "uno", "stale"));
      assertEquals(cache.replace("one", "1"), "1a");
      assertEquals(cache.replace("missing", "x"), null);

      assertTrue(cache.remove("one", "1"));
      assertFalse(cache.remove("one", "1"));
      cache.remove("two");
      assertFalse(cache.containsKey("two"));
      cache.remove("missing");
      assertFalse(cache.containsKey("missing"));

      assertEquals(cache.putIfAbsent("fast", "v"), null);
      assertEquals(cache.putIfAbsent("fast", "new"), "v");
      cache.put("fast", "new");
      assertEquals(cache.get("fast"), "new");
      cache.remove("fast");
      assertFalse(cache.containsKey("fast"));
      cache.remove("fast");
      assertFalse(cache.containsKey("fast"));

      Map<String, String> batch = new HashMap<>();
      batch.put("batch-one", "one");
      batch.put("batch-two", "two");
      cache.putAll(batch);
      cache.replaceAll((key, value) -> value.toUpperCase());
      assertEquals(cache.get("batch-one"), "ONE");
      assertEquals(cache.getOrDefault("missing", null), null);
      cache.clear();
      assertTrue(cache.isEmpty());
    }
  }

  @Test
  public void ordinaryNewPutProbesOnceThenPublishesWithOnePutIfAbsent() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      CountingPutMap countingMap = new CountingPutMap();
      replaceDataMaps(cache, countingMap);

      cache.put("new-key", "value");

      assertEquals(countingMap.lookupCalls.get(), 1);
      assertEquals(countingMap.putIfAbsentCalls.get(), 1);
      assertEquals(cache.get("new-key"), "value");
    }
  }

  @Test
  public void ordinaryExistingPutReplacesThroughTheProbeWithoutInsertion() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      CountingPutMap countingMap = new CountingPutMap();
      replaceDataMaps(cache, countingMap);

      cache.put("key", "old");
      cache.put("key", "new");

      assertEquals(countingMap.lookupCalls.get(), 2, "one probe per put");
      assertEquals(countingMap.putIfAbsentCalls.get(), 1, "only the first put inserts");
      assertEquals(cache.get("key"), "new");
      assertEquals(cache.size(), 1);
    }
  }

  @Test
  public void ordinaryPutRetriesARemovedProbeWinnerWithoutUsingItsNativeKey() throws Exception {
    assertRemovedProbeWinnerIsRetried(false);
  }

  @Test
  public void ordinaryPutRevalidatesProbeIdentityAfterConcurrentReinsertion() throws Exception {
    assertRemovedProbeWinnerIsRetried(true);
  }

  @Test
  public void ordinaryPutRetiresAStalePutIfAbsentWinnerWithoutUsingItsNativeKey() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      PausedInsertCollisionMap map = new PausedInsertCollisionMap();
      replaceDataMaps(cache, map);

      // Keep the retired winner's blocks allocated so a regression reads garbage-but-mapped
      // memory instead of freed native memory.
      ThreadContext pinned = context(cache);
      ReaderGuard guard = new ReaderGuard(worker(cache));
      assertTrue(guard.enter(pinned));
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        // The key is absent, so the put thread's probe misses (and pauses); the main thread
        // inserts a live mapping in that window, the put thread's insert then collides with
        // it in putIfAbsent (second pause), and the main thread retires that winner before
        // the pause lifts — the insert path must observe the stale winner, drop it without
        // touching its native key, and retry to a fresh mapping.
        Future<?> put = executor.submit(() -> cache.put("key", "latest"));
        assertTrue(map.probeObserved.await(5L, TimeUnit.SECONDS));
        cache.put("key", "interleaved");
        map.resumeProbe.countDown();
        assertTrue(map.collisionObserved.await(5L, TimeUnit.SECONDS));
        cache.remove("key");
        assertFalse(map.collisionWinner.isAlive());
        map.resumeCollision.countDown();
        put.get(5L, TimeUnit.SECONDS);

        assertEquals(cache.get("key"), "latest");
        assertEquals(cache.size(), 1);
        assertEquals(
            map.retiredKeyUses.get(),
            0,
            "a retired putIfAbsent winner must not be reused as a native CHM key");
      } finally {
        map.resumeProbe.countDown();
        map.resumeCollision.countDown();
        executor.shutdown();
        try {
          assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        } finally {
          guard.exit(pinned);
        }
      }
      cache.flushAsync().get(5L, TimeUnit.SECONDS);
      assertTrue(cache.stats().retirementActorReclaimedRecords() >= 2L);
    }
  }

  private void assertRemovedProbeWinnerIsRetried(boolean reinsert) throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      PausedProbeMap map = new PausedProbeMap();
      replaceDataMaps(cache, map);
      cache.put("key", "old");
      cache.flushAsync().get(5L, TimeUnit.SECONDS);

      // Keep the retired blocks allocated while observing an invalid key use, so a regression
      // fails an assertion instead of letting the test JVM read freed native memory.
      ThreadContext pinned = context(cache);
      ReaderGuard guard = new ReaderGuard(worker(cache));
      assertTrue(guard.enter(pinned));
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> put = executor.submit(() -> cache.put("key", "latest"));
        assertTrue(map.probeObserved.await(5L, TimeUnit.SECONDS));
        // The put thread is parked inside its probe with the live winner in hand; retire that
        // winner underneath it and (optionally) let a fresh mapping reappear.
        cache.remove("key");
        assertFalse(map.probedWinner.isAlive());
        if (reinsert) {
          cache.put("key", "interleaved");
        }
        map.resumeProbe.countDown();
        put.get(5L, TimeUnit.SECONDS);

        assertEquals(cache.get("key"), "latest");
        assertEquals(cache.size(), 1);
        assertEquals(
            map.retiredKeyUses.get(),
            0,
            "a removed probe winner must not be reused as a native CHM key after its guard exits");
      } finally {
        map.resumeProbe.countDown();
        executor.shutdown();
        try {
          assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        } finally {
          guard.exit(pinned);
        }
      }
      cache.flushAsync().get(5L, TimeUnit.SECONDS);
      assertTrue(cache.stats().retirementActorReclaimedRecords() >= 2L);
    }
  }

  @Test
  public void clearRetriesATemporaryWriterClaim() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      cache.put("key", "value");
      cache.flushAsync().join();

      ClaimedMutationResult<Boolean> result =
          runWithTemporaryWriterClaim(
              cache,
              () -> {
                cache.clear();
                return cache.isEmpty();
              });

      assertFalse(result.completedWhileClaimed);
      assertTrue(result.value);
      assertTrue(cache.isEmpty());
    }
  }

  @Test
  public void conditionalRemoveRetriesATemporaryWriterClaim() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      cache.put("key", "value");
      cache.flushAsync().join();

      ClaimedMutationResult<Boolean> result =
          runWithTemporaryWriterClaim(cache, () -> cache.remove("key", "value"));

      assertFalse(result.completedWhileClaimed);
      assertTrue(result.value);
      assertFalse(cache.containsKey("key"));
    }
  }

  @Test
  public void conditionalReplaceRetriesATemporaryWriterClaim() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      cache.put("key", "old");
      cache.flushAsync().join();

      ClaimedMutationResult<Boolean> result =
          runWithTemporaryWriterClaim(cache, () -> cache.replace("key", "old", "new"));

      assertFalse(result.completedWhileClaimed);
      assertTrue(result.value);
      assertEquals(cache.get("key"), "new");
    }
  }

  @Test
  public void unconditionalReplaceRetriesATemporaryWriterClaim() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      cache.put("key", "old");
      cache.flushAsync().join();

      ClaimedMutationResult<String> result =
          runWithTemporaryWriterClaim(cache, () -> cache.replace("key", "new"));

      assertFalse(result.completedWhileClaimed);
      assertEquals(result.value, "old");
      assertEquals(cache.get("key"), "new");
    }
  }

  @Test
  public void computeMethodsAreAtomicAndPreserveMappingsOnFailure() {
    try (OHCache<String, String> cache = newCache()) {
      AtomicInteger absentCalls = new AtomicInteger();
      assertEquals(
          cache.computeIfAbsent(
              "key", ignored -> "v" + absentCalls.incrementAndGet()),
          "v1");
      assertEquals(
          cache.computeIfAbsent(
              "key", ignored -> "v" + absentCalls.incrementAndGet()),
          "v1");
      assertEquals(absentCalls.get(), 1);

      assertEquals(cache.computeIfPresent("key", (key, value) -> value + "-present"), "v1-present");
      assertEquals(cache.compute("key", (key, value) -> value + "-compute"), "v1-present-compute");
      assertEquals(cache.merge("key", "-merge", (left, right) -> left + right), "v1-present-compute-merge");
      assertEquals(cache.merge("new", "created", (left, right) -> left + right), "created");

      assertEquals(cache.compute("key", (key, value) -> null), null);
      assertFalse(cache.containsKey("key"));
      assertEquals(cache.computeIfPresent("key", (key, value) -> "not-called"), null);

      cache.put("stable", "old");
      expectThrows(
          IllegalStateException.class,
          () ->
              cache.compute(
                  "stable",
                  (key, value) -> {
                    throw new IllegalStateException("boom");
                  }));
      assertEquals(cache.get("stable"), "old");

      cache.put("merge", "left");
      assertEquals(cache.merge("merge", "right", (left, right) -> null), null);
      assertFalse(cache.containsKey("merge"));

      assertEquals(cache.computeIfAbsent("null", ignored -> null), null);
      assertFalse(cache.containsKey("null"));
    }
  }

  @Test(timeOut = 5_000L)
  public void computeMutationSeedKeepsOuterHashAfterNestedRead() {
    long entryWeight =
        Entry.keyAllocationLengthForKeyLength("outer".length())
            + ValueBlock.allocationLength("value".length());
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(entryWeight)
            .eviction(Eviction.LRU)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      assertEquals(
          cache.computeIfAbsent(
              "outer",
              ignored -> {
                assertEquals(cache.get("nested"), null);
                return "value";
              }),
          "value");
      cache.flushAsync().join();

      cache.put("newer", "value");
      cache.flushAsync().join();

      assertEquals(cache.size(), 1L);
      assertEquals(cache.get("outer"), null);
      assertEquals(cache.get("newer"), "value");
    }
  }

  @Test
  public void computeRemappingProtectsNativeValueBeforeTtlAndPayloadReads() throws Exception {
    ReaderStateCheckingSerializer serializer = new ReaderStateCheckingSerializer();
    ReaderStateCheckingTicker ticker = new ReaderStateCheckingTicker();
    try (OffHeapCache<String, String> cache = newCache(ticker, 1_000L, 30_000L, serializer)) {
      cache.put("key", "value");
      ThreadContext context = context(cache);
      ReaderRegistry readers = readers(cache);
      ticker.arm(readers, context);
      serializer.arm(readers, context);

      assertEquals(cache.computeIfPresent("key", (key, value) -> value + "-updated"), "value-updated");
      assertTrue(ticker.checkedValueProtection, "TTL header access must be value-protected");
      assertTrue(serializer.checkedValueProtection, "payload access must be value-protected");
    }
  }

  @Test
  public void computeInsertionFailureBeforeChmPublicationDoesNotLeakLogicalCount()
      throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      ThrowAfterNonNullRemapMap failingMap = new ThrowAfterNonNullRemapMap();
      Field data = OffHeapCache.class.getDeclaredField("data");
      long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
      NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);

      expectThrows(
          OutOfMemoryError.class,
          () -> cache.computeIfAbsent("oom", ignored -> "computed"));

      assertTrue(failingMap.isEmpty());
      assertEquals(cache.size(), 0, "an unpublished compute probe must not change logical size");
    }
  }

  @Test
  public void computeInsertionFailureAfterChmPublicationLeavesTheProbeForTerminalCleanup()
      throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      ThrowAfterComputePublicationMap failingMap = new ThrowAfterComputePublicationMap();
      Field data = OffHeapCache.class.getDeclaredField("data");
      long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
      NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);

      expectThrows(
          IllegalStateException.class,
          () -> cache.computeIfAbsent("published", ignored -> "computed"));

      assertFalse(failingMap.isEmpty());
      assertEquals(cache.size(), 0, "an unpublished probe must not change logical size");
      assertTrue(worker(cache).snapshot().unhealthy);
    }
  }

  @Test(timeOut = 10_000L)
  public void computeHintFailureAfterConcurrentRemovalKeepsLifecycleOwnership() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      MaintenanceEventLoop loop = worker(cache);
      ThreadContext pinned = context(cache);
      ReaderGuard guard = new ReaderGuard(loop);
      assertTrue(guard.enter(pinned));
      ExecutorService remover = Executors.newSingleThreadExecutor();
      AtomicBoolean failOnce = new AtomicBoolean(true);
      Field memoryField = OffHeapCache.class.getDeclaredField("memory");
      memoryField.setAccessible(true);
      NativeMemory.Memory memory = (NativeMemory.Memory) memoryField.get(cache);
      Method pageForHandle = NativeMemory.Memory.class.getDeclaredMethod("pageForHandle", long.class);
      pageForHandle.setAccessible(true);
      Field freedSlots =
          Class.forName("com.red.ohc.storage.WriterArena$PageSharedLine")
              .getDeclaredField("freedSlots");
      freedSlots.setAccessible(true);
      AtomicReference<Object> keyPage = new AtomicReference<>();
      AtomicLong initialFreedSlots = new AtomicLong();
      Field hook = MaintenanceEventLoop.class.getDeclaredField("lifecycleMessageOfferHookForTest");
      hook.setAccessible(true);
      hook.set(
          loop,
          (Runnable)
              () -> {
                if (!failOnce.compareAndSet(true, false)) {
                  return;
                }
                Entry inserted = cache.dataForTest().values().iterator().next();
                assertFalse(inserted.isWriterLocked());
                try {
                  Object page =
                      pageForHandle.invoke(
                          memory, NativeMemory.getLong(inserted.nativeKeyAddress - Long.BYTES));
                  keyPage.set(page);
                  initialFreedSlots.set(freedSlots.getLong(page));
                  remover.submit(() -> cache.remove("computed")).get(5L, TimeUnit.SECONDS);
                } catch (Exception failure) {
                  throw new AssertionError(failure);
                }
                assertFalse(inserted.isAlive());
                throw new IllegalStateException("injected mutation handoff failure after removal");
              });
      try {
        IllegalStateException failure =
            expectThrows(
                IllegalStateException.class,
                () -> cache.computeIfAbsent("computed", ignored -> "value"));
        assertEquals(failure.getMessage(), "injected mutation handoff failure after removal");
        assertTrue(cache.dataForTest().isEmpty());
        assertEquals(
            freedSlots.getLong(keyPage.get()),
            initialFreedSlots.get(),
            "the reader-pinned key belongs to retirement, not private compute cleanup");
        Field ledger = OffHeapCache.class.getDeclaredField("logicalAdmission");
        ledger.setAccessible(true);
        LogicalAdmission admission = (LogicalAdmission) ledger.get(cache);
        assertEquals(admission.logicalCharge(), 0L, "published charge must not be rolled back twice");
        assertEquals(admission.logicalMappingCount(), 0L);
      } finally {
        hook.set(loop, null);
        remover.shutdown();
        try {
          assertTrue(remover.awaitTermination(5L, TimeUnit.SECONDS));
        } finally {
          guard.exit(pinned);
        }
      }
      // Closing drains the durable removal: the compute cleanup must not already have freed its key.
    }
  }

  @Test
  public void putFailureAfterChmPublicationLeavesTheCandidateForTerminalCleanup() throws Exception {
    try (OffHeapCache<String, String> cache = newCache()) {
      ThrowAfterPutIfAbsentMap failingMap = new ThrowAfterPutIfAbsentMap();
      Field data = OffHeapCache.class.getDeclaredField("data");
      long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
      NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);

      expectThrows(IllegalStateException.class, () -> cache.put("published", "value"));

      assertFalse(failingMap.isEmpty());
      assertEquals(cache.size(), 0, "an unpublished candidate must not change logical size");
      assertTrue(worker(cache).snapshot().unhealthy);
    }
  }

  @Test
  public void actorFailureAfterVictimChmRemovalIsTerminalAndKeepsPublishedReplacement()
      throws Exception {
    int keyLength = "target".getBytes(StandardCharsets.UTF_8).length;
    int oldValueLength = "old".getBytes(StandardCharsets.UTF_8).length;
    long oldCharge =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyLength),
            ValueBlock.allocationLength(oldValueLength));
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(oldCharge * 2L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      cache.put("victim", "old");
      cache.put("target", "old");
      cache.flushAsync().join();
      ThrowAfterVictimRemoveMap failingMap = new ThrowAfterVictimRemoveMap();
      failingMap.putAll(cache.dataForTest());
      Field data = OffHeapCache.class.getDeclaredField("data");
      long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
      NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);
      Field worker = OffHeapCache.class.getDeclaredField("worker");
      worker.setAccessible(true);
      Object eventLoop = worker.get(cache);
      Field workerData =
          com.red.ohc.maintenance.MaintenanceEventLoop.class.getDeclaredField("data");
      long workerDataOffset = NativeMemory.unsafe().objectFieldOffset(workerData);
      NativeMemory.unsafe().putObjectVolatile(eventLoop, workerDataOffset, failingMap);

      cache.put("target", "x".repeat(64));

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
      // The injected map publishes removedEntry before throwing. Wait for the actor to finish
      // its exception reconciliation and publish terminal failure before checking logical size.
      while (!((MaintenanceEventLoop) eventLoop).snapshot().unhealthy
          && System.nanoTime() < deadline) {
        Thread.yield();
      }

      assertTrue(((MaintenanceEventLoop) eventLoop).snapshot().unhealthy);
      assertTrue(failingMap.removedEntry != null, "the injected victim failure must run");
      for (Entry entry : failingMap.values()) {
        if (entry != failingMap.removedEntry) {
          assertFalse(
              entry.isWriterLocked(),
              "a replacement failure after pointer publication must release the Entry writer claim");
        }
      }
      assertEquals(failingMap.size(), 1, "the published replacement must remain map-visible");
      assertEquals(
          cache.size(),
          1,
          "a victim removed before its CHM failure must still be reflected in logical accounting");
    } finally {
      cache.close();
    }
  }

  @Test
  public void defaultTtlAppliesToAllDefaultMutationOverloads() {
    MutableTicker ticker = new MutableTicker(1_000L);
    try (OHCache<String, String> cache = newCache(ticker, 1_000L)) {
      assertEquals(cache.putIfAbsent("if-absent", "value"), null);

      cache.put("replace", "old");
      assertTrue(cache.replace("replace", "old", "new"));

      cache.put("replace-all", "old");
      cache.replaceAll((key, value) -> "new");

      cache.put("entry-set", "old");
      Map.Entry<String, String> entry = null;
      for (Map.Entry<String, String> candidate : cache.entrySet()) {
        if (candidate.getKey().equals("entry-set")) {
          entry = candidate;
          break;
        }
      }
      assertTrue(entry != null);
      assertEquals(entry.setValue("new"), "old");

      assertTrue(cache.keySet("mapped").add("mapped"));

      ticker.setMillis(2_001L);
      assertFalse(cache.containsKey("if-absent"));
      assertFalse(cache.containsKey("replace"));
      assertFalse(cache.containsKey("replace-all"));
      assertFalse(cache.containsKey("entry-set"));
      assertFalse(cache.containsKey("mapped"));
    }
  }

  @Test
  public void closeFromComputeOrBulkCallbackIsRejectedWithoutChangingLifecycle() {
    try (OffHeapCache<String, String> cache = newCacheWithCloseTimeout(20L)) {
      expectThrows(
          IllegalStateException.class,
          () ->
              cache.compute(
                  "compute",
                  (key, value) -> {
                    cache.close();
                    return "unreachable";
                  }));
      cache.put("after-compute", "value");
      assertEquals(cache.get("after-compute"), "value");
    }

    try (OffHeapCache<String, String> cache = newCacheWithCloseTimeout(20L)) {
      cache.put("bulk", "value");
      expectThrows(
          IllegalStateException.class,
          () -> cache.forEach(0L, (key, value) -> cache.close()));
      cache.put("after-bulk", "value");
      assertEquals(cache.get("after-bulk"), "value");
    }

    try (OffHeapCache<String, String> cache = newCacheWithCloseTimeout(20L)) {
      for (int index = 0; index < 64; index++) {
        cache.put("parallel-" + index, "value");
      }
      AtomicBoolean callbackSeen = new AtomicBoolean();
      expectThrows(
          IllegalStateException.class,
          () ->
              cache.forEach(
                  1L,
                  (key, value) -> {
                    callbackSeen.set(true);
                    cache.close();
                  }));
      assertTrue(callbackSeen.get());
      cache.put("after-parallel-bulk", "value");
      assertEquals(cache.get("after-parallel-bulk"), "value");
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentMergeDoesNotLoseUpdates() throws Exception {
    try (OHCache<String, String> cache = newCache()) {
      int workers = 4;
      int updatesPerWorker = 100;
      ExecutorService executor = Executors.newFixedThreadPool(workers);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (int worker = 0; worker < workers; worker++) {
          futures.add(
              executor.submit(
                  () -> {
                    for (int update = 0; update < updatesPerWorker; update++) {
                      cache.merge("counter", "1", (left, right) -> Integer.toString(Integer.parseInt(left) + 1));
                    }
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }
        assertEquals(cache.get("counter"), Integer.toString(workers * updatesPerWorker));
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void mapViewsAreLiveAndEntrySnapshotsAreConditionallyMutable() {
    try (OHCache<String, String> cache = newCache()) {
      cache.put("a", "1");
      cache.put("b", "2");

      Set<String> keys = cache.keySet();
      assertTrue(keys.contains("a"));
      assertEquals(keys.size(), 2);
      Iterator<String> keyIterator = keys.iterator();
      while (keyIterator.hasNext()) {
        if ("a".equals(keyIterator.next())) {
          keyIterator.remove();
        }
      }
      assertFalse(cache.containsKey("a"));

      Set<String> mappedKeys = cache.keySet("mapped");
      assertTrue(mappedKeys.add("c"));
      assertFalse(mappedKeys.add("c"));
      assertEquals(cache.get("c"), "mapped");
      assertTrue(cache.values().remove("mapped"));
      assertFalse(cache.containsKey("c"));

      assertTrue(cache.entrySet().contains(new AbstractMap.SimpleEntry<>("b", "2")));
      Map.Entry<String, String> snapshot = cache.entrySet().iterator().next();
      String snapshotKey = snapshot.getKey();
      String oldValue = snapshot.getValue();
      assertEquals(snapshot.setValue("updated"), oldValue);
      assertEquals(cache.get(snapshotKey), "updated");

      Map.Entry<String, String> stale = cache.entrySet().iterator().next();
      cache.put(stale.getKey(), "concurrent");
      expectThrows(ConcurrentModificationException.class, () -> stale.setValue("rejected"));
      assertTrue(cache.entrySet().remove(new AbstractMap.SimpleEntry<>(snapshotKey, "concurrent")));
      assertFalse(cache.containsKey(snapshotKey));
    }
  }

  @Test
  public void mapAndChmBulkMethodsExposeHeapSnapshots() {
    try (OHCache<String, String> cache = newCache()) {
      cache.put("1", "10");
      cache.put("2", "20");
      cache.put("3", "30");
      cache.flushAsync().join();

      Set<String> visited = ConcurrentHashMap.newKeySet();
      cache.forEach(0L, (key, value) -> visited.add(key + value));
      assertEquals(visited, new HashSet<>(Arrays.asList("110", "220", "330")));

      List<String> transformed = new CopyOnWriteArrayList<>();
      cache.forEach(
          Long.MAX_VALUE,
          (key, value) -> key + "=" + value,
          transformed::add);
      assertEquals(new HashSet<>(transformed), new HashSet<>(Arrays.asList("1=10", "2=20", "3=30")));

      String found = cache.search(1L, (key, value) -> "2".equals(key) ? value : null);
      assertEquals(found, "20");
      assertEquals(cache.reduceToInt(0L, (key, value) -> Integer.parseInt(value), 7, Integer::sum), 67);
      assertEquals(cache.reduceToLong(Long.MAX_VALUE, (key, value) -> Long.parseLong(value), 7L, Long::sum), 67L);
      assertEquals(cache.reduceToDouble(0L, (key, value) -> Double.parseDouble(value), 7d, Double::sum), 67d, 0d);

      Set<String> keyValues = ConcurrentHashMap.newKeySet();
      cache.forEachKey(0L, keyValues::add);
      assertEquals(keyValues, new HashSet<>(Arrays.asList("1", "2", "3")));
      assertEquals(cache.searchKeys(Long.MAX_VALUE, key -> "3".equals(key) ? key : null), "3");
      assertEquals(cache.reduceKeysToInt(1L, Integer::parseInt, 7, Integer::sum), 13);

      Set<String> values = ConcurrentHashMap.newKeySet();
      cache.forEachValue(0L, values::add);
      assertEquals(values, new HashSet<>(Arrays.asList("10", "20", "30")));
      assertEquals(cache.searchValues(Long.MAX_VALUE, value -> "20".equals(value) ? value : null), "20");
      assertEquals(cache.reduceValues(1L, (left, right) -> left + "," + right).split(",").length, 3);

      AtomicInteger entryCount = new AtomicInteger();
      cache.forEachEntry(0L, entry -> {
        assertTrue(entry.getKey() instanceof String);
        assertTrue(entry.getValue() instanceof String);
        entryCount.incrementAndGet();
      });
      assertEquals(entryCount.get(), 3);
      assertEquals(
          cache.searchEntries(Long.MAX_VALUE, entry -> "1".equals(entry.getKey()) ? entry.getValue() : null),
          "10");
      assertEquals(cache.reduceEntriesToInt(0L, entry -> Integer.parseInt(entry.getValue()), 7, Integer::sum), 67);
    }
  }

  @Test(timeOut = 5_000L)
  public void bulkCallbackCanRemoveThenFlush() throws Exception {
    try (OHCache<String, String> cache = newCache()) {
      cache.put("remove-me", "value");
      cache.flushAsync().join();
      AtomicInteger callbacks = new AtomicInteger();
      cache.forEach(
          0L,
          (key, value) -> {
            cache.remove(key);
            assertFalse(cache.containsKey(key));
            try {
              cache.flushAsync().get(1L, TimeUnit.SECONDS);
            } catch (Exception failure) {
              throw new RuntimeException(failure);
            }
            callbacks.incrementAndGet();
          });
      assertEquals(callbacks.get(), 1);
      assertTrue(cache.isEmpty());
    }
  }

  @Test
  public void iteratorRemoveUsesTheReturnedKeyAfterAConcurrentReplace() {
    try (OHCache<String, String> cache = newCache()) {
      cache.put("key", "old");
      Iterator<String> iterator = cache.keySet().iterator();
      assertEquals(iterator.next(), "key");
      cache.put("key", "new");
      iterator.remove();
      assertFalse(cache.containsKey("key"));
    }
  }

  @Test
  public void expiredEntriesRemainInPhysicalCountButDisappearFromLogicalOperations() {
    MutableTicker ticker = new MutableTicker(1_000L);
    try (OHCache<String, String> cache = newCache(ticker)) {
      cache.put("expired", "value", 2_000L);
      ticker.setMillis(3_000L);

      Map<String, String> liveMap = new HashMap<>();
      liveMap.put("expired", "value");
      assertFalse(cache.equals(liveMap));
      assertFalse(cache.containsKey("expired"));
      assertEquals(cache.size(), 0);
      assertTrue(cache.isEmpty());
      assertEquals(cache.keySet().size(), 0);
      assertEquals(cache.values().size(), 0);
      assertEquals(cache.entrySet().size(), 0);
      assertFalse(cache.keySet().contains("expired"));
      assertFalse(cache.containsValue("value"));
      assertFalse(cache.entrySet().iterator().hasNext());
      cache.remove("expired");
      assertFalse(cache.containsKey("expired"));
      AtomicInteger callbacks = new AtomicInteger();
      cache.forEach(0L, (key, value) -> callbacks.incrementAndGet());
      assertEquals(callbacks.get(), 0);

      assertFalse(cache.replace("expired", "value", "new"));
      assertFalse(cache.remove("expired", "value"));
    }
  }

  @Test
  public void expiryReaderDoesNotChangeLogicalCountWhileAWriterOwnsTheEntry() {
    MutableTicker ticker = new MutableTicker(1_000L);
    try (OffHeapCache<String, String> cache = newCache(ticker)) {
      cache.put("key", "old", 2_000L);
      Entry entry = cache.dataForTest().values().iterator().next();
      ticker.setMillis(3_000L);

      assertTrue(entry.claimWriter());
      try {
        assertFalse(cache.containsKey("key"));
        assertEquals(
            cache.size(),
            1,
            "a reader that cannot establish version ownership must not change logical size");
      } finally {
        entry.finishWriter();
      }

      assertFalse(cache.containsKey("key"));
      assertEquals(cache.size(), 0, "a later uncontended expiry read must publish absence");
    }
  }

  @Test
  public void expiredRemovePublishesLogicalAbsenceWhileHoldingTheWriterClaim() {
    MutableTicker ticker = new MutableTicker(1_000L);
    try (OHCache<String, String> cache = newCache(ticker)) {
      cache.put("key", "value", 2_000L);
      ticker.setMillis(3_000L);

      cache.remove("key");
      assertEquals(cache.size(), 0);
      assertTrue(cache.isEmpty());
    }
  }

  @Test
  public void mapViewPropagatesNoSuchElementExceptionFromUserDeserialization() {
    ThrowingDeserializeSerializer serializer = new ThrowingDeserializeSerializer();
    try (OHCache<String, String> cache = newCache(serializer)) {
      cache.put("key", "value");
      serializer.failDeserialization = true;

      expectThrows(NoSuchElementException.class, () -> cache.entrySet().iterator().hasNext());
    }
  }

  @Test
  public void mapIdentityMethodsAndCloseViewsFollowMapContract() {
    try (OffHeapCache<String, String> cache = newCache();
        OffHeapCache<String, String> equalCache = newCache()) {
      cache.put("one", "1");
      cache.put("two", "2");
      equalCache.put("one", "1");
      equalCache.put("two", "2");
      Map<String, String> expected = new HashMap<>();
      expected.put("one", "1");
      expected.put("two", "2");
      assertTrue(cache.equals(equalCache));
      assertTrue(equalCache.equals(cache));
      assertFalse(cache.equals(expected));
      assertFalse(expected.equals(cache));
      assertEquals(cache.hashCode(), equalCache.hashCode());
      assertTrue(cache.toString().contains("one=1"));
    }

    OffHeapCache<String, String> closed = newCache();
    closed.close();
    assertTrue(closed.keySet().isEmpty());
    assertTrue(closed.values().isEmpty());
    assertTrue(closed.entrySet().isEmpty());
    assertEquals(closed.size(), 0);
    expectThrows(IllegalStateException.class, () -> closed.put("closed", "value"));
  }

  private static OffHeapCache<String, String> newCache() {
    return newCache(Ticker.DEFAULT);
  }

  private static MaintenanceEventLoop worker(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("worker");
    field.setAccessible(true);
    return (MaintenanceEventLoop) field.get(cache);
  }

  private static void replaceDataMaps(
      OffHeapCache<?, ?> cache,
      ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry>
          replacement)
      throws Exception {
    Field data = OffHeapCache.class.getDeclaredField("data");
    long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
    NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, replacement);

    MaintenanceEventLoop worker = worker(cache);
    Field workerData = MaintenanceEventLoop.class.getDeclaredField("data");
    long workerDataOffset = NativeMemory.unsafe().objectFieldOffset(workerData);
    NativeMemory.unsafe().putObjectVolatile(worker, workerDataOffset, replacement);
  }

  @SuppressWarnings("unchecked")
  private static ThreadContext context(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("contexts");
    field.setAccessible(true);
    return ((ThreadLocal<ThreadContext>) field.get(cache)).get();
  }

  private static ReaderRegistry readers(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("readers");
    field.setAccessible(true);
    return (ReaderRegistry) field.get(cache);
  }

  private static OffHeapCache<String, String> newCache(Ticker ticker) {
    return newCache(ticker, 0L, 30_000L);
  }

  private static OffHeapCache<String, String> newCache(Ticker ticker, long defaultTtlMillis) {
    return newCache(ticker, defaultTtlMillis, 30_000L);
  }

  private static OffHeapCache<String, String> newCacheWithCloseTimeout(long timeoutMillis) {
    return newCache(Ticker.DEFAULT, 0L, timeoutMillis);
  }

  private static OffHeapCache<String, String> newCache(
      Ticker ticker, long defaultTtlMillis, long closeTimeoutMillis) {
    return newCache(ticker, defaultTtlMillis, closeTimeoutMillis, STRING);
  }

  private static OffHeapCache<String, String> newCache(CacheSerializer<String> serializer) {
    return newCache(Ticker.DEFAULT, 0L, 30_000L, serializer);
  }

  private static OffHeapCache<String, String> newCache(
      Ticker ticker,
      long defaultTtlMillis,
      long closeTimeoutMillis,
      CacheSerializer<String> serializer) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .defaultTTLmillis(defaultTtlMillis)
        .closeTimeoutMillis(closeTimeoutMillis)
        .ticker(ticker)
        .keySerializer(serializer)
        .valueSerializer(serializer)
        .buildTyped();
  }

  private static <T> ClaimedMutationResult<T> runWithTemporaryWriterClaim(
      OffHeapCache<String, String> cache, Callable<T> mutation) throws Exception {
    Entry entry = cache.dataForTest().values().iterator().next();
    assertTrue(entry.claimWriter());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    Future<T> future =
        executor.submit(
            () -> {
              started.countDown();
              return mutation.call();
            });
    boolean writerHeld = true;
    try {
      assertTrue(started.await(2L, TimeUnit.SECONDS));
      T value = null;
      boolean completedWhileClaimed;
      try {
        value = future.get(500L, TimeUnit.MILLISECONDS);
        completedWhileClaimed = true;
      } catch (TimeoutException expected) {
        completedWhileClaimed = false;
      } finally {
        entry.finishWriter();
        writerHeld = false;
      }
      if (!completedWhileClaimed) {
        value = future.get(2L, TimeUnit.SECONDS);
      }
      return new ClaimedMutationResult<>(value, completedWhileClaimed);
    } finally {
      if (writerHeld) {
        entry.finishWriter();
      }
      executor.shutdownNow();
    }
  }

  private static final class ClaimedMutationResult<T> {
    private final T value;
    private final boolean completedWhileClaimed;

    private ClaimedMutationResult(T value, boolean completedWhileClaimed) {
      this.value = value;
      this.completedWhileClaimed = completedWhileClaimed;
    }
  }

  private static final class ThrowAfterNonNullRemapMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    @Override
    public com.red.ohc.index.Entry compute(
        com.red.ohc.index.Entry key,
        BiFunction<
                ? super com.red.ohc.index.Entry,
                ? super com.red.ohc.index.Entry,
                ? extends com.red.ohc.index.Entry>
            remappingFunction) {
      com.red.ohc.index.Entry result = remappingFunction.apply(key, null);
      if (result != null) {
        assertTrue(
            result.isWriterLocked(),
            "the insertion probe must remain claimed until CHM publication ownership is known");
        throw new OutOfMemoryError("injected after remapping and before CHM publication");
      }
      return null;
    }
  }

  private static final class ThrowAfterComputePublicationMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    @Override
    public com.red.ohc.index.Entry compute(
        com.red.ohc.index.Entry key,
        BiFunction<
                ? super com.red.ohc.index.Entry,
                ? super com.red.ohc.index.Entry,
                ? extends com.red.ohc.index.Entry>
            remappingFunction) {
      com.red.ohc.index.Entry result = super.compute(key, remappingFunction);
      if (result != null) {
        throw new IllegalStateException("injected after compute CHM publication");
      }
      return null;
    }
  }

  private static final class ThrowAfterPutIfAbsentMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    @Override
    public com.red.ohc.index.Entry putIfAbsent(
        com.red.ohc.index.Entry key,
        com.red.ohc.index.Entry value) {
      com.red.ohc.index.Entry result = super.putIfAbsent(key, value);
      throw new IllegalStateException("injected after putIfAbsent CHM publication");
    }
  }

  private static final class PausedInsertCollisionMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    private final CountDownLatch probeObserved = new CountDownLatch(1);
    private final CountDownLatch resumeProbe = new CountDownLatch(1);
    private final CountDownLatch collisionObserved = new CountDownLatch(1);
    private final CountDownLatch resumeCollision = new CountDownLatch(1);
    private final AtomicBoolean probeClaimed = new AtomicBoolean();
    private final AtomicBoolean collisionClaimed = new AtomicBoolean();
    private final AtomicInteger retiredKeyUses = new AtomicInteger();
    private volatile com.red.ohc.index.Entry collisionWinner;
    private volatile Thread collisionWriter;

    @Override
    public com.red.ohc.index.Entry get(Object key) {
      com.red.ohc.index.Entry result = super.get(key);
      if (probeClaimed.compareAndSet(false, true)) {
        probeObserved.countDown();
        try {
          assertTrue(resumeProbe.await(5L, TimeUnit.SECONDS));
        } catch (InterruptedException interruption) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interruption);
        }
      }
      return result;
    }

    @Override
    public com.red.ohc.index.Entry putIfAbsent(
        com.red.ohc.index.Entry key,
        com.red.ohc.index.Entry value) {
      com.red.ohc.index.Entry winner = super.putIfAbsent(key, value);
      if (winner != null && collisionClaimed.compareAndSet(false, true)) {
        collisionWinner = winner;
        collisionWriter = Thread.currentThread();
        collisionObserved.countDown();
        try {
          assertTrue(resumeCollision.await(5L, TimeUnit.SECONDS));
        } catch (InterruptedException interruption) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interruption);
        }
      }
      return winner;
    }

    @Override
    public com.red.ohc.index.Entry compute(
        com.red.ohc.index.Entry key,
        BiFunction<
                ? super com.red.ohc.index.Entry,
                ? super com.red.ohc.index.Entry,
                ? extends com.red.ohc.index.Entry>
            remappingFunction) {
      if (Thread.currentThread() == collisionWriter && key == collisionWinner) {
        retiredKeyUses.incrementAndGet();
      }
      return super.compute(key, remappingFunction);
    }
  }

  private static final class PausedProbeMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    private final CountDownLatch probeObserved = new CountDownLatch(1);
    private final CountDownLatch resumeProbe = new CountDownLatch(1);
    private final AtomicBoolean pauseClaimed = new AtomicBoolean();
    private final AtomicInteger retiredKeyUses = new AtomicInteger();
    private volatile com.red.ohc.index.Entry probedWinner;
    private volatile Thread probeWriter;

    @Override
    public com.red.ohc.index.Entry get(Object key) {
      com.red.ohc.index.Entry winner = super.get(key);
      if (winner != null && pauseClaimed.compareAndSet(false, true)) {
        probedWinner = winner;
        probeWriter = Thread.currentThread();
        probeObserved.countDown();
        try {
          assertTrue(resumeProbe.await(5L, TimeUnit.SECONDS));
        } catch (InterruptedException interruption) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interruption);
        }
      }
      return winner;
    }

    @Override
    public com.red.ohc.index.Entry compute(
        com.red.ohc.index.Entry key,
        BiFunction<
                ? super com.red.ohc.index.Entry,
                ? super com.red.ohc.index.Entry,
                ? extends com.red.ohc.index.Entry>
            remappingFunction) {
      if (Thread.currentThread() == probeWriter && key == probedWinner) {
        retiredKeyUses.incrementAndGet();
      }
      return super.compute(key, remappingFunction);
    }
  }

  private static final class CountingPutMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    private final AtomicInteger lookupCalls = new AtomicInteger();
    private final AtomicInteger putIfAbsentCalls = new AtomicInteger();

    @Override
    public com.red.ohc.index.Entry get(Object key) {
      lookupCalls.incrementAndGet();
      return super.get(key);
    }

    @Override
    public com.red.ohc.index.Entry putIfAbsent(
        com.red.ohc.index.Entry key,
        com.red.ohc.index.Entry value) {
      putIfAbsentCalls.incrementAndGet();
      return super.putIfAbsent(key, value);
    }
  }

  private static final class ThrowAfterVictimRemoveMap
      extends ConcurrentHashMap<
          com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    private final AtomicBoolean failAfterRemoval = new AtomicBoolean(true);
    private volatile com.red.ohc.index.Entry removedEntry;

    @Override
    public com.red.ohc.index.Entry remove(Object key) {
      com.red.ohc.index.Entry result = super.remove(key);
      if (result != null && failAfterRemoval.compareAndSet(true, false)) {
        removedEntry = result;
        throw new IllegalStateException("injected after victim CHM removal");
      }
      return result;
    }
  }

  private static final class ThrowingDeserializeSerializer
      implements CacheSerializer<String> {
    private volatile boolean failDeserialization;

    @Override
    public void serialize(String value, ByteBuffer buffer) {
      STRING.serialize(value, buffer);
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
      if (failDeserialization) {
        throw new NoSuchElementException("injected user deserialization failure");
      }
      return STRING.deserialize(buffer);
    }

    @Override
    public int serializedSize(String value) {
      return STRING.serializedSize(value);
    }
  }

  private static final class ReaderStateCheckingSerializer implements CacheSerializer<String> {
    private ReaderRegistry readers;
    private ThreadContext context;
    private Thread owner;
    private boolean armed;
    private boolean checkedValueProtection;

    private void arm(ReaderRegistry readers, ThreadContext context) {
      this.readers = readers;
      this.context = context;
      owner = Thread.currentThread();
      armed = true;
    }

    @Override
    public void serialize(String value, ByteBuffer buffer) {
      buffer.put(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
      checkValueProtection();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public int serializedSize(String value) {
      return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private void checkValueProtection() {
      if (!armed || Thread.currentThread() != owner) {
        return;
      }
      long state = readers.readerSequence(context.slot);
      if ((state & ReaderRegistry.VALUE_PROTECTION_BIT) == 0L) {
        throw new AssertionError("compute payload read escaped value protection");
      }
      checkedValueProtection = true;
      armed = false;
    }

  }

  private static final class ReaderStateCheckingTicker implements Ticker {
    private ReaderRegistry readers;
    private ThreadContext context;
    private Thread owner;
    private boolean armed;
    private boolean checkedValueProtection;

    private void arm(ReaderRegistry readers, ThreadContext context) {
      this.readers = readers;
      this.context = context;
      owner = Thread.currentThread();
      armed = true;
    }

    @Override
    public long nanos() {
      if (armed && Thread.currentThread() == owner) {
        long state = readers.readerSequence(context.slot);
        if ((state & ReaderRegistry.VALUE_PROTECTION_BIT) == 0L) {
          throw new AssertionError("compute TTL read escaped value protection");
        }
        checkedValueProtection = true;
        armed = false;
      }
      return System.nanoTime();
    }

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }

  }

  private static final class MutableTicker implements Ticker {
    private final AtomicLong millis;

    private MutableTicker(long initialMillis) {
      millis = new AtomicLong(initialMillis);
    }

    @Override
    public long nanos() {
      return millis.get() * 1_000_000L;
    }

    @Override
    public long currentTimeMillis() {
      return millis.get();
    }

    private void setMillis(long value) {
      millis.set(value);
    }
  }
}
