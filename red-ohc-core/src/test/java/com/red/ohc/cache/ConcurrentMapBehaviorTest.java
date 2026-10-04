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
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    {
      OHCache<String, String> cache = newCache();
      Throwable cacheFailure27 = null;
      try {
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure27 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure27);
      }
    }
  }

  @Test
  public void ordinaryNewPutProbesOnceThenPublishesWithOnePutIfAbsent() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure26 = null;
      try {
        CountingPutMap countingMap = new CountingPutMap();
        replaceDataMaps(cache, countingMap);

        cache.put("new-key", "value");

        assertEquals(countingMap.lookupCalls.get(), 1);
        assertEquals(countingMap.putIfAbsentCalls.get(), 1);
        assertEquals(cache.get("new-key"), "value");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure26 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure26);
      }
    }
  }

  @Test
  public void ordinaryExistingPutReplacesThroughTheProbeWithoutInsertion() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure25 = null;
      try {
        CountingPutMap countingMap = new CountingPutMap();
        replaceDataMaps(cache, countingMap);

        cache.put("key", "old");
        cache.put("key", "new");

        assertEquals(countingMap.lookupCalls.get(), 2, "one probe per put");
        assertEquals(countingMap.putIfAbsentCalls.get(), 1, "only the first put inserts");
        assertEquals(cache.get("key"), "new");
        assertEquals(cache.size(), 1);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure25 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure25);
      }
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
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = null;
      Throwable cacheFailure24 = null;
      try {
        PausedInsertCollisionMap map = new PausedInsertCollisionMap();
        replaceDataMaps(cache, map);

        // Keep the retired winner's blocks allocated so a regression reads garbage-but-mapped
        // memory instead of freed native memory.
        ThreadContext pinned = context(cache);
        ReaderGuard guard = new ReaderGuard(worker(cache));
        guard.enter(pinned);
        executor = Executors.newSingleThreadExecutor();
        Throwable callerFailure = null;
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
        } catch (Throwable error) {
          callerFailure = error;
          throw error;
        } finally {
          map.resumeProbe.countDown();
          map.resumeCollision.countDown();
          try {
            CacheTestSupport.awaitCallers(executor, callerFailure);
          } finally {
            guard.exit(pinned);
          }
        }
        awaitReclaimedRecords(cache, 2L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure24 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure24, executor);
      }
    }
  }

  private void assertRemovedProbeWinnerIsRetried(boolean reinsert) throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = null;
      Throwable cacheFailure23 = null;
      try {
        PausedProbeMap map = new PausedProbeMap();
        replaceDataMaps(cache, map);
        cache.put("key", "old");
        cache.flushAsync().get(5L, TimeUnit.SECONDS);

        // Keep the retired blocks allocated while observing an invalid key use, so a regression
        // fails an assertion instead of letting the test JVM read freed native memory.
        ThreadContext pinned = context(cache);
        ReaderGuard guard = new ReaderGuard(worker(cache));
        guard.enter(pinned);
        executor = Executors.newSingleThreadExecutor();
        Throwable callerFailure = null;
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
              "a removed probe winner must not be reused as a native CHM key after its guard"
                  + " exits");
        } catch (Throwable error) {
          callerFailure = error;
          throw error;
        } finally {
          map.resumeProbe.countDown();
          try {
            CacheTestSupport.awaitCallers(executor, callerFailure);
          } finally {
            guard.exit(pinned);
          }
        }
        awaitReclaimedRecords(cache, 2L);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure23 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure23, executor);
      }
    }
  }

  @Test
  public void clearRetriesATemporaryWriterClaim() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Throwable cacheFailure22 = null;
      try {
        cache.put("key", "value");
        cache.flushAsync().join();

        ClaimedMutationResult<Boolean> result =
            runWithTemporaryWriterClaim(
                cache,
                executor,
                () -> {
                  cache.clear();
                  return cache.isEmpty();
                });

        assertFalse(result.completedWhileClaimed);
        assertTrue(result.value);
        assertTrue(cache.isEmpty());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure22 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure22, executor);
      }
    }
  }

  @Test
  public void conditionalRemoveRetriesATemporaryWriterClaim() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Throwable cacheFailure21 = null;
      try {
        cache.put("key", "value");
        cache.flushAsync().join();

        ClaimedMutationResult<Boolean> result =
            runWithTemporaryWriterClaim(cache, executor, () -> cache.remove("key", "value"));

        assertFalse(result.completedWhileClaimed);
        assertTrue(result.value);
        assertFalse(cache.containsKey("key"));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure21 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure21, executor);
      }
    }
  }

  @Test
  public void conditionalReplaceRetriesATemporaryWriterClaim() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Throwable cacheFailure20 = null;
      try {
        cache.put("key", "old");
        cache.flushAsync().join();

        ClaimedMutationResult<Boolean> result =
            runWithTemporaryWriterClaim(cache, executor, () -> cache.replace("key", "old", "new"));

        assertFalse(result.completedWhileClaimed);
        assertTrue(result.value);
        assertEquals(cache.get("key"), "new");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure20 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure20, executor);
      }
    }
  }

  @Test
  public void unconditionalReplaceRetriesATemporaryWriterClaim() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Throwable cacheFailure19 = null;
      try {
        cache.put("key", "old");
        cache.flushAsync().join();

        ClaimedMutationResult<String> result =
            runWithTemporaryWriterClaim(cache, executor, () -> cache.replace("key", "new"));

        assertFalse(result.completedWhileClaimed);
        assertEquals(result.value, "old");
        assertEquals(cache.get("key"), "new");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure19 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure19, executor);
      }
    }
  }

  @Test
  public void computeMethodsAreAtomicAndPreserveMappingsOnFailure() {
    {
      OHCache<String, String> cache = newCache();
      Throwable cacheFailure18 = null;
      try {
        AtomicInteger absentCalls = new AtomicInteger();
        assertEquals(
            cache.computeIfAbsent("key", ignored -> "v" + absentCalls.incrementAndGet()), "v1");
        assertEquals(
            cache.computeIfAbsent("key", ignored -> "v" + absentCalls.incrementAndGet()), "v1");
        assertEquals(absentCalls.get(), 1);

        assertEquals(
            cache.computeIfPresent("key", (key, value) -> value + "-present"), "v1-present");
        assertEquals(
            cache.compute("key", (key, value) -> value + "-compute"), "v1-present-compute");
        assertEquals(
            cache.merge("key", "-merge", (left, right) -> left + right),
            "v1-present-compute-merge");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure18 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure18);
      }
    }
  }

  @Test(timeOut = 5_000L)
  public void computeMutationSeedKeepsOuterHashAfterNestedRead() {
    long entryWeight =
        Entry.keyAllocationLengthForKeyLength("outer".length())
            + ValueBlock.allocationLength("value".length());
    {
      OffHeapCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(entryWeight)
              .eviction(Eviction.LRU)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .buildTyped();
      Throwable cacheFailure17 = null;
      try {
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure17 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure17);
      }
    }
  }

  @Test
  public void computeRemappingProtectsNativeValueBeforeTtlAndPayloadReads() throws Exception {
    ReaderStateCheckingSerializer serializer = new ReaderStateCheckingSerializer();
    ReaderStateCheckingTicker ticker = new ReaderStateCheckingTicker();
    {
      OffHeapCache<String, String> cache = newCache(ticker, 1_000L, serializer);
      Throwable cacheFailure16 = null;
      try {
        cache.put("key", "value");
        ThreadContext context = context(cache);
        ReaderRegistry readers = readers(cache);
        ticker.arm(readers, context);
        serializer.arm(readers, context);

        assertEquals(
            cache.computeIfPresent("key", (key, value) -> value + "-updated"), "value-updated");
        assertTrue(ticker.checkedValueProtection, "TTL header access must be value-protected");
        assertTrue(serializer.checkedValueProtection, "payload access must be value-protected");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure16 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure16);
      }
    }
  }

  @Test
  public void computeInsertionFailureBeforeChmPublicationDoesNotLeakLogicalCount()
      throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure15 = null;
      try {
        ThrowAfterNonNullRemapMap failingMap = new ThrowAfterNonNullRemapMap();
        Field data = OffHeapCache.class.getDeclaredField("data");
        long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
        NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);

        expectThrows(
            OutOfMemoryError.class, () -> cache.computeIfAbsent("oom", ignored -> "computed"));

        assertTrue(failingMap.isEmpty());
        assertEquals(cache.size(), 0, "an unpublished compute probe must not change logical size");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure15 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure15);
      }
    }
  }

  @Test
  public void computeInsertionFailureAfterChmPublicationLeavesTheProbeForTerminalCleanup()
      throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure14 = null;
      try {
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure14 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure14);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void computeHintAfterConcurrentRemovalKeepsLifecycleOwnership() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure13 = null;
      try {
        ThreadContext pinned = context(cache);
        ReaderGuard guard = new ReaderGuard(worker(cache));
        guard.enter(pinned);
        Field memoryField = OffHeapCache.class.getDeclaredField("memory");
        memoryField.setAccessible(true);
        NativeMemory.Memory memory = (NativeMemory.Memory) memoryField.get(cache);
        Method pageForHandle =
            NativeMemory.Memory.class.getDeclaredMethod("pageForHandle", long.class);
        pageForHandle.setAccessible(true);
        Field freedSlots =
            Class.forName("com.red.ohc.storage.WriterArena$PageSharedLine")
                .getDeclaredField("freedSlots");
        freedSlots.setAccessible(true);
        try {
          cache.computeIfAbsent("computed", ignored -> "value");
          Entry inserted = cache.dataForTest().values().iterator().next();
          assertFalse(inserted.isWriterLocked());
          Object page =
              pageForHandle.invoke(
                  memory, NativeMemory.getLong(inserted.nativeKeyAddress - Long.BYTES));
          long initialFreed = freedSlots.getLong(page);
          assertFalse(cache.dataForTest().isEmpty());

          cache.remove("computed");
          assertFalse(inserted.isAlive());
          assertTrue(cache.dataForTest().isEmpty());
          assertEquals(
              freedSlots.getLong(page),
              initialFreed,
              "the reader-pinned key belongs to retirement, not private compute cleanup");
          Field ledger = OffHeapCache.class.getDeclaredField("logicalAdmission");
          ledger.setAccessible(true);
          LogicalAdmission admission = (LogicalAdmission) ledger.get(cache);
          assertEquals(
              admission.logicalCharge(), 0L, "published charge must not be rolled back twice");
          assertEquals(admission.logicalMappingCount(), 0L);
        } finally {
          guard.exit(pinned);
        }
        // Internal stop drains the durable removal; compute cleanup must not already free its key.

      } catch (Throwable cacheOperationFailure) {
        cacheFailure13 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure13);
      }
    }
  }

  @Test
  public void putFailureAfterChmPublicationLeavesTheCandidateForTerminalCleanup() throws Exception {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure12 = null;
      try {
        ThrowAfterPutIfAbsentMap failingMap = new ThrowAfterPutIfAbsentMap();
        Field data = OffHeapCache.class.getDeclaredField("data");
        long dataOffset = NativeMemory.unsafe().objectFieldOffset(data);
        NativeMemory.unsafe().putObjectVolatile(cache, dataOffset, failingMap);

        expectThrows(IllegalStateException.class, () -> cache.put("published", "value"));

        assertFalse(failingMap.isEmpty());
        assertEquals(cache.size(), 0, "an unpublished candidate must not change logical size");
        assertTrue(worker(cache).snapshot().unhealthy);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure12 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure12);
      }
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
    {
      Throwable explicitCacheFailure1 = null;
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
                "a replacement failure after pointer publication must release the Entry writer"
                    + " claim");
          }
        }
        assertEquals(failingMap.size(), 1, "the published replacement must remain map-visible");
        assertEquals(
            cache.size(),
            1,
            "a victim removed before its CHM failure must still be reflected in logical"
                + " accounting");

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure1 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        CacheTestSupport.stop(cache, explicitCacheFailure1);
      }
    }
  }

  @Test
  public void defaultTtlAppliesToAllDefaultMutationOverloads() {
    MutableTicker ticker = new MutableTicker(1_000L);
    {
      OHCache<String, String> cache = newCache(ticker, 1_000L);
      Throwable cacheFailure11 = null;
      try {
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

        ticker.setMillis(2_001L);
        assertFalse(cache.containsKey("if-absent"));
        assertFalse(cache.containsKey("replace"));
        assertFalse(cache.containsKey("replace-all"));
        assertFalse(cache.containsKey("entry-set"));
        assertFalse(cache.containsKey("mapped"));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure11 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure11);
      }
    }
  }

  @Test(timeOut = 10_000L)
  public void concurrentMergeDoesNotLoseUpdates() throws Exception {
    {
      OHCache<String, String> cache = newCache();
      int workers = 4;
      ExecutorService executor = Executors.newFixedThreadPool(workers);
      Throwable cacheFailure10 = null;
      try {
        int updatesPerWorker = 100;

        List<Future<?>> futures = new ArrayList<>();
        for (int worker = 0; worker < workers; worker++) {
          futures.add(
              executor.submit(
                  () -> {
                    for (int update = 0;
                        update < updatesPerWorker && !Thread.currentThread().isInterrupted();
                        update++) {
                      cache.merge(
                          "counter",
                          "1",
                          (left, right) -> Integer.toString(Integer.parseInt(left) + 1));
                    }
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }
        assertEquals(cache.get("counter"), Integer.toString(workers * updatesPerWorker));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure10 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure10, executor);
      }
    }
  }

  @Test
  public void mapViewsAreLiveAndEntrySnapshotsAreConditionallyMutable() {
    {
      OHCache<String, String> cache = newCache();
      Throwable cacheFailure9 = null;
      try {
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

        assertTrue(cache.entrySet().contains(new AbstractMap.SimpleEntry<>("b", "2")));
        Map.Entry<String, String> snapshot = cache.entrySet().iterator().next();
        String snapshotKey = snapshot.getKey();
        String oldValue = snapshot.getValue();
        assertEquals(snapshot.setValue("updated"), oldValue);
        assertEquals(cache.get(snapshotKey), "updated");

        Map.Entry<String, String> stale = cache.entrySet().iterator().next();
        cache.put(stale.getKey(), "concurrent");
        expectThrows(ConcurrentModificationException.class, () -> stale.setValue("rejected"));
        assertTrue(
            cache.entrySet().remove(new AbstractMap.SimpleEntry<>(snapshotKey, "concurrent")));
        assertFalse(cache.containsKey(snapshotKey));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure9 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure9);
      }
    }
  }

  @Test(timeOut = 5_000L)
  public void bulkCallbackCanRemoveThenFlush() throws Exception {
    {
      OHCache<String, String> cache = newCache();
      Throwable cacheFailure8 = null;
      try {
        cache.put("remove-me", "value");
        cache.flushAsync().join();
        AtomicInteger callbacks = new AtomicInteger();
        cache.forEach(
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure8 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure8);
      }
    }
  }

  @Test
  public void iteratorRemoveUsesTheReturnedKeyAfterAConcurrentReplace() {
    {
      OHCache<String, String> cache = newCache();
      Throwable cacheFailure7 = null;
      try {
        cache.put("key", "old");
        Iterator<String> iterator = cache.keySet().iterator();
        assertEquals(iterator.next(), "key");
        cache.put("key", "new");
        iterator.remove();
        assertFalse(cache.containsKey("key"));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure7 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure7);
      }
    }
  }

  @Test
  public void expiredEntriesRemainInPhysicalCountButDisappearFromLogicalOperations() {
    MutableTicker ticker = new MutableTicker(1_000L);
    {
      OHCache<String, String> cache = newCache(ticker);
      Throwable cacheFailure6 = null;
      try {
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
        cache.forEach((key, value) -> callbacks.incrementAndGet());
        assertEquals(callbacks.get(), 0);

        assertFalse(cache.replace("expired", "value", "new"));
        assertFalse(cache.remove("expired", "value"));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure6 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure6);
      }
    }
  }

  @Test
  public void expiryReaderDoesNotChangeLogicalCountWhileAWriterOwnsTheEntry() {
    MutableTicker ticker = new MutableTicker(1_000L);
    {
      OffHeapCache<String, String> cache = newCache(ticker);
      Throwable cacheFailure5 = null;
      try {
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure5 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure5);
      }
    }
  }

  @Test
  public void expiredRemovePublishesLogicalAbsenceWhileHoldingTheWriterClaim() {
    MutableTicker ticker = new MutableTicker(1_000L);
    {
      OHCache<String, String> cache = newCache(ticker);
      Throwable cacheFailure4 = null;
      try {
        cache.put("key", "value", 2_000L);
        ticker.setMillis(3_000L);

        cache.remove("key");
        assertEquals(cache.size(), 0);
        assertTrue(cache.isEmpty());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure4 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure4);
      }
    }
  }

  @Test
  public void mapViewPropagatesNoSuchElementExceptionFromUserDeserialization() {
    ThrowingDeserializeSerializer serializer = new ThrowingDeserializeSerializer();
    {
      OHCache<String, String> cache = newCache(serializer);
      Throwable cacheFailure3 = null;
      try {
        cache.put("key", "value");
        serializer.failDeserialization = true;

        expectThrows(NoSuchElementException.class, () -> cache.entrySet().iterator().hasNext());

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
    }
  }

  @Test
  public void mapIdentityMethodsFollowMapContract() {
    {
      OffHeapCache<String, String> cache = newCache();
      Throwable cacheFailure2 = null;
      try {
        {
          OffHeapCache<String, String> equalCache = newCache();
          Throwable cacheFailure1 = null;
          try {
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

          } catch (Throwable cacheOperationFailure) {
            cacheFailure1 = cacheOperationFailure;
            throw cacheOperationFailure;
          } finally {
            CacheTestSupport.stop(equalCache, cacheFailure1);
          }
        }
      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
    }
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
    return newCache(ticker, 0L);
  }

  private static OffHeapCache<String, String> newCache(Ticker ticker, long defaultTtlMillis) {
    return newCache(ticker, defaultTtlMillis, STRING);
  }

  private static OffHeapCache<String, String> newCache(CacheSerializer<String> serializer) {
    return newCache(Ticker.DEFAULT, 0L, serializer);
  }

  private static OffHeapCache<String, String> newCache(
      Ticker ticker, long defaultTtlMillis, CacheSerializer<String> serializer) {
    return OHCacheBuilder.<String, String>newBuilder()
        .capacity(1 << 20)
        .defaultTTLmillis(defaultTtlMillis)
        .ticker(ticker)
        .keySerializer(serializer)
        .valueSerializer(serializer)
        .buildTyped();
  }

  private static <T> ClaimedMutationResult<T> runWithTemporaryWriterClaim(
      OffHeapCache<String, String> cache, ExecutorService executor, Callable<T> mutation)
      throws Exception {
    Entry entry = cache.dataForTest().values().iterator().next();
    assertTrue(entry.claimWriter());
    CountDownLatch started = new CountDownLatch(1);
    boolean writerHeld = true;
    Throwable callerFailure = null;
    try {
      Future<T> future =
          executor.submit(
              () -> {
                started.countDown();
                return mutation.call();
              });
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
    } catch (Throwable error) {
      callerFailure = error;
      throw error;
    } finally {
      if (writerHeld) {
        entry.finishWriter();
      }
      CacheTestSupport.awaitCallers(executor, callerFailure);
    }
  }

  private static void awaitReclaimedRecords(OffHeapCache<?, ?> cache, long minimum)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
    do {
      cache.flushAsync().get(5L, TimeUnit.SECONDS);
      if (cache.stats().retirementActorReclaimedRecords() >= minimum) {
        return;
      }
      Thread.yield();
    } while (System.nanoTime() < deadline);
    assertTrue(cache.stats().retirementActorReclaimedRecords() >= minimum);
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
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
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
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
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
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    @Override
    public com.red.ohc.index.Entry putIfAbsent(
        com.red.ohc.index.Entry key, com.red.ohc.index.Entry value) {
      com.red.ohc.index.Entry result = super.putIfAbsent(key, value);
      throw new IllegalStateException("injected after putIfAbsent CHM publication");
    }
  }

  private static final class PausedInsertCollisionMap
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
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
        com.red.ohc.index.Entry key, com.red.ohc.index.Entry value) {
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
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
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
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
    private final AtomicInteger lookupCalls = new AtomicInteger();
    private final AtomicInteger putIfAbsentCalls = new AtomicInteger();

    @Override
    public com.red.ohc.index.Entry get(Object key) {
      lookupCalls.incrementAndGet();
      return super.get(key);
    }

    @Override
    public com.red.ohc.index.Entry putIfAbsent(
        com.red.ohc.index.Entry key, com.red.ohc.index.Entry value) {
      putIfAbsentCalls.incrementAndGet();
      return super.putIfAbsent(key, value);
    }
  }

  private static final class ThrowAfterVictimRemoveMap
      extends ConcurrentHashMap<com.red.ohc.index.Entry, com.red.ohc.index.Entry> {
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

  private static final class ThrowingDeserializeSerializer implements CacheSerializer<String> {
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
