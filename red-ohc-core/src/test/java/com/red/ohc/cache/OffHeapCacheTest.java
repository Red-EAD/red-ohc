package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.maintenance.LogicalAdmission;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.WriterLifecycleJournal;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class OffHeapCacheTest {
  private static final CacheSerializer<String> STRING =
      new CacheSerializer<String>() {
        @Override
        public void serialize(String value, ByteBuffer buffer) {
          buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public int serializedSize(String value) {
          return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
      };

  private static final CacheSerializer<byte[]> BYTES =
      new CacheSerializer<byte[]>() {
        @Override
        public void serialize(byte[] value, ByteBuffer buffer) {
          buffer.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buffer) {
          byte[] value = new byte[buffer.remaining()];
          buffer.get(value);
          return value;
        }

        @Override
        public int serializedSize(byte[] value) {
          return value.length;
        }
      };

  @Test(timeOut = 2_000L)
  public void writerRegistrationBindsMaintenanceForLaterReads() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("registered", "value");
      for (int index = 0; index < 1_024; index++) {
        assertEquals(cache.get("registered"), "value");
      }
      cache.flushAsync().join();
      assertEquals(
          cache.stats().hitCount(),
          1_024L,
          "a thread registered by put must still publish later get statistics");
    }
  }

  @Test
  public void replacementOfARemovedMappingIsCountedExactlyOnce() {
    // put -> remove leaves the CHM entry logically absent; the replacement must republish with
    // the absence tag carried so the ledger counts the new mapping exactly once.
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("key", "first");
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      cache.remove("key");
      cache.flushAsync().join();
      assertEquals(cache.size(), 0L);
      cache.put("key", "second");
      cache.flushAsync().join();
      assertEquals(cache.size(), 1L);
      assertEquals(cache.get("key"), "second");
    }
  }

  @Test
  public void statsExposeWeaklyPublishedRequestRates() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("hit", "value");
      assertEquals(cache.get("hit"), "value");
      assertEquals(cache.get("miss"), null);
      cache.flushAsync().join();

      assertEquals(cache.stats().hitCount(), 1L);
      assertEquals(cache.stats().missCount(), 1L);
      assertEquals(cache.stats().requestCount(), 2L);
      assertEquals(cache.stats().hitRate(), 0.5d, 0.0d);
      assertEquals(cache.stats().missRate(), 0.5d, 0.0d);
    }
  }

  @Test
  public void statsExposeNativeAllocationCounters() {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      cache.put("large", "x".repeat(33_000));
      cache.flushAsync().join();

      OHCacheStats first = cache.stats();
      assertTrue(first.directEntryAllocationCount() > 0L);
      assertEquals(first.smallAllocationFallbackCount(), 0L);

      OHCacheStats second = cache.stats();
      assertTrue(
          second.directEntryAllocationCount() >= first.directEntryAllocationCount(),
          "allocation counters must be monotonic across weakly consistent snapshots");
    }
  }

  @Test(timeOut = 60_000L)
  public void replacementPressureMakesProgressWithBoundedBackpressure() {
    int residentEntries = 4_096;
    int keyBytes = 32;
    int valueBytes = 5 * 1024;
    long entryWeight =
        CacheMath.logicalEntryBytes(
            Entry.keyAllocationLengthForKeyLength(keyBytes), ValueBlock.allocationLength(valueBytes));
    byte[][] keys = new byte[residentEntries][];
    byte[] value = new byte[valueBytes];
    long capacity = entryWeight * residentEntries;
    try (OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(capacity)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .eviction(Eviction.S3_FIFO)
            .buildTyped()) {
      for (int index = 0; index < residentEntries; index++) {
        keys[index] = new byte[keyBytes];
        ByteBuffer.wrap(keys[index]).putInt(index);
        cache.put(keys[index], value);
      }
      cache.flushAsync().join();

      int attempts = 1_000_000;
      int accepted = attempts;
      for (int index = 0; index < attempts; index++) {
        cache.put(keys[index & (residentEntries - 1)], value);
      }
      cache.flushAsync().join();

      OHCacheStats stats = cache.stats();
      assertTrue(accepted > 0, "replacement pressure must make progress");
      assertFalse(stats.maintenanceUnhealthy());
      assertEquals(stats.maintenanceQueueDepth(), 0L);
      // A full retirement ring or native pool is bounded backpressure after the separate resident
      // cache-wide gate is removed. The important invariant is that the failed replacement does
      // not leave native ownership or maintenance state stuck.
    }
  }

  @Test(timeOut = 10_000L)
  public void uncontendedReplacementLeavesSafeWorkToMaintenance() throws Exception {
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch releaseActor = new CountDownLatch(1);
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      cache.put("key", "value");
      cache.flushAsync().join();
      pauseMaintenance(cache, actorPaused, releaseActor);

      MaintenanceEventLoop worker = worker(cache);
      RetirementJournal retirements = worker.retirementJournal();
      for (int record = 0; record < RetirementJournal.SEGMENT_CAPACITY * 6; record++) {
        retirements.append(0L, 0L);
      }
      retirements.cutAllProducersAtWatermark();
      retirements.sealReadySegments(1L);
      retirements.publishSafe(true, true);
      long safeSegmentsBefore = retirements.safeSegmentDebt();

      cache.put("key", "replacement");

      assertEquals(
          retirements.safeSegmentDebt(),
          safeSegmentsBefore,
          "normal replacement must leave already-safe work to maintenance");
    } finally {
      releaseActor.countDown();
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void sameShapeReplacementHandsOffMutationRetryAfterWriterRelease() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OffHeapCache<String, String> cache = newTestCache();
    Entry entry = null;
    boolean writerHeld = false;
    try {
      cache.put("key", "old");
      cache.flushAsync().join();
      long lifecycleBefore = publishedLifecycleRecords(cache);
      entry = cache.data.values().iterator().next();
      assertTrue(entry.claimWriter());
      writerHeld = true;
      assertTrue(entry.requestMutationRetry(Entry.PENDING_UPDATE));

      Future<?> replacement = executor.submit(() -> cache.put("key", "new"));
      try {
        replacement.get(100L, TimeUnit.MILLISECONDS);
        throw new AssertionError("the replacement must wait for the current writer");
      } catch (TimeoutException expected) {
        // The writer must publish only after the protected entry is released.
      }

      entry.finishWriter();
      writerHeld = false;
      replacement.get(1L, TimeUnit.SECONDS);

      assertFalse(
          entry.isMutationRetryRequested(),
          "the replacement writer must claim the actor's retry marker before releasing the entry");
      assertTrue(
          publishedLifecycleRecords(cache) - lifecycleBefore > 0,
          "the retry handoff must publish one durable lifecycle mutation record");
      cache.flushAsync().join();
      assertEquals(cache.get("key"), "new");
    } finally {
      if (writerHeld) {
        entry.finishWriter();
      }
      executor.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void flushAsyncDoesNotRequireAConcurrentWriterSnapshotToBeStable() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OffHeapCache<String, String> cache = newTestCache();
    LogicalAdmission admission = logicalAdmission(cache);
    CountDownLatch reserved = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      Future<?> writer =
          executor.submit(
              () -> {
                admission.tryChargeDelta(1L);
                reserved.countDown();
                try {
                  release.await();
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(interrupted);
                } finally {
                  admission.rollbackUnpublishedDelta(1L);
                }
              });
      assertTrue(reserved.await(1L, TimeUnit.SECONDS));

      // flushAsync is a maintenance FIFO barrier, not a global writer-quiescence barrier. An
      // in-flight logical reservation must not turn its completion into an exceptional future.
      cache.flushAsync().join();

      release.countDown();
      writer.get(1L, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      executor.shutdownNow();
      cache.close();
    }
  }

  @Test
  public void builderConstructsTheOffHeapEntryPoint() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertEquals(cache.getClass().getName(), "com.red.ohc.cache.OffHeapCache");
    }
  }

  @Test(timeOut = 10_000L)
  public void postChargeFailureAfterLedgerCommitTerminatesNewKeyInsert() throws Exception {
    try (OffHeapCache<String, String> cache = newTestCache()) {
      cache.setPostChargeFailurePointForTest(
          OffHeapCache.PostChargeFailurePoint.AFTER_LEDGER_COMMIT);
      assertPostChargeFailureTerminatesCache(cache, () -> cache.put("insert", "value"));
    }
  }

  @Test(timeOut = 10_000L)
  public void postChargeFailureBeforeRetirementCommitTerminatesReplacement() throws Exception {
    try (OffHeapCache<String, String> cache = newTestCache()) {
      cache.put("replace", "old");
      cache.setPostChargeFailurePointForTest(
          OffHeapCache.PostChargeFailurePoint.BEFORE_RETIREMENT_COMMIT);
      assertPostChargeFailureTerminatesCache(cache, () -> cache.put("replace", "new-value"));
    }
  }

  @Test(timeOut = 10_000L)
  public void postChargeFailureBeforeWriterReleaseTerminatesComputedInsert() throws Exception {
    try (OffHeapCache<String, String> cache = newTestCache()) {
      cache.setPostChargeFailurePointForTest(
          OffHeapCache.PostChargeFailurePoint.BEFORE_WRITER_RELEASE);
      assertPostChargeFailureTerminatesCache(
          cache, () -> cache.computeIfAbsent("compute", ignored -> "value"));
    }
  }

  @Test(timeOut = 10_000L)
  public void ttlReadAfterPutHandoffDoesNotRollbackThePublishedPut() throws Exception {
    assertTtlReadAfterPutHandoffDoesNotRollback(false);
  }

  @Test(timeOut = 10_000L)
  public void ttlReadAfterPutIfAbsentHandoffDoesNotRollbackThePublishedPut() throws Exception {
    assertTtlReadAfterPutHandoffDoesNotRollback(true);
  }

  @Test
  public void permanentSameShapeReplacementIgnoresTheUnusedNativeDeadline() throws Exception {
    OffHeapCache<String, String> cache = newTestCache();
    try {
      cache.put("key", "old");
      cache.flushAsync().join();
      Entry entry = cache.dataForTest().values().iterator().next();
      long oldTaggedValue = entry.valueAddress;
      assertFalse(Entry.hasTtl(oldTaggedValue));
      NativeMemory.putLong(Entry.rawValueAddress(oldTaggedValue), 1L);

      long lifecycleBefore = publishedLifecycleRecords(cache);
      cache.put("key", "new");

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "a permanent same-shape replacement must not inspect or maintain timer metadata");
      assertEquals(cache.get("key"), "new");
    } finally {
      cache.close();
    }
  }

  @Test
  public void permanentComputeReplacementIgnoresTheUnusedNativeDeadline() throws Exception {
    OffHeapCache<String, String> cache = newTestCache();
    try {
      cache.put("key", "old");
      cache.flushAsync().join();
      Entry entry = cache.dataForTest().values().iterator().next();
      long oldTaggedValue = entry.valueAddress;
      assertFalse(Entry.hasTtl(oldTaggedValue));
      NativeMemory.putLong(Entry.rawValueAddress(oldTaggedValue), 1L);

      long lifecycleBefore = publishedLifecycleRecords(cache);
      cache.compute("key", (key, value) -> "new");

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "a permanent compute replacement must not inspect or maintain timer metadata");
      assertEquals(cache.get("key"), "new");
    } finally {
      cache.close();
    }
  }

  @Test
  public void explicitPermanentReplacementSkipsTimerMetadataWithDefaultTtlEnabled()
      throws Exception {
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .defaultTTLmillis(60_000L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      cache.put("key", "old", 0L);
      cache.flushAsync().join();
      Entry entry = cache.dataForTest().values().iterator().next();
      long oldTaggedValue = entry.valueAddress;
      assertFalse(Entry.hasTtl(oldTaggedValue));
      NativeMemory.putLong(Entry.rawValueAddress(oldTaggedValue), 1L);

      long lifecycleBefore = publishedLifecycleRecords(cache);
      cache.put("key", "new", 0L);

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "an explicit permanent replacement must skip timer metadata despite default TTL");
      assertEquals(cache.get("key"), "new");
    } finally {
      cache.close();
    }
  }

  @Test
  public void explicitTtlToPermanentReplacementStillMaintainsTheTimer() throws Exception {
    OffHeapCache<String, String> cache = newTestCache();
    try {
      cache.put("key", "old", Long.MAX_VALUE);
      cache.flushAsync().join();

      long lifecycleBefore = publishedLifecycleRecords(cache);
      cache.put("key", "new");

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          1,
          "an explicit TTL must still be removed when default TTL is disabled");
      assertEquals(cache.get("key"), "new");
    } finally {
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void sameTimerSlotTtlExtensionDoesNotEnqueueAnotherMutation() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    AtomicLong wallMillis = new AtomicLong(1_000L);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return wallMillis.get();
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      cache.put("key", "old", 10_000L);
      cache.flushAsync().join();

      long lifecycleBefore = publishedLifecycleRecords(cache);
      wallMillis.set(1_001L);
      nowNanos.set(1_000_000L);
      cache.put("key", "new", 10_010L);

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "an extension in the installed timer slot must not enqueue a mutation");
      assertEquals(cache.get("key"), "new");

      wallMillis.set(1_002L);
      nowNanos.set(2_000_000L);
      cache.put("key", "newer", 10_100L);
      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "a pure extension into another timer slot self-heals in the wheel, no mutation");
    } finally {
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void computeUsesTheSameTimerSlotTtlMerge() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    AtomicLong wallMillis = new AtomicLong(1_000L);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return wallMillis.get();
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .defaultTTLmillis(10_000L)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    try {
      cache.put("key", "old");
      cache.flushAsync().join();

      long lifecycleBefore = publishedLifecycleRecords(cache);
      wallMillis.set(1_001L);
      nowNanos.set(1_000_000L);
      assertEquals(cache.compute("key", (key, value) -> "new"), "new");

      assertEquals(
          publishedLifecycleRecords(cache) - lifecycleBefore,
          0,
          "compute must share the same timer-slot merge as put");
    } finally {
      cache.close();
    }
  }

  private static void assertTtlReadAfterPutHandoffDoesNotRollback(boolean putIfAbsent)
      throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return 1_000L + nowNanos.get() / 1_000_000L;
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .defaultTTLmillis(1L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
    ExecutorService reader = Executors.newSingleThreadExecutor();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch releaseActor = new CountDownLatch(1);
    AtomicBoolean hookCalled = new AtomicBoolean();
    AtomicReference<Throwable> readFailure = new AtomicReference<>();
    try {
      pauseMaintenance(cache, actorPaused, releaseActor);
      MaintenanceEventLoop worker = worker(cache);
      lifecycle(cache)
          .bindReadySignal(
              () -> {
                if (!hookCalled.compareAndSet(false, true)) {
                  return;
                }
                nowNanos.set(2_000_000L);
                Future<?> read =
                    reader.submit(
                        () -> {
                          try {
                            assertEquals(cache.get("key"), null);
                          } catch (Throwable failure) {
                            readFailure.set(failure);
                          }
                        });
                try {
                  read.get(2L, TimeUnit.SECONDS);
                } catch (Throwable failure) {
                  readFailure.set(failure);
                }
              });

      if (putIfAbsent) {
        assertEquals(cache.putIfAbsent("key", "value"), null);
      } else {
        cache.put("key", "value");
      }
      assertTrue(hookCalled.get(), "the test must observe the post-handoff lifecycle offer");
      assertEquals(readFailure.get(), null);
      assertFalse(
          worker.snapshot().unhealthy,
          "an expired read must not make a successfully handed-off insertion unhealthy");
    } finally {
      lifecycle(cache).bindReadySignal(() -> {});
      releaseActor.countDown();
      reader.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void committedComputeRemovalSurvivesAReadySignalPublicationFailure() throws Exception {
    try (OffHeapCache<String, String> cache = newTestCache()) {
      cache.put("remove", "value");
      cache.flushAsync().join();
      MaintenanceEventLoop worker = worker(cache);
      AtomicBoolean injected = new AtomicBoolean();
      Method recordFailure =
          MaintenanceEventLoop.class.getDeclaredMethod("recordTerminalFailure", Throwable.class);
      recordFailure.setAccessible(true);
      lifecycle(cache)
          .bindReadySignal(
              () -> {
                if (injected.compareAndSet(false, true)) {
                  throw new IllegalStateException("injected ready signal failure");
                }
              },
              failure -> {
                try {
                  recordFailure.invoke(worker, failure);
                } catch (Exception invocationFailure) {
                  throw new IllegalStateException(invocationFailure);
                }
              });

      boolean failed = false;
      try {
        cache.computeIfPresent("remove", (key, value) -> null);
      } catch (Throwable expected) {
        failed = true;
      }
      assertTrue(injected.get(), "the ready signal must run during the removal publication");
      assertTrue(failed, "the terminal failure must reach the writer before compute returns");
      assertTrue(worker.snapshot().unhealthy, "the signal failure must reach the terminal path");

      WriterLifecycleJournal lifecycle = lifecycle(cache);
      WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
      assertTrue(lifecycle.lane(0).poll(record));
      assertEquals(
          record.operation,
          WriterLifecycleLane.REMOVE,
          "a committed compute removal must remain durable for terminal cleanup");
      lifecycle.lane(0).release(record);
    }
  }

  @Test(timeOut = 10_000L)
  public void putSerializesTheValueOncePerPublicCall() {
    AtomicLong serializedSizeCalls = new AtomicLong();
    CacheSerializer<String> countingSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            serializedSizeCalls.incrementAndGet();
            return STRING.serializedSize(value);
          }
        };
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(countingSerializer)
            .buildTyped()) {
      cache.put("key", "value");
      assertEquals(serializedSizeCalls.get(), 1L);
    }
  }

  @Test(timeOut = 5_000L)
  public void putRetryReusesSerializedMetadataAndTheOriginalTtlDeadline() throws Exception {
    AtomicLong wallMillis = new AtomicLong(1_000L);
    AtomicLong serializedSizeCalls = new AtomicLong();
    CountDownLatch replacementDeadlineSampled = new CountDownLatch(1);
    AtomicReference<Thread> replacementThread = new AtomicReference<>();
    CacheSerializer<String> countingSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            serializedSizeCalls.incrementAndGet();
            return STRING.serializedSize(value);
          }
        };
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return 0L;
          }

          @Override
          public long currentTimeMillis() {
            long sampledMillis = wallMillis.get();
            if (Thread.currentThread() == replacementThread.get()) {
              replacementDeadlineSampled.countDown();
            }
            return sampledMillis;
          }
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(countingSerializer)
            .buildTyped();
    Entry entry = null;
    boolean writerHeld = false;
    try {
      cache.put("key", "old", 2_000L);
      cache.flushAsync().join();
      entry = cache.data.values().iterator().next();
      assertTrue(entry.claimWriter());
      writerHeld = true;

      Future<?> replacement =
          executor.submit(
              () -> {
                replacementThread.set(Thread.currentThread());
                cache.put("key", "new", 5_000L);
              });
      assertTrue(replacementDeadlineSampled.await(1L, TimeUnit.SECONDS));
      // The ticker has captured the original wall time in a local value. serializedSize runs
      // before that sample, so its completion cannot be used as the clock-change barrier.
      // Recomputing expiry on retry would now make the replacement immediately expired.
      wallMillis.set(9_000L);
      try {
        replacement.get(100L, TimeUnit.MILLISECONDS);
        throw new AssertionError("the replacement must wait for the current writer");
      } catch (TimeoutException expected) {
        // The put has already computed its deterministic metadata and is waiting on the writer.
      }

      entry.finishWriter();
      writerHeld = false;
      replacement.get(1L, TimeUnit.SECONDS);

      assertEquals(serializedSizeCalls.get(), 2L);
      assertEquals(cache.get("key"), "new");
      long taggedValue = cache.data.get(entry).valueAddress;
      long valueAddress = Entry.rawValueAddress(taggedValue);
      assertTrue(Entry.hasTtl(taggedValue));
      assertEquals(ValueBlock.deadlineNanos(valueAddress), 4_000_000_000L);
    } finally {
      if (writerHeld) {
        entry.finishWriter();
      }
      executor.shutdownNow();
      cache.close();
    }
  }

  @Test
  public void putBecomesVisibleAfterFlushAndSupportsDirectValue() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      cache.put("key", "value");
      cache.flushAsync().join();
      assertEquals(readEventually(cache, "key"), "value");
      assertTrue(directEventually(cache));
    }
  }

  @Test(timeOut = 1_000L)
  public void writerClaimDoesNotWaitForAnEntryThatWasAlreadyRetired() throws Exception {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
      Entry entry = EntryTestSupport.entry(0, 0, 0L);
      assertTrue(entry.claimWriter());
      entry.markRetired();
      entry.finishWriter();

      Method claimWriter = OffHeapCache.class.getDeclaredMethod("claimWriter", Entry.class);
      claimWriter.setAccessible(true);
      assertFalse(
          (Boolean) claimWriter.invoke(cache, entry),
          "a stale CHM read must retry rather than park forever on a retired entry");
    }
  }

  @Test(timeOut = 5_000L)
  public void expiredEntryWriterContentionRetriesInsteadOfReportingClosed() throws Exception {
    AtomicLong nowNanos = new AtomicLong();
    AtomicLong nowMillis = new AtomicLong(1_000L);
    CountDownLatch candidateSerialized = new CountDownLatch(1);
    CacheSerializer<String> serializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("new".equals(value)) {
              candidateSerialized.countDown();
            }
            buffer.put(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
          }

          @Override
          public int serializedSize(String value) {
            return value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
          }
        };
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return nowNanos.get();
          }

          @Override
          public long currentTimeMillis() {
            return nowMillis.get();
          }
        };
    OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(serializer)
            .valueSerializer(serializer)
            .buildTyped();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Entry expiredEntry = null;
    boolean writerHeld = false;
    try {
      cache.put("expired", "old", 2_000L);
      cache.flushAsync().join();
      expiredEntry = cache.data.values().iterator().next();
      long oldValue = Entry.rawValueAddress(expiredEntry.valueAddress);
      ValueBlock.initialize(oldValue, 1L, ValueBlock.length(oldValue), 1L);
      nowNanos.set(2L);
      nowMillis.set(3_000L);

      assertTrue(expiredEntry.claimWriter());
      writerHeld = true;
      Future<String> result = executor.submit(() -> cache.putIfAbsent("expired", "new"));

      assertTrue(candidateSerialized.await(1L, TimeUnit.SECONDS));
      try {
        result.get(100L, TimeUnit.MILLISECONDS);
        throw new AssertionError("expired-entry writer contention must not complete early");
      } catch (TimeoutException expected) {
        // The operation must wait for the current writer instead of reporting cache closed.
      }

      expiredEntry.finishWriter();
      writerHeld = false;
      assertEquals(result.get(1L, TimeUnit.SECONDS), null);
      assertEquals(cache.get("expired"), "new");
    } finally {
      if (writerHeld) {
        expiredEntry.finishWriter();
      }
      executor.shutdownNow();
      cache.close();
    }
  }

  @Test(timeOut = 10_000L)
  public void replacementDoesNotWaitForPhysicalRetirement() throws Exception {
    byte[] firstKey = new byte[] {0};
    byte[] largeValue = new byte[8 * 1024];
    byte[] smallValue = new byte[1];
    long largeAllocation = ValueBlock.allocationLength(largeValue.length);
    long debtBudget = WriterArena.allocationWeight(largeAllocation);
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch releaseActor = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .nativeMemoryBudgetBytes(debtBudget)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    try {
      cache.put(firstKey, largeValue);
      cache.flushAsync().join();
      pauseMaintenance(cache, actorPaused, releaseActor);

      Future<?> replacement = executor.submit(() -> cache.put(firstKey, smallValue));
      replacement.get(1L, TimeUnit.SECONDS);
      assertEquals(cache.get(firstKey), smallValue);
      assertTrue(
          worker(cache).retiredBytes() >= WriterArena.allocationWeight(largeAllocation),
          "the old native block must be queued even while the actor is paused");
    } finally {
      releaseActor.countDown();
      executor.shutdownNow();
      cache.close();
    }
  }

  @Test
  public void failedReplacementAllocationLeavesRetirementBacklogUnchanged() throws Exception {
    byte[] key = new byte[] {1};
    byte[] original = new byte[1];
    byte[] replacement = new byte[1_024];
    long debtBudget =
        WriterArena.allocationWeight(ValueBlock.allocationLength(original.length));
    OffHeapCache<byte[], byte[]> cache =
        OHCacheBuilder.<byte[], byte[]>newBuilder()
            .capacity(1 << 20)
            .nativeMemoryBudgetBytes(debtBudget)
            .keySerializer(BYTES)
            .valueSerializer(BYTES)
            .buildTyped();
    AtomicLong nextPageId = nextPageId(cache);
    long savedNextPageId = 0L;
    boolean pageIdsOverridden = false;
    try {
      cache.put(key, original);
      cache.flushAsync().join();
      assertEquals(worker(cache).nativeDebtHeadroomBytes(), debtBudget);

      savedNextPageId = nextPageId.getAndSet(1L << 32);
      pageIdsOverridden = true;
      boolean failed = false;
      try {
        cache.put(key, replacement);
      } catch (IllegalStateException expected) {
        failed = true;
        assertTrue(expected.getMessage().contains("native allocation failed"));
      }

      assertTrue(failed, "page-id exhaustion must fail the replacement allocation");
      assertEquals(
          worker(cache).nativeDebtHeadroomBytes(),
          debtBudget,
          "failed allocation must not create a retirement record");
    } finally {
      if (pageIdsOverridden) {
        nextPageId.set(savedNextPageId);
      }
      cache.close();
    }
  }

  private static String readEventually(OHCache<String, String> cache, String key) {
    for (int i = 0; i < 100; i++) {
      String value = cache.get(key);
      if (value != null) {
        return value;
      }
      Thread.yield();
    }
    return null;
  }

  private static boolean directEventually(OHCache<String, String> cache) {
    for (int i = 0; i < 100; i++) {
      if (cache.getDirect("key", view -> assertEquals(view.length(), 5))) {
        return true;
      }
      Thread.yield();
    }
    return false;
  }

  private static MaintenanceEventLoop worker(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("worker");
    field.setAccessible(true);
    return (MaintenanceEventLoop) field.get(cache);
  }

  private static LogicalAdmission logicalAdmission(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("logicalAdmission");
    field.setAccessible(true);
    return (LogicalAdmission) field.get(cache);
  }

  private static long publishedLifecycleRecords(OffHeapCache<?, ?> cache) throws Exception {
    WriterLifecycleJournal lifecycle = lifecycle(cache);
    long total = 0L;
    for (int index = 0; index < lifecycle.laneCount(); index++) {
      total += lifecycle.lane(index).publishedRecordsTotal();
    }
    return total;
  }

  private static WriterLifecycleJournal lifecycle(OffHeapCache<?, ?> cache) throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("writerLifecycleJournal");
    field.setAccessible(true);
    return (WriterLifecycleJournal) field.get(cache);
  }

  private static OffHeapCache<String, String> newTestCache() {
    return
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped();
  }

  private static void assertPostChargeFailureTerminatesCache(
      OffHeapCache<String, String> cache, Runnable operation) throws Exception {
    boolean failed = false;
    try {
      operation.run();
    } catch (Throwable expected) {
      failed = true;
    }
    assertTrue(failed, "the injected post-charge failure must reach the writer");
    boolean rejected = false;
    try {
      cache.put("after-terminal", "value");
    } catch (Throwable expected) {
      // Terminal state is the expected public result after a handoff failure.
      rejected = true;
    }
    assertTrue(rejected, "a cache with a post-charge failure must reject later puts");
    assertTrue(worker(cache).snapshot().unhealthy, "post-charge failure must publish terminal state");
  }

  private static AtomicLong nextPageId(OffHeapCache<?, ?> cache) throws Exception {
    Field memoryField = OffHeapCache.class.getDeclaredField("memory");
    memoryField.setAccessible(true);
    NativeMemory.Memory memory = (NativeMemory.Memory) memoryField.get(cache);
    Field nextPageIdField = NativeMemory.Memory.class.getDeclaredField("nextPageId");
    nextPageIdField.setAccessible(true);
    return (AtomicLong) nextPageIdField.get(memory);
  }

  private static void pauseMaintenance(
      OffHeapCache<?, ?> cache, CountDownLatch paused, CountDownLatch release) throws Exception {
    assertTrue(
        worker(cache).submitAsyncMutation(
            () -> {
              paused.countDown();
              try {
                release.await();
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
              }
            },
            failure -> {
              throw new AssertionError(failure);
            }));
    assertTrue(paused.await(2L, TimeUnit.SECONDS), "maintenance actor did not pause");
  }

}
