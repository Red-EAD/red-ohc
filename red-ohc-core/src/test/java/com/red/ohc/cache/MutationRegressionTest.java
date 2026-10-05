package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.maintenance.MaintenanceTestSupport;
import com.red.ohc.maintenance.WriterLifecycleJournal;
import com.red.ohc.runtime.ReaderGuard;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.ValueBlock;

/** Deterministic regressions for user callbacks and conditional publication. */
public final class MutationRegressionTest {
  private static final CacheSerializer<String> STRING = new StringSerializer();

  @DataProvider
  public Object[][] clockOffsets() {
    return new Object[][] {{-3_600_000L}, {3_600_000L}};
  }

  @Test(dataProvider = "clockOffsets")
  public void absoluteExpirySamplesCurrentWallClock(long originOffset) throws Exception {
    withCache(
        STRING,
        cache -> {
          MonotonicDeadlineClock clock = (MonotonicDeadlineClock) field(cache, "deadlineClock");
          Field origin = MonotonicDeadlineClock.class.getDeclaredField("originWallMillis");
          origin.setAccessible(true);
          origin.setLong(clock, origin.getLong(clock) + originOffset);
          cache.put("a", "live", System.currentTimeMillis() + 5_000L);
          assertEquals(cache.get("a"), "live");
          @SuppressWarnings("unchecked")
          ThreadLocal<ThreadContext> contexts =
              (ThreadLocal<ThreadContext>) field(cache, "contexts");
          ThreadContext context = contexts.get();
          ReaderGuard guard = (ReaderGuard) field(cache, "readerGuard");
          guard.enter(context);
          try {
            Entry entry = cache.data.values().iterator().next();
            long remaining =
                ValueBlock.deadlineNanos(Entry.rawValueAddress(entry.valueAddress))
                    - clock.nowNanos();
            assertTrue(remaining > 0L && remaining <= 5_000_000_000L);
          } finally {
            guard.exit(context);
          }
        });
  }

  private static Object field(Object owner, String name) throws Exception {
    Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }

  @Test(timeOut = 10_000L)
  public void parkedWriterDoesNotBlockOtherNativeRetirements() throws Exception {
    ExecutorService caller = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> waiter = new AtomicReference<>();
    withCache(
        STRING,
        cache -> {
          cache.put("a", "held");
          cache.put("b", "retire");
          cache.flushAsync().join();
          int heldKeyHash = keyHash("a");
          Entry entry =
              cache.data.values().stream()
                  .filter(e -> e.keyHash() == heldKeyHash)
                  .findFirst()
                  .get();
          Method acquire = OffHeapCache.class.getDeclaredMethod("awaitWriter", Entry.class);
          Method release = OffHeapCache.class.getDeclaredMethod("releaseWriter", Entry.class);
          acquire.setAccessible(true);
          release.setAccessible(true);
          assertTrue(entry.claimWriter());
          boolean ownerHeld = true;
          Throwable callerFailure = null;
          try {
            Future<?> done =
                caller.submit(
                    () -> {
                      waiter.set(Thread.currentThread());
                      try {
                        if ((Long) acquire.invoke(cache, entry) != 0L) {
                          release.invoke(cache, entry);
                        }
                      } catch (Exception failure) {
                        throw new AssertionError(failure);
                      }
                    });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while ((waiter.get() == null || waiter.get().getState() != Thread.State.WAITING)
                && System.nanoTime() < deadline) {
              Thread.yield();
            }
            assertEquals(waiter.get().getState(), Thread.State.WAITING);
            cache.remove("b");
            cache.flushAsync().get(2L, TimeUnit.SECONDS);
            ownerHeld = false;
            release.invoke(cache, entry);
            done.get(2L, TimeUnit.SECONDS);
          } catch (Throwable failure) {
            callerFailure = failure;
            throw failure;
          } finally {
            if (ownerHeld) {
              try {
                release.invoke(cache, entry);
              } catch (Exception cleanupFailure) {
                if (callerFailure == null) {
                  throw cleanupFailure;
                }
                callerFailure.addSuppressed(cleanupFailure);
              }
            }
          }
        },
        caller);
  }

  private static int keyHash(String key) {
    ThreadContext context = new ThreadContext(null);
    com.red.ohc.codec.KeyEncoder.encode(STRING, key, context);
    return context.lookupKey.hash();
  }

  @Test(timeOut = 10_000L)
  public void actorRetryWinnerPreventsDuplicateWriterPublication() throws Exception {
    AtomicReference<Thread> writer = new AtomicReference<>();
    AtomicBoolean armed = new AtomicBoolean();
    CountDownLatch released = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    Ticker ticker =
        new Ticker() {
          @Override
          public long nanos() {
            return System.nanoTime();
          }

          @Override
          public long currentTimeMillis() {
            if (Thread.currentThread() == writer.get() && armed.compareAndSet(true, false)) {
              released.countDown();
              await(resume);
            }
            return System.currentTimeMillis();
          }
        };
    ExecutorService caller = Executors.newSingleThreadExecutor();
    withCache(
        STRING,
        ticker,
        cache -> {
          cache.put("a", "old");
          cache.flushAsync().join();
          Entry entry = cache.data.values().iterator().next();
          MaintenanceEventLoop worker = (MaintenanceEventLoop) field(cache, "worker");
          WriterLifecycleJournal journal = MaintenanceTestSupport.lifecycle(worker);
          long before = journal.publishedRecordsTotal();
          entry.requestMutationRetry(Entry.PENDING_UPDATE);
          Future<?> result =
              caller.submit(
                  () -> {
                    try {
                      writer.set(Thread.currentThread());
                      @SuppressWarnings("unchecked")
                      ThreadLocal<ThreadContext> contexts =
                          (ThreadLocal<ThreadContext>) field(cache, "contexts");
                      ThreadContext context = contexts.get();
                      Field counter =
                          ThreadContext.class.getDeclaredField("replacementSampleCounter");
                      counter.setAccessible(true);
                      counter.setInt(context, ThreadContext.RESIDENCE_SAMPLE_INTERVAL - 1);
                      armed.set(true);
                      cache.put("a", "new");
                    } catch (Exception failure) {
                      throw new AssertionError(failure);
                    }
                  });
          try {
            assertTrue(released.await(2L, TimeUnit.SECONDS));
            CompletableFuture<Void> actorDone = new CompletableFuture<>();
            assertTrue(
                worker.submitActorTaskForTest(
                    () -> {
                      try {
                        assertTrue(entry.claimMutationRetry());
                        Method enqueue =
                            MaintenanceEventLoop.class.getDeclaredMethod(
                                "enqueueActorRetry", Entry.class);
                        enqueue.setAccessible(true);
                        enqueue.invoke(worker, entry);
                        actorDone.complete(null);
                      } catch (Throwable failure) {
                        actorDone.completeExceptionally(failure);
                      }
                    },
                    actorDone::completeExceptionally));
            actorDone.get(2L, TimeUnit.SECONDS);
            resume.countDown();
            result.get(2L, TimeUnit.SECONDS);
            cache.flushAsync().join();
            assertEquals(journal.publishedRecordsTotal(), before + 1L);
            assertEquals(cache.get("a"), "new");
          } finally {
            resume.countDown();
          }
        },
        caller);
  }

  @DataProvider
  public Object[][] nestedWrites() {
    return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
  }

  @Test(dataProvider = "nestedWrites")
  public void nestedReadPreservesTheOuterWriteKey(boolean duringSerialize, boolean absent)
      throws Exception {
    AtomicReference<OffHeapCache<String, String>> owner = new AtomicReference<>();
    AtomicBoolean armed = new AtomicBoolean();
    CacheSerializer<String> serializer =
        new StringSerializer() {
          private void nestedRead() {
            if (armed.compareAndSet(true, false)) {
              assertEquals(owner.get().get("b"), "inner");
            }
          }

          @Override
          public int serializedSize(String value) {
            if (!duringSerialize) {
              nestedRead();
            }
            return super.serializedSize(value);
          }

          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if (duringSerialize) {
              nestedRead();
            }
            super.serialize(value, buffer);
          }
        };
    withCache(
        serializer,
        cache -> {
          owner.set(cache);
          cache.put("b", "inner");
          armed.set(true);
          if (absent) {
            assertNull(cache.putIfAbsent("a", "outer"));
          } else {
            cache.put("a", "outer");
          }
          assertEquals(cache.get("a"), "outer");
          assertEquals(cache.get("b"), "inner");
        });
  }

  @DataProvider
  public Object[][] conditionalRaces() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "conditionalRaces", timeOut = 10_000L)
  public void conditionalWriteChecksTheVersionOfItsSnapshot(boolean remove, boolean nested)
      throws Exception {
    AtomicReference<OffHeapCache<String, String>> owner = new AtomicReference<>();
    AtomicBoolean armed = new AtomicBoolean();
    CountDownLatch snapshotRead = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    CacheSerializer<String> serializer =
        new StringSerializer() {
          @Override
          public String deserialize(ByteBuffer buffer) {
            String value = super.deserialize(buffer);
            if (armed.compareAndSet(true, false)) {
              if (nested) {
                assertEquals(owner.get().get("b"), "old");
              }
              snapshotRead.countDown();
              await(resume);
            }
            return value;
          }
        };
    ExecutorService caller = Executors.newSingleThreadExecutor();
    withCache(
        serializer,
        cache -> {
          owner.set(cache);
          cache.put("a", "old");
          cache.put("b", "old");
          armed.set(true);
          Future<Boolean> result =
              caller.submit(
                  () -> remove ? cache.remove("a", "old") : cache.replace("a", "old", "new"));
          try {
            assertTrue(snapshotRead.await(2L, TimeUnit.SECONDS));
            cache.put("a", "concurrent");
            resume.countDown();
            assertFalse(result.get(2L, TimeUnit.SECONDS));
            assertEquals(cache.get("a"), "concurrent");
            assertEquals(cache.get("b"), "old");
          } finally {
            resume.countDown();
          }
        },
        caller);
  }

  @DataProvider
  public Object[][] brokenIterators() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "brokenIterators")
  public void putAllIteratorCreationFailureDoesNotPoisonTheCallingThread(boolean entrySet)
      throws Exception {
    RuntimeException failure = new IllegalStateException("input failure");
    Map<String, String> input =
        new AbstractMap<String, String>() {
          @Override
          public boolean isEmpty() {
            return false;
          }

          @Override
          public Set<Map.Entry<String, String>> entrySet() {
            if (entrySet) {
              throw failure;
            }
            return new AbstractSet<Map.Entry<String, String>>() {
              @Override
              public int size() {
                return 1;
              }

              @Override
              public Iterator<Map.Entry<String, String>> iterator() {
                throw failure;
              }
            };
          }
        };
    withCache(
        STRING,
        cache -> {
          assertSame(expectThrows(IllegalStateException.class, () -> cache.putAll(input)), failure);
          cache.put("after", "usable");
          assertEquals(cache.get("after"), "usable");
        });
  }

  @Test
  public void putAllCleanupDoesNotMaskTheOriginalInputFailure() throws Exception {
    Map<String, String> input =
        new AbstractMap<String, String>() {
          private int emptyCalls;

          @Override
          public boolean isEmpty() {
            if (++emptyCalls != 1) {
              throw new IllegalStateException("cleanup must not invoke input callbacks");
            }
            return false;
          }

          @Override
          public Set<Map.Entry<String, String>> entrySet() {
            return Collections.singleton(new SimpleImmutableEntry<>("bad", null));
          }
        };
    withCache(
        STRING,
        cache -> {
          expectThrows(NullPointerException.class, () -> cache.putAll(input));
          cache.put("after", "usable");
          assertEquals(cache.get("after"), "usable");
        });
  }

  private static void withCache(
      CacheSerializer<String> serializer, CacheAction action, ExecutorService... callers)
      throws Exception {
    withCache(serializer, Ticker.DEFAULT, action, callers);
  }

  private static void withCache(
      CacheSerializer<String> serializer,
      Ticker ticker,
      CacheAction action,
      ExecutorService... callers)
      throws Exception {
    OffHeapCache<String, String> cache = null;
    Throwable primary = null;
    try {
      cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(serializer)
              .ticker(ticker)
              .buildTyped();
      action.run(cache);
      cache.flushAsync().join();
    } catch (Throwable failure) {
      primary = failure;
      throw failure;
    } finally {
      if (cache != null) {
        CacheTestSupport.stop(cache, primary, callers);
      } else {
        for (ExecutorService caller : callers) {
          CacheTestSupport.awaitCallers(caller, primary);
        }
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(3L, TimeUnit.SECONDS), "callback was not released");
    } catch (InterruptedException interruption) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interruption);
    }
  }

  private interface CacheAction {
    void run(OffHeapCache<String, String> cache) throws Exception;
  }

  private static class StringSerializer implements CacheSerializer<String> {
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
  }
}
