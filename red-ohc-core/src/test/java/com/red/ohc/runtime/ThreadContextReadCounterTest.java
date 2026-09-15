package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;

public final class ThreadContextReadCounterTest {
  @Test
  public void accessRingStoresAGenerationTokenWithTheValueAddress() throws Exception {
    Field generations = AccessRing.class.getDeclaredField("generations");

    assertEquals(generations.getType(), long[].class);
  }

  @Test
  public void oneReadSequenceDrivesStatsAndAccessSampling() {
    ThreadContext context = new ThreadContext(null);
    long first = context.hit();
    context.finishRead(first);
    assertEquals(first, 1L);
    assertEquals(context.slot.publishedHits, 0L);
    for (int index = 0; index < 1_023; index++) {
      long sequence = context.miss();
      context.finishRead(sequence);
    }
    assertEquals(context.slot.publishedHits, 1L);
    assertEquals(context.slot.publishedMisses, 1_023L);
  }

  @Test
  public void accessRingRetains8192DeliveredHitsBeforeDropping() {
    AccessRing ring = new AccessRing();
    Entry entry = EntryTestSupport.entry(0, 1, 0L);

    for (int index = 0; index < 8_192; index++) {
      assertTrue(ring.offer(entry, index + 1L, index + 101L, Entry.POLICY_NONE));
    }
    assertFalse(ring.offer(entry, 8_193L, 8_293L, Entry.POLICY_NONE));

    int[] drained = {0};
    while (ring.poll(
        (ignored, observedValueAddress, observedGeneration, observedPolicyState) -> {
          assertEquals(observedValueAddress, drained[0] + 1L);
          assertEquals(observedGeneration, drained[0] + 101L);
        })) {
      drained[0]++;
    }
    assertEquals(drained[0], 8_192);
    assertTrue(ring.isEmpty());
  }

  @Test
  public void accessRingPublishesTheObservedValueTokenNotTheLaterEntryState() {
    AccessRing ring = new AccessRing();
    Entry entry = EntryTestSupport.entry(0, 4, 0x100L);

    assertTrue(ring.offer(entry, entry.valueAddress, 7L, Entry.POLICY_NONE));
    entry.valueAddress = 0x200L;

    assertTrue(
        ring.poll(
            (ignored, observedValueAddress, observedGeneration, observedPolicyState) -> {
              assertEquals(observedValueAddress, 0x100L);
              assertEquals(observedGeneration, 7L);
            }));
    assertTrue(ring.isEmpty());
  }

  @Test
  public void accessRingCarriesTheProducerPolicyStateSnapshot() {
    AccessRing ring = new AccessRing();
    Entry entry = EntryTestSupport.entry(0, 6, 0x300L);
    entry.policyState(Entry.POLICY_S4_SKIP);

    assertTrue(
        ring.offer(entry, entry.valueAddress, entry.generation(), Entry.POLICY_S4_SKIP));
    entry.policyState(Entry.POLICY_S3_SMALL);

    int[] delivered = {Entry.POLICY_NONE};
    assertTrue(
        ring.poll(
            (ignored, observedValueAddress, observedGeneration, observedPolicyState) ->
                delivered[0] = observedPolicyState));
    assertEquals(
        delivered[0],
        Entry.POLICY_S4_SKIP,
        "the consumer must see the producer snapshot, not the current entry state");
  }

  @Test
  public void accessRingArmsNotificationBeforeAnEmptyFinalScan() {
    AtomicInteger signals = new AtomicInteger();
    AccessRing ring = new AccessRing(signals::incrementAndGet);

    assertFalse(ring.armNotificationAndCheckPending());
    assertTrue(ring.offer(null, 1L, 2L, Entry.POLICY_NONE));
    assertEquals(signals.get(), 1);
    assertTrue(ring.offer(null, 3L, 4L, Entry.POLICY_NONE));
    assertEquals(signals.get(), 1, "one armed scan must coalesce producer notifications");

    assertTrue(ring.armNotificationAndCheckPending());
    assertTrue(ring.offer(null, 5L, 6L, Entry.POLICY_NONE));
    assertEquals(signals.get(), 2, "the next actor scan must re-arm one notification");
  }

  @Test(timeOut = 5_000L)
  public void accessRingWakesTheActorAfterAnEmptyArmedScan() throws Exception {
    AtomicInteger signals = new AtomicInteger();
    AtomicBoolean pending = new AtomicBoolean(true);
    CountDownLatch armed = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AccessRing ring = new AccessRing(signals::incrementAndGet);
    Thread actor =
        new Thread(
            () -> {
              pending.set(ring.armNotificationAndCheckPending());
              armed.countDown();
              try {
                resume.await();
              } catch (InterruptedException interruption) {
                Thread.currentThread().interrupt();
              }
            });
    actor.start();
    assertTrue(armed.await(2, java.util.concurrent.TimeUnit.SECONDS));
    assertFalse(pending.get());

    assertTrue(ring.offer(null, 1L, 2L, Entry.POLICY_NONE));
    assertEquals(signals.get(), 1, "a producer must wake an actor armed on an empty ring");

    resume.countDown();
    actor.join(2_000L);
    assertFalse(actor.isAlive());
    assertTrue(ring.poll((ignored, address, generation, policyState) -> {}));
  }

  @Test(timeOut = 10_000L)
  public void accessRingPreservesRecordsAcrossProducerConsumerInterleaving() throws Exception {
    int records = 4_096;
    AtomicInteger consumed = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch start = new CountDownLatch(1);
    AccessRing ring = new AccessRing();
    Thread producer =
        new Thread(
            () -> {
              try {
                start.await();
                for (int index = 0; index < records; index++) {
                  if (!ring.offer(null, index + 1L, index + 101L, Entry.POLICY_NONE)) {
                    throw new AssertionError("the test producer must not fill the ring");
                  }
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              }
            });
    Thread consumer =
        new Thread(
            () -> {
              try {
                start.await();
                while (consumed.get() < records) {
                  if (!ring.poll(
                      (ignored, address, generation, policyState) -> consumed.incrementAndGet())) {
                    ring.armNotificationAndCheckPending();
                    Thread.yield();
                  }
                }
              } catch (Throwable error) {
                failure.compareAndSet(null, error);
              }
            });
    producer.start();
    consumer.start();
    start.countDown();
    producer.join(5_000L);
    consumer.join(5_000L);

    assertFalse(producer.isAlive());
    assertFalse(consumer.isAlive());
    if (failure.get() != null) {
      throw new AssertionError("producer/consumer interleaving failed", failure.get());
    }
    assertEquals(consumed.get(), records);
    assertEquals(ring.droppedCount(), 0L);
    assertTrue(ring.isEmpty());
  }

  @Test
  public void accessRingSignalsAfterAConsumerFreesASlotFromAFullRing() {
    AtomicInteger signals = new AtomicInteger();
    AccessRing ring = new AccessRing(signals::incrementAndGet);
    for (int index = 0; index < 8_192; index++) {
      assertTrue(ring.offer(null, index + 1L, index + 101L, Entry.POLICY_NONE));
    }

    signals.set(0);
    assertTrue(ring.armNotificationAndCheckPending());
    assertTrue(ring.poll((ignored, address, generation, policyState) -> {}));
    assertTrue(ring.offer(null, 8_193L, 8_293L, Entry.POLICY_NONE));
    assertEquals(signals.get(), 1, "the freed full-ring slot must preserve the armed wakeup");
  }

  @Test
  public void accessRingBatchPollPublishesOnlyTheProcessedConsumerCursor() {
    AccessRing ring = new AccessRing();
    for (int index = 0; index < 4; index++) {
      assertTrue(ring.offer(null, index + 1L, index + 11L, Entry.POLICY_NONE));
    }

    assertEquals(
        ring.poll(
            3,
            (ignored, observedValueAddress, observedGeneration, observedPolicyState) ->
                assertEquals(observedValueAddress, observedGeneration - 10L)),
        3);
    assertEquals(ring.size(), 1);
    assertTrue(ring.poll((ignored, address, generation, policyState) -> assertEquals(address, 4L)));
    assertTrue(ring.isEmpty());
  }

  @Test
  public void accessRingBatchPollRetainsTheFailingRecordForRetry() {
    AccessRing ring = new AccessRing();
    for (int index = 0; index < 4; index++) {
      assertTrue(ring.offer(null, index + 1L, index + 11L, Entry.POLICY_NONE));
    }

    AtomicInteger calls = new AtomicInteger();
    RuntimeException failure =
        expectRuntime(
            () ->
                ring.poll(
                    4,
                    (ignored, address, generation, policyState) -> {
                      if (calls.incrementAndGet() == 3) {
                        throw new RuntimeException("stop");
                      }
                    }));
    assertEquals(failure.getMessage(), "stop");
    assertEquals(ring.size(), 2);

    assertEquals(ring.poll(2, (ignored, address, generation, policyState) -> {}), 2);
    assertTrue(ring.isEmpty());
  }

  @Test
  public void accessRingHeadAndTailPreservePayloadAcrossSignedWrap() throws Exception {
    AccessRing ring = new AccessRing();
    Field producerField = AccessRing.class.getDeclaredField("producer");
    Field consumerField = AccessRing.class.getDeclaredField("consumer");
    producerField.setAccessible(true);
    consumerField.setAccessible(true);
    Object producer = producerField.get(ring);
    Object consumer = consumerField.get(ring);
    Field head = producer.getClass().getDeclaredField("head");
    Field cachedTail = producer.getClass().getDeclaredField("cachedTail");
    Field tail = consumer.getClass().getDeclaredField("tail");
    head.setAccessible(true);
    cachedTail.setAccessible(true);
    tail.setAccessible(true);
    int initial = Integer.MAX_VALUE - 1;
    head.setInt(producer, initial);
    cachedTail.setInt(producer, initial);
    tail.setInt(consumer, initial);

    assertTrue(ring.offer(null, 11L, 21L, 31));
    assertTrue(ring.offer(null, 12L, 22L, 32));
    AtomicInteger expected = new AtomicInteger(11);
    assertEquals(
        ring.poll(
            2,
            (entry, address, generation, policyState) -> {
              int value = expected.getAndIncrement();
              assertEquals(address, value);
              assertEquals(generation, value + 10L);
              assertEquals(policyState, value + 20);
            }),
        2);
    assertTrue(ring.isEmpty());
  }

  private static RuntimeException expectRuntime(Runnable action) {
    try {
      action.run();
    } catch (RuntimeException expected) {
      return expected;
    }
    throw new AssertionError("expected a runtime failure");
  }

  @Test
  public void businessAccessSamplingPublishesOneEventPerSixteenHits() {
    ThreadContext context = new ThreadContext(null);
    Entry entry = EntryTestSupport.entry(0, 2, 0L);

    for (int index = 0; index < 32; index++) {
      context.access(entry);
    }

    int delivered = 0;
    while (context.slot.access.poll(
        (ignored, observedValueAddress, observedGeneration, observedPolicyState) -> {})) {
      delivered++;
    }
    assertEquals(delivered, 2);
  }

  @Test
  public void businessAccessSamplingCapturesProducerPolicyState() {
    ThreadContext context = new ThreadContext(null);
    Entry entry = EntryTestSupport.entry(0, 5, 0L);
    entry.policyState(Entry.POLICY_S4_SKIP);

    for (int index = 0; index < 16; index++) {
      context.access(entry);
    }

    assertTrue(
        context.slot.access.poll(
            (ignored, observedValueAddress, observedGeneration, observedPolicyState) -> {
              assertEquals(observedGeneration, entry.generation());
              assertEquals(observedPolicyState, Entry.POLICY_S4_SKIP);
            }));
  }

  @Test
  public void bulkAccessSamplingPublishesOneEventPerSixteenHits() {
    ThreadContext context = new ThreadContext(null);
    Entry entry = EntryTestSupport.entry(0, 3, 0L);

    context.beginBulkRead();
    for (int index = 0; index < 32; index++) {
      context.bulkHit(entry);
    }
    context.finishBulkRead();

    int delivered = 0;
    while (context.slot.access.poll(
        (ignored, observedValueAddress, observedGeneration, observedPolicyState) -> {})) {
      delivered++;
    }
    assertEquals(delivered, 2);
  }
}
