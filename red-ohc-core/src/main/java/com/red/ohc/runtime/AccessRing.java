package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.red.ohc.index.Entry;

/** Fixed-capacity SPSC access sampler: the business thread publishes, the worker consumes. */
public final class AccessRing {
  static final int CAPACITY = 8_192;
  public static final int HIGH_WATERMARK = CAPACITY / 2;
  public static final int LOW_WATERMARK = HIGH_WATERMARK / 2;
  private static final int MASK = CAPACITY - 1;
  private final Entry[] entries = new Entry[CAPACITY];
  private final long[] valueAddresses = new long[CAPACITY];
  private final long[] generations = new long[CAPACITY];
  private final int[] policyStates = new int[CAPACITY];
  private final Runnable nonEmptySignal;
  private final ProducerControl producer = new ProducerControl();
  private final ConsumerControl consumer = new ConsumerControl();
  private volatile long dropped;
  private static final int NOTIFY_ARMED = 0;
  private static final int NOTIFY_SIGNALED = 1;
  private static final AtomicIntegerFieldUpdater<ProducerControl> HEAD_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(ProducerControl.class, "head");
  private static final AtomicIntegerFieldUpdater<ConsumerControl> NOTIFY_STATE_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(ConsumerControl.class, "notifyState");

  public AccessRing() {
    this(null);
  }

  public AccessRing(Runnable nonEmptySignal) {
    this.nonEmptySignal = nonEmptySignal;
  }

  public boolean offer(
      Entry entry,
      long observedValueAddress,
      long observedGeneration,
      int observedPolicyState) {
    int current = producer.head;
    if (current - producer.cachedTail == CAPACITY) {
      int refreshedTail = consumer.tail;
      producer.cachedTail = refreshedTail;
      if (current - refreshedTail == CAPACITY) {
        dropped++;
        return false;
      }
    }
    int slot = current & MASK;
    entries[slot] = entry;
    valueAddresses[slot] = observedValueAddress;
    generations[slot] = observedGeneration;
    policyStates[slot] = observedPolicyState;
    HEAD_UPDATER.lazySet(producer, current + 1);
    if (consumer.notifyState == NOTIFY_ARMED) {
      maybeWakeActor(current);
    }
    return true;
  }

  private void maybeWakeActor(int current) {
    int observedTail = consumer.tail;
    producer.cachedTail = observedTail;
    int depthBeforePublish = current - observedTail;
    if (depthBeforePublish == 0
        || (depthBeforePublish < HIGH_WATERMARK
            && depthBeforePublish + 1 >= HIGH_WATERMARK)) {
      if (NOTIFY_STATE_UPDATER.compareAndSet(consumer, NOTIFY_ARMED, NOTIFY_SIGNALED)
          && nonEmptySignal != null) {
        nonEmptySignal.run();
      }
    }
  }

  public boolean poll(AccessConsumer consumer) {
    return poll(1, consumer) != 0;
  }

  /** Consumes at most {@code limit} records and publishes one consumer cursor update. */
  public int poll(int limit, AccessConsumer consumer) {
    if (limit <= 0) {
      return 0;
    }
    int current = this.consumer.tail;
    int available = producer.head - current;
    if (available <= 0) {
      return 0;
    }
    int target = Math.min(limit, available);
    int processed = 0;
    try {
      while (processed < target) {
        int slot = (current + processed) & MASK;
        Entry entry = entries[slot];
        long observedValueAddress = valueAddresses[slot];
        long observedGeneration = generations[slot];
        int observedPolicyState = policyStates[slot];
        consumer.accept(entry, observedValueAddress, observedGeneration, observedPolicyState);
        entries[slot] = null;
        valueAddresses[slot] = 0L;
        generations[slot] = 0L;
        policyStates[slot] = 0;
        processed++;
      }
    } catch (RuntimeException | Error failure) {
      this.consumer.tail = current + processed;
      throw failure;
    }
    this.consumer.tail = current + processed;
    return processed;
  }

  public boolean isEmpty() {
    return consumer.tail == producer.head;
  }

  public int size() {
    int size = producer.head - consumer.tail;
    return size > 0 ? size : 0;
  }

  public long droppedCount() {
    return dropped;
  }

  /** Arms the producer-to-actor notification before a final pending scan. */
  public boolean armNotificationAndCheckPending() {
    consumer.notifyState = NOTIFY_ARMED;
    return !isEmpty();
  }

  private static final class ProducerControl {
    private volatile int head;
    private int cachedTail;
  }

  private static final class ConsumerControl {
    private volatile int tail;
    private volatile int notifyState = NOTIFY_ARMED;
  }
}
