package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.runtime.AccessRing;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;

public final class MaintenanceIdleHandoffTest {
  @DataProvider
  public Object[][] pendingSources() {
    return new Object[][] {{"ring"}, {"hits"}, {"misses"}};
  }

  @Test(dataProvider = "pendingSources", timeOut = 5_000L)
  public void idleCheckHandsObservedAccessToTheNextTurn(String source) throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.publishAfterCompletedScan(source);
      assertFalse((Boolean) call(fixture.loop, "hasRunnableWork"));

      call(fixture.loop, "parkUntilWork");

      assertTrue(
          (Boolean) call(fixture.loop, "hasRunnableWork"),
          "the real-time access deadline must schedule the observed source");
      call(fixture.loop, "maintenancePass");
      assertTrue(fixture.slot.access.isEmpty(), "the next turn must drain the observed ring");
      ReaderRegistry.SlotTableSnapshot slots = fixture.readers.slotTableSnapshot();
      int index = slots.firstLiveSlot(0, slots.slotCapacity());
      assertEquals(slots.consumedHits(index), fixture.slot.publishedHits);
      assertEquals(slots.consumedMisses(index), fixture.slot.publishedMisses);
      assertFalse((Boolean) call(fixture.loop, "hasRunnableWork"));
    }
  }

  @Test(timeOut = 5_000L)
  public void accessScanReturnsUrgentWhenAnyReaderReachesHighWatermark() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ReaderSlot pending = new ReaderSlot();
      ReaderSlot urgent = new ReaderSlot();
      fixture.loop.registerReader(pending);
      fixture.loop.registerReader(urgent);
      pending.access = new AccessRing();
      urgent.access = new AccessRing();
      Method scan =
          MaintenanceEventLoop.class.getDeclaredMethod("scanAccessState", boolean.class);
      scan.setAccessible(true);
      assertEquals(((Enum<?>) scan.invoke(fixture.loop, true)).name(), "NONE");

      assertTrue(pending.access.offer(fixture.entry, 0L, 0L, 0));
      assertEquals(((Enum<?>) scan.invoke(fixture.loop, true)).name(), "PENDING");

      for (int index = 0; index < AccessRing.HIGH_WATERMARK; index++) {
        assertTrue(urgent.access.offer(fixture.entry, 0L, 0L, 0));
      }

      assertEquals(((Enum<?>) scan.invoke(fixture.loop, true)).name(), "URGENT");
    }
  }

  @Test(timeOut = 5_000L)
  public void urgentAccessScanArmsReadersAfterTheUrgentSlot() throws Exception {
    try (Fixture fixture = new Fixture()) {
      ReaderSlot urgent = new ReaderSlot();
      ReaderSlot pending = new ReaderSlot();
      fixture.loop.registerReader(urgent);
      fixture.loop.registerReader(pending);
      urgent.access = new AccessRing();
      pending.access = new AccessRing();
      for (int index = 0; index < AccessRing.HIGH_WATERMARK; index++) {
        assertTrue(urgent.access.offer(fixture.entry, 0L, 0L, 0));
      }
      assertTrue(pending.access.offer(fixture.entry, 0L, 0L, 0));
      assertTrue(
          accessNotificationState(pending.access) != 0,
          "the later ring must start signalled so the scan has to re-arm it");

      Method scan =
          MaintenanceEventLoop.class.getDeclaredMethod("scanAccessState", boolean.class);
      scan.setAccessible(true);
      assertEquals(((Enum<?>) scan.invoke(fixture.loop, true)).name(), "URGENT");
      assertEquals(
          accessNotificationState(pending.access),
          0,
          "an urgent result must not leave later reader notifications disarmed");
    }
  }

  @Test(timeOut = 5_000L)
  public void readOnlyAccessScanDoesNotArmRingNotification() throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertTrue(fixture.slot.access.offer(fixture.entry, 0L, 0L, 0));
      int notificationState = accessNotificationState(fixture.slot.access);

      assertTrue((Boolean) call(fixture.loop, "hasPendingAccessReadOnly"));
      assertEquals(
          accessNotificationState(fixture.slot.access),
          notificationState,
          "read-only checks must not write the ring notification state");
    }
  }

  @Test(timeOut = 5_000L)
  public void lowWatermarkAccessDeadlineUsesWallClockWithFrozenTicker() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry readers = new ReaderRegistry(memory);
    ReaderSlot slot = new ReaderSlot();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            new FrozenTicker(),
            1 << 20,
            Eviction.LRU,
            readers,
            Long.MAX_VALUE);
    Entry entry = EntryTestSupport.entry(memory, 0, 1, 0L);
    try {
      loop.registerReader(slot);
      slot.access = new AccessRing(slot::signalAccess);
      assertTrue(slot.access.offer(entry, 0L, 0L, 0));
      loop.start();

      long deadline = System.nanoTime() + 1_000_000_000L;
      while (!slot.access.isEmpty() && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertTrue(
          slot.access.isEmpty(),
          "a low-watermark access record must drain after the real-time idle deadline");
    } finally {
      loop.stop();
      loop.join(2_000L);
      assertFalse(loop.isAlive());
      memory.closeArenas();
    }
  }

  @Test(timeOut = 5_000L)
  public void registeredReaderSelfHealsAfterAccessSignalIsDropped() throws Exception {
    try (Fixture fixture = new Fixture()) {
      // The lifecycle sweep is not the deadline under test. Keep the actor in the no-deadline
      // idle path so this test reaches the capped park transition deterministically.
      Field lifecycleDeadline =
          MaintenanceEventLoop.class.getDeclaredField("nextReaderLifecycleCheckNanos");
      lifecycleDeadline.setAccessible(true);
      lifecycleDeadline.setLong(fixture.loop, Long.MAX_VALUE);
      fixture.slot.access = new AccessRing(() -> {});
      fixture.loop.start();

      long parkDeadline = System.nanoTime() + 1_000_000_000L;
      while ((Long) field(field(fixture.loop, "idleBackoff"), "parkNanos")
              != 10_000_000L
          && System.nanoTime() < parkDeadline) {
        Thread.yield();
      }
      assertEquals(
          field(field(fixture.loop, "idleBackoff"), "parkNanos"),
          10_000_000L,
          "the actor must reach the capped idle park before the dropped notification is injected");
      boolean observedParked = fixture.loop.isParked();
      while (!observedParked && System.nanoTime() < parkDeadline) {
        Thread.yield();
        observedParked = fixture.loop.isParked();
      }
      assertTrue(observedParked);

      assertTrue(fixture.slot.access.offer(fixture.entry, 0L, 0L, 0));
      long drainDeadline = System.nanoTime() + 1_000_000_000L;
      while (!fixture.slot.access.isEmpty() && System.nanoTime() < drainDeadline) {
        Thread.yield();
      }
      assertTrue(
          fixture.slot.access.isEmpty(),
          "a registered reader must eventually drain an accepted record even when its signal is dropped");
      assertFalse(
          ((java.util.concurrent.atomic.AtomicBoolean) field(fixture.loop, "unhealthy")).get());
    }
  }

  @Test(timeOut = 5_000L)
  public void actorWithoutReadersUsesCappedIdlePark() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(),
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            new ReaderRegistry(memory),
            Long.MAX_VALUE);
    try {
      loop.start();
      long deadline = System.nanoTime() + 1_000_000_000L;
      while ((Long) field(field(loop, "idleBackoff"), "parkNanos") != 10_000_000L
          && System.nanoTime() < deadline) {
        Thread.yield();
      }
      assertEquals(field(field(loop, "idleBackoff"), "parkNanos"), 10_000_000L);
      long parkedDeadline = System.nanoTime() + 1_000_000_000L;
      boolean observedParked = loop.isParked();
      while (!observedParked && System.nanoTime() < parkedDeadline) {
        Thread.yield();
        observedParked = loop.isParked();
      }
      assertTrue(observedParked);
    } finally {
      loop.stop();
      loop.join(2_000L);
      assertFalse(loop.isAlive());
      memory.closeArenas();
    }
  }

  static final class Fixture implements AutoCloseable {
    final NativeMemory.Memory memory = new NativeMemory.Memory();
    final ReaderRegistry readers = new ReaderRegistry(memory);
    final ReaderSlot slot = new ReaderSlot();
    final MaintenanceEventLoop loop =
        new MaintenanceEventLoop(
            new ConcurrentHashMap<>(), memory, Ticker.DEFAULT, 1 << 20,
            Eviction.LRU, readers, Long.MAX_VALUE);
    final Entry entry = EntryTestSupport.entry(memory, 0, 1, 0L);

    Fixture() throws Exception {
      loop.registerReader(slot);
      slot.access = new AccessRing(slot::signalAccess);
      assertTrue(slot.access.offer(entry, 0L, 0L, 0));
      Method drain = MaintenanceEventLoop.class.getDeclaredMethod("drainMailbox", int.class);
      drain.setAccessible(true);
      drain.invoke(loop, Integer.MAX_VALUE);
      Field accessDeadline =
          MaintenanceEventLoop.class.getDeclaredField("nextAccessWakeNanos");
      accessDeadline.setAccessible(true);
      accessDeadline.setLong(loop, 0L);
      call(loop, "maintenancePass");
      assertTrue(slot.access.isEmpty());
      assertEquals(((AtomicInteger) field(loop, "requestedWork")).get(), 0);
      assertFalse((Boolean) field(loop, "accessScanActive"));
      // The previous offer signaled, but the actor has not yet armed the final idle check.
      ((WakeGate) field(loop, "wakeGate")).requireProcessing();
    }

    void publishAfterCompletedScan(String source) throws Exception {
      if (source.equals("ring")) {
        // This publication is coalesced with the already-consumed notification. There is no
        // further producer operation to rescue the actor if the idle check forgets the source.
        assertTrue(slot.access.offer(entry, 0L, 0L, 0));
      } else if (source.equals("hits")) {
        // A producer can be paused between publishing its counters and signaling the actor.
        slot.publishedHits++;
      } else {
        slot.publishedMisses++;
      }
      if (!source.equals("ring")) {
        assertEquals(((AtomicInteger) field(loop, "requestedWork")).get(), 0);
      }
    }

    @Override
    public void close() throws InterruptedException {
      loop.stop();
      if (loop.thread().getState() == Thread.State.NEW) {
        loop.start();
      }
      loop.join(2_000L);
      assertFalse(loop.isAlive(), "actor cleanup did not finish");
      memory.closeArenas();
    }
  }

  static Object field(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  static Object call(Object target, String name) throws Exception {
    Method method = target.getClass().getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(target);
  }

  private static int accessNotificationState(AccessRing access) throws Exception {
    Field consumer = AccessRing.class.getDeclaredField("consumer");
    consumer.setAccessible(true);
    Object control = consumer.get(access);
    Field state = control.getClass().getDeclaredField("notifyState");
    state.setAccessible(true);
    return state.getInt(control);
  }

  private static final class FrozenTicker implements Ticker {
    @Override
    public long nanos() {
      return 0L;
    }

    @Override
    public long currentTimeMillis() {
      return 0L;
    }
  }
}
