package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.red.ohc.storage.NativeMemory;

public final class ReaderRegistryTest {
  private static final NativeMemory.Memory TEST_MEMORY =
      new NativeMemory.Memory();
  private static final List<ReaderRegistry> OPEN_REGISTRIES = new ArrayList<>();

  private static synchronized ReaderRegistry newRegistry() {
    ReaderRegistry registry = new ReaderRegistry(TEST_MEMORY);
    OPEN_REGISTRIES.add(registry);
    return registry;
  }

  @AfterClass(alwaysRun = true)
  public void closeRegistries() {
    synchronized (ReaderRegistryTest.class) {
      for (ReaderRegistry registry : OPEN_REGISTRIES) {
        registry.clear();
        registry.close();
      }
      OPEN_REGISTRIES.clear();
    }
    TEST_MEMORY.closeArenas();
  }

  @Test
  public void epochScansDoNotDependOnAPreemptibleStorageMutationVersion() {
    assertFalse(
        hasField("epochStorageVersion", long.class),
        "QSBR scans must not busy-wait behind a paused registration or cleanup owner");
  }

  @Test
  public void qsbrUsesOneUnifiedPaddedStateAndStrongChunkMetadata() throws Exception {
    assertTrue(hasField("storage", nestedClass("SlotStorage")));
    assertTrue(hasNestedField("SlotStorage", "stateChunkAddresses", long[].class));
    assertFalse(hasNestedField("SlotStorage", "stateWordChunks", long[][].class));
    assertTrue(hasNestedField("SlotStorage", "slotChunks", ReaderSlot[][].class));
    assertTrue(hasNestedField("SlotStorage", "liveBitmaps", long[].class));
    assertFalse(hasNestedField("SlotStorage", "writerWordChunks", long[][].class));
    assertFalse(hasField("weakSlots", Object.class));
    assertFalse(hasField("collectedReaders", Object.class));

    ReaderRegistry registry = newRegistry();
    int index = registry.register(new ReaderSlot());
    registry.setEpoch(index, 7L);
    registry.setValueEpoch(index, 7L);
    registry.setWriterActive(index, true);

    assertEquals(registry.minActiveEpoch(), 7L);
    assertEquals(registry.minActiveValueEpoch(), 7L);
    assertEquals(
        registry.readerState(index),
        ReaderRegistry.VALUE_PROTECTION_BIT | 7L);
    assertTrue(registry.hasActiveWriter());

    registry.setEpoch(index, 0L);
    registry.setValueEpoch(index, 0L);
    assertEquals(registry.readerState(index), 0L);
    assertTrue(registry.hasActiveWriter());
    registry.setWriterActive(index, false);
    assertEquals(registry.readerState(index), 0L);
    assertEquals(registry.minActiveEpoch(), Long.MAX_VALUE);
    assertEquals(registry.minActiveValueEpoch(), Long.MAX_VALUE);
  }

  @Test
  public void readerStateRejectsSentinelEpochAndAllowsIndependentWriterAdmission() {
    ReaderRegistry registry = newRegistry();
    int index = registry.register(new ReaderSlot());

    registry.setReaderState(
        index, ReaderRegistry.VALUE_PROTECTION_BIT | ReaderRegistry.MAX_READER_EPOCH);
    assertEquals(
        registry.readerState(index),
        ReaderRegistry.VALUE_PROTECTION_BIT | ReaderRegistry.MAX_READER_EPOCH);

    registry.setReaderState(index, 0L);
    registry.setWriterActive(index, true);
    assertEquals(registry.readerState(index), 0L);
    assertTrue(registry.hasActiveWriter());
    try {
      registry.setReaderState(index, ReaderRegistry.VALUE_PROTECTION_BIT);
      fail("value protection must carry a non-zero epoch");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
    try {
      registry.setReaderState(index, ReaderRegistry.EPOCH_MASK);
      fail("the all-ones epoch is reserved as the scan sentinel");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void readerStatePublicationDoesNotTouchWriterAdmission() {
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    registry.register(slot);

    registry.setWriterActive(slot.registryIndex, true);
    registry.setValueEpoch(slot, 7L);
    registry.setReaderState(slot, 7L);

    assertEquals(registry.readerState(slot), 7L);
    assertTrue(
        registry.hasActiveWriter(),
        "reader publication must not clear an independently active writer admission");
  }

  @Test
  public void readerNotificationRequiresAnActorMarkAndResetsWithTheSlot() {
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    int index = registry.register(slot);

    registry.setEpoch(index, 7L);
    assertFalse(registry.consumeReaderNotification(slot));
    assertEquals(registry.markActiveReaderNotifications(), 1);
    assertTrue(registry.consumeReaderNotification(slot));
    assertFalse(registry.consumeReaderNotification(slot));

    registry.markActiveReaderNotifications();
    registry.clear();
    assertFalse(registry.consumeReaderNotification(slot));
  }

  @Test
  public void registrationPublishesStrongMetadataThroughTheLiveBitmap() {
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();

    int index = registry.register(slot);
    registry.register(slot);

    assertEquals(registry.registeredCount(), 1);
    assertSame(registry.slotAt(index), slot);
    assertTrue(registry.isLive(index));
    assertEquals(registry.firstLiveSlot(0, registry.slotCapacity()), index);
    assertSame(slot.owner, Thread.currentThread());
  }

  @Test(timeOut = 5_000L)
  public void terminatedOwnerIsRemovedAndItsIndexIsReusedByTheActor() throws Exception {
    ReaderRegistry registry = newRegistry();
    AtomicReference<ReaderSlot> slotReference = new AtomicReference<>();
    AtomicReference<Integer> indexReference = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              ReaderSlot slot = new ReaderSlot();
              slotReference.set(slot);
              indexReference.set(registry.register(slot));
            });
    owner.start();
    owner.join();

    ReaderSlot terminated = slotReference.get();
    int index = indexReference.get();
    ReaderRegistry.SlotTableSnapshot slots = registry.slotTableSnapshot();
    slots.consumedHits(index, 7L);
    slots.consumedMisses(index, 9L);
    assertTrue(registry.isTerminated(terminated));
    assertEquals(registry.detachTerminated(index), null);
    assertEquals(registry.registeredCount(), 0);
    assertFalse(registry.isLive(index));
    assertEquals(slots.consumedHits(index), 0L);
    assertEquals(slots.consumedMisses(index), 0L);
    assertEquals(slots.slotAt(index), null);

    ReaderSlot replacement = new ReaderSlot();
    assertEquals(registry.register(replacement), index);
    assertSame(slots.slotAt(index), replacement);
    assertSame(replacement.owner, Thread.currentThread());
  }

  @Test
  public void bitmapScansSkipReleasedHoles() throws Exception {
    ReaderRegistry registry = newRegistry();
    ReaderSlot first = terminatedSlot(registry);
    int firstIndex = first.registryIndex;
    ReaderSlot second = terminatedSlot(registry);
    int secondIndex = second.registryIndex;
    ReaderSlot live = new ReaderSlot();
    int liveIndex = registry.register(live);

    assertEquals(registry.detachTerminated(firstIndex), null);
    assertEquals(registry.detachTerminated(secondIndex), null);
    assertTrue(registry.firstLiveSlot(0, registry.slotCapacity()) == liveIndex);
    assertEquals(registry.registeredCount(), 1);
  }

  @Test
  public void minimumActiveEpochIsSharedByEveryRetirementLane() {
    ReaderRegistry registry = newRegistry();
    ReaderSlot older = new ReaderSlot();
    ReaderSlot newer = new ReaderSlot();
    int olderIndex = registry.register(older);
    int newerIndex = registry.register(newer);

    registry.setEpoch(olderIndex, 7L);
    registry.setEpoch(newerIndex, 9L);
    assertEquals(registry.minActiveEpoch(), 7L);

    registry.setEpoch(olderIndex, 0L);
    assertEquals(registry.minActiveEpoch(), 9L);

    registry.setEpoch(newerIndex, 0L);
    assertEquals(registry.minActiveEpoch(), Long.MAX_VALUE);
  }

  @Test
  public void minimumEpochSnapshotReturnsLookupAndValueEpochsInOnePass() {
    ReaderRegistry registry = newRegistry();
    ReaderSlot lookupOnly = new ReaderSlot();
    ReaderSlot valueReader = new ReaderSlot();
    int lookupIndex = registry.register(lookupOnly);
    int valueIndex = registry.register(valueReader);

    registry.setEpoch(lookupIndex, 7L);
    registry.setValueEpoch(valueIndex, 11L);
    long[] minimums = new long[2];
    registry.minActiveEpochs(minimums);

    assertEquals(minimums[0], 7L);
    assertEquals(minimums[1], 11L);
  }

  @Test(timeOut = 5_000L)
  public void minimumEpochsDoNotWaitForRegistrationLock() throws Exception {
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    int index = registry.register(slot);
    registry.setEpoch(index, 7L);
    Field lockField = ReaderRegistry.class.getDeclaredField("registrationLock");
    lockField.setAccessible(true);
    Object lock = lockField.get(registry);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      synchronized (lock) {
        Future<Long> minimum = executor.submit(registry::minActiveEpoch);
        assertEquals(minimum.get(1L, TimeUnit.SECONDS).longValue(), 7L);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeOut = 5_000L)
  public void epochPublicationDoesNotWaitForRegistrationLock() throws Exception {
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    Field lockField = ReaderRegistry.class.getDeclaredField("registrationLock");
    lockField.setAccessible(true);
    Object lock = lockField.get(registry);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      int index = executor.submit(() -> registry.register(slot)).get(1L, TimeUnit.SECONDS);
      synchronized (lock) {
        Future<?> publication = executor.submit(() -> registry.setEpoch(index, 7L));
        publication.get(250L, TimeUnit.MILLISECONDS);
        assertEquals(registry.epoch(index), 7L);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeOut = 10_000L)
  public void registrationAndBitmapPublicationRemainStableDuringExpansion() throws Exception {
    ReaderRegistry registry = newRegistry();
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
            int capacity = registry.slotCapacity();
            for (int index = 0; index < capacity; index += ReaderRegistry.SLOT_CHUNK_SIZE) {
              long bits = registry.liveBitmap(index >> ReaderRegistry.SLOT_CHUNK_SHIFT);
              while (bits != 0L) {
                int offset = Long.numberOfTrailingZeros(bits);
                assertTrue(registry.slotAt(index + offset) != null);
                bits &= bits - 1L;
              }
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
    assertEquals(registry.registeredCount(), 8_000);
  }

  @Test
  public void statePublicationSurvivesSlotTableExpansion() throws Exception {
    ReaderRegistry registry = newRegistry();
    ReaderSlot first = new ReaderSlot();
    int firstIndex = registry.register(first);
    registry.setEpoch(firstIndex, 7L);
    registry.setValueEpoch(firstIndex, 7L);
    registry.setWriterActive(firstIndex, true);
    long[] stateChunksBefore = wordChunkAddresses(registry);

    List<ReaderSlot> retainedSlots = new ArrayList<>();
    for (int index = 0; index < 64; index++) {
      retainedSlots.add(new ReaderSlot());
      registry.register(retainedSlots.get(index));
    }

    long[] stateChunksAfter = wordChunkAddresses(registry);
    assertEquals(stateChunksAfter[0], stateChunksBefore[0]);
    assertEquals(first.readerStateAddress, stateChunksBefore[0]);
    assertEquals(first.readerStateAddress & 63L, 0L);
    assertEquals(stateChunksAfter.length, 2);
    assertEquals(registry.minActiveEpoch(), 7L);
    assertEquals(registry.minActiveValueEpoch(), 7L);
    assertTrue(registry.hasActiveWriter());
    assertTrue(registry.liveBitmap(1) != 0L);
  }

  @Test
  public void consumedCountersShareExistingChunksAcrossSlotTableExpansion() throws Exception {
    ReaderRegistry registry = newRegistry();
    ReaderSlot first = new ReaderSlot();
    int firstIndex = registry.register(first);
    ReaderRegistry.SlotTableSnapshot snapshotBefore = registry.slotTableSnapshot();
    snapshotBefore.consumedHits(firstIndex, 17L);
    snapshotBefore.consumedMisses(firstIndex, 19L);
    long[][] hitChunksBefore = counterChunks(registry, "consumedHitChunks");
    long[][] missChunksBefore = counterChunks(registry, "consumedMissChunks");

    for (int index = 0; index < ReaderRegistry.SLOT_CHUNK_SIZE; index++) {
      registry.register(new ReaderSlot());
    }

    long[][] hitChunksAfter = counterChunks(registry, "consumedHitChunks");
    long[][] missChunksAfter = counterChunks(registry, "consumedMissChunks");
    assertSame(hitChunksAfter[0], hitChunksBefore[0]);
    assertSame(missChunksAfter[0], missChunksBefore[0]);
    ReaderRegistry.SlotTableSnapshot snapshotAfter = registry.slotTableSnapshot();
    assertEquals(snapshotBefore.consumedHits(firstIndex), 17L);
    assertEquals(snapshotBefore.consumedMisses(firstIndex), 19L);
    assertEquals(snapshotAfter.consumedHits(firstIndex), 17L);
    assertEquals(snapshotAfter.consumedMisses(firstIndex), 19L);
    assertEquals(snapshotBefore.slotCapacity(), ReaderRegistry.SLOT_CHUNK_SIZE);
    assertEquals(snapshotBefore.slotAt(ReaderRegistry.SLOT_CHUNK_SIZE), null);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void closedRegistryRejectsNewStrongSlot() {
    ReaderRegistry registry = newRegistry();
    registry.clear();
    registry.register(new ReaderSlot());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void closeRequiresAllReaderSlotsToBeCleared() {
    ReaderRegistry registry = newRegistry();
    registry.register(new ReaderSlot());
    registry.close();
  }


  @Test
  public void ownerAssertionAllowsCloseAfterSlotValidation() throws Exception {
    assertTrue(ReaderRegistry.class.desiredAssertionStatus(), "owner checks require -ea");
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    registry.register(slot);
    assertSame(slot.registry, registry);
    assertTrue(slot.readerStateAddress != 0L);
    Method checkOwner = ReaderRegistry.class.getDeclaredMethod("assertOwner", ReaderSlot.class);
    checkOwner.setAccessible(true);

    registry.clear();
    assertNull(slot.owner);
    // Resume at the owner check after close invalidated the already-validated slot.
    // Reflection keeps this interleaving deterministic without a hook in the read hot path.
    checkOwner.invoke(null, slot);
    expectThrows(
        IllegalArgumentException.class, () -> registry.setReaderStateKnownEpoch(slot, 7L));
    assertEquals(registry.minActiveEpoch(), Long.MAX_VALUE);
  }

  @Test(timeOut = 10_000L)
  public void statePublicationDuringRegistryClearRemainsAReaderRejection() throws Exception {
    AtomicReference<Throwable> unexpected = new AtomicReference<>();
    for (int round = 0; round < 200 && unexpected.get() == null; round++) {
      ReaderRegistry registry = newRegistry();
      AtomicReference<ReaderSlot> slotReference = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      Thread publisher =
          new Thread(
              () -> {
                ReaderSlot slot = new ReaderSlot();
                registry.register(slot);
                slotReference.set(slot);
                started.countDown();
                while (slot.registry == registry) {
                  try {
                    registry.setReaderStateKnownEpoch(slot, 7L);
                  } catch (IllegalArgumentException expected) {
                    // A clear racing with publication must be reported as a rejected reader.
                  } catch (Throwable failure) {
                    unexpected.compareAndSet(null, failure);
                    return;
                  }
                }
              });
      publisher.start();
      assertTrue(started.await(1L, TimeUnit.SECONDS));
      LockSupport.parkNanos(100_000L);
      registry.clear();
      publisher.join(1_000L);
      assertFalse(publisher.isAlive(), "publisher must stop after the registry is cleared");
      assertNull(slotReference.get().registry);
    }
    assertNull(unexpected.get(), "close racing with trusted publication must not leak an error");
  }

  @Test
  public void ownerAssertionAllowsCloseAfterStorageSnapshot() throws Exception {
    assertTrue(ReaderRegistry.class.desiredAssertionStatus(), "owner checks require -ea");
    ReaderRegistry registry = newRegistry();
    int index = registry.register(new ReaderSlot());
    Field storageField = ReaderRegistry.class.getDeclaredField("storage");
    storageField.setAccessible(true);
    Object capturedStorage = storageField.get(registry);
    Method checkOwner =
        ReaderRegistry.class.getDeclaredMethod("assertOwner", capturedStorage.getClass(), int.class);
    checkOwner.setAccessible(true);

    registry.clear();
    // An index-based publisher can retain the old storage while close removes its slot.
    checkOwner.invoke(null, capturedStorage, index);
    assertEquals(registry.minActiveEpoch(), Long.MAX_VALUE);
  }

  @Test
  public void statePublicationStillRejectsAnotherLiveOwner() throws Exception {
    assertTrue(ReaderRegistry.class.desiredAssertionStatus(), "owner checks require -ea");
    ReaderRegistry registry = newRegistry();
    ReaderSlot slot = new ReaderSlot();
    int index = registry.register(slot);
    registry.setReaderState(slot, ReaderRegistry.VALUE_PROTECTION_BIT | 7L);
    registry.setWriterAdmission(slot, true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor
          .submit(
              () -> {
                expectThrows(
                    AssertionError.class, () -> registry.setReaderStateKnownEpoch(slot, 0L));
              })
          .get(10, TimeUnit.SECONDS);
      assertEquals(
          registry.readerState(slot),
          ReaderRegistry.VALUE_PROTECTION_BIT | 7L);
    } finally {
      executor.shutdownNow();
      registry.clear();
    }
  }

  @Test
  public void clearKeepsNativeChunksUntilAnIdempotentClose() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    ReaderRegistry registry = new ReaderRegistry(memory);
    long allocated = memory.allocated();
    ReaderSlot slot = new ReaderSlot();
    registry.register(slot);

    registry.clear();
    assertEquals(memory.allocated(), allocated);
    assertEquals(slot.readerStateAddress, 0L);

    registry.close();
    assertEquals(memory.allocated(), 0L);
    registry.close();
    assertEquals(memory.allocated(), 0L);
    memory.closeArenas();
  }

  private static ReaderSlot terminatedSlot(ReaderRegistry registry) throws Exception {
    AtomicReference<ReaderSlot> reference = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              ReaderSlot slot = new ReaderSlot();
              registry.register(slot);
              reference.set(slot);
            });
    owner.start();
    owner.join();
    return reference.get();
  }

  private static boolean hasField(String name, Class<?> type) {
    try {
      Field field = ReaderRegistry.class.getDeclaredField(name);
      return field.getType() == type;
    } catch (NoSuchFieldException missing) {
      return false;
    }
  }

  private static boolean hasNestedField(String nestedName, String fieldName, Class<?> type) {
    try {
      Field field = nestedClass(nestedName).getDeclaredField(fieldName);
      return field.getType() == type;
    } catch (NoSuchFieldException missing) {
      return false;
    }
  }

  private static Class<?> nestedClass(String name) {
    for (Class<?> nested : ReaderRegistry.class.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(name)) {
        return nested;
      }
    }
    throw new AssertionError("missing nested class: " + name);
  }

  private static long[] wordChunkAddresses(ReaderRegistry registry) throws Exception {
    Field storageField = ReaderRegistry.class.getDeclaredField("storage");
    storageField.setAccessible(true);
    Object storage = storageField.get(registry);
    Field field = storage.getClass().getDeclaredField("stateChunkAddresses");
    field.setAccessible(true);
    return (long[]) field.get(storage);
  }

  private static long[][] counterChunks(ReaderRegistry registry, String fieldName)
      throws Exception {
    Field storageField = ReaderRegistry.class.getDeclaredField("storage");
    storageField.setAccessible(true);
    Object storage = storageField.get(registry);
    Field field = storage.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (long[][]) field.get(storage);
  }
}
