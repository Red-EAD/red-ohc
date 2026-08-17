package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;

public final class WeakValuesTest {
  private static final CacheSerializer<String> STRING = new StringSerializer();

  @Test
  public void weakHitReturnsThePutObjectWithoutDeserializing() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OHCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));
      int deserializationsAfterPut = serializer.deserializeCalls.get();
      int serializationsAfterPut = serializer.serializeCalls.get();
      int sizeCallsAfterPut = serializer.serializedSizeCalls.get();

      assertSame(cache.get("key"), value);
      assertEquals(serializer.deserializeCalls.get(), deserializationsAfterPut);
      assertTrue(serializer.serializeCalls.get() > serializationsAfterPut);
      assertEquals(serializer.serializedSizeCalls.get(), sizeCallsAfterPut);
      Map<String, Pojo> values = cache.getAll(Collections.singleton("key"));
      assertSame(values.get("key"), value);
      assertEquals(serializer.deserializeCalls.get(), deserializationsAfterPut);
    }
  }

  @Test
  public void reusedFingerprintScratchRestoresBigEndianByteOrder() {
    ByteOrderChangingPojoSerializer serializer = new ByteOrderChangingPojoSerializer();
    try (OHCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("value");
      assertTrue(cache.put("key", value));

      assertSame(cache.get("key"), value);
      assertSame(cache.get("key"), value);
      assertEquals(serializer.firstValidationOrder.get(), ByteOrder.BIG_ENDIAN);
      assertEquals(serializer.secondValidationOrder.get(), ByteOrder.BIG_ENDIAN);
      assertEquals(serializer.deserializations.get(), 0);
    }
  }

  @Test
  public void mutatedWeakValueFallsBackOnceAndQuarantinesThePublication() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));
      value.text = "other";

      int serializeCallsBeforeDirtyRead = serializer.serializeCalls.get();
      int sizeCallsBeforeDirtyRead = serializer.serializedSizeCalls.get();
      Pojo firstFallback = cache.get("key");
      assertEquals(firstFallback.text, "first");
      assertNotSame(firstFallback, value);
      assertTrue(serializer.deserializeCalls.get() > 0);

      Entry entry =
          cache.dataForTest().values().stream()
              .filter(candidate -> candidate.nativeKeyAddress != 0L)
              .findFirst()
              .get();
      assertTrue(entry.valueState().fingerprintDisabled());
      assertEquals(entry.weakValueSlot(), null);
      int serializeCallsAfterDirtyRead = serializer.serializeCalls.get();
      int sizeCallsAfterDirtyRead = serializer.serializedSizeCalls.get();
      assertTrue(serializer.serializeCalls.get() > serializeCallsBeforeDirtyRead);
      assertEquals(serializer.serializedSizeCalls.get(), sizeCallsBeforeDirtyRead);

      Pojo secondFallback = cache.get("key");
      assertEquals(secondFallback.text, "first");
      assertEquals(serializer.serializeCalls.get(), serializeCallsAfterDirtyRead);
      assertEquals(serializer.serializedSizeCalls.get(), sizeCallsAfterDirtyRead);
    }
  }

  @Test
  public void shorterValidationSerializationFallsBackAndQuarantinesThePublication() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OHCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));
      value.text = "x";

      Pojo loaded = cache.get("key");
      assertEquals(loaded.text, "first");
      assertNotSame(loaded, value);
      assertTrue(liveEntry((OffHeapCache<?, ?>) cache).valueState().fingerprintDisabled());
    }
  }

  @Test
  public void lengthMismatchDoesNotComputeNativeFingerprintBeforeSerialization() {
    NativeFingerprintOrderSerializer serializer = new NativeFingerprintOrderSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));

      Entry entry = liveEntry(cache);
      serializer.entry.set(entry);
      serializer.observeValidation = true;
      value.text = "x";

      Pojo loaded = cache.get("key");
      assertNotSame(loaded, value);
      assertEquals(serializer.fingerprintReadyAtSerialize.get(), Boolean.FALSE);
      assertTrue(entry.valueState().fingerprintDisabled());
    }
  }

  @Test
  public void serializationOverflowDoesNotComputeNativeFingerprintBeforeSerialization() {
    NativeFingerprintOrderSerializer serializer = new NativeFingerprintOrderSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));

      Entry entry = liveEntry(cache);
      serializer.entry.set(entry);
      serializer.observeValidation = true;
      value.text = "longer";

      Pojo loaded = cache.get("key");
      assertNotSame(loaded, value);
      assertEquals(serializer.fingerprintReadyAtSerialize.get(), Boolean.FALSE);
      assertTrue(entry.valueState().fingerprintDisabled());
    }
  }

  @Test
  public void longerValidationSerializationOverflowsBoundAndQuarantinesThePublication() {
    ByteArraySerializer serializer = new ByteArraySerializer();
    byte[] value = new byte[256];
    try (OffHeapCache<String, byte[]> cache = newByteArrayCache(serializer)) {
      assertTrue(cache.put("key", value));
      serializer.appendExtraByte = true;

      byte[] loaded = cache.get("key");
      assertNotSame(loaded, value);
      assertTrue(liveEntry(cache).valueState().fingerprintDisabled());
      int serializationsAfterFailure = serializer.serializeCalls.get();

      cache.get("key");
      assertEquals(serializer.serializeCalls.get(), serializationsAfterFailure);
    }
  }

  @Test
  public void replacementRearmsWeakReuseAfterDirtyQuarantine() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo original = new Pojo("original");
      assertTrue(cache.put("key", original));
      original.text = "changed";
      assertNotSame(cache.get("key"), original);

      Entry entry = liveEntry(cache);
      assertTrue(entry.valueState().fingerprintDisabled());

      Pojo replacement = new Pojo("replacement");
      assertTrue(cache.put("key", replacement));
      assertFalse(entry.valueState().fingerprintDisabled());
      int deserializationsAfterReplacement = serializer.deserializeCalls.get();

      assertSame(cache.get("key"), replacement);
      assertEquals(serializer.deserializeCalls.get(), deserializationsAfterReplacement);
    }
  }

  @Test
  public void validationSizeFailureDoesNotAffectWeakReuse() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("value");
      assertTrue(cache.put("key", value));
      serializer.failSerializedSize = true;

      assertSame(cache.get("key"), value);
      Entry entry = liveEntry(cache);
      assertFalse(entry.valueState().fingerprintDisabled());
      assertEquals(serializer.deserializeCalls.get(), 0);
      assertEquals(serializer.serializedSizeCalls.get(), 1);
    }
  }

  @Test
  public void validationSerializeFailureQuarantinesThePublication() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("value");
      assertTrue(cache.put("key", value));
      serializer.failSerialize = true;

      Pojo firstFallback = cache.get("key");
      assertNotSame(firstFallback, value);
      Entry entry = liveEntry(cache);
      assertTrue(entry.valueState().fingerprintDisabled());
      int serializeCallsAfterFailure = serializer.serializeCalls.get();
      int sizeCallsAfterFailure = serializer.serializedSizeCalls.get();

      serializer.failSerialize = false;
      cache.get("key");
      assertEquals(serializer.serializeCalls.get(), serializeCallsAfterFailure);
      assertEquals(serializer.serializedSizeCalls.get(), sizeCallsAfterFailure);
    }
  }

  @Test(timeOut = 10_000)
  public void replacementCannotBeQuarantinedByAnInFlightValidation() throws Exception {
    BlockingValidationSerializer serializer = new BlockingValidationSerializer();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo first = new Pojo("first");
      assertTrue(cache.put("key", first));
      first.text = "other";
      serializer.blockOnInvocation = serializer.serializeInvocations.get() + 1;

      Future<Pojo> inFlight = executor.submit(() -> cache.get("key"));
      assertTrue(serializer.validationEntered.await(5, TimeUnit.SECONDS));

      Pojo replacement = new Pojo("replacement");
      assertTrue(cache.put("key", replacement));
      serializer.allowValidation.countDown();

      assertEquals(inFlight.get(5, TimeUnit.SECONDS).text, "first");
      Entry entry = liveEntry(cache);
      assertFalse(entry.valueState().fingerprintDisabled());
      assertSame(cache.get("key"), replacement);
    } finally {
      serializer.allowValidation.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void fingerprintUsesCrc32cAt256BytesAndAdler32After256Bytes() {
    assertFingerprint(0, new CRC32C());
    assertFingerprint(256, new CRC32C());
    assertFingerprint(257, new Adler32());
    assertFingerprint(64 * 1024, new Adler32());
  }

  @Test
  public void largeValuesUseSharedFingerprintScratchPool() {
    ByteArraySerializer serializer = new ByteArraySerializer();
    byte[] value = new byte[128 * 1024];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) index;
    }
    try (OffHeapCache<String, byte[]> cache = newByteArrayCache(serializer)) {
      assertTrue(cache.put("key", value));
      byte[] loaded = cache.get("key");
      assertSame(loaded, value);
      Entry entry = liveEntry(cache);
      assertTrue(entry.valueState().fingerprintReady());
      assertEquals(serializer.deserializeCalls.get(), 0);
    }
  }

  @Test
  public void valuesAboveFourMiBScratchLimitDisableWeakReuse() {
    ByteArraySerializer serializer = new ByteArraySerializer();
    byte[] value = new byte[4 * 1024 * 1024 + 1];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) index;
    }
    try (OffHeapCache<String, byte[]> cache = newByteArrayCache(serializer)) {
      assertTrue(cache.put("key", value));
      int serializeCallsAfterPut = serializer.serializeCalls.get();

      byte[] loaded = cache.get("key");
      assertNotSame(loaded, value);
      Entry entry = liveEntry(cache);
      assertTrue(entry.valueState().fingerprintDisabled());
      assertEquals(serializer.serializeCalls.get(), serializeCallsAfterPut);
      assertTrue(serializer.deserializeCalls.get() > 0);
    }
  }

  @Test(timeOut = 10_000)
  public void maintenanceClearsGcWeakSlotButKeepsNativeEntryAndFingerprint() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      WeakReference<Pojo> referent = putAndValidateThenDrop(cache);
      Entry entry = liveEntry(cache);
      assertTrue(entry.valueState().fingerprintReady());

      awaitCleared(referent);
      cache.flushAsync().join();

      assertNull(entry.weakValueSlot());
      assertTrue(entry.valueAddress != 0L);
      assertTrue(entry.valueState().fingerprintReady());

      Pojo loaded = cache.get("key");
      assertEquals(loaded.text, "value");
      assertTrue(entry.weakValueSlot() != null);
      assertTrue(entry.valueState().fingerprintReady());
    }
  }

  @Test(timeOut = 5_000)
  public void idleMaintenanceEventuallyClearsQueuedWeakSlotWithoutExplicitFlush() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("value");
      assertTrue(cache.put("key", value));
      Entry entry = liveEntry(cache);
      Entry.WeakValueSlot slot = entry.weakValueSlot();
      slot.clear();
      assertTrue(slot.enqueue());

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (entry.weakValueSlot() != null && System.nanoTime() < deadline) {
        try {
          Thread.sleep(10L);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interrupted);
        }
      }
      assertNull(entry.weakValueSlot());
      assertTrue(entry.valueAddress != 0L);
    }
  }

  @Test
  public void queuedOldWeakSlotCannotClearReplacementSlot() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo first = new Pojo("first");
      assertTrue(cache.put("key", first));
      Entry entry = liveEntry(cache);
      Entry.WeakValueSlot oldSlot = entry.weakValueSlot();
      oldSlot.clear();
      assertTrue(oldSlot.enqueue());

      Pojo replacement = new Pojo("replacement");
      assertTrue(cache.put("key", replacement));
      Entry.WeakValueSlot replacementSlot = entry.weakValueSlot();
      cache.flushAsync().join();

      assertSame(entry.weakValueSlot(), replacementSlot);
      assertSame(cache.get("key"), replacement);
    }
  }

  @Test
  public void queuedWeakSlotAfterRemovalCannotRepopulateTheEntry() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("value");
      assertTrue(cache.put("key", value));
      Entry entry = liveEntry(cache);
      Entry.WeakValueSlot slot = entry.weakValueSlot();
      slot.clear();
      assertTrue(slot.enqueue());

      assertTrue(cache.remove("key"));
      cache.flushAsync().join();

      assertFalse(cache.containsKey("key"));
      assertNull(entry.weakValueSlot());
    }
  }

  @Test
  public void getAllUsesValidatedWeakReadPath() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo weak = new Pojo("weak");
      Pojo nativeValue = new Pojo("native");
      assertTrue(cache.put("weak", weak));
      assertTrue(cache.put("native", nativeValue));
      cache.dataForTest().values().stream()
          .filter(entry -> entry.nativeKeyAddress != 0L)
          .filter(entry -> entry.weakValueSlot() != null)
          .filter(entry -> entry.weakValueSlot().get() == nativeValue)
          .forEach(Entry::clearWeakValueSlot);
      int serializeCallsAfterPut = serializer.serializeCalls.get();
      int deserializeCallsAfterPut = serializer.deserializeCalls.get();

      Map<String, Pojo> result = cache.getAll(java.util.Arrays.asList("weak", "native"));
      assertSame(result.get("weak"), weak);
      assertEquals(result.get("native").text, "native");
      assertTrue(serializer.serializeCalls.get() > serializeCallsAfterPut);
      assertTrue(serializer.deserializeCalls.get() > deserializeCallsAfterPut);
    }
  }

  @Test
  public void getAllCanMixWeakHitsAndNativeFallbacks() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      assertTrue(cache.put("weak", new Pojo("weak-hit")));
      assertTrue(cache.put("native", new Pojo("native-fallback")));
      cache.dataForTest().values().forEach(Entry::clearWeakValueSlot);
      Pojo weakHit = cache.get("weak");

      Map<String, Pojo> result = cache.getAll(java.util.Arrays.asList("weak", "native"));
      assertSame(result.get("weak"), weakHit);
      assertEquals(result.get("native").text, "native-fallback");
      assertTrue(serializer.deserializeCalls.get() >= 1);
    }
  }

  @Test
  public void replacementAndRemovalInvalidateTheOldWeakValue() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OHCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo first = new Pojo("first");
      Pojo second = new Pojo("second");
      assertTrue(cache.put("key", first));
      assertTrue(cache.put("key", second));
      assertSame(cache.get("key"), second);
      assertNotSame(cache.get("key"), first);
      assertTrue(cache.remove("key"));
      assertFalse(cache.containsKey("key"));
      assertEquals(cache.get("key"), null);
    }
  }

  @Test
  public void failedConditionalReplacementKeepsTheOldWeakValue() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo old = new Pojo("old");
      Pojo replacement = new Pojo("replacement");
      assertTrue(cache.put("key", old));
      assertFalse(cache.replaceAsync("key", new Pojo("wrong"), replacement, 0L).join());
      assertSame(cache.get("key"), old);
    }
  }

  @Test
  public void clearingTheWeakReferenceFallsBackToNativeDeserialization() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));
      cache.dataForTest().values().forEach(entry -> entry.clearWeakValueSlot());

      Pojo loaded = cache.get("key");
      assertEquals(loaded.text, "first");
      assertNotSame(loaded, value);
      assertTrue(serializer.deserializeCalls.get() > 0);
    }
  }

  @Test
  public void explicitByteBufferSerializerIsRejectedWhenWeakValuesAreEnabled() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            OHCacheBuilder.<String, ByteBuffer>newBuilder()
                .keySerializer(STRING)
                .valueSerializer(new ByteBufferSerializer())
                .weakValues(true)
                .build());
  }

  @Test
  public void weakValuesRemainDisabledByDefault() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OHCache<String, Pojo> cache = newPojoCache(serializer, false)) {
      Pojo value = new Pojo("first");
      assertTrue(cache.put("key", value));
      assertEquals(cache.get("key").text, "first");
      assertTrue(serializer.deserializeCalls.get() > 0);
    }
  }

  @Test
  public void disabledWeakValuesDoNotCreateWeakSlots() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer, false)) {
      assertTrue(cache.put("key", new Pojo("value")));
      assertTrue(
          cache.dataForTest().values().stream()
              .filter(entry -> entry.nativeKeyAddress != 0L)
              .allMatch(entry -> entry.weakValueSlot() == null));
    }
  }

  @Test
  public void weakValueBackfillRejectsAStaleStateEvenWhenTheAddressIsReused() {
    long address = 8L;
    Entry entry = new Entry(0L, 0, 0, address);
    entry.initializeValueState(address, null);
    Entry.ValueState observed = entry.valueState();

    entry.publishValueState(address, null);

    Entry.WeakValueSlot staleSlot = new Entry.WeakValueSlot("old", address);
    Entry.ValueState staleUpdate = new Entry.ValueState(address, staleSlot);
    assertFalse(entry.compareAndSetValueState(observed, staleUpdate));
    assertEquals(entry.valueState().weakValue(), null);
  }

  @Test
  public void failedValueStatePublicationLeavesThePreviousStateIntact() {
    long previousAddress = 8L;
    Entry entry = new Entry(0L, 0, 0, previousAddress);
    entry.initializeValueState(previousAddress, null);
    Entry.ValueState previousState = entry.valueState();

    Entry.WeakValueSlot invalidSlot = new Entry.WeakValueSlot("value", previousAddress);
    expectThrows(
        IllegalArgumentException.class, () -> entry.publishValueState(0L, invalidSlot));

    assertEquals(entry.valueAddress, previousAddress);
    assertSame(entry.valueState(), previousState);
  }

  @Test(timeOut = 10_000)
  public void replacementCannotBeOverwrittenByAnInFlightNativeRead() throws Exception {
    BlockingPojoSerializer serializer = new BlockingPojoSerializer();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo first = new Pojo("first");
      Pojo second = new Pojo("second");
      assertTrue(cache.put("key", first));
      cache.dataForTest().values().forEach(entry -> entry.clearWeakValueSlot());

      Future<Pojo> inFlight = executor.submit(() -> cache.get("key"));
      assertTrue(serializer.deserializeEntered.await(5, TimeUnit.SECONDS));
      assertTrue(cache.put("key", second));
      serializer.allowDeserialize.countDown();

      assertEquals(inFlight.get(5, TimeUnit.SECONDS).text, "first");
      assertSame(cache.get("key"), second);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void weakValuesBindPutIfAbsentReplaceAndLoaderResults() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache =
        OHCacheBuilder.<String, Pojo>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .weakValues(true)
            .loaderExecutor(Runnable::run)
            .buildTyped()) {
      Pojo first = new Pojo("first");
      assertTrue(cache.putIfAbsentAsync("key", first, 0L).join());
      assertSame(cache.get("key"), first);

      Pojo second = new Pojo("second");
      assertTrue(cache.replaceAsync("key", first, second, 0L).join());
      assertSame(cache.get("key"), second);

      Pojo loaded = new Pojo("loaded");
      assertEquals(cache.getOrLoadAsync("other", ignored -> loaded, 0L).join(), loaded);
      assertSame(cache.get("other"), loaded);
    }
  }

  @Test
  public void putAllBindsEachPublishedObjectToItsEntry() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      Pojo first = new Pojo("first");
      Pojo second = new Pojo("second");
      Map<String, Pojo> input = new LinkedHashMap<>();
      input.put("first", first);
      input.put("second", second);
      assertEquals(cache.putAll(input), 2);
      assertSame(cache.get("first"), first);
      assertSame(cache.get("second"), second);
    }
  }

  @Test
  public void weakValuesFalseKeepsOwnedByteBufferValuesWorking() {
    CacheSerializer<ByteBuffer> serializer = new OwnedByteBufferSerializer();
    ByteBuffer value = ByteBuffer.wrap(new byte[] {1, 2, 3});
    try (OHCache<String, ByteBuffer> cache =
        OHCacheBuilder.<String, ByteBuffer>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .weakValues(false)
            .build()) {
      assertTrue(cache.put("key", value));
      ByteBuffer loaded = cache.get("key");
      assertEquals(loaded.remaining(), 3);
      assertEquals(loaded.get(), (byte) 1);
    }
  }

  @Test
  public void byteBufferKeysDoNotTriggerValueWeakReferenceRestriction() {
    CacheSerializer<ByteBuffer> keySerializer = new OwnedByteBufferSerializer();
    try (OHCache<ByteBuffer, String> cache =
        OHCacheBuilder.<ByteBuffer, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(keySerializer)
            .valueSerializer(STRING)
            .weakValues(true)
            .build()) {
      ByteBuffer key = ByteBuffer.wrap(new byte[] {4, 5});
      assertTrue(cache.put(key, "value"));
      assertEquals(cache.get(ByteBuffer.wrap(new byte[] {4, 5})), "value");
    }
  }

  @Test
  public void rawSerializerRejectsByteBufferBeforeChangingAnExistingEntry() {
    @SuppressWarnings("unchecked")
    CacheSerializer<Object> serializer = (CacheSerializer<Object>) (CacheSerializer<?>) new RawSerializer();
    try (OffHeapCache<String, Object> cache =
        OHCacheBuilder.<String, Object>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .weakValues(true)
            .buildTyped()) {
      assertTrue(cache.put("key", "old"));
      IllegalArgumentException failure =
          expectThrows(IllegalArgumentException.class, () -> cache.put("key", ByteBuffer.allocate(4)));
      assertTrue(failure.getMessage().contains("does not support ByteBuffer values"));
      assertEquals(cache.get("key"), "old");
    }
  }

  @Test
  public void rawSerializerReturningByteBufferFailsAfterInvalidatingTheNativeView() {
    RawSerializer serializer = new RawSerializer();
    @SuppressWarnings("unchecked")
    CacheSerializer<Object> valueSerializer = (CacheSerializer<Object>) (CacheSerializer<?>) serializer;
    try (OffHeapCache<String, Object> cache =
        OHCacheBuilder.<String, Object>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(valueSerializer)
            .weakValues(true)
            .buildTyped()) {
      assertTrue(cache.put("key", "value"));
      cache.dataForTest().values().forEach(entry -> entry.clearWeakValueSlot());
      expectThrows(IllegalArgumentException.class, () -> cache.get("key"));
      ByteBuffer returned = serializer.lastDeserialized.get();
      assertTrue(returned != null);
      assertEquals(returned.capacity(), 0);
      assertEquals(cache.dataForTest().values().stream().filter(e -> e.nativeKeyAddress != 0L).count(), 1L);
    }
  }

  @Test
  public void ttlExpiryNeverUsesAStillLiveWeakReference() {
    AtomicLong now = new AtomicLong(1_000L);
    Ticker ticker = ticker(now);
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache =
        OHCacheBuilder.<String, Pojo>newBuilder()
            .capacity(1 << 20)
            .ticker(ticker)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .weakValues(true)
            .buildTyped()) {
      Pojo value = new Pojo("expires");
      assertTrue(cache.put("key", value, 2_000L));
      assertSame(cache.get("key"), value);
      now.set(3_000L);
      assertEquals(cache.get("key"), null);
      assertTrue(serializer.deserializeCalls.get() == 0);
    }
  }

  @Test
  public void serializerBackedReplacementPublishesTheNewWeakValue() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache = newPojoCache(serializer)) {
      assertTrue(cache.put("key", new Pojo("old")));
      Pojo replacement = new Pojo("new");
      assertTrue(cache.put("key", replacement));
      Pojo result = cache.get("key");
      assertSame(result, replacement);
      assertEquals(serializer.deserializeCalls.get(), 0);
    }
  }

  @Test
  public void closeClearsWeakSlotAndReclaimsNativeMemory() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    OffHeapCache<String, Pojo> cache = newPojoCache(serializer);
    assertTrue(cache.put("key", new Pojo("value")));
    Entry entry = cache.dataForTest().values().stream().filter(e -> e.nativeKeyAddress != 0L).findFirst().get();
    assertTrue(entry.weakValueSlot() != null);
    cache.close();
    assertEquals(entry.weakValueSlot(), null);
    assertEquals(cache.totalAllocatedBytes(), 0L);
  }

  @Test
  public void actorEvictionClearsTheRemovedEntryWeakSlot() {
    CountingPojoSerializer serializer = new CountingPojoSerializer();
    try (OffHeapCache<String, Pojo> cache =
        OHCacheBuilder.<String, Pojo>newBuilder()
            .capacity(256L)
            .keySerializer(STRING)
            .valueSerializer(serializer)
            .weakValues(true)
            .buildTyped()) {
      assertTrue(cache.put("one", new Pojo("value-one")));
      Entry first = cache.dataForTest().values().stream().filter(e -> e.nativeKeyAddress != 0L).findFirst().get();
      assertTrue(cache.put("two", new Pojo("value-two")));
      cache.flushAsync().join();
      assertTrue(cache.stats().evictionCount() > 0L);
      assertEquals(first.weakValueSlot(), null);
    }
  }

  private static Ticker ticker(AtomicLong now) {
    return new Ticker() {
      @Override
      public long nanos() {
        return now.get() * 1_000_000L;
      }

      @Override
      public long currentTimeMillis() {
        return now.get();
      }
    };
  }

  private static OffHeapCache<String, Pojo> newPojoCache(CountingPojoSerializer serializer) {
    return newPojoCache(serializer, true);
  }

  private static OffHeapCache<String, Pojo> newPojoCache(
      CountingPojoSerializer serializer, boolean weakValues) {
    return OHCacheBuilder.<String, Pojo>newBuilder()
        .capacity(1 << 20)
        .keySerializer(STRING)
        .valueSerializer(serializer)
        .weakValues(weakValues)
        .buildTyped();
  }

  private static OffHeapCache<String, byte[]> newByteArrayCache(ByteArraySerializer serializer) {
    return OHCacheBuilder.<String, byte[]>newBuilder()
        .capacity(8L << 20)
        .keySerializer(STRING)
        .valueSerializer(serializer)
        .weakValues(true)
        .buildTyped();
  }

  private static WeakReference<Pojo> putAndValidateThenDrop(OffHeapCache<String, Pojo> cache) {
    Pojo value = new Pojo("value");
    assertTrue(cache.put("key", value));
    assertSame(cache.get("key"), value);
    return new WeakReference<>(value);
  }

  private static void awaitCleared(WeakReference<?> reference) {
    for (int attempt = 0; attempt < 100 && reference.get() != null; attempt++) {
      System.gc();
      System.runFinalization();
      byte[] pressure = new byte[64 * 1024];
      pressure[0] = (byte) attempt;
      try {
        Thread.sleep(10L);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
    }
    assertNull(reference.get(), "test referent was not collected");
  }

  private static Entry liveEntry(OffHeapCache<?, ?> cache) {
    return cache.dataForTest().values().stream()
        .filter(entry -> entry.nativeKeyAddress != 0L)
        .findFirst()
        .orElseThrow(() -> new AssertionError("live entry missing"));
  }

  private static void assertFingerprint(int length, Checksum expected) {
    ByteArraySerializer serializer = new ByteArraySerializer();
    byte[] value = new byte[length];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (index * 31);
    }
    expected.update(value, 0, value.length);
    try (OffHeapCache<String, byte[]> cache = newByteArrayCache(serializer)) {
      assertTrue(cache.put("key", value));
      Entry entry =
          cache.dataForTest().values().stream()
              .filter(candidate -> candidate.nativeKeyAddress != 0L)
              .findFirst()
              .get();
      byte[] loaded = cache.get("key");
      assertSame(
          loaded,
          value,
          "length="
              + length
              + ", state="
              + entry.valueState().fingerprintState()
              + ", slot="
              + entry.weakValueSlot());
      assertTrue(entry.valueState().fingerprintReady());
      assertEquals(entry.valueState().fingerprint(), expected.getValue());
    }
  }

  private static final class Pojo {
    private String text;

    private Pojo(String text) {
      this.text = text;
    }
  }

  private static class CountingPojoSerializer implements CacheSerializer<Pojo> {
    private final AtomicInteger deserializeCalls = new AtomicInteger();
    private final AtomicInteger serializeCalls = new AtomicInteger();
    private final AtomicInteger serializedSizeCalls = new AtomicInteger();
    private boolean failSerialize;
    private boolean failSerializedSize;

    @Override
    public void serialize(Pojo value, ByteBuffer buffer) {
      serializeCalls.incrementAndGet();
      if (failSerialize) {
        throw new IllegalStateException("validation serialize failure");
      }
      byte[] bytes = value.text.getBytes(StandardCharsets.UTF_8);
      buffer.put(bytes);
    }

    @Override
    public Pojo deserialize(ByteBuffer buffer) {
      deserializeCalls.incrementAndGet();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return new Pojo(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public int serializedSize(Pojo value) {
      serializedSizeCalls.incrementAndGet();
      if (failSerializedSize) {
        throw new IllegalStateException("validation size failure");
      }
      return value.text.getBytes(StandardCharsets.UTF_8).length;
    }
  }

  private static final class NativeFingerprintOrderSerializer extends CountingPojoSerializer {
    private final AtomicReference<Entry> entry = new AtomicReference<>();
    private final AtomicReference<Boolean> fingerprintReadyAtSerialize = new AtomicReference<>();
    private volatile boolean observeValidation;

    @Override
    public void serialize(Pojo value, ByteBuffer buffer) {
      if (observeValidation) {
        Entry current = entry.get();
        if (current != null) {
          fingerprintReadyAtSerialize.set(current.valueState().fingerprintReady());
        }
      }
      super.serialize(value, buffer);
    }
  }

  private static final class ByteOrderChangingPojoSerializer extends CountingPojoSerializer {
    private final AtomicInteger validationCalls = new AtomicInteger();
    private final AtomicInteger deserializations = new AtomicInteger();
    private final AtomicReference<ByteOrder> firstValidationOrder = new AtomicReference<>();
    private final AtomicReference<ByteOrder> secondValidationOrder = new AtomicReference<>();

    @Override
    public void serialize(Pojo value, ByteBuffer buffer) {
      int call = validationCalls.incrementAndGet();
      if (call == 2) {
        firstValidationOrder.set(buffer.order());
        super.serialize(value, buffer);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return;
      }
      if (call == 3) {
        secondValidationOrder.set(buffer.order());
      }
      super.serialize(value, buffer);
    }

    @Override
    public Pojo deserialize(ByteBuffer buffer) {
      deserializations.incrementAndGet();
      return super.deserialize(buffer);
    }
  }

  private static final class BlockingValidationSerializer extends CountingPojoSerializer {
    private final CountDownLatch validationEntered = new CountDownLatch(1);
    private final CountDownLatch allowValidation = new CountDownLatch(1);
    private final AtomicInteger serializeInvocations = new AtomicInteger();
    private volatile int blockOnInvocation;

    @Override
    public void serialize(Pojo value, ByteBuffer buffer) {
      if (serializeInvocations.incrementAndGet() == blockOnInvocation) {
        validationEntered.countDown();
        try {
          if (!allowValidation.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("timed out waiting to release validation");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new AssertionError(interrupted);
        }
      }
      super.serialize(value, buffer);
    }
  }

  private static final class ByteArraySerializer implements CacheSerializer<byte[]> {
    private final AtomicInteger serializeCalls = new AtomicInteger();
    private final AtomicInteger deserializeCalls = new AtomicInteger();
    private final AtomicInteger serializedSizeCalls = new AtomicInteger();
    private boolean appendExtraByte;

    @Override
    public void serialize(byte[] value, ByteBuffer buffer) {
      serializeCalls.incrementAndGet();
      buffer.put(value);
      if (appendExtraByte) {
        buffer.put((byte) 1);
      }
    }

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
      deserializeCalls.incrementAndGet();
      byte[] value = new byte[buffer.remaining()];
      buffer.get(value);
      return value;
    }

    @Override
    public int serializedSize(byte[] value) {
      serializedSizeCalls.incrementAndGet();
      return value.length;
    }
  }

  private static final class BlockingPojoSerializer extends CountingPojoSerializer {
    private final CountDownLatch deserializeEntered = new CountDownLatch(1);
    private final CountDownLatch allowDeserialize = new CountDownLatch(1);

    @Override
    public Pojo deserialize(ByteBuffer buffer) {
      deserializeEntered.countDown();
      try {
        if (!allowDeserialize.await(5, TimeUnit.SECONDS)) {
          throw new AssertionError("timed out waiting to release deserialize");
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
      return super.deserialize(buffer);
    }
  }

  private static final class StringSerializer implements CacheSerializer<String> {
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

  private static final class ByteBufferSerializer implements CacheSerializer<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer buffer) {
      buffer.put(value.duplicate());
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer buffer) {
      return buffer.slice();
    }

    @Override
    public int serializedSize(ByteBuffer value) {
      return value.remaining();
    }
  }

  private static final class OwnedByteBufferSerializer implements CacheSerializer<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer buffer) {
      buffer.put(value.duplicate());
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer buffer) {
      ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
      copy.put(buffer.duplicate()).flip();
      return copy;
    }

    @Override
    public int serializedSize(ByteBuffer value) {
      return value.remaining();
    }
  }

  @SuppressWarnings("rawtypes")
  private static final class RawSerializer implements CacheSerializer {
    private final AtomicReference<ByteBuffer> lastDeserialized = new AtomicReference<>();

    @Override
    public void serialize(Object value, ByteBuffer buffer) {
      if (value instanceof String) {
        buffer.put(((String) value).getBytes(StandardCharsets.UTF_8));
      } else {
        buffer.put(((ByteBuffer) value).duplicate());
      }
    }

    @Override
    public Object deserialize(ByteBuffer buffer) {
      ByteBuffer result = buffer;
      lastDeserialized.set(result);
      return result;
    }

    @Override
    public int serializedSize(Object value) {
      return value instanceof String ? ((String) value).getBytes(StandardCharsets.UTF_8).length : ((ByteBuffer) value).remaining();
    }
  }
}
