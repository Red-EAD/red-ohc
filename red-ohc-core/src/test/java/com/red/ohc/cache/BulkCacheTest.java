package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;

public class BulkCacheTest {
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

  @Test
  public void bulkCommandsApplyWithoutCrossKeyAtomicity() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int i = 0; i < 64; i++) {
        values.put("key-" + i, "value-" + i);
      }

      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();
      Map<String, String> fetched = getAllEventually(cache, values.keySet());
      assertTrue(fetched instanceof HashMap);
      assertEquals(fetched, values);

      assertEquals(cache.removeAll(Arrays.asList("key-0", "key-7", "key-63")), 3);
      cache.flushAsync().join();
      assertEquals(getEventually(cache, "key-0"), null);
      assertEquals(getEventually(cache, "key-7"), null);
      assertEquals(getEventually(cache, "key-63"), null);
    }
  }

  @Test
  public void nestedDirectValueKeepsTheOuterViewUsable() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("outer", "outer-value"));
      assertTrue(cache.put("inner", "inner-value"));
      assertTrue(
          cache.getDirect(
              "outer",
              outer -> {
                assertTrue(cache.getDirect("inner", inner -> assertEquals(inner.length(), 11)));
                assertEquals(outer.length(), 11);
              }));
    }
  }

  @Test
  public void directValueKeepsItsBufferAcrossNestedDeserialization() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("outer", "outer-value"));
      assertTrue(cache.put("inner", "inner-value"));
      cache.flushAsync().join();

      assertTrue(
          cache.getDirect(
              "outer",
              outer -> {
                assertEquals(cache.get("inner"), "inner-value");
                ByteBuffer buffer = outer.asReadOnlyByteBuffer();
                assertEquals(buffer.get(0), (byte) 'o');
              }));
    }
  }

  @Test
  public void directValueExposesReadOnlyNativeBufferAndInvalidatesItAfterCallback() {
    AtomicReference<com.red.ohc.api.ValueView> viewRef = new AtomicReference<>();
    AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value"));
      cache.flushAsync().join();

      assertTrue(
          cache.getDirect(
              "key",
              view -> {
                ByteBuffer buffer = view.asReadOnlyByteBuffer();
                viewRef.set(view);
                bufferRef.set(buffer);
                assertTrue(buffer.isDirect());
                assertTrue(buffer.isReadOnly());
                assertFalse(buffer.hasArray());
                assertEquals(buffer.position(), 0);
                assertEquals(buffer.limit(), 5);
                assertEquals(buffer.get(0), (byte) 'v');
                ByteBuffer slice = buffer.slice();
                ByteBuffer duplicate = buffer.duplicate();
                assertTrue(slice.isDirect());
                assertTrue(slice.isReadOnly());
                assertEquals(slice.get(0), (byte) 'v');
                assertEquals(duplicate.get(0), (byte) 'v');
                expectThrows(java.nio.ReadOnlyBufferException.class, () -> buffer.put((byte) 1));
              }));

      assertEquals(bufferRef.get().limit(), 0);
      expectThrows(java.nio.BufferUnderflowException.class, bufferRef.get()::get);
      expectThrows(IllegalStateException.class, () -> viewRef.get().length());
      expectThrows(IllegalStateException.class, () -> viewRef.get().asReadOnlyByteBuffer());
      expectThrows(IllegalStateException.class, () -> viewRef.get().getByte(0));
      expectThrows(
          IllegalStateException.class, () -> viewRef.get().copyTo(new byte[5], 0));
    }
  }

  @Test
  public void directValueReusesAndReinitializesTheNativeBufferShell() {
    AtomicReference<ByteBuffer> first = new AtomicReference<>();
    AtomicReference<ByteBuffer> second = new AtomicReference<>();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value"));
      cache.flushAsync().join();

      assertTrue(
          cache.getDirect(
              "key",
              view -> {
                first.set(view.asReadOnlyByteBuffer());
                first.get().position(2);
                first.get().order(java.nio.ByteOrder.LITTLE_ENDIAN);
              }));
      assertTrue(
          cache.getDirect(
              "key",
              view -> {
                second.set(view.asReadOnlyByteBuffer());
                assertSame(second.get(), first.get());
                assertEquals(second.get().position(), 0);
                assertEquals(second.get().limit(), 5);
                assertEquals(second.get().order(), java.nio.ByteOrder.BIG_ENDIAN);
              }));
    }
  }

  @Test
  public void directValueHandlesAnEmptySerializedPayloadWithoutNativeAccess() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("empty", ""));
      cache.flushAsync().join();

      assertTrue(
          cache.getDirect(
              "empty",
              view -> {
                ByteBuffer buffer = view.asReadOnlyByteBuffer();
                assertTrue(buffer.isDirect());
                assertTrue(buffer.isReadOnly());
                assertEquals(buffer.position(), 0);
                assertEquals(buffer.limit(), 0);
                expectThrows(java.nio.BufferUnderflowException.class, buffer::get);
                expectThrows(IndexOutOfBoundsException.class, () -> view.getByte(0));
              }));
    }
  }

  @Test
  public void nestedDirectValuesUseIndependentNativeBufferDepths() {
    AtomicReference<ByteBuffer> outer = new AtomicReference<>();
    AtomicReference<ByteBuffer> inner = new AtomicReference<>();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("outer", "outer-value"));
      assertTrue(cache.put("inner", "inner-value"));
      cache.flushAsync().join();

      assertTrue(
          cache.getDirect(
              "outer",
              outerView -> {
                outer.set(outerView.asReadOnlyByteBuffer());
                assertTrue(
                    cache.getDirect(
                        "inner",
                        innerView -> {
                          inner.set(innerView.asReadOnlyByteBuffer());
                          assertNotSame(inner.get(), outer.get());
                          assertEquals(outer.get().get(0), (byte) 'o');
                        }));
                assertEquals(outer.get().get(0), (byte) 'o');
              }));

      assertEquals(outer.get().limit(), 0);
      assertEquals(inner.get().limit(), 0);
    }
  }

  @Test
  public void directAllRebindsTheBorrowedBufferForEachCallbackAndCleansUpOnFailure() {
    RuntimeException failure = new RuntimeException("direct-all buffer failure");
    AtomicReference<ByteBuffer> first = new AtomicReference<>();
    AtomicReference<ByteBuffer> last = new AtomicReference<>();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("first", "first-value"));
      assertTrue(cache.put("second", "second-value"));
      cache.flushAsync().join();

      RuntimeException actual =
          expectThrows(
              RuntimeException.class,
              () ->
                  cache.getDirectAll(
                      Arrays.asList("first", "second"),
                      (key, view) -> {
                        ByteBuffer current = view.asReadOnlyByteBuffer();
                        if (first.compareAndSet(null, current)) {
                          assertEquals(current.limit(), 11);
                          return;
                        }
                        assertSame(current, first.get());
                        assertEquals(current.limit(), 12);
                        last.set(current);
                        throw failure;
                      }));

      assertSame(actual, failure);
      assertEquals(last.get().limit(), 0);
      assertTrue(cache.getDirect("first", view -> assertEquals(view.length(), 11)));
    }
  }

  @Test
  public void directValueBufferIsInvalidatedWhenConsumerThrows() {
    RuntimeException failure = new RuntimeException("buffer consumer failure");
    AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>();
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value"));
      cache.flushAsync().join();

      RuntimeException actual =
          expectThrows(
              RuntimeException.class,
              () ->
                  cache.getDirect(
                      "key",
                      view -> {
                        bufferRef.set(view.asReadOnlyByteBuffer());
                        throw failure;
                      }));
      assertSame(actual, failure);
      assertEquals(bufferRef.get().limit(), 0);
      assertTrue(cache.getDirect("key", view -> assertEquals(view.length(), 5)));
    }
  }

  @Test
  public void directAllReturnsUniqueLiveHitsInInputOrderWithoutDeserializing() {
    AtomicInteger deserializations = new AtomicInteger();
    CacheSerializer<String> observingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializations.incrementAndGet();
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(observingValueSerializer)
            .build()) {
      assertTrue(cache.put("key-1", "value-1"));
      assertTrue(cache.put("key-2", "value-2"));
      cache.flushAsync().join();

      List<String> keys = new ArrayList<>();
      List<String> values = new ArrayList<>();
      int hits =
          cache.getDirectAll(
              Arrays.asList("key-1", "missing", "key-2", "key-1"),
              (key, value) -> {
                keys.add(key);
                byte[] bytes = new byte[value.length()];
                value.copyTo(bytes, 0);
                values.add(new String(bytes, StandardCharsets.UTF_8));
              });

      assertEquals(hits, 2);
      assertEquals(keys, Arrays.asList("key-1", "key-2"));
      assertEquals(values, Arrays.asList("value-1", "value-2"));
      assertEquals(deserializations.get(), 0);
    }
  }

  @Test
  public void directAllDeduplicatesEqualSerializedKeysEvenForSetInput() {
    byte[] stored = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] first = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] duplicate = "same-key".getBytes(StandardCharsets.UTF_8);
    Set<byte[]> keys = new LinkedHashSet<>(Arrays.asList(first, duplicate));
    List<byte[]> callbacks = new ArrayList<>();
    try (OHCache<byte[], String> cache =
        OHCacheBuilder.<byte[], String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(BYTES)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put(stored, "value"));

      assertEquals(
          cache.getDirectAll(keys, (key, value) -> callbacks.add(key)),
          1,
          "one serialized cache key must produce one callback");
      assertEquals(callbacks.size(), 1);
      assertSame(callbacks.get(0), first);
    }
  }

  @Test
  public void getAllDeduplicatesEqualSerializedKeysBeforeDeserializing() {
    AtomicInteger deserializations = new AtomicInteger();
    CacheSerializer<String> countingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializations.incrementAndGet();
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    byte[] stored = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] first = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] duplicate = "same-key".getBytes(StandardCharsets.UTF_8);
    try (OHCache<byte[], String> cache =
        OHCacheBuilder.<byte[], String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(BYTES)
            .valueSerializer(countingValueSerializer)
            .build()) {
      assertTrue(cache.put(stored, "value"));

      Map<byte[], String> result = cache.getAll(Arrays.asList(first, duplicate));

      assertEquals(result.size(), 1);
      assertEquals(result.get(first), "value");
      assertSame(result.keySet().iterator().next(), first);
      assertEquals(deserializations.get(), 1);
    }
  }

  @Test
  public void getAllQueriesDistinctEncodingsWhenCallerKeysAreEqual() {
    AtomicInteger deserializations = new AtomicInteger();
    CacheSerializer<AliasedKey> keySerializer =
        new CacheSerializer<AliasedKey>() {
          @Override
          public void serialize(AliasedKey value, ByteBuffer buffer) {
            buffer.put(value.encoded);
          }

          @Override
          public AliasedKey deserialize(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
          }

          @Override
          public int serializedSize(AliasedKey value) {
            return value.encoded.length;
          }
        };
    CacheSerializer<String> countingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializations.incrementAndGet();
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    AliasedKey first = new AliasedKey("logical-key", "encoding-a");
    AliasedKey second = new AliasedKey("logical-key", "encoding-b");
    try (OHCache<AliasedKey, String> cache =
        OHCacheBuilder.<AliasedKey, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(keySerializer)
            .valueSerializer(countingValueSerializer)
            .build()) {
      assertTrue(cache.put(first, "first-value"));
      assertTrue(cache.put(second, "second-value"));

      Map<AliasedKey, String> result = cache.getAll(Arrays.asList(first, second));

      assertEquals(result.size(), 1);
      assertEquals(result.get(first), "second-value");
      assertSame(result.keySet().iterator().next(), first);
      assertEquals(deserializations.get(), 2);
    }
  }

  @Test
  public void directAllKeepsOneReaderEpochForTheWholeCollection() throws Exception {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 23)
                .expectedEntries(2_048)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 1_025; index++) {
        values.put("direct-batch-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();

      AtomicInteger callbacks = new AtomicInteger();
      AtomicBoolean observedReaderEpoch = new AtomicBoolean();
      AtomicBoolean readerActiveAtBoundary = new AtomicBoolean();
      com.red.ohc.runtime.ThreadContext context = threadContext(cache);
      Collection<String> boundedKeys =
          new AbstractCollection<String>() {
            @Override
            public Iterator<String> iterator() {
              Iterator<String> delegate = values.keySet().iterator();
              return new Iterator<String>() {
                private int seen;
                private boolean boundaryRecorded;

                @Override
                public boolean hasNext() {
                  if (seen == 512 && !boundaryRecorded) {
                    boundaryRecorded = true;
                    readerActiveAtBoundary.set(context.readerDepth() > 0);
                  }
                  return delegate.hasNext();
                }

                @Override
                public String next() {
                  seen++;
                  return delegate.next();
                }
              };
            }

            @Override
            public int size() {
              return values.size();
            }
          };
      int hits =
          cache.getDirectAll(
              boundedKeys,
              (key, value) -> {
                callbacks.incrementAndGet();
                if (context.slot.epoch != 0L) {
                  observedReaderEpoch.set(true);
                }
                assertTrue(value.length() > 0);
              });

      assertEquals(hits, values.size());
      assertEquals(callbacks.get(), values.size());
      assertTrue(observedReaderEpoch.get());
      assertTrue(readerActiveAtBoundary.get());
      assertEquals(context.readerDepth(), 0);
      assertEquals(context.slot.epoch, 0L);
    }
  }

  @Test
  public void nestedDirectAllKeepsTheOuterViewUsable() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("outer", "outer-value"));
      assertTrue(cache.put("inner", "inner-value"));
      cache.flushAsync().join();

      assertEquals(
          cache.getDirectAll(
              Arrays.asList("outer"),
              (key, outer) -> {
                assertEquals(
                    cache.getDirectAll(
                        Arrays.asList("inner"),
                        (innerKey, inner) -> {
                          assertEquals(inner.length(), 11);
                        }),
                    1);
                assertEquals(outer.length(), 11);
              }),
          1);
    }
  }

  @Test
  public void directAllReleasesReaderStateWhenConsumerThrows() throws Exception {
    RuntimeException failure = new RuntimeException("consumer failure");
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
      assertTrue(cache.put("key", "value"));
      cache.flushAsync().join();

      com.red.ohc.runtime.ThreadContext context = threadContext(cache);
      boolean propagated = false;
      try {
        cache.getDirectAll(
            Arrays.asList("key"),
            (key, value) -> {
              throw failure;
            });
      } catch (RuntimeException actual) {
        assertTrue(actual == failure);
        propagated = true;
      }

      assertTrue(propagated);
      assertEquals(context.readerDepth(), 0);
      assertEquals(context.slot.epoch, 0L);
      assertTrue(cache.getDirect("key", value -> assertEquals(value.length(), 5)));
    }
  }

  @Test
  public void directAllValidatesInputsAndHandlesEmptyCollections() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      boolean nullKeysRejected = false;
      try {
        cache.getDirectAll(null, (key, value) -> {});
      } catch (NullPointerException expected) {
        nullKeysRejected = true;
      }
      assertTrue(nullKeysRejected);

      boolean nullConsumerRejected = false;
      try {
        cache.getDirectAll(Arrays.asList("key"), null);
      } catch (NullPointerException expected) {
        nullConsumerRejected = true;
      }
      assertTrue(nullConsumerRejected);
      assertEquals(cache.getDirectAll(Arrays.asList(), (key, value) -> {}), 0);
    }
  }

  @Test(timeOut = 15_000L)
  public void repeatedPutAllReplacementsContinueAfterRetirementPressure() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .expectedEntries(64)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 64; index++) {
        values.put("replace-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();

      int successful = 0;
      for (int round = 0; round < 400; round++) {
        successful += cache.putAll(values);
      }
      assertTrue(successful > 0, "replacement pressure must admit at least one batch");
      assertEquals(cache.get("replace-63"), "value-63");
    }
  }

  @Test
  public void removeAllPreservesPerKeyVisibilityAcrossBatchBoundaries() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 23)
            .expectedEntries(2_048)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 1_025; index++) {
        values.put("remove-batch-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      List<String> keys = new ArrayList<>(values.keySet());
      int removed = cache.removeAll(keys);
      assertTrue(removed <= keys.size());
      int remaining = 0;
      for (String key : keys) {
        if (cache.get(key) != null) {
          remaining++;
        }
      }
      assertEquals(remaining, keys.size() - removed);
    }
  }

  @Test
  public void bulkMutationBoundarySizesPreserveCountsAndImmediateVisibility() {
    int[] sizes = {0, 1, 511, 512, 513, 1_024, 1_025};
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 24)
            .expectedEntries(4_096)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      for (int size : sizes) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < size; index++) {
          values.put("boundary-" + size + "-" + index, "value-" + index);
        }
        assertEquals(cache.putAll(values), size, "putAll count at boundary " + size);
        for (Map.Entry<String, String> entry : values.entrySet()) {
          assertEquals(
              cache.get(entry.getKey()), entry.getValue(), "putAll visibility at boundary " + size);
        }
        assertEquals(cache.getAll(values.keySet()), values, "getAll at boundary " + size);
        int removed = cache.removeAll(new ArrayList<>(values.keySet()));
        assertTrue(removed <= size, "removeAll count at boundary " + size);
        int remaining = 0;
        for (String key : values.keySet()) {
          if (cache.get(key) != null) {
            remaining++;
          }
        }
        assertEquals(remaining, size - removed, "removeAll visibility at boundary " + size);
      }
    }
  }

  @Test
  public void removeAllKeepsPrefixAndReleasesWriterAfterSerializerFailure() {
    CacheSerializer<String> failingKeySerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("boom".equals(value)) {
              throw new IllegalStateException("serializer failure");
            }
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(failingKeySerializer)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("before", "value"));
      try {
        cache.removeAll(Arrays.asList("before", "boom", "after"));
        throw new AssertionError("removeAll must propagate serializer failure");
      } catch (IllegalStateException expected) {
        // The completed prefix remains deleted.
      }
      assertEquals(cache.get("before"), null);
      assertTrue(cache.put("after-failure", "writer-released"));
      assertEquals(cache.get("after-failure"), "writer-released");
    }
  }

  @Test
  public void putAllKeepsWriterAdmissionForTheWholeCollection() throws Exception {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 22)
                .expectedEntries(1_024)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 513; index++) {
        values.put("writer-key-" + index, "writer-value-" + index);
      }
      com.red.ohc.runtime.ThreadContext context = threadContext(cache);
      ArrayList<Boolean> writerStates = new ArrayList<>();
      ArrayList<Boolean> writerStatesAtBatchBoundary = new ArrayList<>();
      Map<String, String> observed =
          new AbstractMap<String, String>() {
            @Override
            public Set<Entry<String, String>> entrySet() {
              return new AbstractSet<Entry<String, String>>() {
                @Override
                public Iterator<Entry<String, String>> iterator() {
                  Iterator<Entry<String, String>> delegate = values.entrySet().iterator();
                  return new Iterator<Entry<String, String>>() {
                    private int seen;
                    private boolean boundaryRecorded;

                    @Override
                    public boolean hasNext() {
                      if (seen == 512 && !boundaryRecorded) {
                        boundaryRecorded = true;
                        writerStatesAtBatchBoundary.add(context.slot.writerActive);
                      }
                      return delegate.hasNext();
                    }

                    @Override
                    public Entry<String, String> next() {
                      writerStates.add(context.slot.writerActive);
                      seen++;
                      return delegate.next();
                    }

                    @Override
                    public void remove() {
                      delegate.remove();
                    }
                  };
                }

                @Override
                public int size() {
                  return values.size();
                }
              };
            }
          };

      assertEquals(cache.putAll(observed), values.size());
      assertEquals(writerStates.size(), values.size());
      for (Boolean writerActive : writerStates) {
        assertTrue(
            writerActive, "putAll must keep writer admission for each mutation");
      }
      assertEquals(
          writerStatesAtBatchBoundary,
          Arrays.asList(true),
          "putAll must keep writer admission for the whole collection");
    }
  }

  @Test
  public void removeAllKeepsWriterAdmissionForTheWholeCollection() throws Exception {
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 23)
                .expectedEntries(1_024)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 513; index++) {
        values.put("remove-writer-key-" + index, "remove-writer-value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      com.red.ohc.runtime.ThreadContext context = threadContext(cache);
      AtomicBoolean writerActiveAtBoundary = new AtomicBoolean();
      Collection<String> keys =
          new AbstractCollection<String>() {
            @Override
            public Iterator<String> iterator() {
              Iterator<String> delegate = values.keySet().iterator();
              return new Iterator<String>() {
                private int seen;
                private boolean boundaryRecorded;

                @Override
                public boolean hasNext() {
                  if (seen == 512 && !boundaryRecorded) {
                    boundaryRecorded = true;
                    writerActiveAtBoundary.set(context.slot.writerActive);
                  }
                  return delegate.hasNext();
                }

                @Override
                public String next() {
                  seen++;
                  return delegate.next();
                }
              };
            }

            @Override
            public int size() {
              return values.size();
            }
          };

      assertEquals(cache.removeAll(keys), values.size());
      assertTrue(writerActiveAtBoundary.get());
    }
  }

  @Test
  public void putAllKeepsCompletedWritesAndReleasesWriterAfterSerializerFailure() {
    CacheSerializer<String> failingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            if ("boom".equals(value)) {
              throw new IllegalStateException("serializer failure");
            }
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(failingValueSerializer)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      values.put("before", "kept");
      values.put("failure", "boom");
      values.put("after", "not-written");

      try {
        cache.putAll(values);
        throw new AssertionError("putAll must propagate serializer failure");
      } catch (IllegalStateException expected) {
        // The completed prefix remains visible and the writer admission is released below.
      }
      assertEquals(cache.get("before"), "kept");
      assertTrue(cache.put("after-failure", "writer-released"));
      assertEquals(cache.get("after-failure"), "writer-released");
    }
  }

  @Test(timeOut = 15_000L)
  public void putAllRemainsImmediatelyVisibleWhenMaintenanceIsPausedAndHintQueueIsFull()
      throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .expectedEntries(1)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    ReentrantLock ownerLock = ownerLock(cache);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              ownerLock.lock();
              try {
                locked.countDown();
                await(release);
              } finally {
                ownerLock.unlock();
              }
            });
    holder.start();
    assertTrue(locked.await(2L, java.util.concurrent.TimeUnit.SECONDS));
    try {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 1_025; index++) {
        values.put("paused-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      assertEquals(
          cache.get("paused-1024"),
          "value-1024",
          "CHM/value publication must not wait for advisory maintenance");
    } finally {
      release.countDown();
      holder.join(2_000L);
      cache.flushAsync().join();
      cache.close();
    }
  }

  @Test(timeOut = 15_000L)
  public void singlePutRemainsImmediatelyVisibleWhenMaintenanceIsPausedAndHintQueueIsFull()
      throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .expectedEntries(1)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    ReentrantLock ownerLock = ownerLock(cache);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              ownerLock.lock();
              try {
                locked.countDown();
                await(release);
              } finally {
                ownerLock.unlock();
              }
            });
    holder.start();
    assertTrue(locked.await(2L, java.util.concurrent.TimeUnit.SECONDS));
    try {
      for (int index = 0; index < 1_024; index++) {
        assertTrue(cache.put("single-paused-" + index, "value-" + index));
      }
      OHCacheStats paused = cache.stats();
      assertTrue(
          paused.getMaintenanceQueueDepth() >= paused.getMaintenanceQueueCapacity());
      assertTrue(cache.put("single-paused-1024", "value-1024"));
      assertEquals(
          cache.get("single-paused-1024"),
          "value-1024",
          "a full advisory queue must not delay synchronous CHM publication");
    } finally {
      release.countDown();
      holder.join(2_000L);
      cache.flushAsync().join();
      cache.close();
    }
  }

  @Test(timeOut = 15_000L)
  public void writesDoNotWaitWhenBothHintAndRepairQueuesAreFull() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 26)
                .expectedEntries(1)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    ReentrantLock ownerLock = ownerLock(cache);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              ownerLock.lock();
              try {
                locked.countDown();
                await(release);
              } finally {
                ownerLock.unlock();
              }
            });
    holder.start();
    assertTrue(locked.await(2L, java.util.concurrent.TimeUnit.SECONDS));
    try {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 2_050; index++) {
        values.put("repair-paused-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      assertEquals(cache.get("repair-paused-2049"), "value-2049");
    } finally {
      release.countDown();
      holder.join(2_000L);
      cache.flushAsync().join();
      cache.close();
    }
  }

  @Test(timeOut = 15_000L)
  public void replacementRetiresTheOldValueEvenWhenItsAdvisoryHintIsDropped() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .expectedEntries(1)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    ReentrantLock ownerLock = ownerLock(cache);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              ownerLock.lock();
              try {
                locked.countDown();
                await(release);
              } finally {
                ownerLock.unlock();
              }
            });
    holder.start();
    assertTrue(locked.await(2L, java.util.concurrent.TimeUnit.SECONDS));
    try {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 1_025; index++) {
        values.put("replace-paused-" + index, "old-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      OHCacheStats paused = cache.stats();
      assertTrue(
          paused.getMaintenanceQueueDepth() >= paused.getMaintenanceQueueCapacity(),
          "the paused worker must leave the advisory queue at capacity");
      assertTrue(cache.put("replace-paused-1024", "new-value"));
      assertEquals(cache.get("replace-paused-1024"), "new-value");
      assertTrue(
          cache.stats().getRetirementQueueDepth() > 0L,
          "the old native value must be recorded even when the UPDATE hint is dropped");
    } finally {
      release.countDown();
      holder.join(2_000L);
      cache.flushAsync().join();
      cache.close();
    }
  }

  @Test
  public void bulkReadUsesAStandardHashMapAcrossMultipleReaderEpochBatches() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 22)
            .expectedEntries(2_048)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 1_025; index++) {
        values.put("bulk-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();
      ArrayList<String> duplicateInput = new ArrayList<>(values.keySet());
      duplicateInput.addAll(values.keySet());
      Map<String, String> fetched = cache.getAll(duplicateInput);
      assertTrue(fetched instanceof HashMap);
      assertEquals(fetched, values);
    }
  }

  @Test
  public void bulkReadDeserializesEachHitImmediatelyInsideTheReaderEpoch() {
    AtomicInteger encodedKeys = new AtomicInteger();
    AtomicInteger deserializations = new AtomicInteger();
    AtomicInteger firstDeserializeAfterKeys = new AtomicInteger();
    CacheSerializer<String> countingKeySerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            encodedKeys.incrementAndGet();
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    CacheSerializer<String> observingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            if (deserializations.getAndIncrement() == 0) {
              firstDeserializeAfterKeys.set(encodedKeys.get());
            }
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 23)
            .expectedEntries(1_024)
            .keySerializer(countingKeySerializer)
            .valueSerializer(observingValueSerializer)
            .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 513; index++) {
        values.put("read-key-" + index, "read-value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();
      encodedKeys.set(0);

      assertEquals(cache.getAll(values.keySet()).size(), values.size());
      assertEquals(
          firstDeserializeAfterKeys.get(),
          1,
          "getAll must deserialize each hit while its reader epoch is active");
    }
  }

  @Test
  public void bulkReadEntryDeduplicationDoesNotHashCallerKeys() {
    AtomicInteger hashCalls = new AtomicInteger();
    CacheSerializer<CountingKey> keySerializer =
        new CacheSerializer<CountingKey>() {
          @Override
          public void serialize(CountingKey value, ByteBuffer buffer) {
            buffer.put(value.bytes);
          }

          @Override
          public CountingKey deserialize(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
          }

          @Override
          public int serializedSize(CountingKey value) {
            return value.bytes.length;
          }
        };
    CountingKey first = new CountingKey("set-key-1", hashCalls);
    CountingKey second = new CountingKey("set-key-2", hashCalls);
    Set<CountingKey> keys = new java.util.LinkedHashSet<>(Arrays.asList(first, second));
    hashCalls.set(0);
    try (OHCache<CountingKey, String> cache =
        OHCacheBuilder.<CountingKey, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(keySerializer)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put(first, "set-value-1"));
      assertTrue(cache.put(second, "set-value-2"));
      assertEquals(cache.getAll(keys).size(), keys.size());
      assertEquals(
          hashCalls.get(),
          keys.size(),
          "only the final result HashMap should hash caller keys during getAll");
    }
  }

  @Test
  public void bulkDeserializationRunsInsideTheReaderEpoch() throws Exception {
    AtomicReference<com.red.ohc.runtime.ThreadContext> context = new AtomicReference<>();
    AtomicBoolean deserializedInsideEpoch = new AtomicBoolean();
    CacheSerializer<String> checkingValueSerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            deserializedInsideEpoch.set(context.get().slot.epoch != 0L);
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    try (OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 20)
                .keySerializer(STRING)
                .valueSerializer(checkingValueSerializer)
                .build()) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int index = 0; index < 513; index++) {
        values.put("epoch-key-" + index, "value-" + index);
      }
      assertEquals(cache.putAll(values), values.size());
      cache.flushAsync().join();
      context.set(threadContext(cache));

      AtomicBoolean readerActiveAtBatchBoundary = new AtomicBoolean();
      Collection<String> boundedKeys =
          new AbstractCollection<String>() {
            @Override
            public Iterator<String> iterator() {
              Iterator<String> delegate = values.keySet().iterator();
              return new Iterator<String>() {
                private int seen;
                private boolean boundaryRecorded;

                @Override
                public boolean hasNext() {
                  if (seen == 512 && !boundaryRecorded) {
                    boundaryRecorded = true;
                    readerActiveAtBatchBoundary.set(context.get().readerDepth() > 0);
                  }
                  return delegate.hasNext();
                }

                @Override
                public String next() {
                  seen++;
                  return delegate.next();
                }
              };
            }

            @Override
            public int size() {
              return values.size();
            }
          };

      assertEquals(cache.getAll(boundedKeys).size(), values.size());
      assertTrue(
          deserializedInsideEpoch.get(),
          "getAll must deserialize while its QSBR reader epoch is active");
      assertTrue(
          readerActiveAtBatchBoundary.get(),
          "getAll must keep one reader epoch for the whole collection");
    }
  }

  @SuppressWarnings("unchecked")
  private static com.red.ohc.runtime.ThreadContext threadContext(OffHeapCache<?, ?> cache)
      throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("contexts");
    field.setAccessible(true);
    return ((ThreadLocal<com.red.ohc.runtime.ThreadContext>) field.get(cache)).get();
  }

  private static ReentrantLock ownerLock(OffHeapCache<?, ?> cache) throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    Object worker = workerField.get(cache);
    Field ownerField = worker.getClass().getDeclaredField("ownerLock");
    ownerField.setAccessible(true);
    return (ReentrantLock) ownerField.get(worker);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static Map<String, String> getAllEventually(
      OHCache<String, String> cache, java.util.Collection<String> keys) {
    for (int i = 0; i < 200; i++) {
      Map<String, String> values = cache.getAll(keys);
      if (values.size() == keys.size()) {
        return values;
      }
      Thread.yield();
    }
    return cache.getAll(keys);
  }

  private static String getEventually(OHCache<String, String> cache, String key) {
    for (int i = 0; i < 200; i++) {
      String value = cache.get(key);
      if (value != null) {
        return value;
      }
      Thread.yield();
    }
    return null;
  }

  private static final class CountingKey {
    private final byte[] bytes;
    private final AtomicInteger hashCalls;

    private CountingKey(String value, AtomicInteger hashCalls) {
      this.bytes = value.getBytes(StandardCharsets.UTF_8);
      this.hashCalls = hashCalls;
    }

    @Override
    public int hashCode() {
      hashCalls.incrementAndGet();
      return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof CountingKey && Arrays.equals(bytes, ((CountingKey) other).bytes);
    }
  }

  private static final class AliasedKey {
    private final String logical;
    private final byte[] encoded;

    private AliasedKey(String logical, String encoded) {
      this.logical = logical;
      this.encoded = encoded.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int hashCode() {
      return logical.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof AliasedKey && logical.equals(((AliasedKey) other).logical);
    }
  }
}
