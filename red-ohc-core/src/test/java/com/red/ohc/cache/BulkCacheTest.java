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

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.api.OHCacheStats;
import com.red.ohc.maintenance.MaintenanceEventLoop;

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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure30 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < 64; i++) {
          values.put("key-" + i, "value-" + i);
        }

        cache.putAll(values);
        cache.flushAsync().join();
        Map<String, String> fetched = getAllEventually(cache, values.keySet());
        assertTrue(fetched instanceof HashMap);
        assertEquals(fetched, values);

        assertEquals(cache.removeAll(Arrays.asList("key-0", "key-7", "key-63")), 3);
        cache.flushAsync().join();
        assertEquals(getEventually(cache, "key-0"), null);
        assertEquals(getEventually(cache, "key-7"), null);
        assertEquals(getEventually(cache, "key-63"), null);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure30 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure30);
      }
    }
  }

  @Test
  public void nestedDirectValueKeepsTheOuterViewUsable() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure29 = null;
      try {
        cache.put("outer", "outer-value");
        cache.put("inner", "inner-value");
        assertTrue(
            cache.getDirect(
                "outer",
                outer -> {
                  assertTrue(cache.getDirect("inner", inner -> assertEquals(inner.length(), 11)));
                  assertEquals(outer.length(), 11);
                }));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure29 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure29);
      }
    }
  }

  @Test
  public void directValueKeepsItsBufferAcrossNestedDeserialization() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure28 = null;
      try {
        cache.put("outer", "outer-value");
        cache.put("inner", "inner-value");
        cache.flushAsync().join();

        assertTrue(
            cache.getDirect(
                "outer",
                outer -> {
                  assertEquals(cache.get("inner"), "inner-value");
                  ByteBuffer buffer = outer.asReadOnlyByteBuffer();
                  assertEquals(buffer.get(0), (byte) 'o');
                }));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure28 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure28);
      }
    }
  }

  @Test
  public void directValueExposesReadOnlyNativeBufferAndInvalidatesItAfterCallback() {
    AtomicReference<com.red.ohc.api.ValueView> viewRef = new AtomicReference<>();
    AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>();
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure27 = null;
      try {
        cache.put("key", "value");
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
        expectThrows(IllegalStateException.class, () -> viewRef.get().copyTo(new byte[5], 0));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure27 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure27);
      }
    }
  }

  @Test
  public void directValueReusesAndReinitializesTheNativeBufferShell() {
    AtomicReference<ByteBuffer> first = new AtomicReference<>();
    AtomicReference<ByteBuffer> second = new AtomicReference<>();
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure26 = null;
      try {
        cache.put("key", "value");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure26 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure26);
      }
    }
  }

  @Test
  public void directValueHandlesAnEmptySerializedPayloadWithoutNativeAccess() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure25 = null;
      try {
        cache.put("empty", "");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure25 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure25);
      }
    }
  }

  @Test
  public void nestedDirectValuesUseIndependentNativeBufferDepths() {
    AtomicReference<ByteBuffer> outer = new AtomicReference<>();
    AtomicReference<ByteBuffer> inner = new AtomicReference<>();
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure24 = null;
      try {
        cache.put("outer", "outer-value");
        cache.put("inner", "inner-value");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure24 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure24);
      }
    }
  }

  @Test
  public void directAllRebindsTheBorrowedBufferForEachCallbackAndCleansUpOnFailure() {
    RuntimeException failure = new RuntimeException("direct-all buffer failure");
    AtomicReference<ByteBuffer> first = new AtomicReference<>();
    AtomicReference<ByteBuffer> last = new AtomicReference<>();
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure23 = null;
      try {
        cache.put("first", "first-value");
        cache.put("second", "second-value");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure23 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure23);
      }
    }
  }

  @Test
  public void directValueBufferIsInvalidatedWhenConsumerThrows() {
    RuntimeException failure = new RuntimeException("buffer consumer failure");
    AtomicReference<ByteBuffer> bufferRef = new AtomicReference<>();
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure22 = null;
      try {
        cache.put("key", "value");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure22 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure22);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(observingValueSerializer)
              .build();
      Throwable cacheFailure21 = null;
      try {
        cache.put("key-1", "value-1");
        cache.put("key-2", "value-2");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure21 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure21);
      }
    }
  }

  @Test
  public void directAllDeduplicatesEqualSerializedKeysEvenForSetInput() {
    byte[] stored = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] first = "same-key".getBytes(StandardCharsets.UTF_8);
    byte[] duplicate = "same-key".getBytes(StandardCharsets.UTF_8);
    Set<byte[]> keys = new LinkedHashSet<>(Arrays.asList(first, duplicate));
    List<byte[]> callbacks = new ArrayList<>();
    {
      OHCache<byte[], String> cache =
          OHCacheBuilder.<byte[], String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(BYTES)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure20 = null;
      try {
        cache.put(stored, "value");

        assertEquals(
            cache.getDirectAll(keys, (key, value) -> callbacks.add(key)),
            1,
            "one serialized cache key must produce one callback");
        assertEquals(callbacks.size(), 1);
        assertSame(callbacks.get(0), first);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure20 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure20);
      }
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
    {
      OHCache<byte[], String> cache =
          OHCacheBuilder.<byte[], String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(BYTES)
              .valueSerializer(countingValueSerializer)
              .build();
      Throwable cacheFailure19 = null;
      try {
        cache.put(stored, "value");

        Map<byte[], String> result = cache.getAll(Arrays.asList(first, duplicate));

        assertEquals(result.size(), 1);
        assertEquals(result.get(first), "value");
        assertSame(result.keySet().iterator().next(), first);
        assertEquals(deserializations.get(), 1);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure19 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure19);
      }
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
    {
      OHCache<AliasedKey, String> cache =
          OHCacheBuilder.<AliasedKey, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(keySerializer)
              .valueSerializer(countingValueSerializer)
              .build();
      Throwable cacheFailure18 = null;
      try {
        cache.put(first, "first-value");
        cache.put(second, "second-value");

        Map<AliasedKey, String> result = cache.getAll(Arrays.asList(first, second));

        assertEquals(result.size(), 1);
        assertEquals(result.get(first), "second-value");
        assertSame(result.keySet().iterator().next(), first);
        assertEquals(deserializations.get(), 2);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure18 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure18);
      }
    }
  }

  @Test
  public void directAllKeepsOneReaderEpochForTheWholeCollection() throws Exception {
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 23)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .build();
      Throwable cacheFailure17 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 1_025; index++) {
          values.put("direct-batch-" + index, "value-" + index);
        }
        cache.putAll(values);
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
                  if (context.readerDepth() != 0) {
                    observedReaderEpoch.set(true);
                  }
                  assertTrue(value.length() > 0);
                });

        assertEquals(hits, values.size());
        assertEquals(callbacks.get(), values.size());
        assertTrue(observedReaderEpoch.get());
        assertTrue(readerActiveAtBoundary.get());
        assertEquals(context.readerDepth(), 0);
        assertEquals(context.readerDepth(), 0);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure17 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure17);
      }
    }
  }

  @Test
  public void bulkReadsReleaseTheReaderLeaseAtTheBoundedChunkBoundary() throws Exception {
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 20)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .build();
      Throwable cacheFailure16 = null;
      try {
        List<String> missing = new ArrayList<>();
        for (int index = 0; index < 8_192; index++) {
          missing.add("missing-bulk-" + index);
        }
        com.red.ohc.runtime.ThreadContext context = threadContext(cache);
        AtomicBoolean directLeaseReleased = new AtomicBoolean();
        assertEquals(
            cache.getDirectAll(
                observingChunkBoundary(missing, context, directLeaseReleased), (key, value) -> {}),
            0);
        assertTrue(directLeaseReleased.get());

        AtomicBoolean deserializedLeaseReleased = new AtomicBoolean();
        assertTrue(
            cache
                .getAll(observingChunkBoundary(missing, context, deserializedLeaseReleased))
                .isEmpty());
        assertTrue(deserializedLeaseReleased.get());
        assertEquals(context.readerDepth(), 0);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure16 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure16);
      }
    }
  }

  private static Collection<String> observingChunkBoundary(
      List<String> keys,
      com.red.ohc.runtime.ThreadContext context,
      AtomicBoolean released) {
    return new AbstractCollection<String>() {
      @Override
      public Iterator<String> iterator() {
        Iterator<String> delegate = keys.iterator();
        return new Iterator<String>() {
          private int seen;
          private boolean observed;

          @Override
          public boolean hasNext() {
            if (seen == 4_096 && !observed) {
              observed = true;
              released.set(context.readerDepth() == 0);
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
        return keys.size();
      }
    };
  }

  @Test
  public void nestedDirectAllKeepsTheOuterViewUsable() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure15 = null;
      try {
        cache.put("outer", "outer-value");
        cache.put("inner", "inner-value");
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure15 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure15);
      }
    }
  }

  @Test
  public void directAllReleasesReaderStateWhenConsumerThrows() throws Exception {
    RuntimeException failure = new RuntimeException("consumer failure");
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 20)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .build();
      Throwable cacheFailure14 = null;
      try {
        cache.put("key", "value");
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
        assertEquals(context.readerDepth(), 0);
        assertTrue(cache.getDirect("key", value -> assertEquals(value.length(), 5)));

      } catch (Throwable cacheOperationFailure) {
        cacheFailure14 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure14);
      }
    }
  }

  @Test
  public void directAllValidatesInputsAndHandlesEmptyCollections() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure13 = null;
      try {
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure13 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure13);
      }
    }
  }

  @Test(timeOut = 15_000L)
  public void repeatedPutAllReplacementsContinueAfterRetirementPressure() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure12 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 64; index++) {
          values.put("replace-" + index, "value-" + index);
        }
        cache.putAll(values);
        cache.flushAsync().join();

        int successful = 0;
        for (int round = 0; round < 400; round++) {
          cache.putAll(values);
          successful += values.size();
        }
        assertTrue(successful > 0, "replacement pressure must admit at least one batch");
        assertEquals(cache.get("replace-63"), "value-63");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure12 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure12);
      }
    }
  }

  @Test
  public void removeAllPreservesPerKeyVisibilityAcrossBatchBoundaries() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 23)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure11 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 1_025; index++) {
          values.put("remove-batch-" + index, "value-" + index);
        }
        cache.putAll(values);
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure11 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure11);
      }
    }
  }

  @Test
  public void bulkMutationBoundarySizesPreserveCountsAndImmediateVisibility() {
    int[] sizes = {0, 1, 511, 512, 513, 1_024, 1_025};
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 24)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure10 = null;
      try {
        for (int size : sizes) {
          Map<String, String> values = new LinkedHashMap<>();
          for (int index = 0; index < size; index++) {
            values.put("boundary-" + size + "-" + index, "value-" + index);
          }
          cache.putAll(values);
          for (Map.Entry<String, String> entry : values.entrySet()) {
            assertEquals(
                cache.get(entry.getKey()),
                entry.getValue(),
                "putAll visibility at boundary " + size);
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure10 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure10);
      }
    }
  }

  @Test
  public void removeAllDoesNotSkipAnEntryWhoseWriterIsTemporarilyClaimed() throws Exception {
    CountDownLatch firstKeySerialized = new CountDownLatch(1);
    CountDownLatch secondKeySerialized = new CountDownLatch(1);
    AtomicBoolean removing = new AtomicBoolean();
    CacheSerializer<String> observingKeySerializer =
        new CacheSerializer<String>() {
          @Override
          public void serialize(String value, ByteBuffer buffer) {
            STRING.serialize(value, buffer);
            if (!removing.get()) {
              return;
            }
            if ("before".equals(value)) {
              firstKeySerialized.countDown();
            } else if ("after".equals(value)) {
              secondKeySerialized.countDown();
            }
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
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 20)
                  .keySerializer(observingKeySerializer)
                  .valueSerializer(STRING)
                  .build();
      Thread remover = null;
      com.red.ohc.index.Entry claimedEntry = null;
      Throwable cacheFailure9 = null;
      try {
        cache.put("before", "value");
        cache.flushAsync().join();
        com.red.ohc.index.Entry entry = cache.dataForTest().values().iterator().next();
        assertTrue(entry.claimWriter());
        claimedEntry = entry;

        AtomicReference<Integer> removed = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        remover =
            new Thread(
                () -> {
                  try {
                    removing.set(true);
                    removed.set(cache.removeAll(Arrays.asList("before", "after")));
                  } catch (Throwable throwable) {
                    failure.set(throwable);
                  }
                },
                "bulk-remove-writer-contention");
        boolean skippedClaimedEntry;
        remover.start();
        try {
          assertTrue(
              firstKeySerialized.await(2L, java.util.concurrent.TimeUnit.SECONDS),
              "removeAll did not reach the claimed first key");
          skippedClaimedEntry =
              secondKeySerialized.await(500L, java.util.concurrent.TimeUnit.MILLISECONDS);
        } finally {
          entry.finishWriter();
        }
        CacheTestSupport.awaitCaller(remover);

        assertFalse(
            remover.isAlive(), "removeAll did not resume after the writer claim was released");
        assertEquals(failure.get(), null);
        assertFalse(
            skippedClaimedEntry,
            "removeAll must finish the claimed key before advancing to the next key");
        assertEquals(removed.get(), Integer.valueOf(1));
        assertEquals(cache.get("before"), null);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure9 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        if (claimedEntry != null && claimedEntry.isWriterLocked()) {
          claimedEntry.finishWriter();
        }
        CacheTestSupport.stopAfterCallers(cache, cacheFailure9, remover);
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(failingKeySerializer)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure8 = null;
      try {
        cache.put("before", "value");
        assertEquals(cache.get("before"), "value");
        try {
          cache.removeAll(Arrays.asList("before", "boom", "after"));
          throw new AssertionError("removeAll must propagate serializer failure");
        } catch (IllegalStateException expected) {
          // The completed prefix remains deleted.
          assertEquals(expected.getMessage(), "serializer failure");
        }
        assertEquals(cache.get("before"), null);
        cache.put("after-failure", "writer-released");
        assertEquals(cache.get("after-failure"), "writer-released");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure8 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure8);
      }
    }
  }

  @Test
  public void putAllKeepsWriterAdmissionForTheWholeCollection() throws Exception {
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 22)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .build();
      Throwable cacheFailure7 = null;
      try {
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
                          writerStatesAtBatchBoundary.add(context.isWriterEntered());
                        }
                        return delegate.hasNext();
                      }

                      @Override
                      public Entry<String, String> next() {
                        writerStates.add(context.isWriterEntered());
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

        cache.putAll(observed);
        assertEquals(writerStates.size(), values.size());
        for (Boolean writerActive : writerStates) {
          assertTrue(writerActive, "putAll must keep writer admission for each mutation");
        }
        assertEquals(
            writerStatesAtBatchBoundary,
            Arrays.asList(true),
            "putAll must keep writer admission for the whole collection");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure7 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure7);
      }
    }
  }

  @Test
  public void removeAllKeepsWriterAdmissionForTheWholeCollection() throws Exception {
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 23)
                  .keySerializer(STRING)
                  .valueSerializer(STRING)
                  .build();
      Throwable cacheFailure6 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 513; index++) {
          values.put("remove-writer-key-" + index, "remove-writer-value-" + index);
        }
        cache.putAll(values);
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
                      writerActiveAtBoundary.set(context.isWriterEntered());
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure6 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure6);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(STRING)
              .valueSerializer(failingValueSerializer)
              .build();
      Throwable cacheFailure5 = null;
      try {
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
        cache.put("after-failure", "writer-released");
        assertEquals(cache.get("after-failure"), "writer-released");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure5 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure5);
      }
    }
  }

  @Test(timeOut = 15_000L)
  public void putAllRemainsImmediatelyVisibleWithPausedLifecycleBacklog() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    {
      Throwable explicitCacheFailure4 = null;
      try {
        pauseMaintenance(cache, actorPaused, release);

        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 1_025; index++) {
          values.put("paused-" + index, "value-" + index);
        }
        cache.putAll(values);
        assertEquals(
            cache.get("paused-1024"),
            "value-1024",
            "CHM/value publication must not wait for advisory maintenance");

        release.countDown();
        cache.flushAsync().join();

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure4 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure4);
      }
    }
  }

  @Test(timeOut = 15_000L)
  public void singlePutRemainsImmediatelyVisibleWhenWriterMaintenanceIsPaused() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    {
      Throwable explicitCacheFailure3 = null;
      try {
        pauseMaintenance(cache, actorPaused, release);

        for (int index = 0; index < 1_024; index++) {
          cache.put("single-paused-" + index, "value-" + index);
        }
        OHCacheStats paused = cache.stats();
        assertTrue(
            paused.lifecycleJournalLagRecords() > 0L,
            "the paused worker must expose writer-lane maintenance backlog");
        cache.put("single-paused-1024", "value-1024");
        assertEquals(
            cache.get("single-paused-1024"),
            "value-1024",
            "writer-lane maintenance backlog must not delay synchronous CHM publication");

        release.countDown();
        cache.flushAsync().join();

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure3 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure3);
      }
    }
  }

  @Test(timeOut = 15_000L)
  public void writesDoNotWaitForLifecycleBacklog() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 26)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    {
      Throwable explicitCacheFailure2 = null;
      try {
        pauseMaintenance(cache, actorPaused, release);

        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 2_050; index++) {
          values.put("hint-paused-" + index, "value-" + index);
        }
        cache.putAll(values);
        assertEquals(cache.get("hint-paused-2049"), "value-2049");

        release.countDown();
        cache.flushAsync().join();

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure2 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure2);
      }
    }
  }

  @Test(timeOut = 15_000L)
  public void replacementRetiresTheOldValueWhileWriterMaintenanceIsBacklogged() throws Exception {
    OffHeapCache<String, String> cache =
        (OffHeapCache<String, String>)
            OHCacheBuilder.<String, String>newBuilder()
                .capacity(1 << 24)
                .keySerializer(STRING)
                .valueSerializer(STRING)
                .buildTyped();
    CountDownLatch actorPaused = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    {
      Throwable explicitCacheFailure1 = null;
      try {
        pauseMaintenance(cache, actorPaused, release);

        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 1_025; index++) {
          values.put("replace-paused-" + index, "old-" + index);
        }
        cache.putAll(values);
        OHCacheStats paused = cache.stats();
        assertTrue(
            paused.lifecycleJournalLagRecords() > 0L,
            "the paused worker must expose writer-lane maintenance backlog");
        cache.put("replace-paused-1024", "new-value");
        assertEquals(cache.get("replace-paused-1024"), "new-value");

        release.countDown();
        cache.flushAsync().join();

      } catch (Throwable explicitCacheOperationFailure) {
        explicitCacheFailure1 = explicitCacheOperationFailure;
        throw explicitCacheOperationFailure;
      } finally {

        release.countDown();
        CacheTestSupport.stop(cache, explicitCacheFailure1);
      }
    }
  }

  @Test
  public void bulkReadUsesAStandardHashMapAcrossMultipleReaderEpochBatches() {
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 22)
              .keySerializer(STRING)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure4 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 1_025; index++) {
          values.put("bulk-" + index, "value-" + index);
        }
        cache.putAll(values);
        cache.flushAsync().join();
        ArrayList<String> duplicateInput = new ArrayList<>(values.keySet());
        duplicateInput.addAll(values.keySet());
        Map<String, String> fetched = cache.getAll(duplicateInput);
        assertTrue(fetched instanceof HashMap);
        assertEquals(fetched, values);

      } catch (Throwable cacheOperationFailure) {
        cacheFailure4 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure4);
      }
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
    {
      OHCache<String, String> cache =
          OHCacheBuilder.<String, String>newBuilder()
              .capacity(1 << 23)
              .keySerializer(countingKeySerializer)
              .valueSerializer(observingValueSerializer)
              .build();
      Throwable cacheFailure3 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 513; index++) {
          values.put("read-key-" + index, "read-value-" + index);
        }
        cache.putAll(values);
        cache.flushAsync().join();
        encodedKeys.set(0);

        assertEquals(cache.getAll(values.keySet()).size(), values.size());
        assertEquals(
            firstDeserializeAfterKeys.get(),
            1,
            "getAll must deserialize each hit while its reader epoch is active");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure3 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure3);
      }
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
    {
      OHCache<CountingKey, String> cache =
          OHCacheBuilder.<CountingKey, String>newBuilder()
              .capacity(1 << 20)
              .keySerializer(keySerializer)
              .valueSerializer(STRING)
              .build();
      Throwable cacheFailure2 = null;
      try {
        cache.put(first, "set-value-1");
        cache.put(second, "set-value-2");
        assertEquals(cache.getAll(keys).size(), keys.size());
        assertEquals(
            hashCalls.get(),
            keys.size(),
            "only the final result HashMap should hash caller keys during getAll");

      } catch (Throwable cacheOperationFailure) {
        cacheFailure2 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure2);
      }
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
            deserializedInsideEpoch.set(context.get().readerDepth() != 0);
            return STRING.deserialize(buffer);
          }

          @Override
          public int serializedSize(String value) {
            return STRING.serializedSize(value);
          }
        };
    {
      OffHeapCache<String, String> cache =
          (OffHeapCache<String, String>)
              OHCacheBuilder.<String, String>newBuilder()
                  .capacity(1 << 20)
                  .keySerializer(STRING)
                  .valueSerializer(checkingValueSerializer)
                  .build();
      Throwable cacheFailure1 = null;
      try {
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < 513; index++) {
          values.put("epoch-key-" + index, "value-" + index);
        }
        cache.putAll(values);
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

      } catch (Throwable cacheOperationFailure) {
        cacheFailure1 = cacheOperationFailure;
        throw cacheOperationFailure;
      } finally {
        CacheTestSupport.stop(cache, cacheFailure1);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static com.red.ohc.runtime.ThreadContext threadContext(OffHeapCache<?, ?> cache)
      throws Exception {
    Field field = OffHeapCache.class.getDeclaredField("contexts");
    field.setAccessible(true);
    return ((ThreadLocal<com.red.ohc.runtime.ThreadContext>) field.get(cache)).get();
  }

  private static void pauseMaintenance(
      OffHeapCache<?, ?> cache, CountDownLatch paused, CountDownLatch release) throws Exception {
    Field workerField = OffHeapCache.class.getDeclaredField("worker");
    workerField.setAccessible(true);
    MaintenanceEventLoop worker = (MaintenanceEventLoop) workerField.get(cache);
    assertTrue(
        worker.submitActorTaskForTest(
            () -> {
              paused.countDown();
              await(release);
            },
            failure -> {
              throw new AssertionError(failure);
            }));
    assertTrue(
        paused.await(2L, java.util.concurrent.TimeUnit.SECONDS), "maintenance actor did not pause");
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
