package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.OHCache;
import com.red.ohc.index.Entry;

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

  @Test(timeOut = 2_000L)
  public void writerRegistrationBindsMaintenanceForLaterReads() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("registered", "value"));
      for (int index = 0; index < 1_024; index++) {
        assertEquals(cache.get("registered"), "value");
      }
      cache.flushAsync().join();
      assertEquals(
          cache.stats().getReadHits(),
          1_024L,
          "a thread registered by put must still publish later get statistics");
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

  @Test
  public void putBecomesVisibleAfterFlushAndSupportsDirectValue() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .build()) {
      assertTrue(cache.put("key", "value"));
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
      Entry entry = Entry.bootstrap();
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
}
