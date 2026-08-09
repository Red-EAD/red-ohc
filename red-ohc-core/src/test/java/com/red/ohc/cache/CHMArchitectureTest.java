package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.runtime.ReaderRegistry;

public final class CHMArchitectureTest {
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

  @Test
  public void cacheUsesTheDirectChmAsItsAuthoritativeIndex() throws Exception {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .expectedEntries(64)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      assertTrue(cache.dataForTest() instanceof ConcurrentHashMap);
      Field table = ConcurrentHashMap.class.getDeclaredField("table");
      table.setAccessible(true);
      assertNotNull(table.get(cache.dataForTest()), "CHM table was not prewarmed");
    }
  }

  @Test
  public void expectedEntriesSizingLeavesHeadroomBeforeTheChmThreshold() {
    assertEquals(ChmSizing.plannedEntries(1_000_000L), 1_125_000L);
    assertEquals(ChmSizing.tableLengthFor(1_000_000L, 1L, 1L), 2_097_152);
  }

  @Test
  public void expectedEntriesSizingDoesNotWrapBeforeApplyingHeadroom() {
    assertEquals(ChmSizing.plannedEntries(2_000_000_000_000_000_000L), 2_250_000_000_000_000_000L);
  }

  @Test
  public void readerRegistrationUsesOnlyAppendOnlyCas() throws Exception {
    Field readers = OffHeapCache.class.getDeclaredField("readers");
    assertEquals(
        readers.getType(),
        ReaderRegistry.class,
        "reader registration must be append-only CAS without a global lock");
  }
}
