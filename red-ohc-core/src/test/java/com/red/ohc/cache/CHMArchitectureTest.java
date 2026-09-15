package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.index.ChmSizing;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.maintenance.MaintenanceEventLoop;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.storage.NativeMemory;

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
  public void cacheUsesTheDirectChmAsItsAuthoritativeIndexWithoutBootstrapEntry() throws Exception {
    try (OffHeapCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(1 << 20)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .buildTyped()) {
      assertTrue(cache.dataForTest() instanceof ConcurrentHashMap);
      Field table = ConcurrentHashMap.class.getDeclaredField("table");
      table.setAccessible(true);
      assertNull(table.get(cache.dataForTest()), "CHM must not be warmed by a sentinel Entry");
      cache.put("key", "value");
      assertNotNull(table.get(cache.dataForTest()), "the first real mapping must initialize CHM");
    }
  }

  @Test
  public void entryEstimateSizingLeavesHeadroomBeforeTheChmThreshold() {
    assertEquals(ChmSizing.plannedEntries(1_000_000L), 1_125_000L);
    assertEquals(ChmSizing.tableLengthFor(1_000_000L, 1L), 2_097_152);
  }

  @Test
  public void entryEstimateSizingDoesNotWrapBeforeApplyingHeadroom() {
    assertEquals(ChmSizing.plannedEntries(2_000_000_000_000_000_000L), 2_250_000_000_000_000_000L);
  }

  @Test
  public void byteCapacitySizingStaysConservativeWithoutEntryEstimate() {
    assertEquals(ChmSizing.initialCapacity(0L, 64L << 20), 64L);
  }

  @Test
  public void readerRegistrationUsesOnlyAppendOnlyCas() throws Exception {
    Field readers = OffHeapCache.class.getDeclaredField("readers");
    assertEquals(
        readers.getType(),
        ReaderRegistry.class,
        "reader registration must be append-only CAS without a global lock");
  }

  @Test
  public void maintenanceClockHasNoCallerRefreshEntryPoint() {
    for (Method method : MaintenanceEventLoop.class.getDeclaredMethods()) {
      assertTrue(
          !method.getName().equals("refreshClock") || java.lang.reflect.Modifier.isPrivate(method.getModifiers()),
          "business and loader threads must not write the actor-owned clock");
    }
  }


  @Test
  public void chmLookupMustEvaluateTheProbeSideEquals() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    try {
      byte[] keyBytes = {(byte) 0xa1, 0x02, 0x33, 0x7f};
      LookupKey probe = new LookupKey();
      probe.set(keyBytes, keyBytes.length);
      Entry entry =
          EntryTestSupport.entry(
              memory, keyBytes.length, probe.hashCode(), probe.hash64(), 0x1_000L);
      NativeMemory.copy(keyBytes, 0, entry.nativeKeyBytesAddress(), keyBytes.length);

      assertEquals(probe.hashCode(), entry.hashCode(), "probe and entry must hash alike");
      assertTrue(probe.equals(entry), "the probe compares heap bytes against the native key");
      assertFalse(
          entry.equals(probe),
          "Entry.equals is Entry-to-Entry only; the asymmetry is deliberate");

      ConcurrentHashMap<Object, Object> index = new ConcurrentHashMap<>();
      index.put(entry, entry);
      assertSame(
          index.get(probe),
          entry,
          "every cache lookup relies on CHM calling probe.equals(node.key), never the reverse");
      assertTrue(index.containsKey(probe));
    } finally {
      memory.closeArenas();
    }
  }


}
