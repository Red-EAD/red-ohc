package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.testng.annotations.Test;

import com.red.ohc.storage.NativeMemory;

public final class NativeS3GhostMapTest {
  @Test
  public void usesHalfTableAsTheRehashThreshold() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    Field tableField = NativeS3GhostMap.class.getDeclaredField("table");
    tableField.setAccessible(true);
    try {
      map.put(1L, 1L);
      Object table = tableField.get(map);
      Field capacity = table.getClass().getDeclaredField("capacity");
      Field maxFill = table.getClass().getDeclaredField("maxFill");
      capacity.setAccessible(true);
      maxFill.setAccessible(true);
      assertEquals(maxFill.getInt(table), Math.max(1, capacity.getInt(table) >>> 1));
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void supportsZeroAndFullWidthHashes() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    try {
      assertTrue(map.put(0L, 1L));
      assertTrue(map.put(0x7fffffff, 3L));
      assertEquals(map.remove(0), 1L);
      assertEquals(map.remove(0x7fffffff), 3L);
      assertTrue(map.isEmpty());
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void tagCollisionsStillCheckTheFullHash() {
    long[] collision = findTagCollision();
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    try {
      assertTrue(map.put(collision[0], 11L));
      assertTrue(map.put(collision[1], 22L));
      assertEquals(map.remove((int) collision[0]), 11L);
      assertEquals(map.remove((int) collision[1]), 22L);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void countsReappearanceHitsWithoutChangingTheNodeSize() throws Exception {
    Field nodeRecordBytes = NativeS3GhostMap.class.getDeclaredField("NODE_RECORD_BYTES");
    nodeRecordBytes.setAccessible(true);
    assertEquals(nodeRecordBytes.getLong(null), 32L);

    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    NativeS3GhostMap.Record record = new NativeS3GhostMap.Record();
    try {
      assertTrue(map.put(9L, 17L));
      assertTrue(map.observe(9, record));
      assertEquals(record.frequency, 0, "observe reports the count before this reappearance");
      assertEquals(record.weight, 17L);

      assertTrue(map.put(9L, 23L));
      assertTrue(map.observe(9, record));
      assertEquals(record.frequency, 1);
      assertEquals(record.weight, 23L);
      assertEquals(map.remove(9, record), 23L);
      assertEquals(record.frequency, 2);
      assertTrue(map.isEmpty());
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void seedsAndSaturatesTheHitCountCarriedByAFingerprint() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    NativeS3GhostMap.Record record = new NativeS3GhostMap.Record();
    try {
      assertTrue(map.put(4L, 8L, 2));
      assertTrue(map.observe(4, record));
      assertEquals(record.frequency, 2);

      assertTrue(map.put(4L, 8L, 0));
      assertTrue(map.observe(4, record));
      assertEquals(record.frequency, 3, "refreshing a fingerprint must not erase earned hits");

      assertTrue(map.observe(4, record));
      assertEquals(record.frequency, NativeS3GhostMap.MAX_FREQUENCY);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void refreshMovesAnExistingFingerprintToTheFifoTailAndKeepsItsFrequency() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    NativeS3GhostMap.Record record = new NativeS3GhostMap.Record();
    try {
      assertTrue(map.put(1L, 10L, 1));
      assertTrue(map.put(2L, 20L));
      assertTrue(map.observe(1, record));
      assertEquals(record.frequency, 1);

      assertTrue(map.refresh(1L, 30L, 0, record));
      assertTrue(record.found);
      assertEquals(record.frequency, 2);
      assertEquals(record.weight, 10L);
      assertEquals(map.weight(), 50L);
      assertEquals(map.removeFirst(), 20L);
      assertEquals(map.removeFirst(), 30L);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void refreshDuringIncrementalRehashPreservesFifoOrderAndTableSize() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    NativeS3GhostMap.Record record = new NativeS3GhostMap.Record();
    try {
      for (long hash = 1L; hash <= 8L; hash++) {
        assertTrue(map.put(hash, hash + 10L));
      }
      assertTrue(map.put(9L, 19L));
      assertTrue(map.rehashPending());

      assertTrue(map.refresh(1L, 101L, 0, record));
      assertTrue(record.found);
      assertEquals(record.weight, 11L);
      while (map.rehashPending()) {
        map.advanceRehash(1);
      }

      assertEquals(map.size(), 9);
      for (long hash = 2L; hash <= 9L; hash++) {
        assertEquals(map.removeFirst(), hash + 10L);
      }
      assertEquals(map.removeFirst(), 101L);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void refusesToResurrectItselfAfterClose() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    NativeS3GhostMap.Record record = new NativeS3GhostMap.Record();
    try {
      assertTrue(map.put(1L, 1L));
      map.close();

      assertFalse(map.put(2L, 2L));
      assertFalse(map.observe(1, record));
      assertEquals(map.remove(1), NativeS3GhostMap.MISSING);
      assertEquals(map.removeFirst(), NativeS3GhostMap.MISSING);
      assertEquals(map.nativeBytes(), 0L);
    } finally {
      memory.closeArenas();
    }
  }

  @Test
  public void removesTheOldestEntryAndPreservesFifoOrder() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    try {
      assertTrue(map.put(1L, 1L));
      assertTrue(map.put(2L, 2L));
      assertTrue(map.put(3L, 3L));
      assertEquals(map.removeFirst(), 1L);
      assertEquals(map.removeFirst(), 2L);
      assertEquals(map.removeFirst(), 3L);
      assertEquals(map.removeFirst(), NativeS3GhostMap.MISSING);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void rehashesIncrementallyWhileServingLookupsAndRemovals() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    try {
      for (long hash = 1L; hash <= 8L; hash++) {
        assertTrue(map.put(hash, hash + 10L));
      }
      assertTrue(map.put(9L, 19L));
      assertTrue(map.rehashPending(), "the threshold crossing must start a rehash");
      assertEquals(map.remove(1), 11L);
      assertEquals(map.remove(9), 19L);

      while (map.rehashPending()) {
        map.advanceRehash(1);
      }
      assertFalse(map.rehashPending());
      assertEquals(map.size(), 7);
      assertEquals(map.remove(8), 18L);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void closeReleasesAllNativeGhostStorage() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    try {
      assertTrue(map.put(1L, 1L));
      assertTrue(map.nativeBytes() > 0L);
      assertTrue(memory.allocated() >= map.nativeBytes());
      map.close();
      assertEquals(map.nativeBytes(), 0L);
      assertEquals(memory.allocated(), 0L);
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  @Test
  public void randomizedOperationsMatchTheFifoReference() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    NativeS3GhostMap map = new NativeS3GhostMap(memory);
    Map<Long, Long> reference = new LinkedHashMap<>();
    Random random = new Random(7L);
    try {
      for (int operation = 0; operation < 4_000; operation++) {
        long hash = random.nextInt(96) - 16L;
        if (operation % 3 == 0) {
          long entryWeight = random.nextInt(32) + 1L;
          assertEquals(map.remove((int) hash), valueOrMissing(reference.remove(hash)));
          assertTrue(map.put(hash, entryWeight));
          reference.put(hash, entryWeight);
        } else if (operation % 3 == 1) {
          assertEquals(map.remove((int) hash), valueOrMissing(reference.remove(hash)));
        } else {
          Map.Entry<Long, Long> oldest = reference.entrySet().stream().findFirst().orElse(null);
          long expected = oldest == null ? NativeS3GhostMap.MISSING : oldest.getValue();
          assertEquals(map.removeFirst(), expected);
          if (oldest != null) {
            reference.remove(oldest.getKey());
          }
        }
        assertEquals(map.size(), reference.size());
      }
    } finally {
      map.close();
      memory.closeArenas();
    }
  }

  private static long valueOrMissing(Long value) {
    return value == null ? NativeS3GhostMap.MISSING : value;
  }

  private static long[] findTagCollision() {
    Map<Integer, Long> seen = new HashMap<>();
    for (long candidate = 1L; candidate < 1_000_000L; candidate++) {
      Long previous = seen.put((int) mix(candidate), candidate);
      if (previous != null) {
        return new long[] {previous, candidate};
      }
    }
    throw new AssertionError("could not find a deterministic 32-bit tag collision");
  }

  private static long mix(long value) {
    value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
    value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
    return value ^ (value >>> 31);
  }
}
