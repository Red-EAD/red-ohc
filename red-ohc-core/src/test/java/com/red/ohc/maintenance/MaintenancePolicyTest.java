package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import it.unimi.dsi.fastutil.longs.Long2LongLinkedOpenHashMap;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class MaintenancePolicyTest {
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

  @Test
  public void tinyLfuMovesAWindowOverflowIntoProbationBeforeCapacityEviction() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 128L);
    Entry entry = EntryTestSupport.entry(1, 1, 0L);

    policy.add(entry);

    assertEquals(
        entry.policyState(),
        Entry.POLICY_TINY_PROBATION,
        "a Window overflow must establish Main probation instead of deleting its own candidate");
  }

  @Test
  public void tinyLfuDrainsMultipleWindowCandidatesInPromotionOrder() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 256L);
    Entry oldest = EntryTestSupport.entry(1, 1, 1L, 0L);
    Entry middle = EntryTestSupport.entry(1, 2, 2L, 0L);
    Entry newest = EntryTestSupport.entry(1, 3, 3L, 0L);

    policy.add(oldest);
    policy.add(middle);
    policy.add(newest);

    assertEquals(
        policy.selectVictim(8).entry,
        oldest,
        "the first promoted Window candidate and probation victim are the same oldest entry");
  }

  @Test
  public void tinyLfuDoesNotCarryAnUnneededCandidateIntoTheNextActorWriteBatch() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(512L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .eviction(Eviction.W_TINY_LFU)
            .build()) {
      assertEquals(cache.put("one", "v"), true);
      cache.flushAsync().join();
      assertEquals(cache.put("two", "v"), true);
      cache.flushAsync().join();
      assertEquals(cache.put("three", "v"), true);
      cache.flushAsync().join();

      assertEquals(cache.get("one"), "v");
      assertEquals(cache.get("two"), "v");
      assertNull(
          cache.get("three"),
          "a fresh candidate must compete with probation, not an old candidate from a prior actor"
              + " batch");
    }
  }

  @Test
  public void s3GhostUsesTheFullXxHashInsteadOfTheFoldedChmHash() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    int foldedHash = 0x13579bdf;
    Entry evicted = EntryTestSupport.entry(1, foldedHash, 0x00000001_13579bdeL, 0L);
    Entry collision = EntryTestSupport.entry(1, foldedHash, 0x00000002_13579bddL, 0L);

    policy.add(evicted);
    policy.remove(evicted, true);
    policy.add(collision);

    assertEquals(
        collision.policyState(),
        Entry.POLICY_S3_SMALL,
        "different 64-bit hashes with the same CHM hash must not create a ghost hit");
  }

  @Test
  public void s3FifoGhostsColdSmallEvictionsAndReinsertsThemIntoMain() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    Entry cold = EntryTestSupport.entry(1, 31, 0x1234_5678_9abc_def0L, 0L);
    Entry returnee = EntryTestSupport.entry(1, 31, 0x1234_5678_9abc_def0L, 0L);

    policy.add(cold);
    policy.remove(cold, true);
    policy.add(returnee);

    assertEquals(
        returnee.policyState(),
        Entry.POLICY_S3_MAIN,
        "only a cold Small eviction creates an S3-FIFO ghost admission into Main");
  }

  @Test
  public void s3GhostAcceptsZeroAsARealHashKey() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    Entry evicted = EntryTestSupport.entry(1, 0, 0L, 0L);
    Entry returnee = EntryTestSupport.entry(1, 0, 0L, 0L);

    policy.add(evicted);
    policy.remove(evicted, true);
    policy.add(returnee);

    assertEquals(
        returnee.policyState(),
        Entry.POLICY_S3_MAIN,
        "zero is a valid 64-bit ghost hash and must not be treated as a missing value");
  }

  @Test
  public void s3GhostRemovesTheOldestEntryWhenWeightedCapacityIsExceeded() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_280L);
    Entry oldest = EntryTestSupport.entry(1, 1, 1L, 0L);
    Entry newest = EntryTestSupport.entry(1, 10, 10L, 0L);

    for (int hash = 1; hash <= 10; hash++) {
      Entry entry =
          hash == 1 ? oldest : hash == 10 ? newest : EntryTestSupport.entry(1, hash, hash, 0L);
      policy.add(entry);
      policy.remove(entry, true);
    }

    policy.add(oldest);
    assertEquals(
        oldest.policyState(),
        Entry.POLICY_S3_SMALL,
        "the oldest ghost must be removed first when ghost weight exceeds its limit");

    policy.add(newest);
    assertEquals(
        newest.policyState(),
        Entry.POLICY_S3_MAIN,
        "the newest ghost must remain available for Main admission");
    assertEquals(
        policy.ghostHeapBytes(),
        33L * Long.BYTES * 3L,
        "ghost heap accounting must report primitive backing-array payload, including spare"
            + " capacity");
  }

  @Test
  public void s3GhostUsesAPrimitiveLinkedHashMap() throws Exception {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    java.lang.reflect.Field ghost = MaintenancePolicy.class.getDeclaredField("ghost");
    ghost.setAccessible(true);

    assertTrue(
        ghost.get(policy) instanceof Long2LongLinkedOpenHashMap,
        "S3 ghost state must use the primitive linked map");
  }

  @Test
  public void s3DuplicateFingerprintRefreshesRecencyBeforeWeightedTrim() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 512L);
    Entry firstA = EntryTestSupport.entry(1, 1, 101L, 0L);
    Entry secondA = EntryTestSupport.entry(200, 1, 101L, 0L);
    Entry b = EntryTestSupport.entry(1, 2, 202L, 0L);
    Entry c = EntryTestSupport.entry(1, 3, 303L, 0L);

    policy.add(firstA);
    policy.add(secondA);
    policy.add(b);
    policy.remove(firstA, true);
    policy.remove(b, true);
    policy.remove(secondA, true);
    policy.add(c);
    policy.remove(c, true);

    Entry returningB = EntryTestSupport.entry(1, 2, 202L, 0L);
    policy.add(returningB);
    assertEquals(
        returningB.policyState(),
        Entry.POLICY_S3_SMALL,
        "refreshing a duplicate fingerprint must make the older different fingerprint the first"
            + " trim victim");
    policy.remove(returningB, false);

    Entry returningA = EntryTestSupport.entry(1, 1, 101L, 0L);
    policy.add(returningA);
    assertEquals(
        returningA.policyState(),
        Entry.POLICY_S3_MAIN,
        "the refreshed duplicate fingerprint must remain available for Main admission");
  }

  @Test
  public void s3FifoEvictsSmallWhenItIsExactlyAtItsQuotaAndMainIsPopulated() {
    // A zero-address test Entry still has one 128B rounded key allocation. With a 1280B
    // cache, one Small Entry exactly fills its 10% Small quota.
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_280L);
    Entry cold = EntryTestSupport.entry(1, 41, 0x101L, 0L);
    Entry main = EntryTestSupport.entry(1, 41, 0x101L, 0L);
    Entry small = EntryTestSupport.entry(1, 42, 0x202L, 0L);

    policy.add(cold);
    policy.remove(cold, true);
    policy.add(main); // ghost hit: Main is now populated
    policy.add(small); // Small is exactly at its 10% quota

    assertSame(
        policy.selectVictim(1).entry,
        small,
        "S3-FIFO evicts Small at >= quota, rather than taking a Main victim early");
  }

  @Test
  public void storesPrimitiveByteWeightAndRemovesItWithTheEntry() throws Exception {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, 1_024L);
    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    long expected = WriterArena.allocationWeight(entry.keyAllocationLength());

    policy.add(entry);

    assertEquals(entry.policyByteWeight(), expected);
    assertEquals(policy.usedBytes(), expected);
    assertEquals(policy.usedWeight(), expected);

    policy.remove(entry, false);

    assertEquals(entry.policyByteWeight(), 0L);
    assertEquals(policy.usedBytes(), 0L);
    assertEquals(policy.usedWeight(), 0L);

    for (java.lang.reflect.Field field : MaintenancePolicy.class.getDeclaredFields()) {
      assertFalse(
          field.getName().equals("byteWeights"),
          "per-entry byte weights must not be retained in an IdentityHashMap");
    }
  }

  @Test
  public void updatesByteWeightAndKeepsCountBoundedPolicyWeightAtOne() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long value = 0L;
    try {
      Entry entry = EntryTestSupport.entry(1, 8, 0L);
      MaintenancePolicy bytePolicy = new MaintenancePolicy(Eviction.W_TINY_LFU, 1_024L);
      bytePolicy.add(entry);
      long keyWeight = WriterArena.allocationWeight(entry.keyAllocationLength());

      value = memory.allocate(ValueBlock.allocationLength(32));
      ValueBlock.initialize(value, 0L, 32, 0L);
      entry.valueAddress = value;
      bytePolicy.add(entry);

      long expected =
          keyWeight
              + WriterArena.allocationWeight(ValueBlock.allocationLength(32));
      assertEquals(entry.policyByteWeight(), expected);
      assertEquals(bytePolicy.usedBytes(), expected);
      assertEquals(bytePolicy.usedWeight(), expected);

      MaintenancePolicy countPolicy = new MaintenancePolicy(Eviction.S3_FIFO, 16L, true);
      Entry countEntry = EntryTestSupport.entry(1, 9, 0L);
      countPolicy.add(countEntry);
      assertEquals(countPolicy.usedWeight(), 1L);
      assertEquals(countPolicy.usedWeight(), 1L);
      assertEquals(countPolicy.usedBytes(), keyWeight);
      countPolicy.remove(countEntry, false);
      assertEquals(countPolicy.usedWeight(), 0L);
      assertEquals(countPolicy.usedBytes(), 0L);
    } finally {
      if (value != 0L) {
        memory.free(value, ValueBlock.allocationLength(32));
      }
      memory.closeArenas();
    }
  }

  @Test
  public void removeClearsPolicyMetadataFromAnAlreadyUnlinkedEntry() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, 1_024L);
    Entry entry = EntryTestSupport.entry(1, 10, 0L);
    entry.policyByteWeight(256L);

    policy.remove(entry, false);

    assertEquals(entry.policyByteWeight(), 0L);
    assertEquals(policy.usedWeight(), 0L);
    assertEquals(policy.usedBytes(), 0L);
  }
}
