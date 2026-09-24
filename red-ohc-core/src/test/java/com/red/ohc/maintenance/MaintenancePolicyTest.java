package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

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
  public void defaultMaintenanceOwnersExposeAnExplicitCloseBoundary() {
    assertTrue(AutoCloseable.class.isAssignableFrom(MaintenancePolicy.class));
    assertTrue(AutoCloseable.class.isAssignableFrom(TimerWheel.class));
  }

  @Test
  public void closingAStandalonePolicyClosesItsOwnedLinkArena() throws Exception {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, 1_024L);
    Entry entry = EntryTestSupport.entry(1, 11, 0L);
    java.lang.reflect.Field linksField = MaintenancePolicy.class.getDeclaredField("links");
    linksField.setAccessible(true);
    EntryLinks links = (EntryLinks) linksField.get(policy);
    try {
      policy.add(entry);
      assertTrue(links.nativeBytes() > 0L);
      policy.close();
      assertEquals(links.nativeBytes(), 0L);
    } finally {
      policy.close();
    }
  }

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
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 112L);
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
  public void tinyLfuEvictsForAThirdEntryWhenLogicalCapacityIsFull() {
    try (OHCache<String, String> cache =
        OHCacheBuilder.<String, String>newBuilder()
            .capacity(11_000L)
            .keySerializer(STRING)
            .valueSerializer(STRING)
            .eviction(Eviction.W_TINY_LFU)
            .build()) {
      String value = "x".repeat(5 * 1024);
      cache.put("one", value);
      cache.flushAsync().join();
      cache.put("two", value);
      cache.flushAsync().join();
      cache.put("three", value);
      cache.flushAsync().join();

      assertEquals(cache.size(), 2L);
      assertTrue(
          cache.get("one") == null
              || cache.get("two") == null
              || cache.get("three") == null,
          "the actor may choose any cold victim once the relaxed target is exceeded");
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
  public void s3FifoGhostsColdSmallEvictionsAndDefersTheFirstReinsertionToSkip() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    Entry cold = EntryTestSupport.entry(1, 31, 0x1234_5678_9abc_def0L, 0L);
    Entry returnee = EntryTestSupport.entry(1, 31, 0x1234_5678_9abc_def0L, 0L);

    policy.add(cold);
    policy.remove(cold, true);
    policy.add(returnee);

    assertEquals(
        returnee.policyState(),
        Entry.POLICY_S3_SMALL,
        "the first Ghost reappearance must remain out of Main even when its weight drains Skip");
  }

  @Test
  public void s3FifoStartsNewSmallEntriesInSkipAndSuppressesTheirAccessCount() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    Entry entry = EntryTestSupport.entry(1, 32, 0x1234_5678_9abc_def1L, 0L);

    policy.add(entry);
    assertEquals(
        entry.policyState(),
        Entry.POLICY_S4_SKIP,
        "the newest Small entry must be in the virtual Skip region");

    policy.access(entry);

    assertEquals(entry.policyAccessCount(), 0, "Skip hits must not contribute to Small promotion");
    assertEquals(policy.skipSuppressedAccesses(), 1L);
  }

  @Test
  public void s3FifoRequiresASecondGhostReappearanceBeforeMainAdmission() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    long hash = 0x1234_5678_9abc_def2L;
    Entry firstEviction = EntryTestSupport.entry(1, 33, hash, 0L);
    Entry firstReappearance = EntryTestSupport.entry(1, 33, hash, 0L);
    Entry secondReappearance = EntryTestSupport.entry(1, 33, hash, 0L);

    policy.add(firstEviction);
    policy.remove(firstEviction, true);
    policy.add(firstReappearance);

    assertEquals(
        firstReappearance.policyState(),
        Entry.POLICY_S4_SKIP,
        "the first Ghost reappearance must be admitted to Small when tauG is one");
    assertEquals(policy.ghostDeferredPromotions(), 1L);

    policy.remove(firstReappearance, true);
    policy.add(secondReappearance);

    assertEquals(
        secondReappearance.policyState(),
        Entry.POLICY_S3_MAIN,
        "the second Ghost reappearance must be admitted directly to Main");
  }

  @Test
  public void s3SkipConvertsTheOldestEntriesAtTheSkipWeightBoundary() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    Entry oldest = EntryTestSupport.entry(1, 51, 51L, 0L);
    Entry middle = EntryTestSupport.entry(1, 52, 52L, 0L);
    Entry newest = EntryTestSupport.entry(1, 53, 53L, 0L);

    policy.add(oldest);
    assertEquals(oldest.policyState(), Entry.POLICY_S4_SKIP);
    policy.add(middle);
    assertEquals(
        oldest.policyState(),
        Entry.POLICY_S3_SMALL,
        "the oldest Skip entry must leave first when Skip exceeds 15% of Small");
    assertEquals(middle.policyState(), Entry.POLICY_S4_SKIP);

    policy.add(newest);
    assertEquals(middle.policyState(), Entry.POLICY_S3_SMALL);
    assertEquals(newest.policyState(), Entry.POLICY_S4_SKIP);
  }

  @Test
  public void s3OrdinarySmallNeedsTwoHitsBeforePromotionToMain() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    Entry first = EntryTestSupport.entry(1, 61, 61L, 0L);
    Entry second = EntryTestSupport.entry(1, 62, 62L, 0L);

    policy.add(first);
    policy.add(second);
    assertEquals(first.policyState(), Entry.POLICY_S3_SMALL);
    assertEquals(second.policyState(), Entry.POLICY_S4_SKIP);

    policy.access(first);
    policy.access(first);
    assertEquals(first.policyAccessCount(), 2);
    policy.selectVictim(1);

    assertEquals(
        first.policyState(),
        Entry.POLICY_S3_MAIN,
        "two hits after leaving Skip must promote the Small entry to Main");
  }

  @Test
  public void s3SkipWeightTracksVariableValuesInByteAndCountModes() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long value = 0L;
    try {
      MaintenancePolicy bytePolicy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
      Entry byteEntry = EntryTestSupport.entry(1, 71, 71L, 0L);
      bytePolicy.add(byteEntry);
      assertEquals(byteEntry.policyState(), Entry.POLICY_S4_SKIP);

      value = memory.allocate(ValueBlock.allocationLength(256));
      ValueBlock.initialize(value, 0L, 256, 0L);
      byteEntry.valueAddress = value;
      bytePolicy.add(byteEntry);

      assertEquals(
          byteEntry.policyState(),
          Entry.POLICY_S3_SMALL,
          "a value update that exceeds the Skip budget must drain the oldest Skip entry");
      assertEquals(bytePolicy.usedWeight(), byteEntry.policyByteWeight());
      assertEquals(bytePolicy.usedBytes(), byteEntry.policyByteWeight());

      MaintenancePolicy countPolicy = new MaintenancePolicy(Eviction.S3_FIFO, 40L, true);
      Entry countEntry = EntryTestSupport.entry(1, 72, 72L, 0L);
      countPolicy.add(countEntry);
      assertEquals(countEntry.policyState(), Entry.POLICY_S4_SKIP);
      countEntry.valueAddress = value;
      countPolicy.add(countEntry);

      assertEquals(countEntry.policyState(), Entry.POLICY_S4_SKIP);
      assertEquals(countPolicy.usedWeight(), 1L);
      assertEquals(countPolicy.usedBytes(), countEntry.policyByteWeight());
    } finally {
      if (value != 0L) {
        memory.free(value, ValueBlock.allocationLength(256));
      }
      memory.closeArenas();
    }
  }

  @Test
  public void s3ExplicitRemovalClearsDeferredGhostEvidence() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    long hash = 0x1234_5678_9abc_def3L;
    Entry evicted = EntryTestSupport.entry(1, 73, hash, 0L);
    Entry firstReturn = EntryTestSupport.entry(1, 73, hash, 0L);
    Entry afterRemoval = EntryTestSupport.entry(1, 73, hash, 0L);

    policy.add(evicted);
    policy.remove(evicted, true);
    policy.add(firstReturn);
    policy.remove(firstReturn, false);
    policy.add(afterRemoval);

    assertEquals(
        afterRemoval.policyState(),
        Entry.POLICY_S4_SKIP,
        "an explicitly removed first reappearance must not leave a deferred Ghost hit behind");
  }

  @Test
  public void s3MainPromotionClearsDeferredGhostEvidence() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L);
    long hash = 0x1234_5678_9abc_def4L;
    Entry evicted = EntryTestSupport.entry(1, 74, hash, 0L);
    Entry firstReturn = EntryTestSupport.entry(1, 74, hash, 0L);
    Entry boundary = EntryTestSupport.entry(1, 75, 75L, 0L);
    Entry afterPromotion = EntryTestSupport.entry(1, 74, hash, 0L);

    policy.add(evicted);
    policy.remove(evicted, true);
    policy.add(firstReturn);
    policy.add(boundary);
    policy.access(firstReturn);
    policy.access(firstReturn);
    policy.selectVictim(1);
    assertEquals(firstReturn.policyState(), Entry.POLICY_S3_MAIN);

    policy.remove(firstReturn, false);
    policy.add(afterPromotion);

    assertEquals(
        afterPromotion.policyState(),
        Entry.POLICY_S4_SKIP,
        "a first reappearance promoted to Main must clear its pending Ghost evidence");
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
        Entry.POLICY_S3_SMALL,
        "zero is a valid 64-bit ghost hash and its first reappearance must remain out of Main");
  }

  @Test
  public void s3GhostRemovesTheOldestEntryWhenWeightedCapacityIsExceeded() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 600L);
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
        Entry.POLICY_S3_SMALL,
        "the newest ghost must remain available for deferred promotion");
    policy.remove(newest, true);
    Entry secondNewest = EntryTestSupport.entry(1, 10, 10L, 0L);
    policy.add(secondNewest);
    assertEquals(
        secondNewest.policyState(),
        Entry.POLICY_S3_MAIN,
        "the newest ghost must enter Main on its second reappearance");
    assertTrue(
        policy.ghostNativeBytes() > 0L,
        "native ghost accounting must report allocated table and node storage");
  }

  @Test
  public void s3GhostReportsNativeStorage() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
    Entry entry = EntryTestSupport.entry(1, 41, 0x1234L, 0L);
    policy.add(entry);
    policy.remove(entry, true);

    assertTrue(
        policy.ghostNativeBytes() > 0L,
        "S3 ghost state must report native hash and FIFO storage");
  }

  @Test
  public void s3DuplicateFingerprintRefreshesRecencyBeforeWeightedTrim() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 560L);
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
        Entry.POLICY_S3_SMALL,
        "the refreshed duplicate fingerprint must remain available for deferred admission");
    policy.remove(returningA, true);

    Entry secondReturningA = EntryTestSupport.entry(1, 1, 101L, 0L);
    policy.add(secondReturningA);
    assertEquals(
        secondReturningA.policyState(),
        Entry.POLICY_S3_MAIN,
        "the refreshed duplicate fingerprint must enter Main on its second reappearance");
  }

  @Test
  public void s3FifoEvictsSmallWhenItIsExactlyAtItsQuotaAndMainIsPopulated() {
    // A zero-address test Entry charges 56 logical bytes. With a 560B cache, one Small Entry
    // exactly fills its 10% Small quota.
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 560L);
    Entry cold = EntryTestSupport.entry(1, 41, 0x101L, 0L);
    Entry firstMainReappearance = EntryTestSupport.entry(1, 41, 0x101L, 0L);
    Entry main = EntryTestSupport.entry(1, 41, 0x101L, 0L);
    Entry small = EntryTestSupport.entry(1, 42, 0x202L, 0L);

    policy.add(cold);
    policy.remove(cold, true);
    policy.add(firstMainReappearance); // tauG=1: first ghost hit enters Skip
    policy.remove(firstMainReappearance, true);
    policy.add(main); // second ghost hit: Main is now populated
    policy.add(small); // Small is exactly at its 10% quota

    assertSame(
        policy.selectVictim(1).entry,
        small,
        "S4-FIFO-lite evicts Small at >= quota, rather than taking a Main victim early");
  }

  @Test
  public void storesPrimitiveByteWeightAndRemovesItWithTheEntry() throws Exception {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, 1_024L);
    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    long expected = entry.keyAllocationLength();

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
  public void oversizedByteWeightSaturatesPolicyAccounting() {
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, Long.MAX_VALUE);
    Entry entry = EntryTestSupport.entry(1, 17, 0x1717L, 0L);
    long oversizedValueAllocation = Integer.MAX_VALUE;
    try {
      policy.add(entry, 0L, 0x1717);
      policy.add(entry, oversizedValueAllocation, 0x1717);

      assertEquals(entry.policyByteWeight(), (long) Integer.MAX_VALUE);
      assertEquals(policy.usedBytes(), (long) Integer.MAX_VALUE);
      assertEquals(policy.usedWeight(), (long) Integer.MAX_VALUE);

      policy.remove(entry, false);
      assertEquals(policy.usedBytes(), 0L);
      assertEquals(policy.usedWeight(), 0L);
    } finally {
      policy.close();
    }
  }

  @Test
  public void updatesByteWeightAndKeepsCountBoundedPolicyWeightAtOne() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    long value = 0L;
    try {
      Entry entry = EntryTestSupport.entry(1, 8, 0L);
      MaintenancePolicy bytePolicy = new MaintenancePolicy(Eviction.W_TINY_LFU, 1_024L);
      bytePolicy.add(entry);
      long keyBytes = entry.keyAllocationLength();

      value = memory.allocate(ValueBlock.allocationLength(32));
      ValueBlock.initialize(value, 0L, 32, 0L);
      entry.valueAddress = value;
      bytePolicy.add(entry);

      long expected =
          keyBytes + ValueBlock.allocationLength(32);
      assertEquals(entry.policyByteWeight(), expected);
      assertEquals(bytePolicy.usedBytes(), expected);
      assertEquals(bytePolicy.usedWeight(), expected);

      MaintenancePolicy countPolicy = new MaintenancePolicy(Eviction.S3_FIFO, 16L, true);
      Entry countEntry = EntryTestSupport.entry(1, 9, 0L);
      countPolicy.add(countEntry);
      assertEquals(countPolicy.usedWeight(), 1L);
      assertEquals(countPolicy.usedWeight(), 1L);
      assertEquals(countPolicy.usedBytes(), keyBytes);
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
    entry.policyAccessCount(2);

    policy.remove(entry, false);

    assertEquals(entry.policyByteWeight(), 0L);
    assertEquals(entry.policyAccessCount(), 0);
    assertEquals(policy.usedWeight(), 0L);
    assertEquals(policy.usedBytes(), 0L);
  }

  @Test
  public void removeClearsStalePolicyPresentBeforeReleasingAnUnownedLink() throws Exception {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.LRU, 1_024L, false, links);
    Entry entry = EntryTestSupport.entry(memory, 1, 11, 0L);
    Method setPolicyPresent = Entry.class.getDeclaredMethod("setPolicyPresent", boolean.class);
    setPolicyPresent.setAccessible(true);
    try {
      links.ensure(entry);
      entry.markDead();
      entry.policyState(Entry.POLICY_NONE);
      setPolicyPresent.invoke(entry, true);

      policy.remove(entry, false);

      assertFalse(entry.policyPresent());
      assertEquals(entry.policyLinkId(), 0);
      assertEquals(links.activeLinkCount(), 0);
    } finally {
      policy.close();
      links.close();
    }
  }

  @Test
  public void policyStateAccessAndWeightStaySynchronizedWithTheActorMirror() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    EntryLinks links = new EntryLinks(memory);
    MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 4_000L, false, links);
    Entry entry = EntryTestSupport.entry(memory, 1, 81, 0x8181L, 0L);
    Entry trigger = EntryTestSupport.entry(memory, 1, 82, 0x8282L, 0L);
    try {
      policy.add(entry);
      int linkId = entry.policyLinkId();
      assertEquals(links.keyHash(linkId), 0x8181);
      assertEquals(links.policyByteWeight(linkId), entry.policyByteWeight());
      assertEquals(links.policyState(linkId), entry.policyState());
      assertEquals(links.policyAccessCount(linkId), entry.policyAccessCount());

      policy.add(trigger);
      policy.access(entry);
      assertEquals(links.policyState(linkId), entry.policyState());
      assertEquals(links.policyAccessCount(linkId), entry.policyAccessCount());
      assertEquals(links.policyByteWeight(linkId), entry.policyByteWeight());

      // Keep the record alive as a timer-only record so removal can verify the mirror clear
      // before maybeRelease gets a chance to recycle it.
      entry.timerLocation(0);
      policy.remove(entry, false);
      assertEquals(entry.policyState(), Entry.POLICY_NONE);
      assertEquals(entry.policyAccessCount(), 0);
      assertEquals(entry.policyByteWeight(), 0L);
      assertEquals(links.policyState(linkId), Entry.POLICY_NONE);
      assertEquals(links.policyAccessCount(linkId), 0);
      assertEquals(links.policyByteWeight(linkId), 0L);
      assertEquals(links.keyHash(linkId), 0x8181);

      entry.timerScheduled(false);
      links.maybeRelease(entry);
      assertEquals(entry.policyLinkId(), 0);
    } finally {
      policy.close();
      links.close();
      memory.closeArenas();
    }
  }
}
