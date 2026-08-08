package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenancePolicy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MaintenancePolicyTest {
    private static final CacheSerializer<String> STRING = new CacheSerializer<String>() {
        @Override public void serialize(String value, ByteBuffer buffer) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }
        @Override public String deserialize(ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        @Override public int serializedSize(String value) {
            return value.getBytes(StandardCharsets.UTF_8).length;
        }
    };

    @Test
    public void tinyLfuMovesAWindowOverflowIntoProbationBeforeCapacityEviction() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 128L);
        Entry entry = new Entry(0L, 1, 1, 0L);

        policy.add(entry);

        assertEquals(entry.policyState(), Entry.POLICY_TINY_PROBATION,
                "a Window overflow must establish Main probation instead of deleting its own candidate");
    }

    @Test
    public void tinyLfuDrainsMultipleWindowCandidatesInPromotionOrder() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 256L);
        Entry oldest = new Entry(0L, 1, 1, 1L, 0L);
        Entry middle = new Entry(0L, 1, 2, 2L, 0L);
        Entry newest = new Entry(0L, 1, 3, 3L, 0L);

        policy.add(oldest);
        policy.add(middle);
        policy.add(newest);

        assertEquals(policy.selectVictim(8).entry, oldest,
                "the first promoted Window candidate and probation victim are the same oldest entry");
    }

    @Test
    public void tinyLfuDoesNotCarryAnUnneededCandidateIntoTheNextActorWriteBatch() {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
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
            assertNull(cache.get("three"),
                    "a fresh candidate must compete with probation, not an old candidate from a prior actor batch");
        }
    }

    @Test
    public void s3GhostUsesTheFullXxHashInsteadOfTheFoldedChmHash() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
        int foldedHash = 0x13579bdf;
        Entry evicted = new Entry(0L, 1, foldedHash, 0x00000001_13579bdeL, 0L);
        Entry collision = new Entry(0L, 1, foldedHash, 0x00000002_13579bddL, 0L);

        policy.add(evicted);
        policy.remove(evicted, true);
        policy.add(collision);

        assertEquals(collision.policyState(), Entry.POLICY_S3_SMALL,
                "different 64-bit hashes with the same CHM hash must not create a ghost hit");
    }

    @Test
    public void s3FifoGhostsColdSmallEvictionsAndReinsertsThemIntoMain() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
        Entry cold = new Entry(0L, 1, 31, 0x1234_5678_9abc_def0L, 0L);
        Entry returnee = new Entry(0L, 1, 31, 0x1234_5678_9abc_def0L, 0L);

        policy.add(cold);
        policy.remove(cold, true);
        policy.add(returnee);

        assertEquals(returnee.policyState(), Entry.POLICY_S3_MAIN,
                "only a cold Small eviction creates an S3-FIFO ghost admission into Main");
    }

    @Test
    public void s3FifoEvictsSmallWhenItIsExactlyAtItsQuotaAndMainIsPopulated() {
        // A zero-address test Entry still has one 128B rounded key allocation. With a 1280B
        // cache, one Small Entry exactly fills its 10% Small quota.
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_280L);
        Entry cold = new Entry(0L, 1, 41, 0x101L, 0L);
        Entry main = new Entry(0L, 1, 41, 0x101L, 0L);
        Entry small = new Entry(0L, 1, 42, 0x202L, 0L);

        policy.add(cold);
        policy.remove(cold, true);
        policy.add(main); // ghost hit: Main is now populated
        policy.add(small); // Small is exactly at its 10% quota

        assertSame(policy.selectVictim(1).entry, small,
                "S3-FIFO evicts Small at >= quota, rather than taking a Main victim early");
    }
}
