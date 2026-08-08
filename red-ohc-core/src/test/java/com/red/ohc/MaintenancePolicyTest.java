package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenancePolicy;

public class MaintenancePolicyTest {
    @Test
    public void s3FifoPromotesAfterOneSampledHitBeforeEvictingIt() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 1_024L);
        Entry hot = new Entry(0L, 1, 1, 0L);
        Entry cold = new Entry(0L, 1, 2, 0L);

        policy.add(hot);
        policy.add(cold);
        policy.access(hot);

        assertSame(policy.victim(), cold);
    }

    @Test
    public void tinyLfuRejectsAColdWindowCandidateWhenTheProbationVictimIsHotter() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 128L);
        Entry hotVictim = new Entry(0L, 1, 1, 0L);
        Entry coldCandidate = new Entry(0L, 1, 2, 0L);

        policy.add(hotVictim);
        policy.victim(); // move the first window entry into probation without removing it
        for (int i = 0; i < 8; i++) policy.access(hotVictim);

        policy.add(coldCandidate);

        assertSame(policy.victim(), coldCandidate,
                "a cold candidate must lose admission to a hotter probation victim");
    }

    @Test
    public void s3FifoBoundsAHotTailScanInsteadOfLoopingInsideOneEvictionAttempt() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, 128L);
        Entry[] entries = new Entry[32];
        for (int index = 0; index < entries.length; index++) {
            entries[index] = new Entry(0L, 1, index + 1, 0L);
            policy.add(entries[index]);
            policy.access(entries[index]);
        }

        assertNull(policy.victim(8), "the maintenance actor must yield after its scan budget");
        assertEquals(policy.lastVictimScanCount(), 8,
                "a hot FIFO tail cannot consume an unbounded actor pass");
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
}
