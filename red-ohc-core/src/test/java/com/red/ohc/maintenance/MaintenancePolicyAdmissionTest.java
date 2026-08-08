package com.red.ohc.maintenance;

import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

import com.red.ohc.Eviction;
import com.red.ohc.index.Entry;

public final class MaintenancePolicyAdmissionTest {
    @Test
    public void tinyLfuDoesNotUseTheHashDosRandomAdmissionBelowCaffeinesThreshold() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.W_TINY_LFU, 16_384L);
        Entry victim = entry(1L);
        Entry candidate = entry(127L);

        policy.add(victim);
        access(policy, victim, 4); // add + four accesses gives frequency five
        policy.add(candidate);     // moves victim from Window to Probation

        policy.beginWriteBatch();  // the actor starts a new write batch for candidate admission
        access(policy, candidate, 4);
        policy.add(entry(7L));     // moves candidate to Probation; victim remains the LRU victim

        assertSame(policy.selectVictim(1).entry, candidate,
                "a frequency-five candidate must not take Caffeine's 1/128 HashDoS admission path");
    }

    private static Entry entry(long hash) {
        return new Entry(0L, 1, (int) hash, hash, 0L);
    }

    private static void access(MaintenancePolicy policy, Entry entry, int count) {
        for (int index = 0; index < count; index++) policy.access(entry);
    }
}
