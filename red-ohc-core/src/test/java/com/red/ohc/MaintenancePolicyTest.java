package com.red.ohc;

import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.maintenance.MaintenancePolicy;

public class MaintenancePolicyTest {
    @Test
    public void s3FifoPromotesAHotWindowEntryBeforeEvictingIt() {
        MaintenancePolicy policy = new MaintenancePolicy(Eviction.S3_FIFO, new java.util.concurrent.atomic.AtomicLong());
        Entry hot = new Entry(0L, 1, 1, 0L);
        Entry cold = new Entry(0L, 1, 2, 0L);

        policy.add(hot);
        policy.add(cold);
        policy.access(hot);
        policy.access(hot);

        assertSame(policy.victim(), cold);
    }
}
