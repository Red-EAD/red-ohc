package com.red.ohc.index;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public final class EntryMappingStateTest {
    @Test
    public void detachedEntryDoesNotNeedNativeKeyForCurrentChecks() {
        Entry entry = new Entry(0L, 1, 7, 0L);

        assertTrue(entry.isMapped());
        entry.markUnmapped();
        assertFalse(entry.isMapped());
        entry.markMapped();
        assertTrue(entry.isMapped());
    }
}
