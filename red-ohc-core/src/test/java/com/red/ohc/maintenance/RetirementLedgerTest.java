package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.AllocatorType;
import com.red.ohc.storage.Budget;
import com.red.ohc.storage.NativeMemory;

public class RetirementLedgerTest {
    @Test
    public void ledgerChunkIsAdmittedAndReturnedThroughTheNativeBudget() {
        Budget budget = new Budget(8L << 10);
        NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
        RetirementLedger ledger = new RetirementLedger(memory, budget);
        try {
            ledger.prepare(1);
            assertEquals(budget.ledgerBytes(), 6_144L);
        } finally {
            ledger.freeAll();
            memory.closeArenas();
        }
        assertEquals(budget.ledgerBytes(), 0L);
    }

    @Test
    public void ledgerChunkCannotBypassNativeCapacity() {
        Budget budget = new Budget(1_024L);
        assertTrue(budget.reserveLedger(6_144L));
        assertTrue(budget.reserveLedger(6_144L));
        assertTrue(!budget.reserveLedger(6_144L));
        budget.releaseLedger(6_144L);
        budget.releaseLedger(6_144L);
        assertEquals(budget.ledgerBytes(), 0L);
    }
}
