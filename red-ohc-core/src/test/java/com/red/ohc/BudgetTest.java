package com.red.ohc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.storage.Budget;

public class BudgetTest {
    @Test
    public void failedReservationReturnsCreditToTheCallingThread() {
        Budget budget = new Budget(64L);

        assertTrue(budget.reserve(32L));
        budget.refund(32L);

        assertTrue(budget.reserve(32L));
        assertTrue(budget.reserve(32L));
    }

    @Test
    public void creditsAreBoundedToFixedCpuStripes() {
        Budget budget = new Budget(64L);
        int expected = 1;
        int target = Math.max(1, Runtime.getRuntime().availableProcessors());
        while (expected < target) expected <<= 1;
        assertEquals(budget.stripeCount(), expected);
    }

    @Test
    public void reclaimedResidentWeightRestoresAdmissionWithoutLeavingBatchCreditDebt() {
        Budget budget = new Budget(64L);

        assertTrue(budget.reserve(32L));
        budget.release(32L);

        assertTrue(budget.reserve(64L), "the first allocation was fully reclaimed, so the full budget is admissible again");
    }

    @Test
    public void partialStripeCreditIsReclaimedBeforeRejectingAnOtherwiseAdmissibleWrite() {
        Budget budget = new Budget(128L);

        assertTrue(budget.reserve(1L), "the first allocation retains a small local refill remainder");
        assertTrue(budget.reserve(121L),
                "global free bytes plus the caller's partial stripe credit still fit below capacity");
    }
}
