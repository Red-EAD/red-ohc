package com.red.ohc;

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
}
