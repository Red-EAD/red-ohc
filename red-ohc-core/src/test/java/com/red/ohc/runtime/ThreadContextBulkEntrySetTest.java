package com.red.ohc.runtime;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class ThreadContextBulkEntrySetTest {
  @Test
  public void reusesBoundedSetsAndIsolatesNestedAndHugeCalls() {
    ThreadContext context = new ThreadContext(null, null);
    Set<Entry> outer = context.acquireBulkEntries(256);
    context.releaseBulkEntries(outer);
    Set<Entry> reused = context.acquireBulkEntries(256);
    assertSame(reused, outer, "bounded bulk entry set should be reused");

    Set<Entry> nested = context.acquireBulkEntries(64);
    assertNotSame(nested, outer, "nested bulk calls need independent dedup state");
    context.releaseBulkEntries(nested);
    context.releaseBulkEntries(reused);

    Set<Entry> huge = context.acquireBulkEntries(8_192);
    context.releaseBulkEntries(huge);
    Set<Entry> afterHuge = context.acquireBulkEntries(256);
    assertNotSame(afterHuge, huge, "huge calls must not pin their dedup table");
    context.releaseBulkEntries(afterHuge);
  }

  @Test
  public void bulkDedupUsesOnlyAJdkIdentitySet() {
    ThreadContext context = new ThreadContext(null, null);
    Set<Entry> entries = context.acquireBulkEntries(16);
    try {
      assertTrue(
          entries.getClass().getName().startsWith("java.util."),
          "bulk dedup must not expose a third-party collection: " + entries.getClass().getName());
    } finally {
      context.releaseBulkEntries(entries);
    }
  }
}
