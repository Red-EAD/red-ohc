package com.red.ohc.runtime;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class ThreadContextBulkEntrySetTest {
  @Test
  public void reusesBoundedSetsAndIsolatesNestedAndHugeCalls() {
    ThreadContext context = new ThreadContext(null, null);
    ReferenceOpenHashSet<Entry> outer = context.acquireBulkEntries(256);
    context.releaseBulkEntries(outer);
    ReferenceOpenHashSet<Entry> reused = context.acquireBulkEntries(256);
    assertSame(reused, outer, "bounded bulk entry set should be reused");

    ReferenceOpenHashSet<Entry> nested = context.acquireBulkEntries(64);
    assertNotSame(nested, outer, "nested bulk calls need independent dedup state");
    context.releaseBulkEntries(nested);
    context.releaseBulkEntries(reused);

    ReferenceOpenHashSet<Entry> huge = context.acquireBulkEntries(8_192);
    context.releaseBulkEntries(huge);
    ReferenceOpenHashSet<Entry> afterHuge = context.acquireBulkEntries(256);
    assertNotSame(afterHuge, huge, "huge calls must not pin their dedup table");
    context.releaseBulkEntries(afterHuge);
  }
}
