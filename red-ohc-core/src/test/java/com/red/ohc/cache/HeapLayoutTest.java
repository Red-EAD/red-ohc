package com.red.ohc.cache;

import static org.testng.Assert.assertTrue;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class HeapLayoutTest {
  @Test
  public void entryMetadataStaysWithinTheSmallHeapBudget() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    // The weakValue reference and primitive policy byte weight add two aligned slots to the
    // previous 96-byte Entry; this remains substantially smaller than a per-entry boxed map.
    assertTrue(bytes <= 112L, "Entry metadata grew to " + bytes + " bytes");
  }
}
