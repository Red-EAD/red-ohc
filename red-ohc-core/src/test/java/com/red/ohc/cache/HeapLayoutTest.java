package com.red.ohc.cache;

import static org.testng.Assert.assertTrue;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class HeapLayoutTest {
  @Test
  public void entryMetadataStaysWithinTheSmallHeapBudget() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    // The weakValue reference adds one aligned reference slot to the previous 96-byte Entry.
    assertTrue(bytes <= 104L, "Entry metadata grew to " + bytes + " bytes");
  }
}
