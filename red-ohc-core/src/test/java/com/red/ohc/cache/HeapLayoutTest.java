package com.red.ohc.cache;

import static org.testng.Assert.assertTrue;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class HeapLayoutTest {
  @Test
  public void entryMetadataStaysWithinTheSmallHeapBudget() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    assertTrue(bytes <= 96L, "Entry metadata grew to " + bytes + " bytes");
  }
}
