package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;

public class CacheMathTest {
  @Test
  public void logicalEntryBytesIncludesSerializedKeyAndValueAllocations() {
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(32);
    long valueAllocation = ValueBlock.allocationLength(5 * 1024);

    assertEquals(CacheMath.logicalEntryBytes(keyAllocation, valueAllocation), 5_232L);
  }
}
