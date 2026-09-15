package com.red.ohc.jmh;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;

public class OHCBenchmarkTest {
  @DataProvider(name = "allocationSizes")
  public Object[][] allocationSizes() {
    return new Object[][] {{16, 256}, {17, 257}, {128, 1024}};
  }

  @Test(dataProvider = "allocationSizes")
  public void capacityForMatchesLogicalEntryBytes(int keyBytes, int valueBytes) {
    long capacity = OHCBenchmark.capacityFor("HIT_ONLY", keyBytes, valueBytes);
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(keyBytes);
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    long perEntryBytes = CacheMath.logicalEntryBytes(keyAllocation, valueAllocation);
    long expected = 24_576L * perEntryBytes * 4L / 3L + perEntryBytes;

    Assert.assertEquals(capacity, expected);
    Assert.assertTrue(capacity / perEntryBytes >= 24_576L);
  }
}
