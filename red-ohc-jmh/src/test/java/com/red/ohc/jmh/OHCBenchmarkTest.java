package com.red.ohc.jmh;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.storage.CacheMath;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class OHCBenchmarkTest {
  @DataProvider(name = "allocationSizes")
  public Object[][] allocationSizes() {
    return new Object[][] {{16, 256}, {17, 257}, {128, 1024}};
  }

  @Test(dataProvider = "allocationSizes")
  public void capacityForMatchesAllocatorWeight(int keyBytes, int valueBytes) {
    long capacity = OHCBenchmark.capacityFor("HIT_ONLY", keyBytes, valueBytes);
    long keyAllocation = Math.max(8L, CacheMath.roundUpTo8((long) keyBytes + Long.BYTES));
    long valueAllocation = ValueBlock.allocationLength(valueBytes);
    long perEntryWeight =
        WriterArena.allocationWeight(keyAllocation) + WriterArena.allocationWeight(valueAllocation);
    long expected = 24_576L * perEntryWeight * 4L / 3L + perEntryWeight;

    Assert.assertEquals(capacity, expected);
    Assert.assertTrue(capacity / perEntryWeight >= 24_576L);
  }
}
