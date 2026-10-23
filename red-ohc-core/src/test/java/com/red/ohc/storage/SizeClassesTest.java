package com.red.ohc.storage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public final class SizeClassesTest {
  @Test
  public void powerOfTwoValuesIncludeBothHeadersWithoutTheNextLargeSlot() {
    assertEquals(WriterArena.allocationWeight(ValueBlock.allocationLength(4096)), 4224L);
    assertEquals(WriterArena.allocationWeight(ValueBlock.allocationLength(16384)), 16512L);
  }
}
