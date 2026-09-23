package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.testng.annotations.Test;

public class FrequencySketchTest {
  @Test
  public void countersAreFourBitAndSaturateAtFifteen() {
    FrequencySketch sketch = new FrequencySketch();
    for (int i = 0; i < 32; i++) {
      sketch.increment(0x9abcdef0);
    }

    assertEquals(sketch.frequency(0x9abcdef0), 15);
    assertEquals(sketch.bytes(), 131_072L);
  }

  @Test
  public void sketchStorageTracksPlannedIndexSlotsRatherThanUsingAFixedLargeTable() {
    FrequencySketch sketch = new FrequencySketch(8_192L);

    assertEquals(sketch.bytes(), 8_192L, "8,192 planned slots require 1,024 packed long words");
  }

  @Test
  public void resetAccountsForOddCountersWhenReducingTheSampleWindow() throws Exception {
    FrequencySketch sketch = new FrequencySketch(1L);
    Field table = FrequencySketch.class.getDeclaredField("table");
    table.setAccessible(true);
    ((long[]) table.get(sketch))[0] = 0x1111L;
    Field size = FrequencySketch.class.getDeclaredField("size");
    size.setAccessible(true);
    size.setLong(sketch, 10L);
    Method reset = FrequencySketch.class.getDeclaredMethod("reset");
    reset.setAccessible(true);
    reset.invoke(sketch);
    assertEquals(
        size.getLong(sketch),
        4L,
        "reset must subtract the four odd counter nibbles before halving the sample count");
  }
}
