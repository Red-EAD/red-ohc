package com.red.ohc;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.red.ohc.maintenance.FrequencySketch;

public class FrequencySketchTest {
    @Test
    public void countersAreFourBitAndSaturateAtFifteen() {
        FrequencySketch sketch = new FrequencySketch();
        for (int i = 0; i < 32; i++) sketch.increment(0x123456789abcdef0L);

        assertEquals(sketch.frequency(0x123456789abcdef0L), 15);
        assertEquals(sketch.bytes(), 131_072L);
    }

    @Test
    public void sketchStorageTracksPlannedIndexSlotsRatherThanUsingAFixedLargeTable() {
        FrequencySketch sketch = new FrequencySketch(8_192L);

        assertEquals(sketch.bytes(), 8_192L,
                "8,192 planned slots require 1,024 packed long words");
    }
}
