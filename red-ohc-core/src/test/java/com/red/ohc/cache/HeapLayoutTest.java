package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.storage.ValueBlock;

public class HeapLayoutTest {
  @Test
  public void entryMetadataRemainsAtThe112ByteTarget() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    assertEquals(bytes, 112L, "Entry metadata changed from the 112-byte target");
  }

  @Test
  public void valueHeaderMetadataDoesNotIncreaseNativeAllocationLength() {
    assertEquals(ValueBlock.allocationLength(1), 24L);
    assertEquals(ValueBlock.allocationLength(8), 24L);
    assertEquals(ValueBlock.allocationLength(9), 32L);
  }
}
