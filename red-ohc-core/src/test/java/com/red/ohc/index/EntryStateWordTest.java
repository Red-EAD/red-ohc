package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.testng.annotations.Test;

/** Contract tests for state that is intentionally removed from the heap Entry object. */
public final class EntryStateWordTest {
  @Test
  public void stateAndPendingBitsLiveInTheNativeMetadataWord() {
    assertEquals(Entry.NATIVE_METADATA_BYTES, 64L);

    Entry entry = EntryTestSupport.entry(1, 7, 0L);
    assertTrue(entry.claimWriter());
    assertTrue(entry.isWriterLocked());
    entry.finishWriter();
    assertTrue(entry.generation() > 0L);
    assertFalse(
        Arrays.stream(Entry.class.getDeclaredFields())
            .map(Field::getName)
            .anyMatch(name -> name.equals("lifecycle") || name.equals("pendingFlags")));
  }
}
