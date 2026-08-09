package com.red.ohc.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public final class EntryValueTagTest {
  @Test
  public void valueTagKeepsTheRawNativeAddressAndDistinguishesTtl() {
    long raw = 0x1_0000_0040L;
    long permanent = Entry.tagValueAddress(raw, false);
    long timed = Entry.tagValueAddress(raw, true);
    assertEquals(Entry.rawValueAddress(permanent), raw);
    assertEquals(Entry.rawValueAddress(timed), raw);
    assertFalse(Entry.hasTtl(permanent));
    assertTrue(Entry.hasTtl(timed));
    assertEquals(Entry.rawValueAddress(0L), 0L);
  }
}
