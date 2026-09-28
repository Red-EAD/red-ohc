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

  @Test
  public void absenceTagRidesTheTaggedWordWithoutTouchingLifecycle() {
    long raw = 0x1_0000_0040L;
    long absent = Entry.absentTaggedValue(raw, true);
    assertTrue(Entry.isAbsentTaggedValue(absent));
    assertTrue(Entry.isAliveTagged(absent));
    assertTrue(Entry.hasTtl(absent));
    assertEquals(Entry.rawValueAddress(absent), raw);
    assertFalse(Entry.isAbsentTaggedValue(Entry.tagValueAddress(raw, true)));
    assertTrue(Entry.samePublishedValue(absent, Entry.tagValueAddress(raw, true)));
    assertFalse(Entry.samePublishedValue(absent, Entry.tagValueAddress(raw + 8L, true)));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void tagValueAddressRejectsRawAddressesAtOrAboveBit62() {
    Entry.tagValueAddress(1L << 62, false);
  }

  @Test
  public void lifecycleTransitionsKeepTheAbsenceTag() {
    Entry entry = EntryTestSupport.entry(1, 7, 0x80L);
    assertTrue(entry.markLogicallyPresent());
    assertTrue(entry.claimWriter());
    try {
      assertTrue(entry.markLogicallyAbsentAfterWriterClaim());
      assertFalse(entry.markLogicallyAbsentAfterWriterClaim());
      entry.markRetired();
      assertTrue(Entry.isAbsentTaggedValue(entry.valueAddress));
      entry.restoreAlive();
      assertTrue(Entry.isAbsentTaggedValue(entry.valueAddress));
      entry.clearValue();
      assertTrue(Entry.isAbsentTaggedValue(entry.valueAddress));
      assertEquals(Entry.rawValueAddress(entry.valueAddress), 0L);
    } finally {
      entry.finishWriter();
    }
  }
}
