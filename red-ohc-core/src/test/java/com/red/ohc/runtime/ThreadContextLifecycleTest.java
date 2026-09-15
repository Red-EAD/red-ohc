package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;

public final class ThreadContextLifecycleTest {
  @Test
  public void readOnlyContextDoesNotCreateWriterState() throws Exception {
    ThreadContext context = new ThreadContext(null);

    assertNull(field("writerState").get(context));
    context.hit();
    assertNull(field("writerState").get(context));
  }

  @Test
  public void firstWriterCreatesOneReusableWriterState() throws Exception {
    ThreadContext context = new ThreadContext(null);

    assertTrue(context.tryEnterWriter());
    Object state = field("writerState").get(context);
    assertNotNull(state);
    assertSame(field("writerState").get(context), state);
    context.exitWriter();
    assertSame(field("writerState").get(context), state);
  }

  @Test
  public void nestedWriterAdmissionFailsWithoutChangingTheOuterState() {
    ThreadContext context = new ThreadContext(null);

    assertTrue(context.tryEnterWriter());
    assertTrue(!context.tryEnterWriter());
    assertTrue(context.isWriterEntered());
    context.exitWriter();
  }

  @Test
  public void accessRingIsCreatedOnlyWhenSamplingFirstHits() {
    ThreadContext context = new ThreadContext(null);
    Entry entry = EntryTestSupport.entry(0, 1, 0L);

    assertNull(context.slot.access);
    for (int index = 0; index < 15; index++) {
      context.access(entry);
    }
    assertNull(context.slot.access);

    context.access(entry);
    assertNotNull(context.slot.access);
  }

  @Test
  public void readerNestingUsesOnlyDepthCounters() throws Exception {
    ThreadContext context = new ThreadContext(null);

    assertNoField("readerValueModes");
    assertNotNull(field("valueProtectionDepth"));
    assertTrue(!context.enterReader(false));
    assertEquals(context.readerDepth(), 1);
    assertEquals(field("valueProtectionDepth").getInt(context), 0);

    assertEquals(context.exitReader(), ThreadContext.READER_EXITED_LOOKUP);
    assertEquals(context.readerDepth(), 0);
  }

  @Test
  public void lookupNestedInsideValueProtectionKeepsTheValueBitUntilTheOuterValueExit() {
    ThreadContext context = new ThreadContext(null);

    assertTrue(context.enterReader(false) == false);
    assertTrue(context.enterReader(true));
    assertTrue(context.enterReader(false) == false);
    assertEquals(context.exitReader(), 0);
    assertEquals(context.exitReader(), ThreadContext.READER_EXITED_VALUES);
    assertEquals(context.exitReader(), ThreadContext.READER_EXITED_LOOKUP);
  }

  @Test
  public void optionalScratchArraysAreCreatedByTheirFirstConsumer() throws Exception {
    ThreadContext context = new ThreadContext(null);

    assertNull(field("topLevelDirectView").get(context));
    assertNull(field("directViews").get(context));
    assertNull(field("readOnlyValueBuffers").get(context));
    assertNull(field("bulkEntrySets").get(context));

    ByteBuffer buffer = context.readOnlyValueBuffer(0L, 0);
    context.releaseReadOnlyValueBuffer();
    assertNotNull(buffer);
    assertNotNull(field("readOnlyValueBuffers").get(context));
    ByteBuffer rebound = context.readOnlyValueBuffer(0L, 0);
    context.releaseReadOnlyValueBuffer();
    assertSame(rebound, buffer);

    DirectValueView view = context.pushDirectView(0L, 0);
    assertEquals(
        field("readOnlyValueDepth").getInt(context),
        0,
        "primitive direct access must not materialize a ByteBuffer shell");
    assertEquals(view.length(), 0);
    assertNotNull(view.asReadOnlyByteBuffer());
    assertNotNull(view);
    assertNotNull(field("topLevelDirectView").get(context));
    assertNull(field("directViews").get(context));

    DirectValueView nested = context.pushDirectView(0L, 0);
    assertNotNull(nested);
    assertNotNull(field("directViews").get(context));
    context.popDirectView();
    context.popDirectView();

    context.releaseBulkEntries(context.acquireBulkEntries(1));
    assertNotNull(field("bulkEntrySets").get(context));
  }

  @Test
  public void releasePageMemoIsLazyBoundedAndClearedAtScopeEnd() throws Exception {
    ThreadContext context = new ThreadContext(null);
    Object firstPage = new Object();
    Object collidingPage = new Object();
    long firstKey = 1L;
    long collidingKey = 257L;

    assertNull(field("releasePageMemo").get(context));
    assertNull(context.releasePageMemoLookup(firstKey));
    context.releasePageMemoRemember(firstKey, firstPage);
    assertNull(field("releasePageMemo").get(context), "non-actor callers must not allocate a memo");

    context.beginReleasePageMemo();
    Object memo = field("releasePageMemo").get(context);
    assertNotNull(memo);
    Field pages = memo.getClass().getDeclaredField("pages");
    pages.setAccessible(true);
    assertEquals(((Object[]) pages.get(memo)).length, 256);

    context.releasePageMemoRemember(firstKey, firstPage);
    assertSame(context.releasePageMemoLookup(firstKey), firstPage);
    context.releasePageMemoRemember(collidingKey, collidingPage);
    assertNull(context.releasePageMemoLookup(firstKey), "a direct-map collision must overwrite");
    assertSame(context.releasePageMemoLookup(collidingKey), collidingPage);
    context.releasePageMemoInvalidate(collidingKey, firstPage);
    assertSame(context.releasePageMemoLookup(collidingKey), collidingPage);
    context.releasePageMemoInvalidate(collidingKey, collidingPage);
    assertNull(context.releasePageMemoLookup(collidingKey));

    long[] retainedKeys = new long[256];
    Object[] retainedPages = new Object[256];
    for (long key = 1L; key <= 300L; key++) {
      Object page = new Object();
      int slot = Long.hashCode(key) & 255;
      retainedKeys[slot] = key;
      retainedPages[slot] = page;
      context.releasePageMemoRemember(key, page);
    }
    for (int slot = 0; slot < retainedKeys.length; slot++) {
      if (retainedKeys[slot] != 0L) {
        assertSame(context.releasePageMemoLookup(retainedKeys[slot]), retainedPages[slot]);
      }
    }

    context.releasePageMemoRemember(firstKey, firstPage);
    context.endReleasePageMemo();
    assertNull(context.releasePageMemoLookup(firstKey));
    for (Object retained : (Object[]) pages.get(memo)) {
      assertNull(retained, "scope cleanup must release every touched descriptor");
    }
  }

  private static Field field(String name) throws Exception {
    Field field = ThreadContext.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static void assertNoField(String name) {
    try {
      ThreadContext.class.getDeclaredField(name);
      throw new AssertionError("obsolete field remains: " + name);
    } catch (NoSuchFieldException expected) {
      // Expected.
    }
  }

}
