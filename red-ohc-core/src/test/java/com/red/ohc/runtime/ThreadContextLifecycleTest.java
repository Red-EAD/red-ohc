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
    Entry entry = new Entry(0L, 0, 1, 0L);

    assertNull(context.slot.access);
    for (int index = 0; index < 15; index++) {
      context.access(entry);
    }
    assertNull(context.slot.access);

    context.access(entry);
    assertNotNull(context.slot.access);
  }

  @Test
  public void optionalScratchArraysAreCreatedByTheirFirstConsumer() throws Exception {
    ThreadContext context = new ThreadContext(null);

    assertNull(field("directViews").get(context));
    assertNull(field("readOnlyValueBuffers").get(context));
    assertNull(field("bulkEntrySets").get(context));

    ByteBuffer buffer = context.readOnlyValueBuffer(0L, 0);
    context.releaseReadOnlyValueBuffer();
    assertNotNull(buffer);
    assertNotNull(field("readOnlyValueBuffers").get(context));

    DirectValueView view = context.pushDirectView(0L, 0);
    assertEquals(
        field("readOnlyValueDepth").getInt(context),
        0,
        "primitive direct access must not materialize a ByteBuffer shell");
    assertEquals(view.length(), 0);
    assertNotNull(view.asReadOnlyByteBuffer());
    context.popDirectView();
    assertNotNull(view);
    assertNotNull(field("directViews").get(context));

    context.releaseBulkEntries(context.acquireBulkEntries(1));
    assertNotNull(field("bulkEntrySets").get(context));
  }

  private static Field field(String name) throws Exception {
    Field field = ThreadContext.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
