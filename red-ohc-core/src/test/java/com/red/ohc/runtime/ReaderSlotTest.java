package com.red.ohc.runtime;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;
import org.testng.annotations.Test;

public class ReaderSlotTest {
  @Test
  public void readerSlotHasNoProducerWakeState() {
    for (Class<?> type = ReaderSlot.class; type != Object.class; type = type.getSuperclass()) {
      for (Field field : type.getDeclaredFields()) {
        assertFalse(
            field.getName().equals("accessPending"),
            "reader slots must not carry a producer-to-actor wake CAS state");
      }
    }
  }

  @Test
  public void actorWritesAreIsolatedFromReadersOn64ByteLines() {
    assertActorFieldIsolation(64);
  }

  private static void assertActorFieldIsolation(int lineBytes) {
    ClassLayout layout = ClassLayout.parseClass(ReaderSlot.class);
    Map<String, FieldLayout> fields = new HashMap<>();
    for (FieldLayout field : layout.fields()) {
      fields.put(field.name(), field);
    }
    String[] actorFields = {"consumedSkipAccesses", "readerNotification"};
    String[] readerFields = {
      "readerStateAddress",
      "seqAddress",
      "localHits",
      "localMisses",
      "publishedHits",
      "publishedMisses",
      "localSkipAccesses",
      "publishedSkipAccesses"
    };
    assertFieldsDoNotShareLine(fields, "consumedSkipAccesses", "readerNotification", lineBytes);
    for (String actor : actorFields) {
      for (String reader : readerFields) {
        assertFieldsDoNotShareLine(fields, actor, reader, lineBytes);
      }
    }
  }

  private static void assertFieldsDoNotShareLine(
      Map<String, FieldLayout> fields, String first, String second, int lineBytes) {
    FieldLayout firstField = requireLayoutField(fields, first);
    FieldLayout secondField = requireLayoutField(fields, second);
    // Objects need not start on a cache-line boundary; include straddling fields too.
    for (int base = 0; base < lineBytes; base++) {
      long firstStart = (base + firstField.offset()) / lineBytes;
      long firstEnd = (base + firstField.offset() + firstField.size() - 1L) / lineBytes;
      long secondStart = (base + secondField.offset()) / lineBytes;
      long secondEnd = (base + secondField.offset() + secondField.size() - 1L) / lineBytes;
      assertTrue(
          firstEnd < secondStart || secondEnd < firstStart,
          first
              + " must not share a "
              + lineBytes
              + "-byte cache line with "
              + second
              + " at object base "
              + base);
    }
  }

  private static FieldLayout requireLayoutField(Map<String, FieldLayout> fields, String name) {
    FieldLayout field = fields.get(name);
    assertTrue(field != null, "missing ReaderSlot layout field " + name);
    return field;
  }
}
