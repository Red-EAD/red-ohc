package com.red.ohc.runtime;

import static org.testng.Assert.assertFalse;

import java.lang.reflect.Field;

import org.testng.annotations.Test;

public class ReaderSlotTest {
  @Test
  public void readerSlotHasNoProducerWakeState() {
    for (Field field : ReaderSlot.class.getDeclaredFields()) {
      assertFalse(
          field.getName().equals("accessPending"),
          "reader slots must not carry a producer-to-actor wake CAS state");
    }
  }
}
