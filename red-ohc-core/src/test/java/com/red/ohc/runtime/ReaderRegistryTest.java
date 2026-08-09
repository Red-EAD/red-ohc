package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.lang.ref.WeakReference;

import org.testng.annotations.Test;

public final class ReaderRegistryTest {
  @Test
  public void registrationPublishesAnActorScanArrayBeforeTheReaderCanEnter() {
    ReaderRegistry registry = new ReaderRegistry();
    ReaderSlot slot = new ReaderSlot();

    registry.register(slot);

    WeakReference<ReaderSlot>[] snapshot = registry.snapshot();
    assertEquals(snapshot.length, 1);
    assertSame(snapshot[0].get(), slot);
  }
}
