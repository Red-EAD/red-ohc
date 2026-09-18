package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import com.red.ohc.storage.NativeMemory;

/** The reader-entry publication must close the StoreLoad race with shutdown. */
@JCStressTest
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "shutdown observes the active reader")
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE, desc = "reader observes shutdown")
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "both publications are observed")
@Outcome(
    id = "0, 0",
    expect = Expect.FORBIDDEN,
    desc = "reader enters while shutdown misses its published state")
@State
public class ReaderAdmissionStoreLoadStress {
  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final ReaderRegistry registry = new ReaderRegistry(memory);
  private final int slot = registry.register(new ReaderSlot());
  private final AtomicInteger closing = new AtomicInteger();

  @Actor
  public void reader(II_Result result) {
    registry.setReaderState(slot, ReaderRegistry.VALUE_PROTECTION_BIT | 1L);
    result.r1 = closing.get();
  }

  @Actor
  public void shutdown(II_Result result) {
    closing.set(1);
    result.r2 = registry.readerState(slot) == 0L ? 0 : 1;
  }

  @Arbiter
  public void close() {
    registry.clear();
    registry.close();
    memory.closeArenas();
  }
}
