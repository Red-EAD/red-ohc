package com.red.ohc.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;

/** Writer admission must become visible before the writer accepts an open cache state. */
@JCStressTest
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "shutdown observes the active writer")
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE, desc = "writer observes shutdown")
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "both publications are observed")
@Outcome(
    id = "0, 0",
    expect = Expect.FORBIDDEN,
    desc = "writer enters while shutdown misses its admission")
@State
public class WriterAdmissionStoreLoadStress {
  private final NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
  private final ReaderRegistry registry = new ReaderRegistry(memory);
  private final int slot = registry.register(new ReaderSlot());
  private final AtomicInteger closing = new AtomicInteger();

  @Actor
  public void writer(II_Result result) {
    registry.setWriterActive(slot, true);
    result.r1 = closing.get();
  }

  @Actor
  public void shutdown(II_Result result) {
    closing.set(1);
    result.r2 = registry.hasActiveWriter() ? 1 : 0;
  }

  @Arbiter
  public void close() {
    registry.clear();
    registry.close();
    memory.closeArenas();
  }
}
