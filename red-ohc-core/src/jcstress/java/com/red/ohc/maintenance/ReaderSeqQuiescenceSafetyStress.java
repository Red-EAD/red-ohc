package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/**
 * A reader whose odd sequence word spans the actor's arm-confirm cut must keep its native value
 * out of the reclaim path, regardless of how the entry and the arm interleave.
 */
@JCStressTest
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "the in-op reader blocks native release")
@Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "value released while an armed odd reader held it")
@State
public class ReaderSeqQuiescenceSafetyStress {
  private static final int VALUE_LENGTH = 32_768;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final ReaderRegistry readers = new ReaderRegistry(memory);
  private final ReaderSlot slot = new ReaderSlot();
  private final int slotIndex = readers.register(slot);
  private final RetirementJournal journal = new RetirementJournal(memory);
  private final long allocation = ValueBlock.allocationLength(VALUE_LENGTH);
  private final long value = arena.allocate(allocation);
  private final AtomicInteger valueWasRead = new AtomicInteger();

  public ReaderSeqQuiescenceSafetyStress() {
    ValueBlock.initialize(value, 0L, VALUE_LENGTH, 0L);
    journal.append(value, allocation);
    journal.cutAllProducersAtWatermark();
    if (journal.sealReadySegments(1L) != 1) {
      throw new AssertionError("expected one sealed retirement segment");
    }
  }

  @Actor
  public void reader() {
    readers.beginOpForTest(slotIndex, true);
    NativeMemory.getLong(value);
    valueWasRead.set(1);
    NativeMemory.getLong(value);
  }

  @Actor
  public void actor() {
    if (valueWasRead.get() == 0) {
      return;
    }
    journal.publishSafeForQuiescence(readers);
    journal.reclaimActorResult(memory, Integer.MAX_VALUE);
  }

  @Arbiter
  public void observe(I_Result result) {
    result.r1 = (int) journal.completedRecordsTotal();
    readers.endOpForTest(slotIndex);
    journal.publishSafe(true, true);
    journal.reclaimActorResult(memory, Integer.MAX_VALUE);
    readers.clear();
    readers.close();
    journal.close();
    arena.detach();
  }
}
