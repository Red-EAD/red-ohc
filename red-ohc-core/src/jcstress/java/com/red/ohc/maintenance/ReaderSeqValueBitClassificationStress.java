package com.red.ohc.maintenance;

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
 * A lookup-only odd reader (value bit clear) must not pin a pure value record: confirm's
 * valueSafe channel must stay open while blocked[0] is set.
 */
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "lookup-only reader releases the value record")
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "the cut raced ahead of the reader entry")
@State
public class ReaderSeqValueBitClassificationStress {
  private static final int VALUE_LENGTH = 32_768;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final ReaderRegistry readers = new ReaderRegistry(memory);
  private final ReaderSlot slot = new ReaderSlot();
  private final int slotIndex = readers.register(slot);
  private final RetirementJournal journal = new RetirementJournal(memory);
  private final long allocation = ValueBlock.allocationLength(VALUE_LENGTH);
  private final long value = arena.allocate(allocation);

  public ReaderSeqValueBitClassificationStress() {
    ValueBlock.initialize(value, 0L, VALUE_LENGTH, 0L);
    journal.append(value, allocation);
    journal.cutAllProducersAtWatermark();
    if (journal.sealReadySegments(1L) != 1) {
      throw new AssertionError("expected one sealed retirement segment");
    }
  }

  @Actor
  public void lookupReader() {
    readers.beginOpForTest(slotIndex, false);
    NativeMemory.getLong(value);
    NativeMemory.getLong(value);
  }

  @Actor
  public void actor() {
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
