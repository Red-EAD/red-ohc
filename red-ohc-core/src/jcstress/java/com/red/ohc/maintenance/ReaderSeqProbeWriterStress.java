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
 * A writer-side probe (odd with the value bit clear) and a structural retirement record: the
 * probe's open op must pin the key block through the same arm-confirm cut.
 */
@JCStressTest
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "the in-op probe blocks structural release")
@Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "key block released under the open probe")
@State
public class ReaderSeqProbeWriterStress {
  private static final int KEY_LENGTH = 32;

  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final ReaderRegistry readers = new ReaderRegistry(memory);
  private final ReaderSlot slot = new ReaderSlot();
  private final int slotIndex = readers.register(slot);
  private final RetirementJournal journal = new RetirementJournal(memory);
  private final long keyAllocation =
      com.red.ohc.index.Entry.keyAllocationLengthForKeyLength(KEY_LENGTH);
  private final long key = arena.allocate(keyAllocation);
  private final AtomicInteger keyWasRead = new AtomicInteger();

  public ReaderSeqProbeWriterStress() {
    NativeMemory.putLong(key + 48L, 0x5eedL);
    journal.appendStructural(key, keyAllocation);
    journal.cutAllProducersAtWatermark();
    if (journal.sealReadySegments(1L) != 1) {
      throw new AssertionError("expected one sealed structural record");
    }
  }

  @Actor
  public void probe() {
    readers.beginOpForTest(slotIndex, false);
    NativeMemory.getLong(key);
    keyWasRead.set(1);
    NativeMemory.getLong(key);
  }

  @Actor
  public void actor() {
    if (keyWasRead.get() == 0) {
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
