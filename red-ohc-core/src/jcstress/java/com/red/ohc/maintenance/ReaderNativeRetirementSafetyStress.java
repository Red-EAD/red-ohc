package com.red.ohc.maintenance;

import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ReaderSlot;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

/** A value-protected reader must keep its native value out of the SAFE queue. */
@JCStressTest
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "the active value reader blocks native release")
@Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "native value was released while the reader guard was active")
@State
public class ReaderNativeRetirementSafetyStress {
  private static final long READER_EPOCH = 1L;
  private static final int VALUE_LENGTH = 32_768;

  private final NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.UNSAFE);
  private final WriterArena arena = memory.newWriterArena();
  private final ReaderRegistry readers = new ReaderRegistry(memory);
  private final ReaderSlot slot = new ReaderSlot();
  private final int slotIndex = readers.register(slot);
  private final RetirementJournal journal = new RetirementJournal(memory);
  private final long allocation = ValueBlock.allocationLength(VALUE_LENGTH);
  private final long value = arena.allocate(allocation);
  private final AtomicInteger valueWasRead = new AtomicInteger();

  public ReaderNativeRetirementSafetyStress() {
    ValueBlock.initialize(value, 0L, VALUE_LENGTH, 0L);
    journal.append(value, allocation);
    journal.cutAllProducersAtWatermark();
    if (journal.sealReadySegments(READER_EPOCH) != 1) {
      throw new AssertionError("expected one sealed retirement segment");
    }
  }

  @Actor
  public void readValue() {
    readers.setValueEpoch(slotIndex, READER_EPOCH);
    NativeMemory.getLong(value);
    valueWasRead.set(1);
    // Keep the value guard active while the maintenance actor attempts publication and reclaim.
    NativeMemory.getLong(value);
  }

  @Actor
  public void reclaim() {
    if (valueWasRead.get() == 0) {
      return;
    }
    long[] minimumEpochs = new long[2];
    readers.minActiveEpochs(minimumEpochs);
    journal.publishSafe(minimumEpochs[0], minimumEpochs[1]);
    journal.reclaimActorResult(memory, Integer.MAX_VALUE);
  }

  @Arbiter
  public void observe(I_Result result) {
    result.r1 = (int) journal.completedRecordsTotal();
    readers.setEpoch(slotIndex, 0L);
    journal.publishSafe(Long.MAX_VALUE, Long.MAX_VALUE);
    journal.reclaimActorResult(memory, Integer.MAX_VALUE);
    readers.clear();
    readers.close();
    journal.close();
    arena.detach();
  }
}
