package com.red.ohc.index;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/**
 * The folded absence bit transitions on the tagged value word alone. From an initially absent
 * mapping, racing absent/present flips must linearize: the present flip always reports (the bit
 * starts set), the absent flip reports only when it observes the cleared word, and the final bit
 * agrees with the last reported flip.
 */
@JCStressTest
@Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "only the present flip reported")
@Outcome(id = "7", expect = Expect.ACCEPTABLE, desc = "present flip then absent flip both reported")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "torn or inconsistent transition reports")
@State
public class EntryValueAbsentTransitionStress {
  private final NativeMemory.Memory memory = new NativeMemory.Memory();
  private final WriterArena arena = memory.newWriterArena();
  private final Entry entry;

  private boolean absentReported;
  private boolean presentReported;

  public EntryValueAbsentTransitionStress() {
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(1);
    long keyAddress = arena.allocate(keyAllocation);
    NativeMemory.putByte(keyAddress, (byte) 1);
    long valueAddress = arena.allocate(16L);
    entry = new Entry(keyAddress, 1, Entry.absentTaggedValue(valueAddress, false));
    entry.initializeKeyHash(1);
    entry.initializeNativeMetadata();
  }

  @Actor
  public void markAbsent() {
    absentReported = entry.markLogicallyAbsent();
  }

  @Actor
  public void markPresent() {
    presentReported = entry.markLogicallyPresent();
  }

  @Arbiter
  public void arbiter(I_Result result) {
    result.r1 =
        (absentReported ? 1 : 0)
            | (presentReported ? 2 : 0)
            | (Entry.isAbsentTaggedValue(entry.valueAddress) ? 4 : 0);
  }
}
