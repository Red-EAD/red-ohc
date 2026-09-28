package com.red.ohc.index;

import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/** Native-key fixture used by tests that exercise Entry metadata accessors. */
public final class EntryTestSupport {
  private static final NativeMemory.Memory MEMORY = new NativeMemory.Memory();

  // This fixture is shared across test classes; release it only when the test JVM exits.
  static {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(MEMORY::closeArenas, "ohc-entry-test-fixture-cleanup"));
  }

  private EntryTestSupport() {}

  public static Entry entry(int keyLength, int chmHash, long valueAddress) {
    return entry(MEMORY.newWriterArena(), keyLength, chmHash, valueAddress);
  }

  public static Entry entry(
      NativeMemory.Memory memory, int keyLength, int chmHash, long valueAddress) {
    return entry(memory.newWriterArena(), keyLength, chmHash, valueAddress);
  }

  public static Entry entry(WriterArena arena, int keyLength, int chmHash, long valueAddress) {
    return entry(arena, keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static Entry entry(int keyLength, int chmHash, long keyHash, long valueAddress) {
    return entry(MEMORY.newWriterArena(), keyLength, chmHash, keyHash, valueAddress);
  }

  public static Entry entry(
      NativeMemory.Memory memory, int keyLength, int chmHash, long keyHash, long valueAddress) {
    return entry(memory.newWriterArena(), keyLength, chmHash, keyHash, valueAddress);
  }

  public static Entry entry(
      WriterArena arena, int keyLength, int chmHash, long keyHash, long valueAddress) {
    long allocation = Entry.keyPhysicalAllocationLengthForKeyLength(keyLength);
    long address = arena.allocate(allocation);
    // Fixture entries start logically absent, matching real insertion before the handoff.
    long tagged = valueAddress == 0L ? 0L : valueAddress | Entry.VALUE_LOGICALLY_ABSENT;
    Entry entry = new Entry(address, keyLength, tagged);
    entry.initializeKeyHash((int) keyHash);
    entry.initializeNativeMetadata();
    return entry;
  }

  public static void maintenanceMeta(Entry entry, long value) {
    long metadata = entry.nativeKeyAddress - Entry.NATIVE_METADATA_BYTES;
    NativeMemory.putLong(metadata + 16L, value);
  }

}
