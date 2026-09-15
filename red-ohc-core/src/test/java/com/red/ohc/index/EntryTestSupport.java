package com.red.ohc.index;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.WriterArena;

/** Native-key fixture used by tests that exercise Entry metadata accessors. */
public final class EntryTestSupport {
  private static final NativeMemory.Memory MEMORY = new NativeMemory.Memory(AllocatorType.JNA);

  // This fixture is shared across test classes; release it only when the test JVM exits.
  static {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(MEMORY::closeArenas, "ohc-entry-test-fixture-cleanup"));
  }

  private EntryTestSupport() {}

  public static Entry entry(int keyLength, int chmHash, long keyHash64, long valueAddress) {
    long allocation = Entry.keyPhysicalAllocationLengthForKeyLength(keyLength);
    long address = MEMORY.newWriterArena().allocate(allocation);
    NativeMemory.putLong(address, keyHash64);
    Entry entry = new Entry(address, keyLength, valueAddress);
    entry.initializeNativeMetadata();
    return entry;
  }

  public static Entry entry(int keyLength, int chmHash, long valueAddress) {
    return entry(keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static Entry entry(
      NativeMemory.Memory memory, int keyLength, int chmHash, long keyHash64, long valueAddress) {
    return entry(memory.newWriterArena(), keyLength, chmHash, keyHash64, valueAddress);
  }

  public static Entry entry(
      WriterArena arena, int keyLength, int chmHash, long keyHash64, long valueAddress) {
    long allocation = Entry.keyPhysicalAllocationLengthForKeyLength(keyLength);
    long address = arena.allocate(allocation);
    NativeMemory.putLong(address, keyHash64);
    Entry entry = new Entry(address, keyLength, valueAddress);
    entry.initializeNativeMetadata();
    return entry;
  }

  public static Entry entry(NativeMemory.Memory memory, int keyLength, int chmHash, long valueAddress) {
    return entry(memory, keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static Entry entry(WriterArena arena, int keyLength, int chmHash, long valueAddress) {
    return entry(arena, keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static void maintenanceMeta(Entry entry, long value) {
    long metadata = entry.nativeKeyAddress - Entry.NATIVE_METADATA_BYTES;
    NativeMemory.putLong(metadata + 16L, value);
  }

}
