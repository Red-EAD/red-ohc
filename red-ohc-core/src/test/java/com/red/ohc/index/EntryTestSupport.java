package com.red.ohc.index;

import java.util.ArrayList;
import java.util.List;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.storage.NativeMemory;

/** Native-key fixture used by tests that exercise Entry metadata accessors. */
public final class EntryTestSupport {
  private static final NativeMemory.Memory MEMORY = new NativeMemory.Memory(AllocatorType.JNA);
  private static final List<Block> BLOCKS = new ArrayList<>();

  private EntryTestSupport() {}

  public static Entry entry(int keyLength, int chmHash, long keyHash64, long valueAddress) {
    long allocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long address = MEMORY.allocate(allocation);
    NativeMemory.putLong(address, keyHash64);
    Entry entry = new Entry(address, keyLength, chmHash, keyHash64, valueAddress);
    entry.initializeNativeMetadata();
    synchronized (BLOCKS) {
      BLOCKS.add(new Block(address, allocation));
    }
    return entry;
  }

  public static Entry entry(int keyLength, int chmHash, long valueAddress) {
    return entry(keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static Entry entry(
      NativeMemory.Memory memory, int keyLength, int chmHash, long keyHash64, long valueAddress) {
    long allocation = Entry.keyAllocationLengthForKeyLength(keyLength);
    long address = memory.newWriterArena().allocate(allocation);
    NativeMemory.putLong(address, keyHash64);
    Entry entry = new Entry(address, keyLength, chmHash, keyHash64, valueAddress);
    entry.initializeNativeMetadata();
    return entry;
  }

  public static Entry entry(NativeMemory.Memory memory, int keyLength, int chmHash, long valueAddress) {
    return entry(memory, keyLength, chmHash, chmHash & 0xffff_ffffL, valueAddress);
  }

  public static void close() {
    synchronized (BLOCKS) {
      for (Block block : BLOCKS) {
        MEMORY.free(block.address, block.allocation);
      }
      BLOCKS.clear();
    }
    MEMORY.closeArenas();
  }

  public static void maintenanceMeta(Entry entry, long value) {
    long metadata = entry.nativeKeyAddress + Entry.keyDataAllocationLength(entry.keyLength());
    NativeMemory.putLong(metadata + 16L, value);
  }

  private static final class Block {
    private final long address;
    private final long allocation;

    private Block(long address, long allocation) {
      this.address = address;
      this.allocation = allocation;
    }
  }
}
