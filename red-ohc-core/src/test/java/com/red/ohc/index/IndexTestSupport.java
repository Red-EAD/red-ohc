package com.red.ohc.index;

import java.util.ArrayList;
import java.util.List;

import com.red.ohc.AllocatorType;
import com.red.ohc.codec.Hashing;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;

final class IndexTestSupport implements AutoCloseable {
    private final NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    private final List<Block> blocks = new ArrayList<>();

    Entry entry(byte[] key) { return entry(key, Hashing.xxHash64(key, 0, key.length)); }

    Entry entry(byte[] key, long hash) {
        long length = Math.max(8L, (key.length + 7L) & ~7L);
        long address = memory.allocate(length);
        NativeMemory.copy(key, 0, address, key.length);
        blocks.add(new Block(address, length));
        return new Entry(address, key.length, hash, 0L);
    }

    LookupKey lookup(byte[] key) {
        LookupKey lookup = new LookupKey();
        lookup.set(key, key.length);
        return lookup;
    }

    @Override public void close() {
        for (Block block : blocks) memory.free(block.address, block.length);
        memory.closeArenas();
    }

    private static final class Block {
        final long address;
        final long length;
        Block(long address, long length) { this.address = address; this.length = length; }
    }
}
