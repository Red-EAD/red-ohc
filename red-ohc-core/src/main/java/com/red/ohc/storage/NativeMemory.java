package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.red.ohc.AllocatorType;

import sun.misc.Unsafe;

/** Native allocation and primitive access. JNA remains the production default. */
public final class NativeMemory {
    static final Unsafe U;
    static final long BYTE_ARRAY_BASE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            U = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE = U.arrayBaseOffset(byte[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private NativeMemory() { }

    public static final class Memory {
        private final NativeAllocator allocator;
        private final AtomicLong allocated = new AtomicLong();
        private final AtomicLong rawAllocations = new AtomicLong();
        private final AtomicLong entryAllocations = new AtomicLong();
        private final AtomicInteger nextArenaId = new AtomicInteger();
        private final CopyOnWriteArrayList<WriterArena> arenas = new CopyOnWriteArrayList<>();
        private final PageDepot depot = new PageDepot();

        public Memory(AllocatorType type) {
            this.allocator = new NativeAllocator(type);
        }

        public long allocate(long bytes) {
            long address = allocateRaw(bytes);
            U.setMemory(address, bytes, (byte) 0);
            return address;
        }

        public long allocateRaw(long bytes) {
            long address = allocator.allocate(bytes);
            allocated.addAndGet(bytes);
            rawAllocations.incrementAndGet();
            return address;
        }

        public void free(long address, long bytes) {
            if (address == 0L) return;
            allocator.free(address);
            allocated.addAndGet(-bytes);
        }

        public long allocated() { return allocated.get(); }

        public long rawAllocationCount() { return rawAllocations.get(); }
        public long entryAllocationCount() { return entryAllocations.get(); }

        public long allocateEntryPage() {
            entryAllocations.incrementAndGet();
            return allocateRaw(SizeClasses.PAGE_BYTES);
        }

        public long allocateDirectEntry(long bytes) {
            entryAllocations.incrementAndGet();
            return allocateRaw(bytes);
        }

        public WriterArena newWriterArena() {
            int id = nextArenaId.incrementAndGet();
            WriterArena arena = new WriterArena(this, id, depot);
            arenas.add(arena);
            return arena;
        }

        public void releaseEntry(long entryAddress, long entryBytes) {
            if (entryAddress == 0L) return;
            long block = entryAddress - WriterArena.PREFIX_BYTES;
            long metadata = U.getLong(block);
            int arenaId = (int) metadata;
            int sizeClass = (int) ((metadata >>> 32) & 0xffffL);
            if (sizeClass == WriterArena.DIRECT_CLASS) {
                free(block, WriterArena.directAllocationBytes(entryBytes));
                return;
            }
            int index = arenaId - 1;
            WriterArena arena = index >= 0 && index < arenas.size() ? arenas.get(index) : null;
            if (arena == null) {
                depot.offer(block, sizeClass);
            } else {
                arena.remoteFree(block, sizeClass);
            }
        }

        public void closeArenas() {
            for (WriterArena arena : arenas) arena.releasePages();
            arenas.clear();
            depot.clear();
        }
    }

    public static long getLong(long address) { return U.getLong(address); }
    public static long getLong(byte[] bytes, int offset) { return U.getLong(bytes, BYTE_ARRAY_BASE + offset); }
    public static int getInt(byte[] bytes, int offset) { return U.getInt(bytes, BYTE_ARRAY_BASE + offset); }
    public static Unsafe unsafe() { return U; }
    public static long byteArrayBaseOffset() { return BYTE_ARRAY_BASE; }
    /** The actual heap reference width used by this HotSpot process (compressed or wide OOPs). */
    public static int objectReferenceSize() { return U.arrayIndexScale(Object[].class); }
    public static long getLongVolatile(long address) { return U.getLongVolatile(null, address); }
    public static void putLong(long address, long value) { U.putLong(address, value); }
    public static void putLongRelease(long address, long value) { U.putOrderedLong(null, address, value); }
    public static int getInt(long address) { return U.getInt(address); }
    public static void putInt(long address, int value) { U.putInt(address, value); }
    public static byte getByte(long address) { return U.getByte(address); }
    public static void putByte(long address, byte value) { U.putByte(address, value); }
    public static void copy(byte[] source, int sourceOffset, long destination, long bytes) {
        U.copyMemory(source, BYTE_ARRAY_BASE + sourceOffset, null, destination, bytes);
    }
    public static void copy(long source, byte[] destination, int destinationOffset, long bytes) {
        U.copyMemory(null, source, destination, BYTE_ARRAY_BASE + destinationOffset, bytes);
    }
    public static boolean equals(long address, byte[] bytes, int offset, int length) {
        int i = 0;
        for (; i + 8 <= length; i += 8) {
            if (U.getLong(address + i) != U.getLong(bytes, BYTE_ARRAY_BASE + offset + i)) return false;
        }
        for (; i < length; i++) if (U.getByte(address + i) != bytes[offset + i]) return false;
        return true;
    }
    public static boolean equals(long left, long right, int length) {
        int i = 0;
        for (; i + 8 <= length; i += 8) if (U.getLong(left + i) != U.getLong(right + i)) return false;
        for (; i < length; i++) if (U.getByte(left + i) != U.getByte(right + i)) return false;
        return true;
    }
}
