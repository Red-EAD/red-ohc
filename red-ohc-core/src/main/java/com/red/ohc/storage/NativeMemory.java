package com.red.ohc.storage;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import sun.misc.Unsafe;

import com.red.ohc.api.AllocatorType;

/** Native allocation and primitive access. JNA remains the production default. */
public final class NativeMemory {
  static final Unsafe U;
  static final long BYTE_ARRAY_BASE;
  /** Cached once so bounded CAS paths never query the runtime on their hot path. */
  public static final int LOGICAL_CPU_COUNT =
      Math.max(1, Runtime.getRuntime().availableProcessors());
  private static final int DEFAULT_WRITER_STRIPE_COUNT = computeWriterStripeCount();
  private static final int DEFAULT_RETIREMENT_RECORDS_PER_STRIPE =
      computeRetirementRecordsPerStripe(DEFAULT_WRITER_STRIPE_COUNT);

  /** Start block-wise comparison only when the key is large enough to amortize aggregation. */
  static final int BULK_EQUALS_THRESHOLD = 128;

  /** Eight machine words form one comparison block (64 bytes). */
  static final int BULK_EQUALS_LONGS = 8;

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

  private NativeMemory() {}

  public static int defaultWriterStripeCount() {
    return DEFAULT_WRITER_STRIPE_COUNT;
  }

  public static int defaultRetirementRecordsPerStripe() {
    return DEFAULT_RETIREMENT_RECORDS_PER_STRIPE;
  }

  private static int computeWriterStripeCount() {
    int target = Math.max(1, LOGICAL_CPU_COUNT * 4);
    int stripes = 1;
    while (stripes < target && stripes < (1 << 30)) {
      stripes <<= 1;
    }
    return stripes;
  }

  private static int computeRetirementRecordsPerStripe(int stripeCount) {
    long perStripe = (512L << 10) / ((long) stripeCount * 32L);
    int records = 2;
    while ((records << 1) <= perStripe) {
      records <<= 1;
    }
    return records;
  }

  public static final class Memory {
    private final NativeAllocator allocator;
    private final long hardLimit;
    private final AtomicLong allocated = new AtomicLong();
    private final AtomicLong rawAllocations = new AtomicLong();
    private final AtomicLong entryAllocations = new AtomicLong();
    private final WriterArena[] arenas;
    private final int stripeMask;
    private static final int PAGE_CHUNK_BITS = 10;
    private static final int PAGE_CHUNK_SIZE = 1 << PAGE_CHUNK_BITS;
    private static final int PAGE_CHUNK_COUNT = 1 << (WriterArena.PAGE_ID_BITS - PAGE_CHUNK_BITS);
    private static final int MAX_PAGE_ID = WriterArena.PAGE_ID_MASK;
    private static final int PAGE_VERSION_BITS = 22 - WriterArena.PAGE_ID_BITS;
    private static final int PAGE_VERSION_MASK = (1 << PAGE_VERSION_BITS) - 1;
    private final AtomicInteger nextPageId = new AtomicInteger(1);

    /**
     * Sparse page-id free stack; nodes are indices in fixed primitive arrays, never Page objects.
     * The low 32 bits are the page id; the high 32 bits advance on every stack mutation.
     */
    private final AtomicLong freePageIdHead = new AtomicLong();

    private final AtomicIntegerArray freePageIdNext = new AtomicIntegerArray(MAX_PAGE_ID + 1);
    private final AtomicIntegerArray pageVersions = new AtomicIntegerArray(MAX_PAGE_ID + 1);
    private final AtomicReferenceArray<AtomicReferenceArray<WriterArena.Page>> pageChunks =
        new AtomicReferenceArray<>(PAGE_CHUNK_COUNT);
    private final PageDepot pageDepot;
    private static final int MIN_POOLED_PAGES = 512;
    private static final int MAX_POOLED_PAGES = 2_048;
    private final int pooledPageLimit;
    private final AtomicInteger pooledPageCount = new AtomicInteger();

    public Memory(AllocatorType type) {
      this(type, Long.MAX_VALUE);
    }

    public Memory(AllocatorType type, long hardLimit) {
      if (hardLimit <= 0L) {
        throw new IllegalArgumentException("hardLimit must be positive");
      }
      this.allocator = new NativeAllocator(type);
      this.hardLimit = hardLimit;
      int stripeCount = DEFAULT_WRITER_STRIPE_COUNT;
      this.arenas = new WriterArena[stripeCount];
      for (int index = 0; index < stripeCount; index++) {
        arenas[index] = new WriterArena(this, index + 1);
      }
      this.stripeMask = stripeCount - 1;
      long targetPages = Math.max(MIN_POOLED_PAGES, Math.min(MAX_POOLED_PAGES, (long) stripeCount * 8L));
      long physicalPageLimit = hardLimit / SizeClasses.PAGE_BYTES;
      this.pooledPageLimit = (int) Math.min(targetPages, physicalPageLimit);
      this.pageDepot = new PageDepot(this, pooledPageLimit);
    }

    public long allocate(long bytes) {
      long address = allocateRaw(bytes);
      U.setMemory(address, bytes, (byte) 0);
      return address;
    }

    public long allocateRaw(long bytes) {
      if (bytes <= 0L) {
        throw new IllegalArgumentException("bytes must be positive");
      }
      reservePhysical(bytes);
      try {
        long address = allocator.allocate(bytes);
        rawAllocations.incrementAndGet();
        return address;
      } catch (Throwable failure) {
        allocated.addAndGet(-bytes);
        throw failure;
      }
    }

    public void free(long address, long bytes) {
      if (address == 0L) {
        return;
      }
      allocator.free(address);
      allocated.addAndGet(-bytes);
    }

    public long allocated() {
      return allocated.get();
    }

    public long hardLimit() {
      return hardLimit;
    }

    public long rawAllocationCount() {
      return rawAllocations.get();
    }

    public long entryAllocationCount() {
      return entryAllocations.get();
    }

    int pooledPageLimit() {
      return pooledPageLimit;
    }

    public long allocateEntryPage() {
      entryAllocations.incrementAndGet();
      return allocateRaw(SizeClasses.PAGE_BYTES);
    }

    public long allocateDirectEntry(long bytes) {
      entryAllocations.incrementAndGet();
      return allocateRaw(bytes);
    }

    public WriterArena newWriterArena() {
      return writerForCurrentThread();
    }

    public WriterArena writerForCurrentThread() {
      return arenas[((int) Thread.currentThread().getId()) & stripeMask];
    }

    public int writerStripeIndex() {
      return ((int) Thread.currentThread().getId()) & stripeMask;
    }

    public int writerStripeCount() {
      return arenas.length;
    }

    public void releaseEntry(long entryAddress, long entryBytes) {
      if (entryAddress == 0L) {
        return;
      }
      long block = entryAddress - WriterArena.PREFIX_BYTES;
      long metadata = U.getLong(block);
      int arenaId = (int) metadata;
      int sizeClass = (int) ((metadata >>> 32) & 0xffffL);
      if (sizeClass == WriterArena.DIRECT_CLASS) {
        free(block, WriterArena.directAllocationBytes(entryBytes));
        return;
      }
      int index = arenaId - 1;
      if (index < 0 || index >= arenas.length) {
        throw new IllegalStateException("unknown allocator stripe: " + arenaId);
      }
      arenas[index].remoteFree(block, sizeClass);
    }

    public void closeArenas() {
      for (WriterArena arena : arenas) {
        arena.releasePages();
      }
      pageDepot.clear();
      for (int chunkIndex = 0; chunkIndex < pageChunks.length(); chunkIndex++) {
        AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(chunkIndex);
        if (chunk == null) {
          continue;
        }
        for (int slot = 0; slot < chunk.length(); slot++) {
          WriterArena.Page page = chunk.get(slot);
          if (page != null) {
            freeEntryPage(page);
          }
        }
      }
    }

    /** Releases fully empty depot pages during writer-side native pressure. */
    public long trimIdlePages() {
      return pageDepot.trimIdlePages();
    }

    WriterArena.Page acquireEntryPage(int sizeClass) {
      WriterArena.Page page = tryAcquireEntryPage(sizeClass);
      if (page == null) {
        throw new AllocationLimitException(hardLimit, allocated.get(), SizeClasses.PAGE_BYTES);
      }
      return page;
    }

    WriterArena.Page tryAcquireEntryPage(int sizeClass) {
      WriterArena.Page reused = pageDepot.acquire(sizeClass);
      if (reused != null) {
        return reused;
      }
      boolean counted = false;
      for (int attempt = 0; attempt < LOGICAL_CPU_COUNT; attempt++) {
        int current = pooledPageCount.get();
        if (current >= pooledPageLimit) {
          return null;
        }
        if (pooledPageCount.compareAndSet(current, current + 1)) {
          counted = true;
          break;
        }
        Thread.onSpinWait();
      }
      if (!counted) {
        return null;
      }
      int pageKey = 0;
      long address = 0L;
      try {
        pageKey = nextPageKey();
        int pageId = pageKey & MAX_PAGE_ID;
        address = allocateEntryPage();
        WriterArena.Page page =
            new WriterArena.Page(
                pageId, pageKey, address, sizeClass, SizeClasses.slotBytes(sizeClass));
        registerPage(page);
        return page;
      } catch (Throwable failure) {
        if (address != 0L) {
          free(address, SizeClasses.PAGE_BYTES);
        }
        if (pageKey != 0) {
          recyclePageId(pageKey & MAX_PAGE_ID);
        }
        pooledPageCount.decrementAndGet();
        throw failure;
      }
    }

    void returnUnusedPage(WriterArena.Page page) {
      freeEntryPage(page);
    }

    void releaseEmptyPage(WriterArena.Page page) {
      pageDepot.release(page);
    }

    void freeEntryPage(WriterArena.Page page) {
      if (!page.freePhysical()) {
        return;
      }
      unregisterPage(page);
      free(page.address, SizeClasses.PAGE_BYTES);
      pooledPageCount.decrementAndGet();
      recyclePageId(page.id);
    }

    int nextPageKey() {
      int pageId = takeFreePageId();
      if (pageId == 0) {
        pageId = nextPageId.getAndIncrement();
      }
      if (pageId <= 0 || pageId > MAX_PAGE_ID) {
        throw new AllocationLimitException(hardLimit, allocated.get(), SizeClasses.PAGE_BYTES);
      }
      int version = pageVersions.incrementAndGet(pageId) & PAGE_VERSION_MASK;
      return (version << WriterArena.PAGE_ID_BITS) | pageId;
    }

    void registerPage(WriterArena.Page page) {
      int chunkIndex = page.id >>> PAGE_CHUNK_BITS;
      AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(chunkIndex);
      if (chunk == null) {
        AtomicReferenceArray<WriterArena.Page> created =
            new AtomicReferenceArray<>(PAGE_CHUNK_SIZE);
        if (!pageChunks.compareAndSet(chunkIndex, null, created)) {
          created = pageChunks.get(chunkIndex);
        }
        chunk = created;
      }
      if (!chunk.compareAndSet(page.id & (PAGE_CHUNK_SIZE - 1), null, page)) {
        throw new IllegalStateException("duplicate native page id " + page.id);
      }
    }

    void unregisterPage(WriterArena.Page page) {
      AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(page.id >>> PAGE_CHUNK_BITS);
      if (chunk != null) {
        chunk.compareAndSet(page.id & (PAGE_CHUNK_SIZE - 1), page, null);
      }
    }

    WriterArena.Page pageForHandle(int handle) {
      int pageKey = handle >>> WriterArena.HANDLE_SLOT_BITS;
      int pageId = pageKey & MAX_PAGE_ID;
      if (pageId == 0) {
        return null;
      }
      AtomicReferenceArray<WriterArena.Page> chunk = pageChunks.get(pageId >>> PAGE_CHUNK_BITS);
      WriterArena.Page page = chunk == null ? null : chunk.get(pageId & (PAGE_CHUNK_SIZE - 1));
      return page != null && page.pageKey == pageKey ? page : null;
    }

    private int takeFreePageId() {
      while (true) {
        long head = freePageIdHead.get();
        int pageId = (int) head;
        if (pageId == 0) {
          return 0;
        }
        int next = freePageIdNext.get(pageId);
        long updated = packPageIdHead(((int) (head >>> 32)) + 1, next);
        if (freePageIdHead.compareAndSet(head, updated)) {
          return pageId;
        }
      }
    }

    private void recyclePageId(int pageId) {
      while (true) {
        long head = freePageIdHead.get();
        freePageIdNext.set(pageId, (int) head);
        long updated = packPageIdHead(((int) (head >>> 32)) + 1, pageId);
        if (freePageIdHead.compareAndSet(head, updated)) {
          return;
        }
      }
    }

    private static long packPageIdHead(int stamp, int pageId) {
      return ((long) stamp << 32) | (pageId & 0xffff_ffffL);
    }

    private void reservePhysical(long bytes) {
      for (int attempt = 0; attempt < LOGICAL_CPU_COUNT; attempt++) {
        long current = allocated.get();
        if (current > hardLimit - bytes) {
          throw new AllocationLimitException(hardLimit, current, bytes);
        }
        if (allocated.compareAndSet(current, current + bytes)) {
          return;
        }
        Thread.onSpinWait();
      }
      throw new AllocationLimitException(hardLimit, allocated.get(), bytes);
    }
  }

  /** Admission failure is recoverable for cache writes; it is not a JVM-wide OutOfMemoryError. */
  public static final class AllocationLimitException extends RuntimeException {
    AllocationLimitException(long hardLimit, long allocated, long requested) {
      super(
          "native hard limit "
              + hardLimit
              + " exceeded: allocated="
              + allocated
              + ", requested="
              + requested);
    }
  }

  public static long getLong(long address) {
    return U.getLong(address);
  }

  public static long getLong(byte[] bytes, int offset) {
    return U.getLong(bytes, BYTE_ARRAY_BASE + offset);
  }

  public static int getInt(byte[] bytes, int offset) {
    return U.getInt(bytes, BYTE_ARRAY_BASE + offset);
  }

  public static Unsafe unsafe() {
    return U;
  }

  public static long byteArrayBaseOffset() {
    return BYTE_ARRAY_BASE;
  }

  /** The actual heap reference width used by this HotSpot process (compressed or wide OOPs). */
  public static int objectReferenceSize() {
    return U.arrayIndexScale(Object[].class);
  }

  public static long getLongVolatile(long address) {
    return U.getLongVolatile(null, address);
  }

  public static void putLong(long address, long value) {
    U.putLong(address, value);
  }

  public static void putLongRelease(long address, long value) {
    U.putOrderedLong(null, address, value);
  }

  public static int getInt(long address) {
    return U.getInt(address);
  }

  public static void putInt(long address, int value) {
    U.putInt(address, value);
  }

  public static byte getByte(long address) {
    return U.getByte(address);
  }

  public static void putByte(long address, byte value) {
    U.putByte(address, value);
  }

  public static void copy(byte[] source, int sourceOffset, long destination, long bytes) {
    U.copyMemory(source, BYTE_ARRAY_BASE + sourceOffset, null, destination, bytes);
  }

  public static void copy(long source, byte[] destination, int destinationOffset, long bytes) {
    U.copyMemory(null, source, destination, BYTE_ARRAY_BASE + destinationOffset, bytes);
  }

  public static boolean equals(long address, byte[] bytes, int offset, int length) {
    int i = 0;
    if (length >= BULK_EQUALS_THRESHOLD) {
      int blockBytes = BULK_EQUALS_LONGS * Long.BYTES;
      for (; i + blockBytes <= length; i += blockBytes) {
        // Aggregate all word differences in the block before branching. This is a
        // portable SWAR-style optimization: it reduces branch frequency without
        // requiring the JVM to generate platform-specific SIMD instructions.
        long diff = 0L;
        diff |= U.getLong(address + i) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i);
        diff |= U.getLong(address + i + 8) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 8);
        diff |= U.getLong(address + i + 16) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 16);
        diff |= U.getLong(address + i + 24) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 24);
        diff |= U.getLong(address + i + 32) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 32);
        diff |= U.getLong(address + i + 40) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 40);
        diff |= U.getLong(address + i + 48) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 48);
        diff |= U.getLong(address + i + 56) ^ U.getLong(bytes, BYTE_ARRAY_BASE + offset + i + 56);
        if (diff != 0L) {
          return false;
        }
      }
    }
    for (; i + 8 <= length; i += 8) {
      if (U.getLong(address + i) != U.getLong(bytes, BYTE_ARRAY_BASE + offset + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (U.getByte(address + i) != bytes[offset + i]) {
        return false;
      }
    }
    return true;
  }

  public static boolean equals(long left, long right, int length) {
    int i = 0;
    if (length >= BULK_EQUALS_THRESHOLD) {
      int blockBytes = BULK_EQUALS_LONGS * Long.BYTES;
      for (; i + blockBytes <= length; i += blockBytes) {
        // Keep the same block shape as the heap-array overload so both hot paths
        // have identical early-exit semantics and are easy to benchmark together.
        long diff = 0L;
        diff |= U.getLong(left + i) ^ U.getLong(right + i);
        diff |= U.getLong(left + i + 8) ^ U.getLong(right + i + 8);
        diff |= U.getLong(left + i + 16) ^ U.getLong(right + i + 16);
        diff |= U.getLong(left + i + 24) ^ U.getLong(right + i + 24);
        diff |= U.getLong(left + i + 32) ^ U.getLong(right + i + 32);
        diff |= U.getLong(left + i + 40) ^ U.getLong(right + i + 40);
        diff |= U.getLong(left + i + 48) ^ U.getLong(right + i + 48);
        diff |= U.getLong(left + i + 56) ^ U.getLong(right + i + 56);
        if (diff != 0L) {
          return false;
        }
      }
    }
    for (; i + 8 <= length; i += 8) {
      if (U.getLong(left + i) != U.getLong(right + i)) {
        return false;
      }
    }
    for (; i < length; i++) {
      if (U.getByte(left + i) != U.getByte(right + i)) {
        return false;
      }
    }
    return true;
  }
}
