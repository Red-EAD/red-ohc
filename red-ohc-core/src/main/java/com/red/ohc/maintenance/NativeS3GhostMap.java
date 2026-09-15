package com.red.ohc.maintenance;

import java.util.Arrays;

import com.red.ohc.storage.NativeMemory;

/**
 * Actor-owned native hash table and FIFO order for S4-FIFO-lite ghost fingerprints.
 *
 * <p>The hash table stores a 32-bit hash tag together with each stable native node id. Keeping the
 * FIFO node separate from the hash table means a rehash can migrate slots incrementally without
 * rewriting the recency links or blocking the maintenance actor on one large copy.
 */
final class NativeS3GhostMap implements AutoCloseable {
  static final long MISSING = Long.MIN_VALUE;
  static final int REHASH_OPERATION_BUDGET = 8;
  static final int REHASH_PASS_BUDGET = 1_024;
  static final int MAX_FREQUENCY = 3;

  private static final int MIN_TABLE_SIZE = 16;
  private static final int MAX_TABLE_SIZE = 1 << 30;
  private static final int NODE_CHUNK_SHIFT = 12;
  private static final int NODE_CHUNK_SIZE = 1 << NODE_CHUNK_SHIFT;
  private static final int NODE_CHUNK_MASK = NODE_CHUNK_SIZE - 1;
  private static final long NODE_RECORD_BYTES = 32L;
  private static final long HASH_OFFSET = 0L;
  private static final long WEIGHT_OFFSET = 8L;
  private static final long PREVIOUS_OFFSET = 16L;
  private static final long NEXT_OFFSET = 20L;
  private static final long STATE_OFFSET = 24L;
  private static final int ACTIVE = 1;
  private static final int FREQUENCY_SHIFT = 1;
  private static final int FREQUENCY_MASK = MAX_FREQUENCY << FREQUENCY_SHIFT;

  private final NativeMemory.Memory memory;
  private long[] nodeChunks = new long[2];
  private int nextNodeId = 1;
  private int freeNodeHead;
  private int deferredFreeNodeHead;
  private int firstNode;
  private int lastNode;
  private int size;
  private long weight;
  private Table table;
  private Table oldTable;
  private int currentTableEntries;
  private int oldTableEntries;
  private int rehashCursor;
  private long nativeBytes;
  private boolean closed;

  NativeS3GhostMap(NativeMemory.Memory memory) {
    if (memory == null) {
      throw new NullPointerException("memory");
    }
    this.memory = memory;
  }

  /** Reusable result for a producer-independent Ghost lookup. */
  static final class Record {
    boolean found;
    int frequency;
    long weight;

    void reset() {
      found = false;
      frequency = 0;
      weight = MISSING;
    }
  }

  /** Removes a fingerprint and returns its weight, or {@link #MISSING}. */
  long remove(long hash) {
    return remove(hash, null);
  }

  /** Removes a fingerprint and optionally reports the hit evidence it carried. */
  long remove(long hash, Record result) {
    if (result != null) {
      result.reset();
    }
    if (closed) {
      return MISSING;
    }
    advanceRehash(REHASH_OPERATION_BUDGET);
    int tag = (int) mix(hash);
    long found = find(hash, tag);
    if (found == 0L) {
      return MISSING;
    }
    Table owner = tableFor(found);
    int slot = slotOf(found);
    int nodeId = (int) owner.get(slot);
    long removedWeight = nodeWeight(nodeId);
    if (result != null) {
      result.found = true;
      result.frequency = nodeFrequency(nodeId);
      result.weight = removedWeight;
    }
    if (owner == table) {
      removeCurrentSlot(owner, slot);
      currentTableEntries--;
    } else {
      oldTableEntries--;
    }
    unlinkNode(nodeId);
    size--;
    weight -= removedWeight;
    releaseNode(nodeId);
    return removedWeight;
  }

  /** Reports the hit count carried before this reappearance and records the new hit. */
  boolean observe(long hash, Record result) {
    result.reset();
    if (closed) {
      return false;
    }
    advanceRehash(REHASH_OPERATION_BUDGET);
    int tag = (int) mix(hash);
    long found = find(hash, tag);
    if (found == 0L) {
      return false;
    }
    Table owner = tableFor(found);
    int nodeId = (int) owner.get(slotOf(found));
    int frequency = nodeFrequency(nodeId);
    result.found = true;
    result.frequency = frequency;
    result.weight = nodeWeight(nodeId);
    if (frequency < MAX_FREQUENCY) {
      putNodeFrequency(nodeId, frequency + 1);
    }
    return true;
  }

  /** Adds or updates a fingerprint without changing its FIFO position on update. */
  boolean put(long hash, long entryWeight) {
    return put(hash, entryWeight, 0);
  }

  /** Adds a fingerprint seeded with the hit count the object earned before it was evicted. */
  boolean put(long hash, long entryWeight, int frequency) {
    if (closed) {
      return false;
    }
    advanceRehash(REHASH_OPERATION_BUDGET);
    int tag = (int) mix(hash);
    long found = find(hash, tag);
    if (found != 0L) {
      int nodeId = (int) tableFor(found).get(slotOf(found));
      long previous = nodeWeight(nodeId);
      putNodeWeight(nodeId, entryWeight);
      putNodeFrequency(nodeId, Math.max(nodeFrequency(nodeId), frequency));
      weight += entryWeight - previous;
      return true;
    }
    return insertNew(hash, tag, entryWeight, frequency);
  }

  /** Refreshes a fingerprint to the FIFO tail while reusing a current-table node when possible. */
  boolean refresh(long hash, long entryWeight, int frequency, Record result) {
    if (result != null) {
      result.reset();
    }
    if (closed) {
      return false;
    }
    advanceRehash(REHASH_OPERATION_BUDGET);
    int tag = (int) mix(hash);
    long found = find(hash, tag);
    if (found == 0L) {
      advanceRehash(REHASH_OPERATION_BUDGET);
      return insertNew(hash, tag, entryWeight, frequency);
    }
    Table owner = tableFor(found);
    int nodeId = (int) owner.get(slotOf(found));
    long previousWeight = nodeWeight(nodeId);
    int previousFrequency = nodeFrequency(nodeId);
    if (result != null) {
      result.found = true;
      result.frequency = previousFrequency;
      result.weight = previousWeight;
    }
    int updatedFrequency = Math.max(previousFrequency, frequency);
    if (owner == table) {
      putNodeWeight(nodeId, entryWeight);
      putNodeFrequency(nodeId, updatedFrequency);
      unlinkNode(nodeId);
      linkLast(nodeId);
      weight += entryWeight - previousWeight;
      advanceRehash(REHASH_OPERATION_BUDGET);
      return true;
    }

    // An old-table node cannot be moved in place: the rehash cursor may still visit its slot.
    // Retire it exactly as remove() does, then insert the refreshed node into the current table.
    oldTableEntries--;
    unlinkNode(nodeId);
    size--;
    weight -= previousWeight;
    releaseNode(nodeId);
    advanceRehash(REHASH_OPERATION_BUDGET);
    return insertNew(hash, tag, entryWeight, updatedFrequency);
  }

  private boolean insertNew(long hash, int tag, long entryWeight, int frequency) {
    if (!ensureInsertCapacity()) {
      return false;
    }
    int nodeId = allocateNode(hash, entryWeight, frequency);
    if (nodeId == 0) {
      return false;
    }
    insertCurrentSlot(encodeSlot(tag, nodeId));
    currentTableEntries++;
    linkLast(nodeId);
    size++;
    weight += entryWeight;
    return true;
  }

  /** Removes the oldest fingerprint in O(1) FIFO order plus one expected hash probe. */
  long removeFirst() {
    if (closed || firstNode == 0) {
      return MISSING;
    }
    return remove(nodeHash(firstNode));
  }

  long weight() {
    return weight;
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  long nativeBytes() {
    return nativeBytes;
  }

  boolean rehashPending() {
    return oldTable != null;
  }

  /** Advances an in-progress rehash by at most {@code budget} old table slots. */
  int advanceRehash(int budget) {
    if (budget <= 0 || oldTable == null) {
      return 0;
    }
    int visited = 0;
    while (visited < budget && rehashCursor < oldTable.capacity) {
      long slotValue = oldTable.get(rehashCursor++);
      visited++;
      if (slotValue == 0L) {
        continue;
      }
      int nodeId = (int) slotValue;
      if (!isActive(nodeId)) {
        continue;
      }
      insertCurrentSlot(slotValue);
      currentTableEntries++;
      oldTableEntries--;
    }
    if (rehashCursor == oldTable.capacity) {
      Table completed = oldTable;
      oldTable = null;
      oldTableEntries = 0;
      completed.close();
      drainDeferredFreeNodes();
    }
    return visited;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (oldTable != null) {
      oldTable.close();
      oldTable = null;
    }
    if (table != null) {
      table.close();
      table = null;
    }
    for (int index = 0; index < nodeChunks.length; index++) {
      long address = nodeChunks[index];
      if (address != 0L) {
        memory.free(address, NODE_CHUNK_SIZE * NODE_RECORD_BYTES);
        nodeChunks[index] = 0L;
      }
    }
    nativeBytes = 0L;
    freeNodeHead = 0;
    deferredFreeNodeHead = 0;
    firstNode = 0;
    lastNode = 0;
    size = 0;
    weight = 0L;
    currentTableEntries = 0;
    oldTableEntries = 0;
    rehashCursor = 0;
  }

  private boolean ensureInsertCapacity() {
    if (table == null) {
      table = tryCreateTable(MIN_TABLE_SIZE);
      if (table == null) {
        return false;
      }
      nativeBytes += table.bytes;
      return true;
    }
    if (currentTableEntries < table.maxFill) {
      return true;
    }
    if (oldTable != null || table.capacity >= MAX_TABLE_SIZE) {
      return false;
    }
    int nextCapacity = table.capacity << 1;
    Table next = tryCreateTable(nextCapacity);
    if (next == null) {
      return false;
    }
    nativeBytes += next.bytes;
    oldTable = table;
    oldTableEntries = currentTableEntries;
    table = next;
    currentTableEntries = 0;
    rehashCursor = 0;
    return true;
  }

  private Table tryCreateTable(int capacity) {
    long bytes = (long) capacity * Long.BYTES;
    long address = memory.tryAllocateRaw(bytes);
    if (address == 0L) {
      return null;
    }
    NativeMemory.setMemory(address, bytes, (byte) 0);
    return new Table(capacity, address);
  }

  private int allocateNode(long hash, long entryWeight, int frequency) {
    int nodeId = freeNodeHead;
    if (nodeId != 0) {
      freeNodeHead = nodeNext(nodeId);
    } else {
      nodeId = nextNodeId++;
      if (!ensureNodeChunk(nodeId)) {
        nextNodeId--;
        return 0;
      }
    }
    long address = nodeAddress(nodeId);
    NativeMemory.putLong(address + HASH_OFFSET, hash);
    NativeMemory.putLong(address + WEIGHT_OFFSET, entryWeight);
    NativeMemory.putInt(address + PREVIOUS_OFFSET, 0);
    NativeMemory.putInt(address + NEXT_OFFSET, 0);
    NativeMemory.putInt(address + STATE_OFFSET, ACTIVE | encodeFrequency(frequency));
    return nodeId;
  }

  private boolean ensureNodeChunk(int nodeId) {
    int chunk = (nodeId - 1) >>> NODE_CHUNK_SHIFT;
    if (chunk >= nodeChunks.length) {
      int length = nodeChunks.length;
      while (length <= chunk) {
        length <<= 1;
      }
      nodeChunks = Arrays.copyOf(nodeChunks, length);
    }
    if (nodeChunks[chunk] != 0L) {
      return true;
    }
    long bytes = NODE_CHUNK_SIZE * NODE_RECORD_BYTES;
    long address = memory.tryAllocateRaw(bytes);
    if (address == 0L) {
      return false;
    }
    NativeMemory.setMemory(address, bytes, (byte) 0);
    nodeChunks[chunk] = address;
    nativeBytes += bytes;
    return true;
  }

  /** Reuses the tag computed by lookup, or already carried by an old-table slot. */
  private void insertCurrentSlot(long slotValue) {
    int slot = (int) (slotValue >>> 32) & table.mask;
    while (table.get(slot) != 0) {
      slot = (slot + 1) & table.mask;
    }
    table.set(slot, slotValue);
  }

  private long find(long hash, int tag) {
    int slot = findSlot(table, hash, tag);
    if (slot >= 0) {
      return encodeFind(true, slot);
    }
    slot = findSlot(oldTable, hash, tag);
    return slot < 0 ? 0L : encodeFind(false, slot);
  }

  private int findSlot(Table candidate, long hash, int tag) {
    if (candidate == null) {
      return -1;
    }
    int slot = tag & candidate.mask;
    while (true) {
      long slotValue = candidate.get(slot);
      if (slotValue == 0L) {
        return -1;
      }
      if ((int) (slotValue >>> 32) == tag) {
        int nodeId = (int) slotValue;
        long address = nodeAddress(nodeId);
        if ((NativeMemory.getInt(address + STATE_OFFSET) & ACTIVE) != 0
            && NativeMemory.getLong(address + HASH_OFFSET) == hash) {
          return slot;
        }
      }
      slot = (slot + 1) & candidate.mask;
    }
  }

  private void removeCurrentSlot(Table owner, int slot) {
    int last = slot;
    int position = slot;
    while (true) {
      position = (position + 1) & owner.mask;
      long slotValue = owner.get(position);
      if (slotValue == 0L) {
        owner.set(last, 0);
        return;
      }
      int home = (int) (slotValue >>> 32) & owner.mask;
      if (last <= position
          ? last >= home || home > position
          : last >= home && home > position) {
        owner.set(last, slotValue);
        last = position;
      }
    }
  }

  private void linkLast(int nodeId) {
    long address = nodeAddress(nodeId);
    NativeMemory.putInt(address + PREVIOUS_OFFSET, lastNode);
    NativeMemory.putInt(address + NEXT_OFFSET, 0);
    if (lastNode == 0) {
      firstNode = nodeId;
    } else {
      putNodeNext(lastNode, nodeId);
    }
    lastNode = nodeId;
  }

  private void unlinkNode(int nodeId) {
    long address = nodeAddress(nodeId);
    int previous = NativeMemory.getInt(address + PREVIOUS_OFFSET);
    int next = NativeMemory.getInt(address + NEXT_OFFSET);
    if (previous == 0) {
      firstNode = next;
    } else {
      putNodeNext(previous, next);
    }
    if (next == 0) {
      lastNode = previous;
    } else {
      putNodePrevious(next, previous);
    }
    NativeMemory.putInt(address + PREVIOUS_OFFSET, 0);
    NativeMemory.putInt(address + NEXT_OFFSET, 0);
  }

  private void releaseNode(int nodeId) {
    long address = nodeAddress(nodeId);
    NativeMemory.putInt(address + STATE_OFFSET, 0);
    if (oldTable != null) {
      NativeMemory.putInt(address + NEXT_OFFSET, deferredFreeNodeHead);
      deferredFreeNodeHead = nodeId;
    } else {
      NativeMemory.putInt(address + NEXT_OFFSET, freeNodeHead);
      freeNodeHead = nodeId;
    }
  }

  private void drainDeferredFreeNodes() {
    while (deferredFreeNodeHead != 0) {
      int nodeId = deferredFreeNodeHead;
      deferredFreeNodeHead = nodeNext(nodeId);
      putNodeNext(nodeId, freeNodeHead);
      freeNodeHead = nodeId;
    }
  }

  private Table tableFor(long found) {
    return ((int) (found >>> 32)) == 1 ? table : oldTable;
  }

  private static int slotOf(long found) {
    return (int) found - 1;
  }

  private static long encodeFind(boolean current, int slot) {
    return ((long) (current ? 1 : 2) << 32) | ((long) slot + 1L);
  }

  private long nodeAddress(int nodeId) {
    int zeroBased = nodeId - 1;
    int chunk = zeroBased >>> NODE_CHUNK_SHIFT;
    return nodeChunks[chunk] + (long) (zeroBased & NODE_CHUNK_MASK) * NODE_RECORD_BYTES;
  }

  private long nodeHash(int nodeId) {
    return NativeMemory.getLong(nodeAddress(nodeId) + HASH_OFFSET);
  }

  private long nodeWeight(int nodeId) {
    return NativeMemory.getLong(nodeAddress(nodeId) + WEIGHT_OFFSET);
  }

  private void putNodeWeight(int nodeId, long value) {
    NativeMemory.putLong(nodeAddress(nodeId) + WEIGHT_OFFSET, value);
  }

  private void putNodePrevious(int nodeId, int value) {
    NativeMemory.putInt(nodeAddress(nodeId) + PREVIOUS_OFFSET, value);
  }

  private int nodeNext(int nodeId) {
    return NativeMemory.getInt(nodeAddress(nodeId) + NEXT_OFFSET);
  }

  private void putNodeNext(int nodeId, int value) {
    NativeMemory.putInt(nodeAddress(nodeId) + NEXT_OFFSET, value);
  }

  private boolean isActive(int nodeId) {
    return (NativeMemory.getInt(nodeAddress(nodeId) + STATE_OFFSET) & ACTIVE) != 0;
  }

  private int nodeFrequency(int nodeId) {
    int state = NativeMemory.getInt(nodeAddress(nodeId) + STATE_OFFSET);
    return (state & FREQUENCY_MASK) >>> FREQUENCY_SHIFT;
  }

  private void putNodeFrequency(int nodeId, int frequency) {
    long address = nodeAddress(nodeId) + STATE_OFFSET;
    int state = NativeMemory.getInt(address);
    NativeMemory.putInt(address, (state & ~FREQUENCY_MASK) | encodeFrequency(frequency));
  }

  private static int encodeFrequency(int frequency) {
    int bounded = frequency < 0 ? 0 : Math.min(frequency, MAX_FREQUENCY);
    return bounded << FREQUENCY_SHIFT;
  }

  private static long encodeSlot(int tag, int nodeId) {
    return ((long) tag << 32) | (nodeId & 0xffff_ffffL);
  }

  private static long mix(long value) {
    value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
    value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
    return value ^ (value >>> 31);
  }

  private final class Table implements AutoCloseable {
    private final int capacity;
    private final int mask;
    private final int maxFill;
    private final long address;
    private final long bytes;
    private boolean released;

    private Table(int capacity, long address) {
      this.capacity = capacity;
      this.mask = capacity - 1;
      this.maxFill = Math.max(1, capacity - (capacity >>> 1));
      this.address = address;
      this.bytes = (long) capacity * Long.BYTES;
    }

    private long get(int slot) {
      return NativeMemory.getLong(address + (long) slot * Long.BYTES);
    }

    private void set(int slot, long slotValue) {
      NativeMemory.putLong(address + (long) slot * Long.BYTES, slotValue);
    }

    @Override
    public void close() {
      if (released) {
        return;
      }
      released = true;
      memory.free(address, bytes);
      nativeBytes -= bytes;
    }

  }
}
