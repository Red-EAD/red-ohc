package com.red.ohc.index;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.security.SecureRandom;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import com.red.ohc.EncodedKey;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;

/**
 * OHC's Entry-only concurrent index. Hot reads use one 64-bit hash, two groups, and direct
 * native key comparison; strategy dispatch and generic Map semantics are deliberately absent.
 */
public final class FlatConcurrentMap {
    private static final int GROUP_SLOTS = 16;
    private static final int EMPTY = 0x80;
    private static final int DELETED = 0xfe;
    private static final int LOCKED = 1;
    private static final int MIGRATED = 1 << 1;
    private static final long BYTE_ONE = 0x0101010101010101L;
    private static final long BYTE_LOW_BITS = 0x7f7f7f7f7f7f7f7fL;
    private static final long BYTE_HIGH_BITS = 0x8080808080808080L;
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(Object[].class);
    private static final VarHandle CONTROLS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle GROUP_META = MethodHandles.arrayElementVarHandle(int[].class);

    private final long indexSeed;
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicInteger nextTableId = new AtomicInteger(1);
    private final AtomicReference<ResizeState> resize = new AtomicReference<>();
    private final CollisionTree overflow = new CollisionTree();
    private volatile Table table;

    public FlatConcurrentMap(long expectedEntries) {
        this(expectedEntries, new SecureRandom().nextLong());
    }

    public FlatConcurrentMap(long expectedEntries, long indexSeed) {
        this.indexSeed = indexSeed;
        this.table = new Table(nextTableId.get(), groupCountFor(expectedEntries));
    }

    public Entry get(LookupKey key) {
        if (key == null) return null;
        long hash = key.hash();
        ResizeState state = resize.get();
        Entry found = state == null ? findBytes(table, hash, key.bytes(), key.length())
                : findBytes(state.newTable, hash, key.bytes(), key.length());
        if (found != null) return found;
        if (state != null && (found = findBytes(state.oldTable, hash, key.bytes(), key.length())) != null) return found;
        return overflow.get(key);
    }

    public Entry get(EncodedKey key) {
        if (key == null) return null;
        ResizeState state = resize.get();
        Entry found = state == null ? findEncoded(table, key) : findEncoded(state.newTable, key);
        if (found != null) return found;
        if (state != null && (found = findEncoded(state.oldTable, key)) != null) return found;
        return overflow.get(key);
    }

    public Entry get(Entry key) {
        if (key == null) return null;
        ResizeState state = resize.get();
        Entry found = state == null ? findEntry(table, key) : findEntry(state.newTable, key);
        if (found != null) return found;
        if (state != null && (found = findEntry(state.oldTable, key)) != null) return found;
        return overflow.get(key);
    }

    /** Returns the existing self-keyed mapping, or null when {@code value} was inserted. */
    public Entry putIfAbsent(Entry key, Entry value) {
        if (key == null) throw new NullPointerException("key");
        if (key != value) throw new IllegalArgumentException("FlatConcurrentMap requires key == value");
        for (;;) {
            Entry overflowWinner = overflow.get(key);
            if (overflowWinner != null) return overflowWinner;
            ResizeState state = resize.get();
            if (state != null) {
                migrateKeyGroups(state, key);
                if (resize.get() != state) continue;
            }
            Table current = state == null ? table : state.newTable;
            Groups groups = groupsFor(current, key.hash);
            boolean inserted = false;
            boolean reroute = false;
            boolean grow = false;
            lock(groups, current);
            try {
                ResizeState observed = resize.get();
                if ((observed == null ? table : observed.newTable) != current) {
                    reroute = true;
                } else {
                    Entry winner = findEntry(current, key, groups);
                    if (winner != null) return winner;
                    int slot = freeSlot(current, groups.first, groups.second);
                    if (slot >= 0) {
                        publish(current, slot, groups.tag, value);
                        grow = size.incrementAndGet() >= current.resizeThreshold;
                        inserted = true;
                    }
                }
            } finally {
                unlock(groups, current);
            }
            if (reroute) continue;
            if (inserted) {
                if (grow) startResize(current);
                helpResize(8);
                return null;
            }
            if (relocateAndInsert(current, key, value, groups, state)) {
                helpResize(8);
                return null;
            }
            if (!isWriteTable(current, state)) continue;
            Entry winner = findEntry(current, key, groups);
            if (winner != null) return winner;
            if (current.growthGeneration > 0) {
                winner = overflow.putIfAbsent(value);
                if (winner == null) { size.incrementAndGet(); return null; }
                return winner;
            }
            if (resize.get() == state) startResize(current);
            helpResize(8);
        }
    }

    public boolean remove(LookupKey key, Entry expected) {
        return key != null && expected != null && removeBytes(key.hash(), key.bytes(), key.length(), expected, key, null);
    }

    public boolean remove(EncodedKey key, Entry expected) {
        return key != null && expected != null && removeEncoded(key, expected);
    }

    public boolean remove(Entry key, Entry expected) {
        if (key == null || expected == null) return false;
        ResizeState state = resize.get();
        Table current = state == null ? table : state.newTable;
        if (removeFromEntryTable(current, key, expected)) return true;
        return (state != null && removeFromEntryTable(state.oldTable, key, expected)) || overflow.remove(key, expected);
    }

    private boolean removeBytes(long hash, byte[] bytes, int length, Entry expected, LookupKey lookup, EncodedKey encoded) {
        ResizeState state = resize.get();
        Table current = state == null ? table : state.newTable;
        if (removeFromBytesTable(current, hash, bytes, length, expected)) return true;
        if (state != null && removeFromBytesTable(state.oldTable, hash, bytes, length, expected)) return true;
        return lookup != null ? overflow.remove(lookup, expected) : overflow.remove(encoded, expected);
    }

    private boolean removeEncoded(EncodedKey key, Entry expected) {
        ResizeState state = resize.get();
        Table current = state == null ? table : state.newTable;
        if (removeFromEncodedTable(current, key, expected)) return true;
        if (state != null && removeFromEncodedTable(state.oldTable, key, expected)) return true;
        return overflow.remove(key, expected);
    }

    public boolean removeIfSame(Entry entry) {
        if (entry == null) return false;
        for (int attempt = 0; attempt < 2; attempt++) {
            long location = entry.indexLocation;
            if (location == 0L) {
                if (overflow.removeSame(entry)) { size.decrementAndGet(); return true; }
                return false;
            }
            Table owner = tableForLocation(location);
            if (owner == null) continue;
            int slot = slot(location);
            if (slot < 0 || slot >= owner.slotCapacity) continue;
            int group = slot / GROUP_SLOTS;
            lockGroup(owner, group);
            boolean removed = false;
            try {
                if (entry.indexLocation != location || SLOTS.getAcquire(owner.slots, slot) != entry) continue;
                removeSlot(owner, slot, entry);
                removed = true;
            } finally {
                unlockGroup(owner, group);
            }
            if (removed) {
                helpResize(8);
                return true;
            }
        }
        return false;
    }

    public boolean isCurrent(Entry entry) {
        if (entry == null) return false;
        long location = entry.indexLocation;
        if (location == 0L) return overflow.containsSame(entry);
        Table owner = tableForLocation(location);
        int slot = owner == null ? -1 : slot(location);
        return slot >= 0 && slot < owner.slotCapacity && SLOTS.getAcquire(owner.slots, slot) == entry;
    }

    public int size() { return size.get(); }
    public Collection<Entry> values() { return new ValuesView(); }

    public void clear() {
        Table current = table;
        clearLocations(current);
        ResizeState state = resize.getAndSet(null);
        if (state != null) clearLocations(state.oldTable);
        overflow.clear();
        table = new Table(nextTableId.incrementAndGet(), current.groupCount);
        size.set(0);
    }

    /** Cooperatively migrates at most {@code maxGroups} old-table groups. */
    public int helpResize(int maxGroups) {
        if (maxGroups <= 0) return 0;
        ResizeState state = resize.get();
        if (state == null) return 0;
        int migrated = 0;
        while (migrated < maxGroups) {
            int group = state.cursor.getAndIncrement();
            if (group >= state.oldTable.groupCount) {
                group = nextUnmigratedGroup(state);
                if (group < 0) break;
            }
            migrateGroup(state, group);
            migrated++;
        }
        finishResizeIfComplete(state);
        return migrated;
    }

    public FlatIndexStats snapshot() {
        ResizeState state = resize.get();
        Table current = state == null ? table : state.newTable;
        long payload = heapPayload(current) + (state == null ? 0L : heapPayload(state.oldTable));
        int pending = state == null ? 0 : pendingMigrationGroups(state);
        return new FlatIndexStats(size(), current.slotCapacity, current.groupCount, 2, state != null,
                payload, overflow.size(), overflow.heapPayloadBytes(), state == null ? 0 : state.cursor.get(),
                state == null ? 0 : state.migrated.get(), pending);
    }

    private Entry findBytes(Table current, long hash, byte[] bytes, int length) {
        long route = hash ^ indexSeed;
        int first = (int) route & current.groupMask;
        int second = (int) (route >>> 32) & current.groupMask;
        if (first == second) second = (first + 1) & current.groupMask;
        int tag = (int) (route >>> 57) & 0x7f;
        Entry entry = matchingBytes(current, hash, bytes, length, first, tag);
        return entry != null ? entry : matchingBytes(current, hash, bytes, length, second, tag);
    }

    private Entry findEncoded(Table current, EncodedKey key) {
        long hash = key.hash();
        int length = key.length();
        long route = hash ^ indexSeed;
        int first = (int) route & current.groupMask;
        int second = (int) (route >>> 32) & current.groupMask;
        if (first == second) second = (first + 1) & current.groupMask;
        int tag = (int) (route >>> 57) & 0x7f;
        Entry entry = matchingEncoded(current, key, hash, length, first, tag);
        return entry != null ? entry : matchingEncoded(current, key, hash, length, second, tag);
    }

    private Entry findEntry(Table current, Entry key) {
        long route = key.hash ^ indexSeed;
        int first = (int) route & current.groupMask;
        int second = (int) (route >>> 32) & current.groupMask;
        if (first == second) second = (first + 1) & current.groupMask;
        int tag = (int) (route >>> 57) & 0x7f;
        Entry entry = matchingEntry(current, key, first, tag);
        return entry != null ? entry : matchingEntry(current, key, second, tag);
    }

    private Entry findEntry(Table current, Entry key, Groups groups) {
        Entry entry = matchingEntry(current, key, groups.first, groups.tag);
        return entry != null ? entry : matchingEntry(current, key, groups.second, groups.tag);
    }

    private Entry matchingBytes(Table table, long hash, byte[] bytes, int length, int group, int tag) {
        int base = group * GROUP_SLOTS;
        Entry entry = matchingBytes(table, hash, bytes, length, base,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return entry != null ? entry : matchingBytes(table, hash, bytes, length, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private Entry matchingBytes(Table table, long hash, byte[] bytes, int length, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, base + (Long.numberOfTrailingZeros(matches) >>> 3));
            if (candidate != null && candidate.hash == hash && candidate.keyLength == length
                    && NativeMemory.equals(candidate.nativeKeyAddress, bytes, 0, length)) return candidate;
        }
        return null;
    }

    private Entry matchingEncoded(Table table, EncodedKey key, long hash, int length, int group, int tag) {
        int base = group * GROUP_SLOTS;
        Entry entry = matchingEncoded(table, key, hash, length, base,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return entry != null ? entry : matchingEncoded(table, key, hash, length, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private Entry matchingEncoded(Table table, EncodedKey key, long hash, int length, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, base + (Long.numberOfTrailingZeros(matches) >>> 3));
            if (candidate != null && candidate.hash == hash && candidate.keyLength == length && key.matches(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private Entry matchingEntry(Table table, Entry key, int group, int tag) {
        int base = group * GROUP_SLOTS;
        Entry entry = matchingEntry(table, key, base, (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return entry != null ? entry : matchingEntry(table, key, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private Entry matchingEntry(Table table, Entry key, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, base + (Long.numberOfTrailingZeros(matches) >>> 3));
            if (candidate != null && (candidate == key || (candidate.hash == key.hash && candidate.keyLength == key.keyLength
                    && NativeMemory.equals(candidate.nativeKeyAddress, key.nativeKeyAddress, key.keyLength)))) return candidate;
        }
        return null;
    }

    private boolean removeFromBytesTable(Table current, long hash, byte[] bytes, int length, Entry expected) {
        Groups groups = groupsFor(current, hash);
        lock(groups, current);
        try {
            int slot = matchingBytesSlot(current, hash, bytes, length, groups);
            if (slot < 0 || SLOTS.getAcquire(current.slots, slot) != expected) return false;
            removeSlot(current, slot, expected);
            return true;
        } finally { unlock(groups, current); }
    }

    private boolean removeFromEncodedTable(Table current, EncodedKey key, Entry expected) {
        long hash = key.hash();
        int length = key.length();
        Groups groups = groupsFor(current, hash);
        lock(groups, current);
        try {
            int slot = matchingEncodedSlot(current, key, hash, length, groups);
            if (slot < 0 || SLOTS.getAcquire(current.slots, slot) != expected) return false;
            removeSlot(current, slot, expected);
            return true;
        } finally { unlock(groups, current); }
    }

    private boolean removeFromEntryTable(Table current, Entry key, Entry expected) {
        Groups groups = groupsFor(current, key.hash);
        lock(groups, current);
        try {
            int slot = matchingEntrySlot(current, key, groups);
            if (slot < 0 || SLOTS.getAcquire(current.slots, slot) != expected) return false;
            removeSlot(current, slot, expected);
            return true;
        } finally { unlock(groups, current); }
    }

    private int matchingBytesSlot(Table table, long hash, byte[] bytes, int length, Groups groups) {
        int slot = matchingBytesSlot(table, hash, bytes, length, groups.first, groups.tag);
        return slot >= 0 ? slot : matchingBytesSlot(table, hash, bytes, length, groups.second, groups.tag);
    }

    private int matchingBytesSlot(Table table, long hash, byte[] bytes, int length, int group, int tag) {
        int base = group * GROUP_SLOTS;
        int slot = matchingBytesSlot(table, hash, bytes, length, base,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return slot >= 0 ? slot : matchingBytesSlot(table, hash, bytes, length, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private int matchingBytesSlot(Table table, long hash, byte[] bytes, int length, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            int slot = base + (Long.numberOfTrailingZeros(matches) >>> 3);
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, slot);
            if (candidate != null && candidate.hash == hash && candidate.keyLength == length
                    && NativeMemory.equals(candidate.nativeKeyAddress, bytes, 0, length)) return slot;
        }
        return -1;
    }

    private int matchingEncodedSlot(Table table, EncodedKey key, long hash, int length, Groups groups) {
        int slot = matchingEncodedSlot(table, key, hash, length, groups.first, groups.tag);
        return slot >= 0 ? slot : matchingEncodedSlot(table, key, hash, length, groups.second, groups.tag);
    }

    private int matchingEncodedSlot(Table table, EncodedKey key, long hash, int length, int group, int tag) {
        int base = group * GROUP_SLOTS;
        int slot = matchingEncodedSlot(table, key, hash, length, base,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return slot >= 0 ? slot : matchingEncodedSlot(table, key, hash, length, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private int matchingEncodedSlot(Table table, EncodedKey key, long hash, int length, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            int slot = base + (Long.numberOfTrailingZeros(matches) >>> 3);
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, slot);
            if (candidate != null && candidate.hash == hash && candidate.keyLength == length && key.matches(candidate)) {
                return slot;
            }
        }
        return -1;
    }

    private int matchingEntrySlot(Table table, Entry key, Groups groups) {
        int slot = matchingEntrySlot(table, key, groups.first, groups.tag);
        return slot >= 0 ? slot : matchingEntrySlot(table, key, groups.second, groups.tag);
    }

    private int matchingEntrySlot(Table table, Entry key, int group, int tag) {
        int base = group * GROUP_SLOTS;
        int slot = matchingEntrySlot(table, key, base, (long) CONTROLS.getAcquire(table.controlWords, group * 2), tag);
        return slot >= 0 ? slot : matchingEntrySlot(table, key, base + Long.BYTES,
                (long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), tag);
    }

    private int matchingEntrySlot(Table table, Entry key, int base, long controls, int tag) {
        for (long matches = matchingByteMask(controls, tag); matches != 0L; matches &= matches - 1L) {
            int slot = base + (Long.numberOfTrailingZeros(matches) >>> 3);
            Entry candidate = (Entry) SLOTS.getAcquire(table.slots, slot);
            if (candidate != null && (candidate == key || (candidate.hash == key.hash && candidate.keyLength == key.keyLength
                    && NativeMemory.equals(candidate.nativeKeyAddress, key.nativeKeyAddress, key.keyLength)))) return slot;
        }
        return -1;
    }

    private static int freeSlot(Table table, int first, int second) {
        int slot = findFreeSlot(table, first, EMPTY);
        if (slot >= 0) return slot;
        slot = findFreeSlot(table, second, EMPTY);
        if (slot >= 0) return slot;
        slot = findFreeSlot(table, first, DELETED);
        return slot >= 0 ? slot : findFreeSlot(table, second, DELETED);
    }

    private static int findFreeSlot(Table table, int group, int desiredControl) {
        int base = group * GROUP_SLOTS;
        long first = matchingByteMask((long) CONTROLS.getAcquire(table.controlWords, group * 2), desiredControl);
        if (first != 0L) return base + (Long.numberOfTrailingZeros(first) >>> 3);
        long second = matchingByteMask((long) CONTROLS.getAcquire(table.controlWords, group * 2 + 1), desiredControl);
        return second == 0L ? -1 : base + Long.BYTES + (Long.numberOfTrailingZeros(second) >>> 3);
    }

    private static int freeSlotInGroup(Table table, int group) {
        int slot = findFreeSlot(table, group, EMPTY);
        return slot >= 0 ? slot : findFreeSlot(table, group, DELETED);
    }

    private void publish(Table table, int slot, int tag, Entry entry) {
        SLOTS.setRelease(table.slots, slot, entry);
        entry.indexLocation = location(table.tableId, slot);
        setControl(table, slot / GROUP_SLOTS, slot & (GROUP_SLOTS - 1), tag);
    }

    private void removeSlot(Table table, int slot, Entry entry) {
        SLOTS.setRelease(table.slots, slot, null);
        setControl(table, slot / GROUP_SLOTS, slot & (GROUP_SLOTS - 1), DELETED);
        entry.indexLocation = 0L;
        size.decrementAndGet();
    }

    private boolean relocateAndInsert(Table table, Entry candidate, Entry value, Groups candidateGroups, ResizeState expectedResize) {
        if (resize.get() != expectedResize) return false;
        int[] groups = new int[8];
        int[] parents = new int[8];
        int[] sourceSlots = new int[8];
        Entry[] sources = new Entry[8];
        int count = 2;
        groups[0] = candidateGroups.first;
        groups[1] = candidateGroups.second;
        parents[0] = parents[1] = -1;
        int endpoint = -1;
        for (int cursor = 0; cursor < count && cursor < 8 && endpoint < 0; cursor++) {
            int group = groups[cursor];
            if (freeSlotInGroup(table, group) >= 0) { endpoint = cursor; break; }
            int base = group * GROUP_SLOTS;
            for (int offset = 0; offset < GROUP_SLOTS && count < 8; offset++) {
                int sourceSlot = base + offset;
                Entry source = (Entry) SLOTS.getAcquire(table.slots, sourceSlot);
                if (source == null || !visibleAt(table, sourceSlot, source)) continue;
                Groups alternatives = groupsFor(table, source.hash);
                int alternate = alternatives.first == group ? alternatives.second : alternatives.first;
                if (contains(groups, count, alternate)) continue;
                groups[count] = alternate;
                parents[count] = cursor;
                sourceSlots[count] = sourceSlot;
                sources[count] = source;
                count++;
            }
        }
        if (endpoint < 0) return false;
        int[] locks = java.util.Arrays.copyOf(groups, count);
        java.util.Arrays.sort(locks);
        for (int group : locks) lockGroup(table, group);
        try {
            if (!isWriteTable(table, expectedResize) || findEntry(table, candidate, candidateGroups) != null) return false;
            int destination = freeSlotInGroup(table, groups[endpoint]);
            if (destination < 0) return false;
            for (int node = endpoint; parents[node] >= 0; node = parents[node]) {
                int sourceSlot = sourceSlots[node];
                if (SLOTS.getAcquire(table.slots, sourceSlot) != sources[node]
                        || !visibleAt(table, sourceSlot, sources[node])) return false;
            }
            for (int node = endpoint; parents[node] >= 0; node = parents[node]) {
                int sourceSlot = sourceSlots[node];
                Entry source = sources[node];
                move(table, sourceSlot, destination, groupsFor(table, source.hash).tag, source);
                destination = sourceSlot;
            }
            publish(table, destination, candidateGroups.tag, value);
            size.incrementAndGet();
            return true;
        } finally {
            for (int index = locks.length - 1; index >= 0; index--) unlockGroup(table, locks[index]);
        }
    }

    private void move(Table table, int source, int destination, int tag, Entry entry) {
        SLOTS.setRelease(table.slots, destination, entry);
        entry.indexLocation = location(table.tableId, destination);
        setControl(table, destination / GROUP_SLOTS, destination & (GROUP_SLOTS - 1), tag);
        SLOTS.setRelease(table.slots, source, null);
        setControl(table, source / GROUP_SLOTS, source & (GROUP_SLOTS - 1), DELETED);
    }

    private void migrateKeyGroups(ResizeState state, Entry key) {
        Groups groups = groupsFor(state.oldTable, key.hash);
        migrateGroup(state, groups.first);
        migrateGroup(state, groups.second);
        finishResizeIfComplete(state);
    }

    private void migrateGroup(ResizeState state, int group) {
        Table old = state.oldTable;
        lockGroup(old, group);
        try {
            int meta = (int) GROUP_META.getVolatile(old.groupMeta, group);
            if ((meta & MIGRATED) != 0) return;
            int base = group * GROUP_SLOTS;
            for (int offset = 0; offset < GROUP_SLOTS; offset++) {
                int sourceSlot = base + offset;
                Entry entry = (Entry) SLOTS.getAcquire(old.slots, sourceSlot);
                if (entry == null) continue;
                if (!visibleAt(old, sourceSlot, entry)) continue;
                Groups targets = groupsFor(state.newTable, entry.hash);
                lock(targets, state.newTable);
                try {
                    if (findEntry(state.newTable, entry, targets) != null) continue;
                    int target = freeSlot(state.newTable, targets.first, targets.second);
                    if (target < 0) throw new IllegalStateException("flat index growth target is unexpectedly full");
                    publish(state.newTable, target, targets.tag, entry);
                } finally { unlock(targets, state.newTable); }
            }
            GROUP_META.setRelease(old.groupMeta, group, meta | MIGRATED);
            state.migrated.incrementAndGet();
        } finally { unlockGroup(old, group); }
    }

    private void startResize(Table expected) {
        if (resize.get() != null || table != expected) return;
        if (expected.groupCount >= (1 << 25)) throw new IllegalStateException("flat index maximum capacity reached");
        Table next = new Table(nextTableId.incrementAndGet(), expected.groupCount << 1, expected.growthGeneration + 1);
        ResizeState started = new ResizeState(expected, next);
        if (!resize.compareAndSet(null, started)) return;
        // The table can have advanced between the optimistic check above and this CAS. Never
        // resurrect that already-migrated table as the old side of a fresh resize state.
        if (table != expected) {
            resize.compareAndSet(started, null);
            return;
        }
        table = next;
    }

    private void finishResizeIfComplete(ResizeState state) {
        if (table == state.newTable && state.migrated.get() == state.oldTable.groupCount) resize.compareAndSet(state, null);
    }

    private boolean isWriteTable(Table candidate, ResizeState expectedResize) {
        ResizeState observed = resize.get();
        return observed == expectedResize && (observed == null ? table : observed.newTable) == candidate;
    }

    private Table tableForLocation(long location) {
        int id = tableId(location);
        ResizeState state = resize.get();
        Table current = state == null ? table : state.newTable;
        if (current.tableId == id) return current;
        return state != null && state.oldTable.tableId == id ? state.oldTable : null;
    }

    private Groups groupsFor(Table table, long hash) {
        long route = hash ^ indexSeed;
        int first = (int) route & table.groupMask;
        int second = (int) (route >>> 32) & table.groupMask;
        if (first == second) second = (first + 1) & table.groupMask;
        return new Groups(first, second, (int) (route >>> 57) & 0x7f);
    }

    private static boolean visibleAt(Table table, int slot, Entry entry) {
        return entry.indexLocation == location(table.tableId, slot);
    }

    static long matchingByteMask(long controls, int control) {
        long difference = controls ^ (((long) control & 0xffL) * BYTE_ONE);
        return ~(((difference & BYTE_LOW_BITS) + BYTE_LOW_BITS) | difference | BYTE_LOW_BITS) & BYTE_HIGH_BITS;
    }

    private static void setControl(Table table, int group, int offset, int value) {
        int word = group * 2 + (offset >>> 3);
        int shift = (offset & 7) << 3;
        long old = (long) CONTROLS.getAcquire(table.controlWords, word);
        CONTROLS.setRelease(table.controlWords, word, (old & ~(0xffL << shift)) | ((long) value << shift));
    }

    private static void lock(Groups groups, Table table) {
        int low = Math.min(groups.first, groups.second);
        int high = Math.max(groups.first, groups.second);
        lockGroup(table, low);
        lockGroup(table, high);
    }

    private static void unlock(Groups groups, Table table) {
        int low = Math.min(groups.first, groups.second);
        int high = Math.max(groups.first, groups.second);
        unlockGroup(table, high);
        unlockGroup(table, low);
    }

    private static void lockGroup(Table table, int group) {
        int spins = 0;
        for (;;) {
            int observed = (int) GROUP_META.getVolatile(table.groupMeta, group);
            if ((observed & LOCKED) == 0 && GROUP_META.compareAndSet(table.groupMeta, group, observed, observed | LOCKED)) return;
            if (spins++ < 64) Thread.onSpinWait(); else LockSupport.parkNanos(1_000L);
        }
    }

    private static void unlockGroup(Table table, int group) {
        int observed = (int) GROUP_META.getVolatile(table.groupMeta, group);
        GROUP_META.setRelease(table.groupMeta, group, observed & ~LOCKED);
    }

    private static boolean contains(int[] values, int length, int candidate) {
        for (int index = 0; index < length; index++) if (values[index] == candidate) return true;
        return false;
    }

    private static int nextUnmigratedGroup(ResizeState state) {
        for (int group = 0; group < state.oldTable.groupCount; group++) {
            if ((((int) GROUP_META.getVolatile(state.oldTable.groupMeta, group)) & MIGRATED) == 0) return group;
        }
        return -1;
    }

    private static int pendingMigrationGroups(ResizeState state) {
        int pending = 0;
        for (int group = 0; group < state.oldTable.groupCount; group++) {
            if ((((int) GROUP_META.getVolatile(state.oldTable.groupMeta, group)) & MIGRATED) == 0) pending++;
        }
        return pending;
    }

    private static long location(int tableId, int slot) { return ((long) tableId << 32) | ((slot + 1L) & 0xffffffffL); }
    private static int tableId(long location) { return (int) (location >>> 32); }
    private static int slot(long location) { return (int) location - 1; }

    private static int groupCountFor(long expectedEntries) {
        long bounded = Math.max(1L, Math.min(expectedEntries, 1L << 28));
        long requiredSlots = Math.max(32L, (bounded * 5L + 3L) / 4L);
        long requiredGroups = Math.max(2L, (requiredSlots + GROUP_SLOTS - 1L) / GROUP_SLOTS);
        int groups = 1;
        while (groups < requiredGroups && groups < (1 << 25)) groups <<= 1;
        return groups;
    }

    private static long heapPayload(Table table) {
        return (long) table.slots.length * NativeMemory.objectReferenceSize()
                + (long) table.controlWords.length * Long.BYTES + (long) table.groupMeta.length * Integer.BYTES;
    }

    private static void clearLocations(Table table) {
        for (int slot = 0; slot < table.slotCapacity; slot++) {
            Entry entry = (Entry) SLOTS.getAcquire(table.slots, slot);
            if (entry != null && visibleAt(table, slot, entry)) entry.indexLocation = 0L;
        }
    }

    private final class ValuesView extends AbstractCollection<Entry> {
        @Override public Iterator<Entry> iterator() {
            ResizeState state = resize.get();
            return new ValuesIterator(state == null ? table : state.newTable, state == null ? null : state.oldTable,
                    overflow.valuesSnapshot().iterator());
        }
        @Override public int size() { return FlatConcurrentMap.this.size(); }
        @Override public void clear() { FlatConcurrentMap.this.clear(); }
    }

    private static final class ValuesIterator implements Iterator<Entry> {
        private final Table first;
        private final Table second;
        private final Iterator<Entry> overflow;
        private Table current;
        private int cursor;
        private Entry next;
        ValuesIterator(Table first, Table second, Iterator<Entry> overflow) {
            this.first = first; this.second = second; this.overflow = overflow; this.current = first;
        }
        @Override public boolean hasNext() {
            while (next == null && current != null) {
                while (cursor < current.slotCapacity) {
                    int slot = cursor++;
                    Entry candidate = (Entry) SLOTS.getAcquire(current.slots, slot);
                    if (candidate == null || !visibleAt(current, slot, candidate)) continue;
                    next = candidate;
                    return true;
                }
                if (current == first) { current = second; cursor = 0; } else current = null;
            }
            if (next == null && overflow.hasNext()) next = overflow.next();
            return next != null;
        }
        @Override public Entry next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry result = next; next = null; return result;
        }
        @Override public void remove() { throw new UnsupportedOperationException(); }
    }

    private static final class Groups {
        final int first;
        final int second;
        final int tag;
        Groups(int first, int second, int tag) { this.first = first; this.second = second; this.tag = tag; }
    }

    private static final class ResizeState {
        final Table oldTable;
        final Table newTable;
        final AtomicInteger cursor = new AtomicInteger();
        final AtomicInteger migrated = new AtomicInteger();
        ResizeState(Table oldTable, Table newTable) { this.oldTable = oldTable; this.newTable = newTable; }
    }

    private static final class Table {
        final Object[] slots;
        final long[] controlWords;
        final int[] groupMeta;
        final int groupCount;
        final int groupMask;
        final int tableId;
        final int slotCapacity;
        final int resizeThreshold;
        final int growthGeneration;
        Table(int tableId, int groupCount) { this(tableId, groupCount, 0); }
        Table(int tableId, int groupCount, int growthGeneration) {
            this.tableId = tableId;
            this.groupCount = groupCount;
            this.groupMask = groupCount - 1;
            this.slotCapacity = groupCount * GROUP_SLOTS;
            this.resizeThreshold = slotCapacity * 4 / 5;
            this.growthGeneration = growthGeneration;
            this.slots = new Object[slotCapacity];
            this.controlWords = new long[groupCount * 2];
            this.groupMeta = new int[groupCount];
            java.util.Arrays.fill(controlWords, 0x8080808080808080L);
        }
    }
}
