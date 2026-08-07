package com.red.ohc.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.red.ohc.EncodedKey;
import com.red.ohc.codec.LookupKey;
import com.red.ohc.storage.NativeMemory;

/** Exceptional-path ordering for deliberately colliding 64-bit hashes. */
final class CollisionTree {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final TreeMap<Key, Entry> entries = new TreeMap<>(new Comparator<Key>() {
        @Override public int compare(Key left, Key right) { return compareKeys(left, right); }
    });
    private volatile int size;

    Entry get(LookupKey key) {
        return size == 0 ? null : get(Key.lookup(key));
    }

    Entry get(EncodedKey key) {
        return size == 0 ? null : get(Key.lookup(key));
    }

    Entry get(Entry key) {
        return size == 0 ? null : get(Key.entry(key));
    }

    private Entry get(Key key) {
        lock.readLock().lock();
        try { return entries.get(key); }
        finally { lock.readLock().unlock(); }
    }

    Entry putIfAbsent(Entry entry) {
        lock.writeLock().lock();
        try {
            Key key = Key.entry(entry);
            Entry winner = entries.get(key);
            if (winner != null) return winner;
            entries.put(key, entry);
            size++;
            return null;
        } finally { lock.writeLock().unlock(); }
    }

    boolean remove(LookupKey key, Entry expected) {
        return size != 0 && remove(Key.lookup(key), expected);
    }

    boolean remove(EncodedKey key, Entry expected) {
        return size != 0 && remove(Key.lookup(key), expected);
    }

    boolean remove(Entry key, Entry expected) {
        return size != 0 && remove(Key.entry(key), expected);
    }

    private boolean remove(Key key, Entry expected) {
        lock.writeLock().lock();
        try {
            Entry winner = entries.get(key);
            if (winner != expected) return false;
            entries.remove(key);
            size--;
            return true;
        } finally { lock.writeLock().unlock(); }
    }

    boolean removeSame(Entry entry) { return remove(entry, entry); }
    boolean containsSame(Entry entry) { return get(entry) == entry; }
    int size() { return size; }

    Collection<Entry> valuesSnapshot() {
        lock.readLock().lock();
        try { return new ArrayList<>(entries.values()); }
        finally { lock.readLock().unlock(); }
    }

    void clear() {
        lock.writeLock().lock();
        try { entries.clear(); size = 0; }
        finally { lock.writeLock().unlock(); }
    }

    long heapPayloadBytes() { return (long) size * 40L; }

    private static int compareKeys(Key left, Key right) {
        if (left == right) return 0;
        Entry leftEntry = left.entry;
        Entry rightEntry = right.entry;
        if (leftEntry != null && rightEntry != null) return compare(leftEntry, rightEntry);
        if (leftEntry != null) return compare(leftEntry, right.lookup);
        if (rightEntry != null) return -compare(rightEntry, left.lookup);
        throw new AssertionError("collision tree cannot compare two lookups");
    }

    private static int compare(Entry left, Entry right) {
        int comparison = Long.compareUnsigned(left.hash, right.hash);
        if (comparison != 0) return comparison;
        comparison = Integer.compare(left.keyLength, right.keyLength);
        if (comparison != 0) return comparison;
        for (int index = 0; index < left.keyLength; index++) {
            comparison = Integer.compare(NativeMemory.getByte(left.nativeKeyAddress + index) & 0xff,
                    NativeMemory.getByte(right.nativeKeyAddress + index) & 0xff);
            if (comparison != 0) return comparison;
        }
        return 0;
    }

    private static int compare(Entry stored, Object lookup) {
        if (lookup instanceof LookupKey) {
            LookupKey key = (LookupKey) lookup;
            int comparison = Long.compareUnsigned(stored.hash, key.hash());
            if (comparison != 0) return comparison;
            comparison = Integer.compare(stored.keyLength, key.length());
            return comparison != 0 ? comparison : compareBytes(stored, key.bytes(), key.length());
        }
        EncodedKey key = (EncodedKey) lookup;
        int comparison = Long.compareUnsigned(stored.hash, key.hash());
        if (comparison != 0) return comparison;
        comparison = Integer.compare(stored.keyLength, key.length());
        return comparison != 0 ? comparison : compareBytes(stored, key.bytes(), key.length());
    }

    private static int compareBytes(Entry stored, byte[] bytes, int length) {
        for (int index = 0; index < length; index++) {
            int comparison = Integer.compare(NativeMemory.getByte(stored.nativeKeyAddress + index) & 0xff,
                    bytes[index] & 0xff);
            if (comparison != 0) return comparison;
        }
        return 0;
    }

    private static final class Key {
        final Entry entry;
        final Object lookup;
        private Key(Entry entry, Object lookup) { this.entry = entry; this.lookup = lookup; }
        static Key entry(Entry entry) { return new Key(entry, null); }
        static Key lookup(LookupKey lookup) { return new Key(null, lookup); }
        static Key lookup(EncodedKey lookup) { return new Key(null, lookup); }
    }
}
