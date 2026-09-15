package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import java.util.concurrent.ConcurrentHashMap;

import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.Ticker;
import com.red.ohc.index.Entry;
import com.red.ohc.index.EntryTestSupport;
import com.red.ohc.runtime.ReaderRegistry;
import com.red.ohc.runtime.ThreadContext;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;

public class MaintenanceGenerationTest {
  @Test
  public void staleRemovalCannotDeleteAValuePublishedAfterTheMaintenanceSnapshot() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long oldValue = memory.allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(oldValue, 0L, 1, 0L);
    long newValue = memory.allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(newValue, 0L, 1, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 7, oldValue);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.putIfAbsent(entry, entry);
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop worker =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    try {
      long generation = entry.generation();
      assertTrue(entry.claimWriter());
      entry.valueAddress = newValue;
      entry.finishWriter();

      assertFalse(worker.removeFromMap(entry, false, generation, oldValue));
      assertSame(data.get(entry), entry);
      assertEquals(entry.valueAddress, newValue);
    } finally {
      readers.clear();
      readers.close();
      memory.free(oldValue, ValueBlock.allocationLength(1));
      memory.free(newValue, ValueBlock.allocationLength(1));
      memory.closeArenas();
    }
  }

  @Test
  public void detachedEntryCannotRemoveAChmMappingWithNoValueAddress() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    Entry entry = EntryTestSupport.entry(memory, 0, 7, 0L);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.putIfAbsent(entry, entry);
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop worker =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);

    try {
      assertFalse(worker.removeFromMap(entry, false, entry.generation(), 0L));
      assertSame(data.get(entry), entry);
      data.clear();
    } finally {
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  @Test
  public void identityRemovalDoesNotDeleteAReinsertedSameKey() {
    Entry stale = EntryTestSupport.entry(0, 17, 0x1717L, 1L);
    Entry current = EntryTestSupport.entry(0, 17, 0x1717L, 2L);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(stale, stale);
    data.put(current, current);

    ThreadContext context = new ThreadContext(null);
    assertFalse(context.removeEntryIfSame(data, stale));
    assertSame(data.get(stale), current);
    assertTrue(context.removeEntryIfSame(data, current));
    assertEquals(data.get(stale), null);
  }

  @Test(timeOut = 500L)
  public void evictionSkipsAnEntryWhoseWriterMutexIsHeld() {
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    long value = memory.allocate(ValueBlock.allocationLength(1));
    ValueBlock.initialize(value, 0L, 1, 0L);
    Entry entry = EntryTestSupport.entry(memory, 0, 9, value);
    ConcurrentHashMap<Entry, Entry> data = index();
    data.put(entry, entry);
    ReaderRegistry readers = new ReaderRegistry(memory);
    MaintenanceEventLoop worker =
        new MaintenanceEventLoop(
            data,
            memory,
            Ticker.DEFAULT,
            1 << 20,
            Eviction.LRU,
            readers, Long.MAX_VALUE);
    try {
      assertTrue(entry.claimWriter());
      assertFalse(worker.removeFromMap(entry, true, entry.generation(), entry.valueAddress));
      assertSame(data.get(entry), entry);
    } finally {
      entry.finishWriter();
      data.clear();
      memory.free(value, ValueBlock.allocationLength(1));
      readers.clear();
      readers.close();
      memory.closeArenas();
    }
  }

  private static void assertTrue(boolean value) {
    if (!value) {
      throw new AssertionError("expected true");
    }
  }

  private static ConcurrentHashMap<Entry, Entry> index() {
    return new ConcurrentHashMap<>();
  }
}
