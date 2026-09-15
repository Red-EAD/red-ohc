package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class HeapLayoutTest {
  @Test
  public void entryMetadataRemainsNativeAndLifecycleCompact() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    assertEquals(bytes, 32L, "Entry must keep per-mapping metadata within the compact object");
  }

  @Test
  public void entryContainsOnlyAdmissionAndNativeFields() {
    Set<String> fields =
        new HashSet<>(
            Arrays.stream(Entry.class.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toSet()));

    assertEquals(
        fields,
        new HashSet<>(
            Arrays.asList(
                "nativeKeyAddress",
                "keyLength",
                "valueAddress")));
  }

  @Test
  public void entryHasOnlyNativeAndLifecycleFields() {
    for (Field field : Entry.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      assertTrue(field.getType().isPrimitive(), field.getName() + " must be scalar metadata");
    }
  }

  @Test
  public void valueHeaderMetadataDoesNotIncreaseNativeAllocationLength() {
    assertEquals(ValueBlock.allocationLength(1), 24L);
    assertEquals(ValueBlock.allocationLength(8), 24L);
    assertEquals(ValueBlock.allocationLength(9), 32L);
  }

  @Test(timeOut = 180_000L)
  public void fiveMillionEntrySmokeReportsAllocatorClassOverhead() {
    if (!Boolean.getBoolean("redohc.heap.smoke")) {
      return;
    }
    int count = Integer.getInteger("redohc.heap.smoke.count", 5_000_000);
    long keyAllocation = Entry.keyAllocationLengthForKeyLength(0);
    long minimumNativeBytes = (long) count * WriterArena.directAllocationBytes(keyAllocation);
    Runtime runtime = Runtime.getRuntime();
    System.gc();
    long heapBefore = runtime.totalMemory() - runtime.freeMemory();
    NativeMemory.Memory memory = new NativeMemory.Memory(AllocatorType.JNA);
    ConcurrentHashMap<Entry, Entry> index = new ConcurrentHashMap<>(count);
    try {
      for (int i = 0; i < count; i++) {
        long keyAddress = memory.newWriterArena().allocate(keyAllocation);
        NativeMemory.putLong(keyAddress, i);
        Entry entry = new Entry(keyAddress, 0, 0L);
        entry.initializeNativeMetadata();
        index.put(entry, entry);
      }
      long nativeResident = memory.allocated();
      long heapAfter = runtime.totalMemory() - runtime.freeMemory();
      System.out.printf(
          "5m Entry smoke: count=%d, entryBytes=%d, allocatorWeight=%d, nativeResident=%d, "
              + "nativePerEntry=%.2f, directBytes=%d, heapDelta=%d, chmSize=%d%n",
          count,
          keyAllocation,
          WriterArena.allocationWeight(keyAllocation),
          nativeResident,
          (double) nativeResident / count,
          WriterArena.directAllocationBytes(keyAllocation),
          heapAfter - heapBefore,
          index.mappingCount());
      assertTrue(nativeResident >= minimumNativeBytes);
    } finally {
      for (Entry entry : index.keySet()) {
        memory.releaseEntry(entry.nativeKeyAddress, keyAllocation);
      }
      index.clear();
      memory.closeArenas();
    }
  }
}
