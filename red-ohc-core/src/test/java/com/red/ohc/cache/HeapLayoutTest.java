package com.red.ohc.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;
import org.testng.annotations.Test;

import com.red.ohc.api.AllocatorType;
import com.red.ohc.index.Entry;
import com.red.ohc.storage.NativeMemory;
import com.red.ohc.storage.ValueBlock;
import com.red.ohc.storage.WriterArena;

public class HeapLayoutTest {
  @Test
  public void entryMetadataRemainsAtThe64ByteTarget() {
    long bytes = ClassLayout.parseClass(Entry.class).instanceSize();
    assertEquals(bytes, 64L, "Entry metadata changed from the 64-byte target");
  }

  @Test
  public void entryContainsOnlyHotInstanceFields() {
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
                "keyMeta",
                "valueAddress",
                "lifecycle",
                "policyPrev",
                "policyNext",
                "timerPrev",
                "timerNext",
                "pendingFlags")));
    assertFalse(fields.contains("maintenanceMeta"));
    assertFalse(fields.contains("policyByteWeight"));
  }

  @Test
  public void entryReferencesUseFourByteCompressedOopsSlots() {
    Set<String> referenceFields =
        new HashSet<>(Arrays.asList("policyPrev", "policyNext", "timerPrev", "timerNext"));
    int referenceCount = 0;
    for (FieldLayout field : ClassLayout.parseClass(Entry.class).fields()) {
      if (referenceFields.contains(field.name())) {
        referenceCount++;
        assertEquals(field.size(), 4L, field.name() + " must use a compressed reference slot");
      }
    }
    assertEquals(referenceCount, referenceFields.size());
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
        Entry entry = new Entry(keyAddress, 0, i, i, 0L);
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
