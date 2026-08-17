package com.red.ohc.index;

import java.util.concurrent.ConcurrentHashMap;

import com.red.ohc.storage.NativeMemory;

/** Cache-owned weak publication state kept outside the hot Entry object. */
public final class WeakValueStateStore {
  private static final Entry.ValueState EMPTY_VALUE_STATE = new Entry.ValueState(0L, null);

  private final ConcurrentHashMap<Entry, Entry.ValueState> states = new ConcurrentHashMap<>();

  public Entry.ValueState get(Entry entry) {
    return states.get(entry);
  }

  public void initialize(Entry entry, long taggedValueAddress, Entry.WeakValueSlot slot) {
    states.put(entry, new Entry.ValueState(taggedValueAddress, slot));
  }

  public void publish(Entry entry, long taggedValueAddress, Entry.WeakValueSlot slot) {
    Entry.ValueState next = new Entry.ValueState(taggedValueAddress, slot);
    entry.valueAddress = taggedValueAddress;
    states.put(entry, next);
  }

  public boolean compareAndSet(
      Entry entry, Entry.ValueState expected, Entry.ValueState update) {
    if (expected == null || update == null) {
      return false;
    }
    if (states.get(entry) != expected || entry.valueAddress != expected.taggedValueAddress()) {
      return false;
    }
    return states.replace(entry, expected, update);
  }

  public void clearValue(Entry entry) {
    entry.valueAddress = 0L;
    states.put(entry, EMPTY_VALUE_STATE);
  }

  public void clearWeakValueSlot(Entry entry) {
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      Entry.ValueState current = states.get(entry);
      if (current == null || entry.valueAddress != current.taggedValueAddress()) {
        return;
      }
      Entry.ValueState next = current.withWeakValue(null);
      if (states.replace(entry, current, next)) {
        return;
      }
      Thread.onSpinWait();
    }
  }

  public boolean clearWeakValueIfCurrent(Entry.WeakValueSlot expected) {
    if (expected == null) {
      return false;
    }
    Entry entry = expected.owner();
    if (entry == null) {
      return false;
    }
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      Entry.ValueState current = states.get(entry);
      if (current == null
          || current.weakValue() != expected
          || current.taggedValueAddress() != expected.taggedValueAddress()
          || entry.valueAddress != current.taggedValueAddress()) {
        return false;
      }
      Entry.ValueState next = current.withWeakValue(null);
      if (states.replace(entry, current, next)) {
        expected.clear();
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  public boolean disableWeakValue(Entry entry, Entry.ValueState expected) {
    if (expected == null) {
      return false;
    }
    for (int attempt = 0; attempt < NativeMemory.LOGICAL_CPU_COUNT; attempt++) {
      Entry.ValueState current = states.get(entry);
      if (current == null
          || current.publication() != expected.publication()
          || current.taggedValueAddress() != expected.taggedValueAddress()
          || entry.valueAddress != current.taggedValueAddress()) {
        return false;
      }
      current.disableFingerprint();
      Entry.ValueState disabled = current.withWeakValue(null);
      if (states.replace(entry, current, disabled)) {
        if (current.weakValue() != null) {
          current.weakValue().clear();
        }
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  public void remove(Entry entry) {
    states.remove(entry);
  }

  public void clear() {
    states.clear();
  }
}
