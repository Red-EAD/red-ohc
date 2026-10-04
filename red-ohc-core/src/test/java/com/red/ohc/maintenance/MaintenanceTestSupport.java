package com.red.ohc.maintenance;

import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Map;

import com.red.ohc.index.Entry;

/** Test producers use the same journal transport as cache writers. */
public final class MaintenanceTestSupport {
  private static final ThreadLocal<Map<MaintenanceEventLoop, WriterLifecycleLane>> PRODUCERS =
      ThreadLocal.withInitial(IdentityHashMap::new);

  private MaintenanceTestSupport() {}

  public static WriterLifecycleJournal lifecycle(MaintenanceEventLoop loop) {
    try {
      Field field = MaintenanceEventLoop.class.getDeclaredField("writerLifecycleJournal");
      field.setAccessible(true);
      WriterLifecycleJournal journal = (WriterLifecycleJournal) field.get(loop);
      if (journal == null) {
        journal = new WriterLifecycleJournal();
        loop.bindWriterLifecycleJournal(journal);
      }
      return journal;
    } catch (ReflectiveOperationException failure) {
      throw new AssertionError(failure);
    }
  }

  public static void start(MaintenanceEventLoop loop) {
    lifecycle(loop);
    loop.start();
  }

  public static void publishMutation(MaintenanceEventLoop loop, Entry entry, int flags) {
    if (loop.prepareMutation(entry, flags)) {
      enqueueMutationHint(loop, entry, true);
    }
  }

  public static void enqueueMutationHint(MaintenanceEventLoop loop, Entry entry, boolean wake) {
    WriterLifecycleLane lane =
        PRODUCERS.get().computeIfAbsent(loop, key -> lifecycle(key).createLane());
    loop.enqueueWriterMutationHint(
        lane, entry, 0, 0L, WriterLifecycleLane.UNSEEDED_MUTATION_VERSION, wake);
  }
}
