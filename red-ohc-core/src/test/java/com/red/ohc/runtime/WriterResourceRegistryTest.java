package com.red.ohc.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.red.ohc.maintenance.RetirementJournal;
import com.red.ohc.maintenance.WriterLifecycleJournal;
import com.red.ohc.maintenance.WriterLifecycleLane;
import com.red.ohc.storage.NativeMemory;

public final class WriterResourceRegistryTest {
  @Test
  public void retiredResourceReturnsToTheHighWaterPoolWithANewGeneration() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    RetirementJournal retirement = new RetirementJournal(memory);
    WriterResourceRegistry registry =
        new WriterResourceRegistry(memory, lifecycle, retirement);
    try {
      WriterResource first = registry.acquire();
      long generation = first.generation();
      long version = first.registrationVersion();
      assertEquals(registry.activeCount(), 1);
      assertEquals(lifecycle.laneCount(), 1);
      assertEquals(retirement.laneCount(), 1);

      registry.requestRetirement(first);
      assertEquals(registry.processRetirements(version), 1);
      assertEquals(registry.activeCount(), 0);
      assertEquals(registry.retiringCount(), 0);
      assertEquals(registry.pooledCount(), 1);
      assertTrue(first.generation() > generation);

      WriterResource reused = registry.acquire();
      assertSame(reused, first);
      assertTrue(reused.registrationVersion() > version);
      assertEquals(registry.resourceCount(), 1);
      registry.requestRetirement(reused);
      assertEquals(registry.processRetirements(), 1);
    } finally {
      retirement.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorSnapshotDefersResourcesRegisteredAfterItsVersion() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    RetirementJournal retirement = new RetirementJournal(memory);
    WriterResourceRegistry registry =
        new WriterResourceRegistry(memory, lifecycle, retirement);
    try {
      WriterResource before = registry.acquire();
      long actorVersion = registry.captureRegistrationVersion();
      WriterResource after = registry.acquire();
      registry.requestRetirement(before);
      registry.requestRetirement(after);

      assertEquals(registry.processRetirements(actorVersion), 1);
      assertEquals(registry.retiringCount(), 1);
      assertEquals(registry.pooledCount(), 1);
      assertEquals(registry.processRetirements(), 1);
      assertEquals(registry.retiringCount(), 0);
      assertEquals(registry.pooledCount(), 2);
    } finally {
      retirement.close();
      memory.closeArenas();
    }
  }

  @Test
  public void actorResourceDrainHonorsThePerTurnMaximum() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    RetirementJournal retirement = new RetirementJournal(memory);
    WriterResourceRegistry registry =
        new WriterResourceRegistry(memory, lifecycle, retirement);
    try {
      WriterResource first = registry.acquire();
      WriterResource second = registry.acquire();
      registry.requestRetirement(first);
      registry.requestRetirement(second);

      long version = registry.captureRegistrationVersion();
      assertEquals(registry.processRetirements(version, 1), 1);
      assertEquals(registry.retiringCount(), 1);
      assertEquals(registry.pooledCount(), 1);

      assertEquals(registry.processRetirements(version, 1), 1);
      assertEquals(registry.retiringCount(), 0);
      assertEquals(registry.pooledCount(), 2);
    } finally {
      retirement.close();
      memory.closeArenas();
    }
  }

  @Test
  public void newRetirementDuringAPartialSweepIsVisitedBeforeTheActorBecomesIdle() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    RetirementJournal retirement = new RetirementJournal(memory);
    WriterResourceRegistry registry = new WriterResourceRegistry(memory, lifecycle, retirement);
    try {
      WriterResource first = registry.acquire();
      WriterResource second = registry.acquire();
      long firstSequence = first.lifecycleLane().reserve();
      long secondSequence = second.lifecycleLane().reserve();
      registry.requestRetirement(first);
      registry.requestRetirement(second);
      assertEquals(registry.processRetirements(), 0);
      assertFalse(registry.hasRetirementWork());
      assertEquals(registry.processRetirements(Long.MAX_VALUE, 1), 0);
      assertTrue(registry.hasRetirementWork());

      registry.requestRetirement(registry.acquire());
      for (int pass = 0; pass < 4 && registry.hasRetirementWork(); pass++) {
        registry.processRetirements(Long.MAX_VALUE, 1);
      }
      assertEquals(registry.pooledCount(), 1,
          "new requests appended behind previously visited blocked resources must also be scanned");
      assertEquals(registry.retiringCount(), 2);
      assertFalse(registry.hasRetirementWork());
      first.lifecycleLane().cancel(firstSequence);
      second.lifecycleLane().cancel(secondSequence);
      WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
      assertTrue(first.lifecycleLane().poll(record));
      first.lifecycleLane().release(record);
      assertTrue(second.lifecycleLane().poll(record));
      second.lifecycleLane().release(record);
      assertEquals(registry.processRetirements(), 2);
    } finally {
      retirement.close();
      memory.closeArenas();
    }
  }

  @Test
  public void resourceIsNotPooledUntilItsCapturedLifecycleWatermarkIsConsumed() {
    NativeMemory.Memory memory = new NativeMemory.Memory();
    WriterLifecycleJournal lifecycle = new WriterLifecycleJournal();
    RetirementJournal retirement = new RetirementJournal(memory);
    WriterResourceRegistry registry =
        new WriterResourceRegistry(memory, lifecycle, retirement);
    try {
      WriterResource resource = registry.acquire();
      long sequence = resource.lifecycleLane().reserve();
      registry.requestRetirement(resource);

      assertEquals(registry.processRetirements(), 0);
      assertEquals(registry.retiringCount(), 1);
      resource.lifecycleLane().cancel(sequence);
      WriterLifecycleLane.Record record = new WriterLifecycleLane.Record();
      assertTrue(resource.lifecycleLane().poll(record));
      resource.lifecycleLane().release(record);

      assertEquals(registry.processRetirements(), 1);
      assertEquals(registry.retiringCount(), 0);
      assertEquals(registry.pooledCount(), 1);
    } finally {
      retirement.close();
      memory.closeArenas();
    }
  }
}
