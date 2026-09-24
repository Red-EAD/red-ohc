package com.red.ohc.cache;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.locks.ReentrantLock;

import org.testng.annotations.Test;

import com.red.ohc.maintenance.LogicalAdmission;

/** Structural contracts for the lock-free data-plane admission path. */
public final class LockFreeAdmissionArchitectureTest {
  @Test
  public void cacheUsesNoReentrantLocksOnTheLifecyclePath() {
    for (Field field : OffHeapCache.class.getDeclaredFields()) {
      assertFalse(
          ReentrantLock.class.isAssignableFrom(field.getType()),
          "close is quiesced-only; the lifecycle path must stay lock-free: " + field.getName());
    }
  }

  @Test
  public void cacheUsesTheAtomicLogicalAdmissionLedger() {
    boolean foundAdmission = false;
    for (Field field : OffHeapCache.class.getDeclaredFields()) {
      foundAdmission |= field.getType() == LogicalAdmission.class;
      assertFalse(
          field.getType().getName().contains("WriterDebtAdmission"),
          "the writer must not own a reclaim-debt admission controller");
    }
    assertTrue(foundAdmission);
  }

  @Test
  public void oldAdmissionPolicyAndTransactionTypesAreGone() throws Exception {
    assertFalse(classExists("com.red.ohc.maintenance.NativeAdmissionPolicy"));
    assertFalse(classExists("com.red.ohc.runtime.ThreadContext$AdmissionScratch"));
    for (Class<?> nested : LogicalAdmission.class.getDeclaredClasses()) {
      assertFalse(nested.getSimpleName().equals("VictimHandler"));
    }
  }

  @Test
  public void maintenanceHasNoRetirementPressureWatermarkTrigger() {
    for (Method method : com.red.ohc.maintenance.MaintenanceEventLoop.class.getDeclaredMethods()) {
      assertFalse(method.getName().equals("writerRetirementPressureWorkDue"));
    }
  }

  private static boolean classExists(String name) {
    try {
      Class.forName(name, false, LockFreeAdmissionArchitectureTest.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException missing) {
      return false;
    }
  }
}
