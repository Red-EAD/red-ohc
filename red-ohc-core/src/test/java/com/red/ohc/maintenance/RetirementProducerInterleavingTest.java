package com.red.ohc.maintenance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.ClassType;
import com.sun.jdi.Method;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.ClassPrepareRequest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.red.ohc.storage.NativeMemory;

/** Uses JDK debugger breakpoints to control races without adding hooks to the producer hot path. */
public final class RetirementProducerInterleavingTest {
  @DataProvider
  public Object[][] operations() {
    return new Object[][] {{"reserve"}, {"commit"}, {"cancel"}};
  }

  @Test(dataProvider = "operations", timeOut = 30_000L)
  public void actorInterleavingPreservesOwnershipAndWakeup(String operation) throws Exception {
    ListeningConnector connector =
        Bootstrap.virtualMachineManager().listeningConnectors().stream()
            .filter(candidate -> candidate.name().equals("com.sun.jdi.SocketListen"))
            .findFirst()
            .orElseThrow();
    Map<String, Connector.Argument> arguments = connector.defaultArguments();
    arguments.get("localAddress").setValue("127.0.0.1");
    arguments.get("timeout").setValue("20000");
    String classpath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    String address = connector.startListening(arguments);
    Process process;
    VirtualMachine vm;
    try {
      process =
          new ProcessBuilder(
                  System.getProperty("java.home") + "/bin/java",
                  "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address,
                  "-cp",
                  classpath,
                  Probe.class.getName(),
                  operation)
              .start();
      try {
        vm = connector.accept(arguments);
      } catch (Throwable failure) {
        process.destroyForcibly();
        throw failure;
      }
    } finally {
      connector.stopListening(arguments);
    }
    int interleavings = 0;
    try {
      ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
      prepare.addClassFilter(RetirementSegment.class.getName());
      prepare.enable();
      boolean finished = false;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
      while (!finished && System.nanoTime() < deadline) {
        EventSet events = vm.eventQueue().remove(1_000L);
        if (events == null) {
          continue;
        }
        for (Event event : events) {
          if (event instanceof ClassPrepareEvent) {
            String methodName =
                operation.equals("reserve") ? "reservationState" : operation + "Reserved";
            for (Method method :
                ((ClassPrepareEvent) event).referenceType().methodsByName(methodName)) {
              BreakpointRequest breakpoint =
                  vm.eventRequestManager().createBreakpointRequest(method.location());
              breakpoint.enable();
            }
          } else if (event instanceof BreakpointEvent) {
            BreakpointEvent breakpoint = (BreakpointEvent) event;
            // reset/close also read reservationState; pause only the reservation fast path.
            if (operation.equals("reserve")
                && !breakpoint
                    .thread()
                    .frame(1)
                    .location()
                    .method()
                    .name()
                    .equals("tryReserveForLane")) {
              continue;
            }
            for (BreakpointRequest request : vm.eventRequestManager().breakpointRequests()) {
              request.disable();
            }
            ClassType probe = (ClassType) vm.classesByName(Probe.class.getName()).get(0);
            probe.invokeMethod(
                breakpoint.thread(),
                probe.methodsByName("interleaveActor").get(0),
                Collections.emptyList(),
                ClassType.INVOKE_SINGLE_THREADED);
            interleavings++;
          } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
            finished = true;
          }
        }
        if (!finished) {
          events.resume();
        }
      }
      assertTrue(finished, "debuggee did not finish");
      assertTrue(process.waitFor(5L, TimeUnit.SECONDS));
      String errors = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(interleavings, 1, "the target race must be exercised exactly once");
      assertEquals(process.exitValue(), 0, errors);
    } finally {
      process.destroyForcibly();
      process.waitFor(5L, TimeUnit.SECONDS);
    }
  }

  /**
   * Separate JVM so the debugger can pause immediately before the actual state/publication read.
   */
  public static final class Probe {
    private static String operation;
    private static NativeMemory.Memory memory;
    private static RetirementJournal journal;
    private static RetirementSegment original;
    private static AtomicReference<RetirementSegment> owner;

    public static void main(String[] arguments) {
      operation = arguments[0];
      memory = new NativeMemory.Memory();
      if (operation.equals("reserve")) {
        original = new RetirementSegment(memory, RetirementSegment.CAPACITY, 0L, 1);
        owner = new AtomicReference<>(original);
        try {
          RetirementSegment.Reservation ticket = new RetirementSegment.Reservation();
          assertEquals(
              original.tryReserveForLane(1, owner, ticket),
              -1,
              "a delayed producer must not reserve a recycled segment owned by another lane");
          assertEquals(original.reservationCount(), 0);
        } finally {
          original.freePayload();
          memory.closeArenas();
        }
      } else {
        journal = new RetirementJournal(memory, 1);
        try {
          RetirementJournal.Lane lane = journal.lane(1);
          RetirementSegment.Reservation ticket = new RetirementSegment.Reservation();
          assertTrue(lane.reserve(ticket));
          lane.write(ticket, 0L, 0L);
          if (operation.equals("commit")) {
            lane.commit(ticket);
          } else {
            lane.cancel(ticket);
          }
          assertTrue(
              journal.hasRunnableWork(), "late completion must wake an actor that drained the cut");
          assertEquals(journal.sealReadySegments(), 1);
          journal.publishSafe(true, true);
          journal.reclaimActorResult(memory, Integer.MAX_VALUE);
          journal.finishReadyDrains();
          assertEquals(journal.completedRecordsTotal(), 1L);
          assertEquals(journal.workState(), 0);
        } finally {
          journal.close();
          memory.closeArenas();
        }
      }
    }

    public static void interleaveActor() {
      if (operation.equals("reserve")) {
        // The empty, unreserved descriptor can be retired and reused after the owner moves on.
        owner.set(null);
        original.closeForSnapshot();
        original.reset(0L, 2);
      } else {
        journal.cutAllProducersAtWatermark();
        assertEquals(journal.sealReadySegments(), 0);
        journal.finishReadyDrains();
        assertFalse(journal.hasWork(), "the actor must drain its signal before completion");
      }
    }
  }
}
