# Operations guide

[Documentation](README.md) · [API reference](api.md) · [Architecture](architecture/ARCHITECTURE.md)

This guide interprets [`OHCacheStats`](../red-ohc-core/src/main/java/com/red/ohc/api/OHCacheStats.java)
and the allocator/maintenance behavior in `red-ohc-core`. Use a bounded number
of shared process-lifetime caches and monitor the JVM as well as native storage.

## Memory accounting

| Observation | Unit and scope | Use |
| --- | --- | --- |
| `capacity()` | Logical serialized-entry bytes; `-1` in count mode | Configured actor target, not a hard native/RSS limit |
| `size()` / stats `size()` | Logical live entries | Application occupancy; weakly consistent |
| `mappingCount()` | Physical CHM mappings | Includes expired entries before unlink |
| stats `liveWeight()` | Actor-applied logical serialized-entry bytes | Can lag publication; includes logical key metadata and value headers |
| `totalAllocatedBytes()` / stats `nativeAllocatedBytes()` | Physical cache-owned native allocation | Includes pages, shared structures and storage waiting for reclamation |
| Process RSS | Resident pages of the entire process | Includes JVM heap, native libraries, stacks and unrelated allocations |

Logical charge and physical layout are defined in
[architecture](architecture/ARCHITECTURE.md#memory-layout-and-allocation).
Capacity does not include all physical overhead. Freed slots can remain in an
allocated page, pooled empty pages can remain owned, and retired values can remain
protected. A stable live count therefore does not imply a stable RSS ceiling.

Choose the target using the actual key/value size distribution and producer count.
Leave measured headroom for retirement backlog, partially used pages and JVM
memory. Entry-count targets do not bound the bytes of a large value. Repeated
cache creation and short-lived producer threads can also affect resource retention.
There is no universal target-to-RSS multiplier supplied by the cache.

`nativeMemoryBudgetBytes` observes retirement debt. Its default is described in
[builder configuration](api.md#builder-configuration). Setting it does not impose
backpressure, reject writes, cap RSS or force writers to reclaim memory.

## Observe maintenance

Poll `stats()` at an application-appropriate interval and compare successive
snapshots. Counters and rates describe different windows; snapshots do not form
an atomic cross-field transaction. Reader counters can lag collection by the actor.

```java
com.red.ohc.api.OHCacheStats stats = users.stats();
System.out.println("live=" + users.size()
    + ", native=" + users.totalAllocatedBytes()
    + ", lifecycleLag=" + stats.lifecycleJournalLagRecords()
    + ", retirementLag=" + stats.retirementLagRecords()
    + ", unhealthy=" + stats.maintenanceUnhealthy());
```

| Metric group | Representative getters | Interpretation |
| --- | --- | --- |
| Requests and residency | `hitCount`, `missCount`, `requestCount`, `hitRate`, `size`, `liveWeight` | Logical demand and actor-applied occupancy; counters are collected asynchronously |
| Automatic removals | `evictionCount`, `evictionWeight`, `expirationCount`, residence sample getters | Eviction weight is logical bytes; residence is sampled, not an all-entry duration |
| Lifecycle publication | `lifecycleJournalPublishedRecords`, `lifecycleJournalCompletedRecords`, `lifecycleJournalLagRecords`, `lifecycleJournalHeadOfLineStopCount` | Reliable lifecycle progress; reserved unpublished heads can stop a lane |
| TTL | `ttlLagMillis`, `ttlBacklog` | Lag and scheduled expiry work; backlog includes scheduled future entries |
| Unsafe retirement | `retirementUnsafeRecords`, `retirementUnsafeBytes`, `activeReaderCount`, `retirementReclaimBlockedCount` | Work that can be held behind readers; active-reader count alone does not prove a spanning blocker |
| Safe retirement | `retirementSafeRecords`, `retirementSafeBytes`, `retirementSafeSegmentCount`, `retirementOldestSafeWaitNanos` | Work already safe but awaiting actor release |
| Retirement throughput | `retirementGeneratedBytesPerSecond`, `retirementCompletedBytesPerSecond`, `retirementLagRecords` | Persistent generation above completion can grow memory debt |
| Debt budget | `nativeDebtBudgetBytes`, `nativeDebtHeadroomBytes` | Diagnostic headroom, not enforced free process memory |
| Actor scheduling | `maintenanceActiveNanosTotal`, `maintenanceParkNanosTotal`, `maintenancePassCount`, `maintenanceWakeCount` | Cumulative actor activity and parking; use deltas and elapsed wall time |
| Access sampling | `accessRingDroppedCount` | Dropped policy observations; does not count failed cache reads |
| Allocation and resources | `allocatorPageOccupancyByClass`, `allocatorRetainedPagesCurrent`, `allocatorPooledPageCount`, writer resource getters | Fragmentation, retained pages and producer resource footprint |
| Health | `maintenanceUnhealthy`, `nativeAllocationFailureCount` | Terminal failure diagnostics; not automatically recoverable |

The getter list is representative; the class documents all counters and arrays.
Do not subtract counters with different units, such as segments and records.
Observed zero lifecycle lag does not prove all readers or retired allocations have cleared.

## Native allocation grows while size is stable

First compare physical native allocation with RSS to locate the growth.
Inspect retirement unsafe/safe bytes and generated/completed rates. Unsafe growth
suggests readers or unresolved retirement publication; safe growth suggests the
actor's physical-release service rate is behind generation.

Check long serializer/direct/compute callbacks, slow automatic-removal listeners
and sustained replacement churn. Then inspect per-class page occupancy, pooled
pages and writer resource counts. A value-size distribution spanning many classes
and many producers can retain partially filled pages.

Reduce the application work keeping readers protected, move blocking work outside
callbacks, and bound producer/cache proliferation. If generation remains above
maintenance capacity, reduce churn or cache demand and measure again. Increasing
the diagnostic debt budget does not fix the underlying service-rate mismatch.

## Flush waiting

A flush can wait for pre-capture reservations, lifecycle application, due TTL,
capacity work, reader lifecycle and protected native retirement. A later flush
caller can also extend a merged request. Check head-of-line stops, lifecycle lag,
unsafe retirement and callback duration instead of assuming an empty CHM is enough.

Use a timeout outside callbacks, for example
`users.flushAsync().get(5, TimeUnit.SECONDS)`. Timeout bounds the caller's wait;
maintenance continues. Waiting from a direct/compute callback can depend on that
callback's own exit. An actor listener must not call flush.

Use each asynchronous task's own future/latch for task completion. Flush is a cache
maintenance boundary and does not order unrelated executor tasks. See the
[full completion contract](api.md#flush-completion).

## Maintenance failure

Writes and flush fail after terminal maintenance failure; `CacheMaintenanceException`
retains its cause. Native exhaustion can initiate this state. Record the root cause,
cache statistics, workload and process memory before applying application recovery.
A read hit is not evidence that the actor restarted.

Ordinary pre-publication serializer exceptions differ from terminal failures.
Report both at their actual call boundary. Since the API has no shutdown/restart
operation, a process-lifetime service should define its recovery policy rather
than repeatedly replacing unhealthy caches within the same process.

## Throughput investigations

Compare ordinary materialized reads separately from direct or full-scan reads.
Serializer allocation, value size, hit rate, read/write ratio, producer count and
maintenance CPU can all change the result. The heap index also means the cache
retains JVM GC and pointer-access costs despite off-heap payload storage.

Use [benchmarks](benchmarks.md) on a dedicated target machine. Record Linux CPU
model, NUMA/affinity, JVM, capacity, TTL, size distribution, wall time, process CPU
and memory trends. Source-level locality mechanisms and macOS smoke runs do not
establish Linux x86-64 throughput improvements.
