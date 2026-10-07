# API reference

[Documentation](README.md) · [Usage recipes](usage.md) · [Architecture](architecture/ARCHITECTURE.md)

The supported application surface is [`com.red.ohc.api`](../red-ohc-core/src/main/java/com/red/ohc/api)
and [`OHCacheBuilder`](../red-ohc-core/src/main/java/com/red/ohc/cache/OHCacheBuilder.java).
Implementation classes, native structures and test teardown helpers are internal,
even when Java visibility is public. `OHCache` is its own interface and does not
extend `Map`, `ConcurrentMap` or `AutoCloseable`.

## Builder configuration

Usage: call `OHCacheBuilder.<K, V>newBuilder()`, set one target and both serializers,
then call `build()`. Cache instances are shared for the process lifetime.

| Method | Default | Contract |
| --- | --- | --- |
| `capacity(long)` | Required unless `maxSize` is set | Positive logical serialized-entry byte target; mutually exclusive with `maxSize` |
| `maxSize(long)` | Required unless `capacity` is set | Positive entry-count target; mutually exclusive with `capacity` |
| `keySerializer(CacheSerializer<K>)` | Required | Non-null deterministic, thread-safe key serializer |
| `valueSerializer(CacheSerializer<V>)` | Required | Non-null thread-safe value serializer |
| `defaultTTLmillis(long)` | `0` | Non-negative duration; zero gives permanent ordinary writes |
| `eviction(Eviction)` | `S3_FIFO` | `S3_FIFO`, `LRU` or `W_TINY_LFU`; non-null |
| `ticker(Ticker)` | `Ticker.DEFAULT` | Non-null elapsed and wall-clock source |
| `evictionListener(EvictionListener<K,V>)` | None | Optional non-null callback for automatic SIZE/EXPIRED removals |
| `nativeMemoryBudgetBytes(long)` | Larger of 64 MiB and byte capacity; 64 MiB in count mode | Positive retirement-debt diagnostic budget; does not throttle or reject writes |
| `build()` | No implicit target or serializers | Validates requirements, creates the cache and starts its actor |

Byte capacity includes fixed entry metadata, key/value headers and logical
alignment; it excludes physical page/size-class slack and shared structures.
Both target modes are asynchronous. A cache can exceed its target while the actor
catches up and can exhaust native memory if production outpaces maintenance.
`capacity()` returns `-1` for a count-target cache.

## OHCache operation groups

This table covers the methods declared by [`OHCache`](../red-ohc-core/src/main/java/com/red/ohc/api/OHCache.java),
including overloads. Expired entries are logically absent for reads, conditional
operations and traversal.

| Operations | Result | Behavior boundary |
| --- | --- | --- |
| `put(key,value)`, `put(key,value,expireAtMillis)` | `void` | Synchronously publish bytes; no previous-value materialization or continued-residency guarantee |
| `putIfAbsent` with or without expiry | Previous live value or null | Insert only if absent or expired |
| `replace(key,value)` | Previous live value or null | Replace only a live mapping |
| `replace(key,expected,value)` with or without expiry | Boolean | Match deserialized current value using `Objects.equals` |
| `remove(key)` | `void` | Missing key is a no-op; no previous-value materialization |
| `remove(key,value)` | Boolean | Match deserialized value using `Objects.equals` |
| `get`, `getOrDefault`, `containsKey`, `containsValue` | Snapshot/default/boolean | Ordinary values are owned deserialized objects; `containsValue` traverses mappings |
| `computeIfAbsent`, `computeIfPresent`, `compute`, `merge` | Computed value or null | Synchronous caller functions; retries may repeat user code; same-cache reentrant writes rejected |
| `putAll`, `getAll`, `removeAll` | `void`, hit map, removed count | Individual operations; partial completion possible; no multi-key atomicity |
| `getDirect`, `getDirectAll` | Boolean, unique-hit count | Caller-thread callback-scoped native views; no value deserialization |
| `keySet`, `values`, `entrySet` | Backed views | Weakly consistent traversal; materialized keys/values; logical size |
| `forEach`, `replaceAll` | `void` | Synchronous callbacks over weakly consistent traversal |
| `clear` | `void` | Remove mappings weakly consistently; retain cache infrastructure |
| `size`, `isEmpty` | Logical live count as int, boolean | Weakly consistent; unobserved clock transitions may lag |
| `mappingCount` | Physical CHM count as long | Can include expired mappings awaiting physical unlink |
| `equals`, `hashCode`, `toString` | Map-style comparison/hash/text | Deserialized weakly consistent observations; no object-identity preservation |
| `capacity`, `totalAllocatedBytes`, `stats` | Target, physical bytes, stats | Separate logical, physical and diagnostic observations |
| `flushAsync` | `CompletableFuture<Void>` | Maintenance watermark completion, described below |

Null keys, null stored values and invalid sizes are rejected; a compute function may return
null for its method's no-insertion/removal semantics. Serialized key bytes define
identity. Conditional value comparisons follow Java equality on returned
snapshots, including identity equality for arrays.

Bulk operations, iteration and stats are weakly consistent. Cross-key work has no
transactional snapshot. Read results can be evicted immediately after observation,
and asynchronous policy maintenance does not make the cache a durable store.

## TTL and clocks

Explicit `expireAtMillis` overloads use absolute wall-clock milliseconds.
Non-positive explicit expiry creates a permanent entry. Other writes use the
configured default duration; reads do not refresh expiry.

[`Ticker`](../red-ohc-core/src/main/java/com/red/ohc/api/Ticker.java) supplies
`nanos()` for monotonic elapsed time and `currentTimeMillis()` for wall time.
Positive absolute expiries are converted into monotonic deadlines on the write
path. A later wall-clock adjustment does not change an existing deadline.

## Serialization and direct views

[`CacheSerializer`](../red-ohc-core/src/main/java/com/red/ohc/api/CacheSerializer.java)
provides `serializedSize`, `serialize` and `deserialize`. Use relative writes;
final position must equal the declared size. Buffers begin at position zero with a limit equal to the declared size and use
big-endian order. Value buffer capacity also equals that size; reusable key
scratch may have a larger capacity.

Value serialization uses direct non-array buffers. Deserialization uses borrowed
read-only native buffers and must return owned non-null values. Neither serializers
nor consumers may retain a buffer, slice, duplicate or address after the callback.
Key serialization may use reusable heap lookup scratch; `array()` is not a supported assumption.

[`ValueView`](../red-ohc-core/src/main/java/com/red/ohc/api/ValueView.java) exposes
`length`, `getByte`, big-endian `getLong`, `copyTo` and `asReadOnlyByteBuffer`.
All methods and derived views are valid only inside the current direct callback.
[`DirectValueConsumer`](../red-ohc-core/src/main/java/com/red/ohc/api/DirectValueConsumer.java)
and [`DirectEntryConsumer`](../red-ohc-core/src/main/java/com/red/ohc/api/DirectEntryConsumer.java)
run synchronously on the caller. Owned copies/results can outlive the callback.

## Automatic eviction callbacks

[`EvictionListener`](../red-ohc-core/src/main/java/com/red/ohc/api/EvictionListener.java)
receives lazy key/value suppliers and [`RemovalCause`](../red-ohc-core/src/main/java/com/red/ohc/api/RemovalCause.java)
`SIZE` or `EXPIRED` on the actor after a successful automatic removal.
Suppliers deserialize at most once and expire on return. Listener exceptions are
swallowed to preserve retirement; callers should keep listeners short and report
errors within their own callback. The listener is not a general explicit-removal hook.

## Flush completion

`flushAsync()` captures existing lifecycle/retirement lane reservation watermarks.
Reservations captured by the request must commit/cancel and be consumed. Later
reservations and newly created lifecycle lanes do not expand that capture.
The actor extends the retirement boundary for required lifecycle, TTL and capacity work.

Completion includes the capacity target for occupancy applied through the captured
lifecycle boundary. Concurrent later writes can leave later occupancy over target.
The actor also checks reader lifecycle, access work and due TTL before completing.
Protected readers can delay physical reclaim and completion.

Concurrent flush callers can extend a merged pending request to newer watermarks.
Each caller has an independent waiter even though actor completion is shared.
Cancellation or timeout does not cancel maintenance or other callers' waiters.

Flush does not persist bytes, stop writers, provide a transaction, or establish
completion of unrelated tasks. Do not synchronously wait for flush from protected
read/compute callbacks. Actor-thread invocation is rejected. Use a timeout at the
application boundary; it bounds that wait only.

## Failure and lifetime

[`CacheMaintenanceException`](../red-ohc-core/src/main/java/com/red/ohc/api/CacheMaintenanceException.java)
reports terminal maintenance failure for writes/flush. Native allocation failure
can make the cache terminal. Waiting writers are notified; the actor does not
silently restart. Reads have no general health guarantee; inspect maintenance
health rather than treating a successful hit as recovery.

Serializer and user-function exceptions propagate to the operation's caller.
A failure before publication rolls back owned private state; failure after
publication does not prove that the operation had no effect. Bulk callers must
account for entries processed before failure.

The public cache has no shutdown method. The actor is a daemon, but abandoning a
cache does not establish prompt cleanup during a running process. `clear` and
flush leave its infrastructure alive. Test/tool shutdown helpers require quiesced
callers and remain internal.

## Statistics

[`OHCacheStats`](../red-ohc-core/src/main/java/com/red/ohc/api/OHCacheStats.java)
contains weakly consistent observations. The [operations guide](operations.md)
explains the metric groups, their units and diagnostic use. `size`, `liveWeight`,
`nativeAllocatedBytes` and process RSS answer different questions.

The first stable version is 1.0.0. Future incompatible application API changes
require a major version. This policy does not stabilize implementation details,
native layout or benchmark tooling.
