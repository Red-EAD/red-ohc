# Usage guide

[Documentation](README.md) · [Quick start](quickstart.md) · [API reference](api.md)

Application code uses `com.red.ohc.api` and
[`OHCacheBuilder`](../red-ohc-core/src/main/java/com/red/ohc/cache/OHCacheBuilder.java).
The snippets below assume a shared `OHCache<String, String> users` and the
`StringSerializer` from the [complete example](quickstart.md#complete-java-program).

## Capacity and eviction

Select either a logical byte target or an entry-count target. Calling both builder
methods fails. The builder requires a positive target and both serializers.

```java
OHCache<String, String> users = OHCacheBuilder.<String, String>newBuilder()
    .maxSize(100_000)
    .eviction(Eviction.S3_FIFO)
    .keySerializer(new StringSerializer())
    .valueSerializer(new StringSerializer())
    .build();
```

Import `com.red.ohc.api.Eviction`. The selectors are `S3_FIFO` (default), `LRU` and
`W_TINY_LFU`. Policy maintenance consumes sampled access observations, so choose
through hit-rate and workload measurements rather than assuming exact recency.
The S3 selector's implemented Small/Skip/Main behavior is described in the
[architecture guide](architecture/ARCHITECTURE.md#eviction-and-ttl).

Writes publish synchronously even when occupancy exceeds the target. Eviction
catches up asynchronously; a completed void `put` does not guarantee continued
residency. Size entry-count caches using the key/value size distribution, not
just the configured count. See [memory accounting](operations.md#memory-accounting).

## Serialization

Keys match by serialized bytes. Distinct Java objects with identical serialized
keys address the same mapping. Use deterministic, thread-safe serializers and
stable key formats. Values returned by ordinary `get` are deserialized snapshots;
mutating such an object does not update the cached bytes.

Each size must be non-negative and match the exact relative bytes written.
Serialization buffers start at position zero, use big-endian order and are bounded
to the declared size. Value buffers are direct; serializers must not assume
`array()` exists. Deserialization receives a read-only borrowed buffer and must
return a non-null owned object. Retaining any view or address violates its lifetime.

A UTF-8 length is a byte length; multibyte characters make character counts
incorrect. Schema changes also change the bytes used for identity and values.
Use the same formats across callers and invalidate/repopulate application data
when changing incompatible formats. [CacheSerializer](../red-ohc-core/src/main/java/com/red/ohc/api/CacheSerializer.java)
is the authoritative buffer contract.

## Expiration

Configure `defaultTTLmillis(60_000)` for ordinary writes with a one-minute TTL.
Its default is zero, which creates permanent entries. Explicit expiry overloads
take an **absolute epoch-millisecond timestamp**, not a duration:

```java
users.put("user:42", "Alice", System.currentTimeMillis() + 30_000);
users.put("user:permanent", "Service account", 0);
```

A non-positive explicit expiry creates a permanent entry even when a default TTL
exists. The cache converts a positive absolute timestamp to a monotonic deadline;
later wall-clock changes do not shift that existing deadline. Reads treat expired
entries as absent before the actor physically unlinks or frees them.

Updates without an explicit expiry use the configured default TTL. Expiration is
based on write deadlines; reads do not refresh TTL. Custom `Ticker` instances must
supply monotonic nanoseconds and wall-clock milliseconds as defined by
[Ticker](../red-ohc-core/src/main/java/com/red/ohc/api/Ticker.java).

## Loading and conditional updates

The ordinary get/load/put recipe in [quick start](quickstart.md#use-the-cache-in-an-application)
keeps slow database or network work outside cache callbacks, while allowing duplicate
loads on concurrent misses. `computeIfAbsent` offers cache-local per-key coordination:

```java
String name = users.computeIfAbsent("user:42", key -> "Alice");
boolean changed = users.replace("user:42", "Alice", "Bob");
String existing = users.putIfAbsent("user:42", "Carol");
boolean removed = users.remove("user:42", "Bob");
```

Compute functions run synchronously on the caller. Keep them short, avoid blocking
I/O, and reject dependencies on callback execution count: internal retries can
invoke user code again. Same-cache reentrant writes are rejected. Nested reads
are supported, but callbacks must not synchronously await that cache's flush.

`compute`, `computeIfPresent`, `merge` and `replaceAll` also accept user functions.
A null compute result means no insertion or removal as appropriate for the method.
`replaceAll` functions must return non-null replacements.
Conditional value matching uses `Objects.equals` on deserialized values. For
`byte[]`, that means array identity, so use a value type with content equality
when conditional content matching is required.

## Bulk operations and views

```java
users.putAll(Map.of("user:42", "Alice", "user:43", "Bob"));
Map<String, String> found = users.getAll(List.of("user:42", "user:43"));
int removed = users.removeAll(List.of("user:42", "user:43"));
```

Import `java.util.Map` and `java.util.List`. Bulk operations process individual
entries, are weakly consistent and can partially complete before failure.
`getAll` returns the hits only. Duplicate keys do not produce duplicate result
mappings, and direct bulk callbacks run once per unique hit.

`keySet`, `values` and `entrySet` provide backed weakly consistent views.
Traversal materializes snapshots and filters expired mappings. Concurrent writes
can change subsequent observations; iteration, equality and statistics do not
form a transaction or a consistent snapshot across keys.

## Direct reads

Use `getDirect` when code can consume serialized bytes inside a callback.
This avoids value deserialization; key encoding and other cache work still occur.
For the UTF-8 example, copying into an owned array is safe when bytes must survive:

```java
java.util.concurrent.atomic.AtomicReference<byte[]> owned =
    new java.util.concurrent.atomic.AtomicReference<>();
boolean hit = users.getDirect("user:42", view -> {
    byte[] bytes = new byte[view.length()];
    view.copyTo(bytes, 0);
    owned.set(bytes);
});
```

`ValueView`, its `ByteBuffer`, slices, duplicates and native addresses are valid
only during that callback on the calling thread. Retain owned bytes or a computed
result, never the borrowed view. `getLong` and the read-only buffer use big-endian
order. `getDirectAll` has the same lifetime for each callback separately.

A long callback keeps a reader protected and can delay reclamation. Reentrant
writes fail; synchronously waiting for flush from a protected callback can wait
on that callback's own reader exit. The [operations guide](operations.md#flush-waiting)
explains stalled completion.

## Automatic-removal listeners

```java
OHCacheBuilder.<String, String>newBuilder()
    .capacity(128L * 1024 * 1024)
    .keySerializer(new StringSerializer())
    .valueSerializer(new StringSerializer())
    .evictionListener((key, value, cause) -> {
        recordRemoval(cause); // Application metric; no blocking work
    });
```

The snippet configures a builder; call `build()` to create the cache. `recordRemoval`
is an application hook. The listener runs synchronously on the maintenance actor
for successful automatic `SIZE` or `EXPIRED` removal. Explicit remove/clear and
replacement do not provide a general removal-notification contract.

Key/value suppliers deserialize lazily and at most once, and expire when the
listener returns. Materialized owned objects may be handed to another thread.
Do not retain suppliers or perform blocking work on the actor. Listener exceptions
are swallowed so they cannot prevent native retirement; report failures inside
application listener code if needed.

## Maintenance flush

```java
users.flushAsync().get(5, java.util.concurrent.TimeUnit.SECONDS);
```

Run this outside cache callbacks, for a maintenance boundary in a tool or an
application operation that explicitly needs convergence. It includes captured
reservations and the actor retirements required by maintenance. Active readers
can delay completion. A timeout or cancellation affects the caller's waiter and
does not stop maintenance; handle exceptions at the calling boundary.

Ordinary successful writes are already published without flush. Flush does not
persist data, stop concurrent writers, guarantee later occupancy, or substitute
for another task's own future. The [API reference](api.md#flush-completion)
defines the exact watermark and concurrent-request semantics.
