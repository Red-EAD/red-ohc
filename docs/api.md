# Public API

The supported application surface is `com.red.ohc.api` together with
`com.red.ohc.cache.OHCacheBuilder`. Implementation classes, native structures
and test teardown helpers are internal even when Java visibility is public.

`OHCache<K,V>` provides serialized get/put/remove, conditional mutation, bulk
operations, TTL, statistics and maintenance flush. Operations are concurrent;
bulk operations, iteration and statistics are weakly consistent, not atomic
snapshots. A void `put` does not prove that an entry will remain admitted.

Configure capacity in logical serialized bytes and optionally `maxSize` in
entries. Logical accounting includes serialized storage overhead, while page
rounding, fragmentation, retired values and in-flight writes affect physical
usage separately. The native-memory budget is diagnostic and not a hard RSS cap.

Implement `CacheSerializer<T>` with `serializedSize`, `serialize` and
`deserialize`. Buffer positions and limits define the usable bytes. Keys use
serialized byte identity rather than object identity. Never mutate a key in a way
that changes its serialization between operations. Nulls and invalid sizes are
rejected by the API. Exceptions from serializers propagate to callers.

Keep a cache for the lifetime of the application. The API deliberately has no
shutdown method. The maintenance actor is a daemon; this does not make repeated
creation safe for a long-running process. Native callback views must never escape
their callback, including through futures, fields or thread-local storage.

`flushAsync()` returns an independent caller future for a merged request.
The request captures existing lifecycle/retirement lane reservation watermarks.
Pre-capture reservations must commit/cancel and be processed. Later reservations
and new lanes do not expand that boundary. Actor-generated retirements required
by captured mutations, TTL and capacity maintenance extend the retirement
boundary. Active readers can delay physical reclamation and flush completion.
Use a timeout at the application boundary if a reader can remain active.

Flush does not stop writers, persist data, or replace a caller's own future/latch
for task completion. Reentrant writes from a protected read callback are rejected.
After terminal maintenance failure, writes and flush fail; the cache does not
silently recover into a new actor. Handle the failure at the application level.

The first stable release starts at 1.0.0. Future incompatible application API
changes require a major version. This policy does not stabilize implementation
details or benchmark tooling.
