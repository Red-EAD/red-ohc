# Synchronous publication with actor-owned maintenance

Status: records the implemented design. This document does not claim a historical
approval date or introduce an architecture change.

## Context

Red OHC serves concurrent serialized cache operations while controlling native
storage ownership. Callers need published values without waiting for policy repair,
and obsolete native blocks must remain reachable until protected readers finish.
The JDK CHM is the authoritative index.

## Decision

Publish mapping/value changes on caller threads. Send lifecycle hints through
exclusive producer lanes and keep reader access sampling separate. Give one actor
ownership of policy links, TTL scheduling and safe native reclamation. Use a
separate retirement journal and reader operation sequences for memory safety.

Expose flush as captured maintenance watermarks. Keep caches for process lifetime
with internal shutdown only for quiesced tests/tools. The existing code and source
links in [architecture](../ARCHITECTURE.md) establish this design.

## Consequences

Callers do not wait for the actor to apply every successful write, and producer
lanes distribute publication. Policy mutation and physical free have one owner.
The cache still pays CHM access, serialization, native allocation and coordination costs.

Capacity is asynchronous, so live occupancy and native allocation can exceed the
target. Reader protection or a slow listener can increase retirement debt. A single
actor also bounds maintenance throughput; per-thread resources make producer count
part of memory sizing. Direct views require strict callback lifetimes.

The application must handle misses, budget physical headroom and observe
maintenance health. Linux performance measurements are required before any
throughput conclusion. This record stabilizes no internal Java API or native layout.
