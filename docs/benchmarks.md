# Benchmark protocol

The primary comparison is `FairSerializedBenchmark.operation`, with six isolated
runtime graphs: Red OHC, upstream snazy/OHC 0.7.4 (linked implementation),
Ehcache 3.10.8, MapDB 3.1.0 (direct-memory DB, 16 hash-map segments),
Chronicle Map 3.27ea1 and Redis via Jedis 5.0.2. Redis's installed server version
must also be recorded. Caffeine and engine-specific microbenchmarks remain
developer diagnostics, not additional members of this six-framework ranking.

All adapters use ordinary synchronous get/put APIs and full `byte[]` values.
They do not patch engine internals, preencode only one backend, simulate eviction
in Chronicle, or flush one cache on every operation. Keys and values are generated
before timing. Identical key-array instances are reused across lookups; this
matches Ehcache's native Java key equality/hash contract for arrays. This suite
does not claim equivalent behavior for freshly allocated equal array keys.

`MATERIALIZE` reads return the complete owned array and pass it to JMH's
Blackhole. Red OHC does not use the lightweight first-byte codec from its tuning
benchmark. `FULL_SCAN` additionally calculates the same checksum over every byte
for all adapters. Report the two consumption modes separately: scanning can
dominate cache lookup cost. There is no artificial extra copy on just one backend.

Default data: 32-byte keys, 5120-byte values, 24,576 keys, UNIFORM distribution.
Resident mode preloads and verifies every entry; if capacity is insufficient it
fails setup. Reads, replacement writes and 90:10 mixtures use the same precomputed
access sequence. ZIPF_099 and other key/value sizes form separate experiments.

The default resident group disables TTL. Native byte capacities are configured
for Red OHC, snazy/OHC, Ehcache and Redis; MapDB uses an entry expiry target and
Chronicle is sized by entry count. The default 256 MiB allows the resident data to
fit; it is **not an equal RSS budget**. Redis has server/allocator overhead and
network/client costs, and belongs in an end-to-end column rather than a local
lookup-only winner table. Its synchronous GET/SET use no pipeline.

The `PRESSURE` group preloads 19,660 entries and uses the larger key universe for
reads and finite-key churn writes (the prior MIXED shape). Explicitly configure
and report each engine's native capacity contract and steady resident count;
byte limits and MapDB's entry target are not interchangeable. Do not label a
run as eviction pressure unless the measurements confirm eviction actually
occurs. Chronicle's native TTL/eviction comparison is N/A and is excluded.
The `TTL` group requires positive TTL and reports expiry/misses separately;
never use it as the resident throughput ranking.

Red OHC's void put has no synchronous admission acknowledgement. Completed
operations mean the API returned; they do not prove durable admission. snazy/OHC's
false return is counted as a reported rejection. The other void APIs likewise
cannot establish admission from completion. Log attempted/completed, hits/misses,
writes, reported rejections and errors. Normal resident runs require reconciliation
and zero unexpected rejection/error. Residency and native/RSS trends must be
checked separately after repeated churn.

## Preparing and running

Use Python 3.9+ and the same JDK/JVM options, logical CPUs/affinity, workload,
capacity configuration and seed for all matched processes. On Linux, preserve
governor, NUMA placement, allocator and background-load information. Do not
compare backend graphs from the all-dependencies compiler/test classpath.

The runner and JMH tests share the module access options in the JMH POM,
following [OpenHFT's Java guidance](https://github.com/OpenHFT/OpenHFT/blob/ea/docs/Java-Version-Support.adoc).
The runner applies these same options to every backend, in addition to the
selected `--jvm-args`. Chronicle's JDK 21 round trip is tested rather than skipped.

```bash
python3 scripts/run_fair_benchmarks.py --output /path/to/results --prepare
python3 scripts/run_fair_benchmarks.py --output /path/to/results --run \
  --threads 1 8 32 --workloads READ_100 WRITE_100 READ_90_WRITE_10
```

Without `--run`, the tool only prints the plan (and builds if `--prepare` is
requested); it executes no benchmark. Formal runs are serial, five rounds with a
deterministic shuffled backend order, each process one fork, warmup 3 x 5s and
measurement 5 x 10s. Existing tuning methods such as `OHCSerializedBenchmark`
remain separate, selected explicitly by regex rather than included in this run.

Prepare from the exact clean commit to measure. The tool reads the current
project version, builds fresh artifacts, and writes a receipt binding the source
SHA, compiler JDK and JAR checksums. Formal execution rejects dirty source,
mismatched receipts or changed artifacts; rerun preparation after any commit.

The tool resolves each backend with `tools/benchmark-runtime/pom.xml`, records its
dependency tree/classpath, uses the thin benchmark JAR and adds only that backend's
runtime libraries. MapDB's published version ranges are pinned reproducibly in
both compilation and runtime graphs. It saves the commit, dirty status, JDK,
parameters, all JAR SHA-256 values, order, raw JMH JSON/logs, wall time, available
user/system child CPU and logical CPU count. Child CPU includes the fork and
owned Redis server; sampled Linux per-process CPU/RSS are lower bounds, not exact
final CPU totals. Normalized CPU is total CPU / wall / logical CPUs. Record
affinity and effective CPU availability separately when restricted.

Redis must be installed explicitly. The adapter starts its own loopback process
with persistence disabled, a temporary directory and a random password; it does
not accept an external host/port and never flushes or reconfigures an existing
server. Each raw run records the authenticated server's actual version, maxmemory
and owned PID without retaining its random password. Startup failures and Trial
teardown close all owned resources. Core
Invocation benchmarks retain their internal stop/join/native-accounting teardown.

Preparation does not establish benchmark correctness under long contention,
cross-platform native compatibility or Linux x86-64 performance. Run dedicated
Linux experiments after approval, retain all rounds, investigate stable regressions
and publish limitations alongside results. No comparative performance result is published yet.
