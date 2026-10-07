# Red OHC

[中文](README.zh-CN.md) · [API guide](docs/api.md) · [Benchmarks](docs/benchmarks.md)

Red OHC is a serialized off-heap cache for Java 11 and later. A JDK
`ConcurrentHashMap` owns the index; keys and values live in native memory.
Caller threads publish lifecycle changes to per-thread lanes. One maintenance
actor applies eviction, expiry and deferred reclamation using reader quiescence.

This is a source candidate for the first public release, **1.0.0**.
The development version is `1.0.0-SNAPSHOT`. No Maven Central release is available
yet. Release availability and measured platform support will be recorded in
GitHub releases after validation.

## Using the released library

After 1.0.0 is published to Maven Central, use the following dependency; no custom
repository is needed:

```xml
<dependency>
  <groupId>io.github.red-ead</groupId>
  <artifactId>red-ohc-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

Create one cache at application startup and share it:

```java
OHCache<String, String> users = OHCacheBuilder.<String, String>newBuilder()
    .capacity(128L * 1024 * 1024)
    .keySerializer(new StringSerializer())
    .valueSerializer(new StringSerializer())
    .build();
users.put("user:42", "Alice");
String name = users.get("user:42");
users.put("user:42", "Bob");
users.remove("user:42");
```

Imports and the complete UTF-8 serializer are in the runnable
[user-cache example](examples/user-cache/README.md).

## Cache contract

- Serialized key bytes define identity. Serializers must be deterministic and
  thread-safe, and their reported size must match the bytes written.
- `capacity` and `maxSize` are asynchronous policy targets. They do not impose a
  strict bound on process RSS or instantaneous native allocation.
- Eviction policies include LRU and S3-FIFO. TTL expiration is maintained in the
  background; an expired entry is logically absent on reads.
- `flushAsync()` waits for captured lifecycle and retirement watermarks. It is
  useful for maintenance convergence and tests; it is not durable persistence or
  a global transaction barrier. Later writes do not indefinitely extend a flush.
- Caches have process lifetime. There is no public `close`, `stop` or `destroy`.
  Reuse caches; do not create one per request. `clear` and `flush` do not release
  the entire cache infrastructure. Tests and tools use internal teardown helpers.
- Direct-read buffers are valid only inside their callback. Do not retain them.
  See [API semantics](docs/api.md) for failure and concurrency boundaries.

## Build and validation

The checked-in Maven Wrapper downloads Maven 3.9.16 and verifies its SHA-256.
The core is the default build; benchmark dependencies are opt-in.

```bash
./mvnw -B clean verify
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am clean verify
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am checkstyle:check
git diff --check
```

Use `-s config/maven/settings.xml -gs config/maven/settings.xml` to build with
only public Maven configuration. CI performs the full validation matrix.
Native backends implement Linux/macOS mappings and Windows VirtualAlloc;
implementation coverage does not establish platform certification. See
[support and verification](docs/support.md).

## Contributing and releases

Read [CONTRIBUTING](CONTRIBUTING.md), [SECURITY](SECURITY.md) and
[release procedures](docs/releasing.md). Benchmarks compare ordinary get/put
APIs for Red OHC, [snazy/OHC](https://github.com/snazy/ohc), Ehcache, MapDB,
Chronicle Map and Redis. Workload and memory contracts are reported explicitly;
there are no published performance claims without reproducible measurements.

Licensed under [Apache License 2.0](LICENSE.txt). Retained third-party code and
notices are documented in [NOTICE](NOTICE).
