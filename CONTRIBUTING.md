# Contributing

Use a focused issue or pull request to explain the problem and observable behavior.
Follow the organization's existing contribution agreement. No additional DCO or
CLA flow is introduced by this repository.

Read the [architecture guide](docs/architecture/ARCHITECTURE.md) before changing
publication, reader protection or allocation. The [documentation index](docs/README.md)
separates application guides from implementation and maintainer procedures.

Build with Java 11 and the Maven Wrapper. Preserve the CHM authority, reader
protection and native ownership protocols unless an architectural proposal has
been agreed. Add regression tests for correctness fixes. Do not introduce legacy
compatibility layers or replace a working path with an unfinished abstraction.

Run the relevant tests and the final style checks:

```bash
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am verify
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am checkstyle:check
git diff --check
```

Test/tool caches must be released in `finally`, even if flush/setup fails. Keep
the primary failure and suppress cleanup failures. Long tests and benchmarks
should run on a dedicated machine, not implicitly in developer smoke commands.

Use accurate commit subjects such as `fix(cache): preserve replacement ownership`.
Explain the behavior and validation actually performed. Keep performance claims
with raw results, exact JVM/workload settings and process CPU/RSS measurements.
Never include credentials, private paths, internal endpoints or unverified claims.

## Source layout and documentation

- `red-ohc-core/src/main/java/com/red/ohc/api` defines the application API.
- `cache` implements operations; `codec` and `index` define lookup and entry state.
- `runtime` holds per-thread resources and reader protection.
- `maintenance` owns lifecycle transport, policies, TTL, flush and retirement.
- `storage` owns native allocation, size classes and allocator pages.
- `red-ohc-jmh` and `tools/benchmark-runtime` contain opt-in benchmark tooling.
- `examples/user-cache` is a standalone consumer, outside the core reactor.

Update the guide that owns changed behavior in the same PR. Keep the root English
and Chinese README aligned; write all other documentation in English. Validate
relative links and compile/run example changes. Public docs describe behavior and actual
availability; private export mappings and approval records remain outside the repo.

To build with only the repository's public Maven settings, add
`-s config/maven/settings.xml -gs config/maven/settings.xml` to root Wrapper commands.

Layout tests use in-process field offsets. The core test JVM disables JOL dynamic
attach and Serviceability Agent attach so CI does not need debugger privileges or
spawn an external attach process. These settings do not skip the layout assertions.

Core Surefire appends `-DargLine` to its required module access option. CI checks
that requested `-XX` layout flags actually reach the forked test JVM, then checks
field isolation and minimum Entry size using that JVM's header and alignment.
The existing OS/JDK jobs also test 16-byte alignment, uncompressed pointers and
their combination. JDK 25 runs core tests with compact object headers enabled.
These are test configurations; production code does not select an implementation
by JDK version. See [paired JDK measurements](docs/benchmarks.md#paired-revisions-across-jdks)
for the separate Linux performance protocol.
