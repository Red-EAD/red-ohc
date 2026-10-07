# Contributing

Use a focused issue or pull request to explain the problem and observable behavior.
Follow the organization's existing contribution agreement. No additional DCO or
CLA flow is introduced by this repository.

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
