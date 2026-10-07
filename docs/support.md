# Support and verification

Target runtime: Java 11, 17, 21 and 25 on Linux, macOS and Windows. Native mapping
implementations exist for each OS; the fallback allocator is part of the current
implementation. CPU architecture, JVM access rules and native library availability
must be tested on the actual target.

Before the first release, run the complete CI matrix, all core/JMH tests and
the JCStress litmus suite. The repository includes fourteen JCStress tests.
The default sanity run is a local smoke check, not sufficient long-term stress:

```bash
bash scripts/run_jcstress.sh
```

Record JDK vendor/version, OS/architecture, actual JCStress options, test counts,
failures and duration in release evidence. Benchmark results require their own
matched Linux x86-64 runs. A compile, a macOS smoke test, Checkstyle and a static
review each establish different facts.

No full cross-platform test matrix, long stress test or Linux performance result
is claimed by these preparation documents. Each release must attach its actual
validation manifest before this support target is presented as verified support.
