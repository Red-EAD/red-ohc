# Documentation

Start with the [quick start](quickstart.md).
It runs the standalone program in [`examples/user-cache`](../examples/user-cache).
The library's application surface is `com.red.ohc.api` and `OHCacheBuilder`.

## Application development

- [Quick start](quickstart.md): prerequisites, dependency installation, complete source and expected output.
- [Usage guide](usage.md): configuration, serialization, TTL, cache loading, conditional operations, bulk and direct access.
- [API reference](api.md): every public operation group, builder defaults, buffer ownership and failure contracts.
- [Operations guide](operations.md): sizing, statistics, maintenance backlog and troubleshooting.
- [Support and verification](support.md): platform targets and evidence required for a release.

## Implementation and contribution

- [Architecture](architecture/ARCHITECTURE.md): boundaries, components, data ownership, publication, TTL, flush and reclamation.
- [Architecture decision](architecture/adr/0001-synchronous-data-and-actor-maintenance.md): the implemented synchronous data path and actor-owned maintenance, including costs.
- [Benchmark protocol](benchmarks.md): the six-framework comparison and separate internal diagnostics.
- [Contributing](../CONTRIBUTING.md): source layout, build and validation workflow.
- [Release procedure](releasing.md): versions, signatures, bundle checks and publication.
- [Security policy](../SECURITY.md), [code of conduct](../CODE_OF_CONDUCT.md) and [maintainers](../MAINTAINERS.md): community contacts and responsibilities.

The English usage, API and operations pages own the detailed application contract.
Implementation files linked from the architecture guide provide the source trail;
Java visibility alone does not make those classes supported application APIs.
