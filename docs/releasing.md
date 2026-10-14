# Release procedure

Before publication, obtain organization approval, complete the namespace claim,
verify ownership/license notices and inspect the candidate's entire public
history. Private export mappings and review records must remain outside this repo.
The initial source history has an independent root. Once public, do not rewrite
public commits or move/delete a released tag.

Version policy: develop on `1.0.0-SNAPSHOT`, first stable version `1.0.0`, tag
`v1.0.0`. Commit the version change, validate that exact clean commit, then create
the tag. Record its SHA, JDK, dependency graphs, source/JAR checksums, test results
and reproducibility evidence. Never fabricate signatures or validation results.

## Development SNAPSHOTs

The `io.github.red-ead` namespace must be verified and its SNAPSHOT option enabled
in Central Portal. A maintainer-generated Central user token is stored as
`CENTRAL_USERNAME` and `CENTRAL_PASSWORD` in GitHub's `snapshot` environment.
That environment permits only the `main` branch. Maven reads the secrets through
`config/maven/publishing-settings.xml`, under server ID `central`; the file
contains environment references only. SNAPSHOT publishing requires no GPG key.

`Publish SNAPSHOT` runs after a successful `CI` main push or a manual dispatch on
main. Before upload, it confirms the current main SHA, the trusted CI workflow
and all 14 successful jobs: Linux/macOS/Windows with JDK 11/17/21/25, JCStress and
the runnable example. PR and fork runs cannot publish. The parent and core must
share public coordinates and a `-SNAPSHOT` version; stable versions are skipped.

Uploads run serially. CI cancellation and the publication queue can consolidate
fast consecutive merges. A superseded checkout is skipped, so the workflow does
not promise a separate published artifact for every intermediate merge. A failed
upload or verification is visible as a failed workflow; after correcting the
cause, dispatch `Publish SNAPSHOT` on main to retry with the same CI gate.

The publisher builds parent/core with JDK 11, skips already-validated Java tests,
and uses the existing Central plugin's SNAPSHOT support. It then resolves the
dependency anonymously using empty Maven settings and a new cache, comparing the
downloaded core JAR's SHA-256 against the built JAR. The workflow summary and logs
record source SHA, validated CI run, version and parent/core artifact checksums.
The benchmark module is excluded.

Consumers add the official repository
`https://central.sonatype.com/repository/maven-snapshots/` with SNAPSHOTs enabled;
see the [quick start](quickstart.md#add-the-dependency). This endpoint is separate
from the stable Maven Central repository. No download token is required.
See Sonatype's [SNAPSHOT documentation](https://central.sonatype.org/publish/publish-portal-snapshots/).

## Stable releases

The default build publishes nothing. The opt-in `release` profile attaches
sources/Javadoc and signs with the maintainer's existing GPG key. A local
rehearsal requires a clean committed stable version and a real signing key,
but needs no publishing token and invokes no Central publishing plugin:

```bash
./mvnw -B -Prelease clean verify
python3 scripts/package_release_bundle.py --output target/release-preview.zip
```

Inspect the local ZIP and artifacts. The packager verifies existing signatures
and includes parent/core artifacts, sources/Javadoc and required MD5/SHA-1 plus
SHA-256/SHA-512 checksums in Maven repository layout. It has no uploader.
This is a bundle rehearsal, not a Central validation or upload. See the official
[requirements](https://central.sonatype.org/publish/requirements/) and
[bundle layout](https://central.sonatype.org/publish/publish-portal-upload/).
Never turn publishing on before approval.
After approval, use protected `release` environment credentials (Central user
token and GPG passphrase/key) through Maven server id `central`; never commit them.
The separate `central-publish` profile enables Central's plugin with publishing
skipped by default and `autoPublish=false`. After approval only, enable upload
explicitly for the reviewed release:

```bash
./mvnw -B -Prelease,central-publish -Dcentral.skipPublishing=false deploy
```

Review the staged deployment in Central before publishing manually.

Only parent/core artifacts are published. JMH is enabled by `benchmarks` and is
explicitly excluded by artifact ID in Central's plugin as well as having ordinary
Maven deployment disabled; raw experiments and dependencies do not belong in Maven
Central. Publish benchmark source and reproducible result manifests separately
after approval and review.

Post-approval checklist: publish reviewed source; enable private security reports;
configure branch/tag protections with rules usable by the initial maintainer;
run Linux/macOS/Windows Java 11/17/21/25 CI; run all JCStress tests with recorded
parameters; run fair Linux x86-64 benchmark pairs; verify license/dependency scans;
perform signed reproducible bundle validation; publish Central 1.0.0; verify a
clean consumer can resolve it without custom repositories; attach release notes.
