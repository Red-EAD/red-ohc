# Release procedure

Before publication, obtain organization approval, complete the namespace claim,
verify ownership/license notices and inspect the candidate's entire public
history. Private export mappings and review records must remain outside this repo.
The initial source history has an independent root. Once public, do not rewrite
public commits or move/delete a released tag.

Version policy: the first stable version is `1.0.0`, with tag `v1.0.0`.
Development versions use a `-SNAPSHOT` suffix. Commit the version change,
validate that exact clean commit, then create
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

Stable publishing is manually dispatched on `main` through **Publish release**.
It requires an existing `vMAJOR.MINOR.PATCH` tag, matching public parent/core POM
versions and a successful `CI` main-push run on that exact commit. All 14 jobs
must pass; pending, missing, skipped, duplicate or unsuccessful jobs stop the run.
The tagged commit must belong to protected main history. A tag push alone never
publishes a stable artifact.
The candidate records the successful CI attempt and reads jobs from that attempt.
A rerun during inspection or a changed attempt after approval stops publication
and requires a fresh candidate and approval.

### Signing and environment setup

Configure the `release` environment with only `main` permitted, required reviewer
`heliang666s`, self-review allowed and administrator bypass disabled. Only that
maintainer approves the protected job. The candidate job exposes its tag, commit
and successful CI URL before approval; the job that can sign and upload starts
only after this explicit approval.

Store these four encrypted environment secrets, never their values in source,
logs, command arguments or chat:

| Secret | Value |
| --- | --- |
| `CENTRAL_USERNAME` | Central user token username for `io.github.red-ead` |
| `CENTRAL_PASSWORD` | Matching Central user token password |
| `GPG_PRIVATE_KEY` | Armored, passphrase-protected primary signing key export |
| `GPG_PASSPHRASE` | The password set when creating that signing key |

Set environment variable `GPG_FINGERPRINT` to the complete uppercase primary
fingerprint. A fingerprint identifies a public key; it is not a password.
Distribute the public key through `keyserver.ubuntu.com` and keep the private key
and revocation certificate backed up outside the repository. Renew the key before
expiry; configure and verify the new fingerprint before using it for a release.
See [Sonatype's signing requirements](https://central.sonatype.org/publish/requirements/gpg/).

Run **Release readiness** on main after CI succeeds, then approve its `release`
job yourself. It imports the configured private key, signs a harmless probe and
verifies it using a fresh keyring downloaded from the public keyserver. This
checks the real passphrase, primary fingerprint and public distribution. It
performs no Maven upload, creates no tag or GitHub release, and replaces the old
remote bundle-preview workflow. Secret names alone do not prove readiness.

### Publish an approved version

1. Prepare a release PR setting the parent, both module parent versions, example
   and benchmark runtime coordinates to the stable version, starting with `1.0.0`.
   Review the changes and prepare the English release notes for the annotated tag.
   Merge through the protected PR path after all required checks succeed.
2. Wait for every main-push CI job on the resulting commit. Create the annotated
   tag `v1.0.0` on that exact commit, with its release notes as the annotation,
   and push only that tag. The `Immutable release
   tags` ruleset prevents updates and deletions of `v*` tags with no bypass.
   Check the commit carefully before creating the tag.
3. In Actions, select **Publish release**, choose the `main` branch and supply
   `v1.0.0`. Inspect the candidate summary and its exact successful CI run.
4. Approve **Review deployments → release** yourself. Without this action, the
   signing and publishing job remains blocked.
5. The approved job uses JDK 11 to build parent/core, sources and Javadoc. It
   requires valid signatures from the pinned primary key, verifies the local
   bundle, then rechecks the immutable tag and exact CI immediately before upload.
   Before upload, it saves the exact signed bundle and expected hashes as a
   90-day workflow artifact, identified by the publisher's immutable artifact ID.
   The existing Central plugin validates and publishes the approved artifacts,
   waiting for `PUBLISHED`. Ordinary builds still skip publishing and retain
   `autoPublish=false`; only this approved workflow opts into automatic completion.
6. A fresh Maven cache and empty settings resolve the stable dependency from
   default Maven Central without custom repositories or credentials. Parent/core
   POM and all three core JAR checksums must match the build. Only then does the
   separate `finalize` job create a GitHub release with the verified bundle and publication
   manifest, including tag, source SHA, CI run, fingerprint and artifact hashes.
   GitHub release notes include the immutable tag annotation plus the verified
   publication metadata. Finalization retries use the same content.
7. Resume development in a separate PR with the next `-SNAPSHOT` version.

If `finalize` fails after `publish` succeeds, select **Re-run failed jobs** on the
same run. Finalization downloads the preserved bundle and receipt, verifies all
five artifact hashes and real signatures, and performs no build or upload to
Central. Parent/core resolution and both classifier downloads share the CDN retry
boundary. GitHub finalization creates a draft, fills only missing assets, checks
existing asset hashes and then publishes; it never replaces different assets or
unrelated notes. An already-complete matching GitHub release is a successful no-op.

If `publish` fails or times out, inspect Central's actual deployment state before
any retry; an uncertain upload may already have published. Do not select **Re-run
all jobs** after publication, re-upload a stable version, change a tag or enable
`ignorePublishedComponents` to hide a conflict. A published stable version is
immutable. The workflow never automatically retries uploads. For uncertain
upload outcomes or expired workflow artifacts, a maintainer must recover using
the original bundle and verified public artifacts instead of rebuilding a version.

### Local signed bundle rehearsal

The default build publishes nothing. On a clean committed stable version, with
the real primary signing key available locally:

```bash
./mvnw -B -Prelease -pl red-ohc-core -am "-Dgpg.keyname=$GPG_FINGERPRINT!" clean verify
python3 scripts/package_release_bundle.py --tag v1.0.0 --fingerprint "$GPG_FINGERPRINT" \
  --output target/release-bundle.zip
```

The packager verifies real signatures against the pinned primary key and writes
parent/core files with MD5, SHA-1, SHA-256 and SHA-512 checksums in Maven layout.
It has no uploader. A local bundle is not evidence of Central validation,
publication or anonymous availability. See the official
[requirements](https://central.sonatype.org/publish/requirements/) and
[bundle layout](https://central.sonatype.org/publish/publish-portal-upload/).

Only parent/core artifacts are published. JMH is enabled by `benchmarks` and is
explicitly excluded by artifact ID in Central's plugin as well as having ordinary
Maven deployment disabled; raw experiments and dependencies do not belong in Maven
Central. Publish benchmark source and reproducible result manifests separately
after approval and review.

Cross-platform correctness, JCStress sanity, longer concurrency stress, Linux
performance and artifact publication remain separate acceptance gates. Record
the checks actually run; do not infer Linux performance from CI or a release.
