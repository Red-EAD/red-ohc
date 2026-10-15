#!/usr/bin/env python3
"""Check stable release candidates, real signatures and anonymous Central downloads."""
import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
import zipfile

from snapshot_publication import REPOSITORY, checked_sha, github, pom_element, publication_decision
from ci_checks import NOT_MEASURED


def checked_version(value):
    if not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', value or ''):
        raise ValueError('Expected a stable major.minor.patch version')
    return value


def release_version(root, tag):
    if not tag or not tag.startswith('v'):
        raise ValueError('Release tags must start with v')
    version = checked_version(tag[1:])
    parent = pom_element(root / 'pom.xml')
    core = pom_element(root / 'red-ohc-core/pom.xml')
    if (parent.findtext('groupId') != 'io.github.red-ead'
            or parent.findtext('artifactId') != 'red-ohc' or parent.findtext('version') != version
            or core.findtext('artifactId') != 'red-ohc-core'
            or core.findtext('parent/groupId') != 'io.github.red-ead'
            or core.findtext('parent/artifactId') != 'red-ohc'
            or core.findtext('parent/version') != version
            or core.findtext('groupId') not in (None, 'io.github.red-ead')
            or core.findtext('version') not in (None, version)):
        raise ValueError('Release tag and public parent/core coordinates must match')
    return version


def verified_ci(sha):
    checked_sha(sha)
    workflow_id = github('actions/workflows/ci.yml')['id']
    query = urllib.parse.urlencode({'event': 'push', 'head_sha': sha, 'per_page': 100})
    runs = github('actions/workflows/ci.yml/runs?' + query)['workflow_runs']
    if not runs:
        raise ValueError('No trusted main push CI exists for this commit')
    run_id = runs[0]['id']
    run = github('actions/runs/' + str(run_id))
    attempt = run.get('run_attempt')
    if not isinstance(attempt, int) or attempt < 1:
        raise ValueError('A specific CI attempt is required')
    jobs = []
    page = 1
    while True:
        batch = github('actions/runs/{}/attempts/{}/jobs?per_page=100&page={}'.format(run_id, attempt, page))['jobs']
        jobs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    # Stable releases may target an older, verified main commit after development advances.
    current = github('actions/runs/' + str(run_id))
    if current.get('run_attempt') != attempt:
        raise ValueError('CI was rerun during inspection; inspect the new attempt')
    decision = publication_decision(sha, sha, 'workflow_dispatch',
                                    {'repository': {'full_name': REPOSITORY}}, current, jobs,
                                    workflow_id, 'refs/heads/main')
    if not decision['publish']:
        raise ValueError(decision['reason'])
    unmeasured = [job for job in jobs if job.get('name') == NOT_MEASURED
                  and job.get('status') == 'completed' and job.get('conclusion') == 'skipped']
    if len(unmeasured) > 1 or any(job.get('status') != 'completed'
                                or (job.get('conclusion') != 'success' and job not in unmeasured)
                                for job in jobs):
        raise ValueError('All active CI jobs, including additional jobs, must succeed')
    return {'ci_run_id': run_id, 'ci_run_attempt': attempt}


def resolve_tag(tag):
    checked_version(tag[1:] if tag and tag.startswith('v') else None)
    obj = github('git/ref/tags/' + tag)['object']
    for _ in range(8):
        sha = checked_sha(obj.get('sha'))
        if obj.get('type') == 'commit':
            return sha
        if obj.get('type') != 'tag':
            break
        obj = github('git/tags/' + sha)['object']
    raise ValueError('Release tag must resolve to a commit')


def tag_notes(tag):
    checked_version(tag[1:] if tag and tag.startswith('v') else None)
    obj = github('git/ref/tags/' + tag)['object']
    if obj.get('type') != 'tag':
        raise ValueError('Release notes require an annotated tag')
    annotation = github('git/tags/' + checked_sha(obj.get('sha')))
    message = annotation.get('message')
    if not isinstance(message, str) or not message.strip():
        raise ValueError('Nonempty annotated tag release notes are required')
    return message.rstrip()


def inspect_release(root, tag, expected_sha):
    version = release_version(root, tag)
    sha = resolve_tag(tag)
    if sha != checked_sha(expected_sha):
        raise ValueError('The release tag moved or does not match the checked-out candidate')
    tag_notes(tag)
    main_sha = checked_sha(github('branches/main')['commit']['sha'])
    if github('compare/' + sha + '...' + main_sha)['status'] not in ('ahead', 'identical'):
        raise ValueError('The candidate must be in protected main history')
    result = {'repository': REPOSITORY, 'tag': tag, 'version': version, 'sha': sha}
    result.update(verified_ci(sha))
    return result


def checked_fingerprint(fingerprint):
    if not re.fullmatch('[A-F0-9]{40}', fingerprint or ''):
        raise ValueError('Expected the complete uppercase primary GPG fingerprint')
    return fingerprint


def require_signature(status, fingerprint):
    checked_fingerprint(fingerprint)
    lines = [line.split() for line in status.splitlines() if line.startswith('[GNUPG:] ')]
    if any(line[1] in ('BADSIG', 'ERRSIG', 'EXPKEYSIG', 'EXPSIG', 'REVKEYSIG', 'KEYEXPIRED',
                       'SIGEXPIRED', 'KEYREVOKED') for line in lines):
        raise ValueError('The signature or signing key is invalid, expired or revoked')
    valid = [line for line in lines if line[1] == 'VALIDSIG']
    if len(valid) != 1 or len(valid[0]) != 12 or valid[0][2] != fingerprint or valid[0][-1] != fingerprint:
        raise ValueError('A valid signature from the pinned primary release key is required')


def verify_signature(signature, artifact, fingerprint, homedir=None):
    command = ['gpg', '--batch']
    if homedir:
        command.extend(['--homedir', str(homedir)])
    result = subprocess.run(command + ['--status-fd', '1', '--verify', str(signature), str(artifact)],
                            check=True, capture_output=True, text=True)
    require_signature(result.stdout, fingerprint)


def check_gpg(fingerprint):
    checked_fingerprint(fingerprint)
    passphrase = os.environ.get('MAVEN_GPG_PASSPHRASE')
    if not passphrase:
        raise ValueError('The release signing passphrase secret must be configured')
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        artifact = root / 'probe.txt'
        artifact.write_text('Red OHC release signing readiness; not a Maven artifact.\n')
        signature = root / 'probe.txt.asc'
        subprocess.run(['gpg', '--batch', '--pinentry-mode', 'loopback', '--passphrase-fd', '0',
                        '--local-user', fingerprint + '!', '--armor', '--detach-sign',
                        '--output', str(signature), str(artifact)],
                       input=(passphrase + '\n').encode(), check=True, capture_output=True, timeout=60)
        # Verify with a separately downloaded public key, not the runner's secret keyring.
        public = root / 'public-keyring'
        public.mkdir(mode=0o700)
        import_public_key(fingerprint, public)
        verify_signature(signature, artifact, fingerprint, public)
    print('Real primary signing key and public keyserver verified: ' + fingerprint)


def consumer_pom(version):
    checked_version(version)
    return '''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>red.ohc.verification</groupId><artifactId>release-consumer</artifactId><version>1</version>
  <dependencies><dependency>
    <groupId>io.github.red-ead</groupId><artifactId>red-ohc-core</artifactId><version>{version}</version>
  </dependency></dependencies>
</project>
'''.format(version=version)


def artifact_files(root, version):
    files = {}
    for artifact, source in [('red-ohc', root), ('red-ohc-core', root / 'red-ohc-core')]:
        suffixes = ['.pom'] if artifact == 'red-ohc' else ['.pom', '.jar', '-sources.jar', '-javadoc.jar']
        for suffix in suffixes:
            filename = artifact + '-' + version + suffix
            built = source / 'pom.xml' if suffix == '.pom' else source / 'target' / filename
            name = 'io/github/red-ead/{}/{}/{}'.format(artifact, version, filename)
            files[name] = built
    return files


def write_receipt(root, tag, sha, ci_run_id, ci_run_attempt, fingerprint, bundle):
    version = release_version(root, tag)
    with zipfile.ZipFile(bundle) as archive:
        artifacts = [{'path': name, 'file': Path(name).name,
                      'sha256': hashlib.sha256(archive.read(name)).hexdigest(),
                      'bytes': len(archive.read(name))} for name in artifact_files(root, version)]
    return {'repository': REPOSITORY, 'tag': tag, 'version': version, 'source_sha': checked_sha(sha),
            'ci_run_id': ci_run_id, 'ci_run_attempt': ci_run_attempt,
            'gpg_fingerprint': checked_fingerprint(fingerprint),
            'bundle_sha256': hashlib.sha256(bundle.read_bytes()).hexdigest(), 'artifacts': artifacts}


def import_public_key(fingerprint, homedir):
    checked_fingerprint(fingerprint)
    query = urllib.parse.urlencode({'op': 'get', 'search': '0x' + fingerprint})
    with urllib.request.urlopen('https://keyserver.ubuntu.com/pks/lookup?' + query, timeout=30) as response:
        public_key = response.read(1024 * 1024)
    subprocess.run(['gpg', '--batch', '--homedir', str(homedir), '--import'],
                   input=public_key, check=True, capture_output=True, timeout=60)


def verify_persisted_publication(root, tag, sha, ci_run_id, ci_run_attempt, fingerprint, bundle, receipt, cache):
    expected = write_receipt(root, tag, sha, ci_run_id, ci_run_attempt, fingerprint, bundle)
    if receipt != expected:
        raise ValueError('Preserved bundle or release metadata does not match the approved publication')
    with tempfile.TemporaryDirectory() as temporary, zipfile.ZipFile(bundle) as archive:
        directory = Path(temporary)
        public = directory / 'public-keyring'
        public.mkdir(mode=0o700)
        import_public_key(fingerprint, public)
        for entry in expected['artifacts']:
            content = archive.read(entry['path'])
            resolved = cache / entry['path']
            if hashlib.sha256(resolved.read_bytes()).hexdigest() != entry['sha256']:
                raise ValueError('Anonymous Maven Central download differs: ' + entry['file'])
            artifact = directory / entry['file']
            artifact.write_bytes(content)
            signature = directory / (entry['file'] + '.asc')
            signature.write_bytes(archive.read(entry['path'] + '.asc'))
            verify_signature(signature, artifact, fingerprint, public)
    return expected


def finalize_github_release(tag, notes, assets):
    checked_version(tag[1:] if tag and tag.startswith('v') else None)
    body = notes.read_text()
    release = find_github_release(tag)
    if release is None:
        subprocess.run(['gh', 'release', 'create', tag, '--repo', REPOSITORY, '--verify-tag',
                        '--draft', '--title', 'Red OHC ' + tag, '--notes-file', str(notes)], check=True)
        release = find_github_release(tag)
    if release.get('tag_name') != tag or (release.get('body') or '').strip() != body.strip():
        raise ValueError('Refusing to modify unrelated existing release notes')
    existing = {asset['name']: asset for asset in release.get('assets', [])}
    for path in assets:
        if path.name in existing:
            digest = existing[path.name].get('digest')
            if digest != 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest():
                raise ValueError('Refusing to replace a different existing release asset: ' + path.name)
        elif release['draft']:
            subprocess.run(['gh', 'release', 'upload', tag, str(path), '--repo', REPOSITORY], check=True)
        else:
            raise ValueError('An existing public release is missing a required asset; do not overwrite it')
    if release['draft']:
        subprocess.run(['gh', 'release', 'edit', tag, '--repo', REPOSITORY, '--draft=false'], check=True)
    print('Verified GitHub release completed: ' + tag)


def find_github_release(tag):
    # gh owns published/draft lookup; REST's by-tag endpoint only returns published releases.
    result = subprocess.run(['gh', 'release', 'view', tag, '--repo', REPOSITORY, '--json', 'databaseId'],
                            capture_output=True, text=True)
    if result.returncode and result.stderr.strip() == 'release not found':
        return None
    result.check_returncode()
    release_id = json.loads(result.stdout)['databaseId']
    if not isinstance(release_id, int) or release_id < 1:
        raise ValueError('A valid GitHub release ID is required')
    return github('releases/' + str(release_id))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('resolve-tag', 'tag-notes', 'check', 'check-ci', 'check-gpg',
                                           'consumer-pom', 'receipt', 'manifest', 'github-release'))
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--tag')
    parser.add_argument('--sha')
    parser.add_argument('--fingerprint')
    parser.add_argument('--ci-run-id', type=int)
    parser.add_argument('--ci-run-attempt', type=int)
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--receipt', type=Path)
    parser.add_argument('--notes', type=Path)
    parser.add_argument('--asset', type=Path, action='append')
    args = parser.parse_args()
    if args.command == 'resolve-tag':
        print(resolve_tag(args.tag))
    elif args.command == 'tag-notes':
        print(tag_notes(args.tag))
    elif args.command == 'check-gpg':
        check_gpg(args.fingerprint)
    elif args.command == 'consumer-pom':
        version = release_version(args.root, args.tag)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(consumer_pom(version))
    elif args.command in ('receipt', 'manifest'):
        result = write_receipt(args.root, args.tag, args.sha, args.ci_run_id, args.ci_run_attempt,
                               args.fingerprint, args.bundle)
        if args.command == 'manifest':
            result = verify_persisted_publication(args.root, args.tag, args.sha, args.ci_run_id, args.ci_run_attempt,
                                                 args.fingerprint, args.bundle,
                                                 json.loads(args.receipt.read_text()), args.cache)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result))
    elif args.command == 'github-release':
        finalize_github_release(args.tag, args.notes, args.asset)
    else:
        if (os.environ.get('GITHUB_REPOSITORY') != REPOSITORY
                or os.environ.get('GITHUB_EVENT_NAME') != 'workflow_dispatch'
                or os.environ.get('GITHUB_REF') != 'refs/heads/main'):
            raise ValueError('Only a manual dispatch from this repository main is allowed')
        if args.command == 'check-ci':
            if github('branches/main')['commit']['sha'] != checked_sha(args.sha):
                raise ValueError('The readiness checkout must be current main')
            result = {'sha': args.sha}
            result.update(verified_ci(args.sha))
        else:
            result = inspect_release(args.root, args.tag, args.sha)
            if (args.ci_run_id is not None and (args.ci_run_id != result['ci_run_id']
                    or args.ci_run_attempt != result['ci_run_attempt'])):
                raise ValueError('The approved CI attempt changed; require a fresh candidate and approval')
        print(json.dumps(result))
        with Path(os.environ['GITHUB_OUTPUT']).open('a') as stream:
            for name, value in result.items():
                stream.write('{}={}\n'.format(name, value))


if __name__ == '__main__':
    main()
