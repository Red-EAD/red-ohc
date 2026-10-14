#!/usr/bin/env python3
"""Gate public SNAPSHOT uploads and prepare anonymous Maven consumer verification."""
import argparse
import collections
import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

REPOSITORY = 'Red-EAD/red-ohc'
SNAPSHOTS = 'https://central.sonatype.com/repository/maven-snapshots/'
REQUIRED_JOBS = {'verify ({}, {})'.format(system, java)
                 for system in ('ubuntu-latest', 'macos-latest', 'windows-latest')
                 for java in ('11', '17', '21', '25')} | {'jcstress', 'example'}


def checked_sha(value):
    if not re.fullmatch('[0-9a-f]{40}', value or ''):
        raise ValueError('Expected a full lowercase Git commit SHA')
    return value


def checked_version(value):
    if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]*-SNAPSHOT', value or ''):
        raise ValueError('Expected a path-safe SNAPSHOT version')
    return value


def publication_decision(source_sha, main_sha, event_name, event, run, jobs, ci_workflow_id, ref):
    checked_sha(source_sha)
    checked_sha(main_sha)
    decision = {'publish': False, 'sha': source_sha, 'ci_run_id': run.get('id'), 'reason': ''}
    if event.get('repository', {}).get('full_name') != REPOSITORY or ref != 'refs/heads/main':
        decision['reason'] = 'Only this repository main may publish'
    elif source_sha != main_sha:
        decision['reason'] = 'Main advanced; this checkout is superseded'
    elif event_name not in ('workflow_run', 'workflow_dispatch'):
        decision['reason'] = 'Unsupported publication event'
    elif event_name == 'workflow_run' and event.get('workflow_run', {}).get('id') != run.get('id'):
        decision['reason'] = 'The event does not identify the inspected CI run'
    elif (run.get('workflow_id') != ci_workflow_id or run.get('event') != 'push'
          or run.get('head_branch') != 'main' or run.get('head_sha') != source_sha
          or run.get('repository', {}).get('full_name') != REPOSITORY
          or run.get('head_repository', {}).get('full_name') != REPOSITORY):
        decision['reason'] = 'CI did not verify this exact repository main push'
    elif run.get('status') != 'completed' or run.get('conclusion') != 'success':
        decision['reason'] = 'CI has not completed successfully'
    else:
        counts = collections.Counter(job.get('name') for job in jobs)
        successful = {job.get('name') for job in jobs
                      if job.get('status') == 'completed' and job.get('conclusion') == 'success'}
        if not REQUIRED_JOBS.issubset(successful) or any(counts[name] != 1 for name in REQUIRED_JOBS):
            decision['reason'] = 'Every required CI job must complete successfully exactly once'
        else:
            decision.update(publish=True, reason='Exact current main passed all required CI jobs')
    return decision


def pom_element(path):
    node = ET.parse(path).getroot()
    # Standard Maven XML namespaces are irrelevant to the coordinate checks.
    for element in node.iter():
        element.tag = element.tag.rsplit('}', 1)[-1]
    return node


def snapshot_version(root):
    parent = pom_element(root / 'pom.xml')
    if parent.findtext('groupId') != 'io.github.red-ead' or parent.findtext('artifactId') != 'red-ohc':
        raise ValueError('Only the public Red OHC parent may be published')
    version = parent.findtext('version')
    if not version or not version.endswith('-SNAPSHOT'):
        return None
    checked_version(version)
    core = pom_element(root / 'red-ohc-core/pom.xml')
    if (core.findtext('artifactId') != 'red-ohc-core'
            or core.findtext('parent/groupId') != 'io.github.red-ead'
            or core.findtext('parent/artifactId') != 'red-ohc'
            or core.findtext('parent/version') != version
            or core.findtext('groupId') not in (None, 'io.github.red-ead')
            or core.findtext('version') not in (None, version)):
        raise ValueError('Core must inherit the matching public parent coordinates')
    return version


def consumer_pom(version):
    checked_version(version)
    return '''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>red.ohc.verification</groupId><artifactId>snapshot-consumer</artifactId><version>1</version>
  <repositories><repository>
    <id>central-portal-snapshots</id><url>{repository}</url>
    <releases><enabled>false</enabled></releases><snapshots><enabled>true</enabled></snapshots>
  </repository></repositories>
  <dependencies><dependency>
    <groupId>io.github.red-ead</groupId><artifactId>red-ohc-core</artifactId><version>{version}</version>
  </dependency></dependencies>
</project>
'''.format(repository=SNAPSHOTS, version=version)


def github(path):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if not token:
        raise ValueError('A read-only GitHub token is required to inspect CI')
    request = urllib.request.Request('https://api.github.com/repos/' + REPOSITORY + '/' + path,
                                     headers={'Authorization': 'Bearer ' + token,
                                              'Accept': 'application/vnd.github+json',
                                              'X-GitHub-Api-Version': '2022-11-28'})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError('GitHub CI inspection failed with HTTP {}'.format(error.code)) from None


def inspect_publication(root, source_sha, event_name, event, ref):
    version = snapshot_version(root)
    if version is None:
        return {'publish': False, 'reason': 'Stable versions use the separate release process'}
    main_sha = github('branches/main')['commit']['sha']
    workflow_id = github('actions/workflows/ci.yml')['id']
    if event_name == 'workflow_run':
        run_id = event.get('workflow_run', {}).get('id')
        if not isinstance(run_id, int) or run_id <= 0:
            raise ValueError('The completion event requires a numeric CI run ID')
    elif event_name == 'workflow_dispatch':
        query = urllib.parse.urlencode({'event': 'push', 'head_sha': checked_sha(source_sha), 'per_page': 100})
        runs = github('actions/workflows/ci.yml/runs?' + query)['workflow_runs']
        if not runs:
            return {'publish': False, 'reason': 'No main push CI exists for this commit'}
        run_id = runs[0]['id']
    else:
        return {'publish': False, 'reason': 'Unsupported publication event'}
    run = github('actions/runs/' + str(run_id))
    jobs = []
    page = 1
    while True:
        batch = github('actions/runs/{}/jobs?filter=latest&per_page=100&page={}'.format(run_id, page))['jobs']
        jobs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    decision = publication_decision(source_sha, main_sha, event_name, event, run, jobs, workflow_id, ref)
    decision['version'] = version
    return decision


def write_manifest(root, sha, ci_run_id, output):
    version = snapshot_version(root)
    if version is None:
        raise ValueError('A SNAPSHOT version is required')
    files = [root / 'pom.xml', root / 'red-ohc-core/pom.xml',
             root / 'red-ohc-core/target/red-ohc-core-{}.jar'.format(version),
             root / 'red-ohc-core/target/red-ohc-core-{}-sources.jar'.format(version)]
    artifacts = [{'path': str(path.relative_to(root)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                  'bytes': path.stat().st_size} for path in files]
    data = {'repository': REPOSITORY, 'source_sha': checked_sha(sha), 'ci_run_id': ci_run_id,
            'version': version, 'artifacts': artifacts}
    output.write_text(json.dumps(data, indent=2) + '\n')
    return data


def verify_consumer(root, cache):
    version = snapshot_version(root)
    filename = 'red-ohc-core-{}.jar'.format(version)
    built = root / 'red-ohc-core/target' / filename
    resolved = cache / 'io/github/red-ead/red-ohc-core' / version / filename
    if hashlib.sha256(built.read_bytes()).digest() != hashlib.sha256(resolved.read_bytes()).digest():
        raise ValueError('Anonymous consumer resolved a different SNAPSHOT JAR')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('check', 'assert-current', 'consumer-pom', 'manifest'))
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--sha')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--ci-run-id', type=int)
    parser.add_argument('--cache', type=Path)
    args = parser.parse_args()
    if args.command == 'consumer-pom':
        version = snapshot_version(args.root)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(consumer_pom(version))
    elif args.command == 'manifest':
        verify_consumer(args.root, args.cache)
        print(json.dumps(write_manifest(args.root, args.sha, args.ci_run_id, args.output)))
    else:
        event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
        result = inspect_publication(args.root, checked_sha(args.sha), os.environ['GITHUB_EVENT_NAME'],
                                     event, os.environ['GITHUB_REF'])
        print(json.dumps(result))
        if args.command == 'assert-current' and not result['publish']:
            raise SystemExit('Publication stopped: ' + result['reason'])
        if args.command == 'check':
            with Path(os.environ['GITHUB_OUTPUT']).open('a') as stream:
                stream.write('publish={}\n'.format(str(result['publish']).lower()))
                if result['publish']:
                    stream.write('sha={}\nversion={}\nci_run_id={}\n'.format(
                        result['sha'], result['version'], result['ci_run_id']))


if __name__ == '__main__':
    main()
