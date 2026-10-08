#!/usr/bin/env python3
"""Matched Red OHC revisions on Linux x86-64; preview unless explicitly requested."""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import statistics
import struct
import subprocess
import time
import xml.etree.ElementTree as ET
import zipfile

from run_fair_benchmarks import cpu_usage, digest, sample_linux_tree

ROOT = Path(__file__).resolve().parents[1]
BASE = '306f87b0e2d36f1e8fc02b2024cc889be9a24535'
JDKS = (11, 17, 21, 25)
SIZES = ((32, 5120), (32, 64), (32, 16384), (1024, 5120))
WORKLOADS = (('read_t1', 'oneThread', 1, 'READ_100'),
             ('read_t32', 'cpuThreads', 32, 'READ_100'),
             ('write_t8', 'cpuThreads', 8, 'WRITE_100'),
             ('mix_t8', 'cpuThreads', 8, 'READ_90_WRITE_10'))
JVM_ARGS = '-Xms1g -Xmx1g -XX:MaxDirectMemorySize=1g -XX:+UseG1GC'


def plan():
    for jdk in JDKS:
        for key, value in SIZES:
            for name, method, threads, workload in WORKLOADS:
                for round_index in range(1, 6):
                    labels = ('BASE', 'NEW') if round_index % 2 else ('NEW', 'BASE')
                    for label in labels:
                        yield {'jdk': jdk, 'key': key, 'value': value, 'scenario': name,
                               'method': method, 'threads': threads, 'workload': workload,
                               'round': round_index, 'label': label}


def require_linux():
    if platform.system() != 'Linux' or platform.machine().lower() not in ('x86_64', 'amd64'):
        raise RuntimeError('preparation and measurement require dedicated Linux x86-64')


def java_info(home, expected):
    java = Path(home).resolve() / 'bin/java'
    text = subprocess.check_output([str(java), '-XshowSettings:properties', '-version'],
                                   stderr=subprocess.STDOUT, text=True)
    version = re.search(r'java.specification.version\s*=\s*(\d+)', text)
    if not version or int(version[1]) != expected:
        raise RuntimeError('JDK home does not match Java ' + str(expected))
    return {'java': str(java), 'details': text}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def check_java11_jar(path):
    with zipfile.ZipFile(path) as jar:
        classes = [name for name in jar.namelist()
                   if name.startswith('com/red/ohc/') and name.endswith('.class')]
        if not classes or any(struct.unpack('>IHH', jar.read(name)[:8])[2] != 55
                              for name in classes):
            raise RuntimeError('application JAR must contain Java 11 bytecode: ' + str(path))


def prepare(output, new_sha, compiler):
    if git('status', '--porcelain'):
        raise RuntimeError('prepare from a clean committed source tree')
    if new_sha == BASE:
        raise RuntimeError('NEW must be a committed revision after BASE')
    subprocess.run(['git', 'merge-base', '--is-ancestor', BASE, new_sha], cwd=ROOT, check=True)
    output.mkdir(parents=True, exist_ok=False)
    receipt = {'base': BASE, 'new': new_sha, 'runner_head': git('rev-parse', 'HEAD'),
               'compiler': compiler, 'artifacts': {}}
    env = os.environ.copy()
    env.update(JAVA_HOME=str(Path(compiler['java']).parents[1]),
               PATH=str(Path(compiler['java']).parent) + os.pathsep + env.get('PATH', ''))
    for label, sha in (('BASE', BASE), ('NEW', new_sha)):
        checkout = output / 'build' / label
        checkout.mkdir(parents=True)
        archive = subprocess.check_output(['git', 'archive', sha], cwd=ROOT)
        subprocess.run(['tar', '-x', '-C', str(checkout)], input=archive, check=True)
        ns = '{http://maven.apache.org/POM/4.0.0}'
        version = ET.parse(checkout / 'pom.xml').getroot().findtext(ns + 'version')
        if not version:
            raise RuntimeError('missing project version')
        repository = output / 'm2' / label
        prefix = [str(checkout / 'mvnw'), '-B', '-s', str(checkout / 'config/maven/settings.xml'),
                  '-gs', str(checkout / 'config/maven/settings.xml'),
                  '-Dmaven.repo.local=' + str(repository)]
        classpath = output / (label + '.classpath')
        with (output / (label + '.build.log')).open('w') as log:
            subprocess.run([*prefix, '-Pbenchmarks', '-pl', 'red-ohc-core,red-ohc-jmh', '-am',
                            '-DskipTests', 'clean', 'install'], cwd=checkout, env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
            subprocess.run([*prefix, '-f', 'tools/benchmark-runtime/pom.xml', '-Pred',
                            '-Dred-ohc.version=' + version,
                            'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath',
                            '-Dmdep.outputFile=' + str(classpath)], cwd=checkout, env=env,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        thin = checkout / 'red-ohc-jmh/target' / ('red-ohc-jmh-' + version + '.jar')
        dependencies = [Path(path) for path in classpath.read_text().strip().split(os.pathsep)]
        artifacts = output / 'artifacts' / label
        artifacts.mkdir(parents=True)
        jars = {}
        for index, path in enumerate([thin, *dependencies]):
            target = artifacts / (str(index) + '-' + path.name)
            shutil.copyfile(path, target)
            if path == thin or path.name.startswith('red-ohc-core-'):
                check_java11_jar(target)
            jars[str(target)] = digest(target)
        receipt['artifacts'][label] = jars
    (output / 'build-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')


def checked_receipt(output, new_sha):
    receipt = json.loads((output / 'build-receipt.json').read_text())
    if (git('status', '--porcelain') or receipt['base'] != BASE or receipt['new'] != new_sha
            or receipt['runner_head'] != git('rev-parse', 'HEAD')):
        raise RuntimeError('source/receipt changed; prepare from the exact clean commit')
    for jars in receipt['artifacts'].values():
        if not jars or any(digest(path) != checksum for path, checksum in jars.items()):
            raise RuntimeError('prepared artifact bytes changed')
    return receipt


def validate_results(results):
    if len(results) != 1:
        raise RuntimeError('expected exactly one JMH result')
    result = results[0]
    metrics = result['secondaryMetrics']
    for name in ('attemptedOperations', 'completedOperations', 'attemptedWrites',
                 'acceptedWrites', 'rejectedWrites', 'exceptionCount', 'incompleteOperations'):
        metric = metrics[name]
        raw = metric['rawData']
        if len(raw) != 1 or len(raw[0]) != 5:
            raise RuntimeError('expected one fork and five measurement iterations')
        if any(not math.isfinite(value) or value < 0 for value in [metric['score'], *raw[0]]):
            raise RuntimeError('invalid operation counter: ' + name)
    for name in ('exceptionCount', 'incompleteOperations', 'rejectedWrites'):
        if any(value != 0 for value in [metrics[name]['score'], *metrics[name]['rawData'][0]]):
            raise RuntimeError('unexpected rejection/error: ' + name)
    for attempted, completed in (('attemptedOperations', 'completedOperations'),
                                 ('attemptedWrites', 'acceptedWrites')):
        for left, right in zip([metrics[attempted]['score'], *metrics[attempted]['rawData'][0]],
                               [metrics[completed]['score'], *metrics[completed]['rawData'][0]]):
            if not math.isclose(left, right, rel_tol=1e-9):
                raise RuntimeError('operation counts do not reconcile: ' + attempted)
    throughput = result['primaryMetric']['score']
    if not math.isfinite(throughput) or throughput <= 0:
        raise RuntimeError('missing finite positive throughput')
    return throughput


def measurement_cpu(log, throughput):
    matches = re.findall(r'measurement-window: wallNanos=(\d+), processCpuNanos=(\d+), '
                         r'actorCpuNanos=(\d+), processCpuKnown=(true|false), '
                         r'actorCpuKnown=(true|false)', log)
    if len(matches) != 5:
        raise RuntimeError('expected five measured process CPU windows')
    windows = [{'wall_seconds': int(wall) / 1e9,
                'process_cpu_seconds': int(cpu) / 1e9 if known == 'true' else None,
                'actor_cpu_seconds': int(actor) / 1e9 if actor_known == 'true' else None}
               for wall, cpu, actor, known, actor_known in matches]
    measured_cpu = None
    cpu_per_operation = None
    if all(window['process_cpu_seconds'] is not None for window in windows):
        measured_cpu = sum(window['process_cpu_seconds'] for window in windows)
        wall = sum(window['wall_seconds'] for window in windows)
        cpu_per_operation = measured_cpu / wall / throughput if wall > 0 else None
    return {'measurement_windows': windows, 'measured_process_cpu_seconds': measured_cpu,
            'estimated_cpu_seconds_per_completed_operation': cpu_per_operation}


def command(java, jars, row, result_file, jvm_args):
    return [java, '-cp', os.pathsep.join(jars), 'org.openjdk.jmh.Main',
            '^' + re.escape('com.red.ohc.jmh.OHCSerializedBenchmark.' + row['method']) + '$',
            '-jvm', java, '-f', '1', '-wi', '3', '-w', '5s', '-i', '5', '-r', '10s',
            '-t', str(row['threads']), '-p', 'keyBytes=' + str(row['key']),
            '-p', 'valueBytes=' + str(row['value']), '-p', 'workload=' + row['workload'],
            '-p', 'distribution=UNIFORM', '-p', 'writeShape=MIXED', '-jvmArgs', jvm_args,
            '-jvmArgsPrepend', '', '-jvmArgsAppend', '',
            '-foe', 'true', '-rf', 'json', '-rff', str(result_file)]


def validate_fork_configuration(result, row, java, jvm_args):
    if (result['benchmark'] != 'com.red.ohc.jmh.OHCSerializedBenchmark.' + row['method']
            or result['threads'] != row['threads'] or result['forks'] != 1
            or result['primaryMetric']['scoreUnit'] != 'ops/s'
            or result['jvm'] != java
            or re.match(r'\d+', result['jdkVersion'])[0] != str(row['jdk'])
            or result['jvmArgs'] != shlex.split(jvm_args)):
        raise RuntimeError('effective JMH fork configuration does not match the planned run')
    parameters = {'keyBytes': str(row['key']), 'valueBytes': str(row['value']),
                  'workload': row['workload'], 'distribution': 'UNIFORM', 'writeShape': 'MIXED'}
    if any(result['params'].get(key) != value for key, value in parameters.items()):
        raise RuntimeError('effective JMH parameters do not match the planned run')


def stop_process(process, grace_seconds=35):
    deadline = time.monotonic() + grace_seconds
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        pass
    # A controller may exit before its fork. Wait for the group, not just the controller.
    while True:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        if time.monotonic() >= deadline:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            break
        time.sleep(0.1)
    process.wait()


def run_one(output, row, java, jars, jvm_args):
    name = '{jdk}-{key}-{value}-{scenario}-{round}-{label}'.format(**row)
    result_file = output / (name + '.jmh.json')
    log_file = output / (name + '.log')
    args = command(java, jars, row, result_file, jvm_args)
    before = cpu_usage()
    started = time.monotonic()
    samples = {}
    process = None
    failure = None
    try:
        with log_file.open('w') as log:
            process = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            while process.poll() is None:
                sample_linux_tree(process.pid, samples)
                if time.monotonic() - started > 300:
                    raise RuntimeError('benchmark exceeded 300s: ' + name)
                time.sleep(0.1)
            if process.returncode:
                raise RuntimeError('JMH failed: ' + name)
    except BaseException as error:
        failure = error
    finally:
        if process is not None:
            stop_process(process)
        elapsed = time.monotonic() - started
        after = cpu_usage()
        user, system = after[0] - before[0], after[1] - before[1]
        metrics = row | {'command': args, 'exit_code': process.returncode if process else None,
                         'wall_seconds': elapsed, 'user_seconds': user, 'system_seconds': system,
                         'cpu_seconds': user + system, 'logical_cpus': os.cpu_count(),
                         'affinity_cpus': sorted(os.sched_getaffinity(0)),
                         'linux_process_samples': samples, 'process_samples_are_lower_bounds': True,
                         'normalized_cpu_percent': 100 * (user + system) / elapsed / os.cpu_count()}
        (output / (name + '.process.json')).write_text(json.dumps(metrics, indent=2) + '\n')
    if failure is not None:
        raise failure
    results = json.loads(result_file.read_text())
    throughput = validate_results(results)
    result = results[0]
    validate_fork_configuration(result, row, java, jvm_args)
    metrics.update(throughput=throughput, **measurement_cpu(log_file.read_text(), throughput))
    (output / (name + '.process.json')).write_text(json.dumps(metrics, indent=2) + '\n')
    return metrics


def paired_summary(records):
    groups = {}
    for record in records:
        key = (record['jdk'], record['key'], record['value'], record['scenario'])
        groups.setdefault(key, {}).setdefault(record['round'], {})[record['label']] = record
    summary = []
    for key, rounds in groups.items():
        pairs = []
        for round_index, pair in rounds.items():
            if set(pair) != {'BASE', 'NEW'}:
                continue
            ratios = {}
            for metric in ('throughput', 'cpu_seconds',
                           'estimated_cpu_seconds_per_completed_operation'):
                base, new = pair['BASE'][metric], pair['NEW'][metric]
                ratios[metric] = new / base if base and new is not None else None
            pairs.append({'round': round_index, 'new_over_base': ratios})
        summary.append({'jdk': key[0], 'key': key[1], 'value': key[2], 'scenario': key[3],
                        'pairs': pairs, 'complete': len(pairs) == 5,
                        'median_throughput_ratio': statistics.median(
                            pair['new_over_base']['throughput'] for pair in pairs) if pairs else None})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--new-ref', default='HEAD')
    parser.add_argument('--jdk-homes', nargs=4, metavar='HOME',
                        help='Java 11, 17, 21, 25 homes, in that order; Java 11 builds both revisions')
    parser.add_argument('--jvm-args', default=JVM_ARGS)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    rows = list(plan())
    print(json.dumps({'base': BASE, 'new_ref': args.new_ref, 'runs': len(rows),
                      'execute': args.run, 'order': rows}, indent=2))
    if not (args.prepare or args.run):
        return
    require_linux()
    if not args.output or not args.jdk_homes:
        parser.error('--output and all four --jdk-homes are required')
    shlex.split(args.jvm_args)  # Reject malformed quoting before any build or process starts.
    output = args.output.resolve()
    if output == ROOT or ROOT in output.parents:
        raise RuntimeError('keep build artifacts and results outside the source checkout')
    new_sha = git('rev-parse', args.new_ref + '^{commit}')
    jdks = {version: java_info(home, version) for version, home in zip(JDKS, args.jdk_homes)}
    if args.prepare:
        prepare(output, new_sha, jdks[11])
    if not args.run:
        return
    receipt = checked_receipt(output, new_sha)
    runs = output / 'runs'
    runs.mkdir(exist_ok=False)
    manifest = {'receipt': receipt, 'runtime_jdks': jdks, 'jvm_args': args.jvm_args,
                'platform': platform.platform(), 'machine': platform.machine(),
                'logical_cpus': os.cpu_count(), 'affinity_cpus': sorted(os.sched_getaffinity(0)),
                'order': rows, 'acceptance': 'individual assessment; no fixed percentage threshold'}
    (runs / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    def interrupted(signum, frame):
        raise KeyboardInterrupt('interrupted by signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    records = []
    for index, row in enumerate(rows, 1):
        # Check bytes before each fork so later JVMs cannot silently use overwritten snapshots.
        checked_receipt(output, new_sha)
        print(str(index) + '/' + str(len(rows)) + ' ' + json.dumps(row), flush=True)
        records.append(run_one(runs, row, jdks[row['jdk']]['java'],
                               receipt['artifacts'][row['label']], args.jvm_args))
        (runs / 'summary.json').write_text(json.dumps(paired_summary(records), indent=2) + '\n')


if __name__ == '__main__':
    main()
