#!/usr/bin/env python3
"""Prepare isolated classpaths; run formal comparisons only with explicit --run."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import re
import signal
import subprocess
import time
import xml.etree.ElementTree as ET

try:
    import resource
except ImportError:
    resource = None

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = {'RED_OHC': 'red', 'SNAZY_OHC': 'snazy', 'EHCACHE': 'ehcache',
            'MAPDB': 'mapdb', 'CHRONICLE': 'chronicle', 'REDIS': 'redis'}
MAVEN = ROOT / ('mvnw.cmd' if os.name == 'nt' else 'mvnw')
SETTINGS = ROOT / 'config/maven/settings.xml'
JAVA = str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else 'java'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_identity():
    return {'sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
            'status': subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)}


def project_version():
    version = ET.parse(ROOT / 'pom.xml').getroot().findtext('{http://maven.apache.org/POM/4.0.0}version')
    if not version:
        raise RuntimeError('missing root project version')
    return version


def benchmark_jar(version):
    return ROOT / 'red-ohc-jmh/target' / ('red-ohc-jmh-' + version + '.jar')


def java_version():
    return subprocess.check_output([JAVA, '-version'], stderr=subprocess.STDOUT, text=True)


def benchmark_module_options():
    model = ET.parse(ROOT / 'red-ohc-jmh/pom.xml').getroot()
    options = model.findtext('{http://maven.apache.org/POM/4.0.0}properties/'
                             '{http://maven.apache.org/POM/4.0.0}benchmark.moduleOptions')
    if not options:
        raise RuntimeError('missing shared benchmark module-access options')
    return ' '.join(options.split())


def runtime_jars(output, backends, version):
    jars = {benchmark_jar(version)}
    for backend in backends:
        jars.update(Path(x) for x in (output / (BACKENDS[backend] + '.classpath')).read_text().strip().split(os.pathsep))
    return {str(p): digest(p) for p in sorted(jars)}


def maven(args):
    subprocess.run([str(MAVEN), '-B', '-s', str(SETTINGS), '-gs', str(SETTINGS),
                    *args], cwd=ROOT, check=True)


def prepare(output, backends, version):
    identity = source_identity()
    compiler_jdk = java_version()
    maven(['-Pbenchmarks', '-DskipTests', 'clean', 'install'])
    for backend in backends:
        profile = BACKENDS[backend]
        target = output / (profile + '.classpath')
        maven(['-f', 'tools/benchmark-runtime/pom.xml', '-P' + profile,
               '-Dred-ohc.version=' + version,
               'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath',
               '-Dmdep.outputFile=' + str(target)])
        maven(['-f', 'tools/benchmark-runtime/pom.xml', '-P' + profile,
               '-Dred-ohc.version=' + version,
               'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree',
               '-DoutputFile=' + str(output / (profile + '.dependencies.txt'))])

    if source_identity() != identity:
        raise RuntimeError('source changed while preparing benchmark artifacts')
    receipt = identity | {'version': version, 'compiler_jdk': compiler_jdk,
                          'backends': backends, 'jars': runtime_jars(output, backends, version)}
    (output / 'build-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')


def checked_receipt(output, backends, version):
    identity = source_identity()
    if identity['status']:
        raise RuntimeError('formal measurement requires a clean committed source tree')
    receipt = json.loads((output / 'build-receipt.json').read_text())
    if any(receipt.get(key) != value for key, value in identity.items()) or receipt.get('version') != version:
        raise RuntimeError('build receipt does not match source; rerun --prepare on this clean commit')
    if not set(backends).issubset(receipt['backends']) or receipt['compiler_jdk'] != java_version():
        raise RuntimeError('backend/JDK changed; rerun --prepare')
    current = runtime_jars(output, backends, version)
    if any(receipt['jars'].get(path) != checksum for path, checksum in current.items()):
        raise RuntimeError('artifact bytes changed after preparation; rerun --prepare')
    return receipt, current


def cpu_usage():
    if resource is None:
        return None
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return (usage.ru_utime, usage.ru_stime)


def sample_linux_tree(root_pid, samples):
    """Sample per-process CPU/RSS; final child rusage separately records total CPU."""
    if platform.system() != 'Linux':
        return
    processes = {}
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            text = path.read_text()
            right = text.rindex(')')
            fields = text[right + 2:].split()
            pid = int(path.parent.name)
            processes[pid] = (int(fields[1]), text[text.index('(') + 1:right],
                              int(fields[11]), int(fields[12]), int(fields[21]))
        except (OSError, ValueError, IndexError):
            continue
    children = {root_pid}
    while True:
        extended = children | {pid for pid, data in processes.items() if data[0] in children}
        if extended == children:
            break
        children = extended
    for pid in children:
        if pid not in processes:
            continue
        _, comm, user, system, rss = processes[pid]
        old = samples.setdefault(str(pid), {'comm': comm, 'user_ticks': 0,
                                           'system_ticks': 0, 'peak_rss_pages': 0})
        old['user_ticks'] = max(old['user_ticks'], user)
        old['system_ticks'] = max(old['system_ticks'], system)
        old['peak_rss_pages'] = max(old['peak_rss_pages'], rss)


def run_one(output, args, version, round_index, backend, threads, workload):
    name = f'{round_index}-{backend}-{threads}-{workload}'
    jar = benchmark_jar(version)
    dependencies = (output / (BACKENDS[backend] + '.classpath')).read_text().strip().split(os.pathsep)
    classpath = os.pathsep.join([str(jar), *dependencies])
    command = [JAVA, '-cp', classpath, 'org.openjdk.jmh.Main',
               r'com.red.ohc.jmh.FairSerializedBenchmark.operation', '-f', '1',
               '-wi', '3', '-w', '5s', '-i', '5', '-r', '10s', '-t', str(threads),
               '-p', 'backend=' + backend, '-p', 'keyBytes=' + str(args.key_bytes),
               '-p', 'valueBytes=' + str(args.value_bytes), '-p', 'distribution=' + args.distribution,
               '-p', 'workload=' + workload, '-p', 'scenario=' + args.scenario,
               '-p', 'ttlMillis=' + str(args.ttl_millis), '-p', 'capacityBytes=' + str(args.capacity_bytes),
               '-p', 'consumption=' + args.consumption, '-jvmArgs',
               benchmark_module_options() + ' ' + args.jvm_args,
               '-foe', 'true', '-rf', 'json', '-rff', str(output / (name + '.jmh.json'))]
    before = cpu_usage()
    start = time.monotonic()
    samples = {}
    try:
        with (output / (name + '.log')).open('w') as log:
            process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=os.name != 'nt')
            try:
                while process.poll() is None:
                    sample_linux_tree(process.pid, samples)
                    time.sleep(0.1)
            except BaseException:
                if os.name != 'nt':
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                try:
                    process.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    if os.name != 'nt':
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait()
                raise
        code = process.returncode
    finally:
        elapsed = time.monotonic() - start
    after = cpu_usage()
    metrics = {'command': command, 'wall_seconds': elapsed, 'exit_code': code,
               'logical_cpus': os.cpu_count(), 'linux_process_samples': samples,
               'process_samples_are_lower_bounds': True}
    if before is not None:
        user, system = after[0] - before[0], after[1] - before[1]
        metrics.update(user_seconds=user, system_seconds=system, cpu_seconds=user + system,
                       normalized_cpu_percent=100 * (user + system) / elapsed / os.cpu_count())
    if backend == 'REDIS':
        log_text = (output / (name + '.log')).read_text()
        metrics['redis'] = {key.lower(): re.findall(r'RED_OHC_REDIS_' + key + r'=([^\r\n]+)', log_text)
                            for key in ['VERSION', 'MAXMEMORY', 'PID']}
    (output / (name + '.process.json')).write_text(json.dumps(metrics, indent=2) + '\n')
    if code:
        raise RuntimeError('benchmark failed; inspect ' + name + '.log')
    if backend == 'REDIS' and any(not values for values in metrics['redis'].values()):
        raise RuntimeError('missing owned Redis server metadata')
    results = json.loads((output / (name + '.jmh.json')).read_text())
    if not results:
        raise RuntimeError('JMH produced no measurement')
    for result in results:
        secondary = result['secondaryMetrics']
        if secondary['errors']['score'] or (args.scenario == 'RESIDENT' and secondary['reportedRejections']['score']):
            raise RuntimeError('unexpected error/rejection; inspect raw results')
        attempted = secondary['attempted']['score']
        completed = secondary['completed']['score']
        if abs(attempted - completed) > max(1e-6, attempted * 1e-9):
            raise RuntimeError('attempted/completed rates do not reconcile')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--backends', nargs='+', choices=BACKENDS, default=list(BACKENDS))
    parser.add_argument('--threads', nargs='+', type=int, default=[1, 8, 32])
    parser.add_argument('--workloads', nargs='+', choices=['READ_100', 'WRITE_100', 'READ_90_WRITE_10'],
                        default=['READ_100', 'WRITE_100', 'READ_90_WRITE_10'])
    parser.add_argument('--scenario', choices=['RESIDENT', 'PRESSURE', 'TTL'], default='RESIDENT')
    parser.add_argument('--ttl-millis', type=int, default=0)
    parser.add_argument('--key-bytes', type=int, default=32)
    parser.add_argument('--value-bytes', type=int, default=5120)
    parser.add_argument('--capacity-bytes', type=int, default=256 * 1024 * 1024)
    parser.add_argument('--distribution', choices=['UNIFORM', 'ZIPF_099'], default='UNIFORM')
    parser.add_argument('--consumption', choices=['MATERIALIZE', 'FULL_SCAN'], default='MATERIALIZE')
    parser.add_argument('--jvm-args', default='-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g')
    args = parser.parse_args()
    if any(n < 1 for n in args.threads) or args.key_bytes < 8 or args.value_bytes < 1:
        parser.error('threads/value sizes must be positive; keys need at least 8 bytes')
    if (args.scenario == 'TTL') != (args.ttl_millis > 0):
        parser.error('positive TTL is required only for the TTL group')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    backends = [b for b in args.backends if b != 'CHRONICLE' or args.scenario == 'RESIDENT']
    plan = [(round_index, backend, threads, workload)
            for round_index in range(1, 6) for threads in args.threads for workload in args.workloads
            for backend in random.Random(7 + round_index).sample(backends, len(backends))]
    print(json.dumps({'runs': len(plan), 'order': plan, 'execute': args.run}, indent=2))
    version = project_version()
    if args.prepare:
        prepare(output, backends, version)
    if not args.run:
        return
    receipt, jars = checked_receipt(output, backends, version)
    manifest = source_identity() | {'version': version, 'compiler_jdk': receipt['compiler_jdk'],
                'runtime_jdk': java_version(), 'platform': platform.platform(), 'machine': platform.machine(),
                'logical_cpus': os.cpu_count(), 'options': vars(args) | {'output': str(output)},
                'jars': jars, 'order': plan, 'module_access_options': benchmark_module_options()}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    for row in plan:
        run_one(output, args, version, *row)


if __name__ == '__main__':
    main()
