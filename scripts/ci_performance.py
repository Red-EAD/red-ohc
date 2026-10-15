"""CI pairing policy, JMH contracts and per-fork statistics (no process launch)."""
import math
import os
from pathlib import PurePosixPath
import re
import statistics

from run_paired_benchmarks import validate_results

FORK_ARGS = ['-Xms2g', '-Xmx2g', '-XX:MaxDirectMemorySize=2g', '-XX:+UseG1GC']
CONTROLLER_ARGS = ['-Xms64m', '-Xmx256m']
BENCHMARK = 'com.red.ohc.jmh.OHCSerializedBenchmark.'
MODES = ('off', 'jdk21', 'all-jdks', 'aa-jdk21')
SAMPLING = {
    'screening': {'pairs': 5, 'warmup_iterations': 3, 'warmup_seconds': 5,
                  'measurement_iterations': 5, 'measurement_seconds': 10},
    'confirmation': {'pairs': 3, 'warmup_iterations': 3, 'warmup_seconds': 10,
                     'measurement_iterations': 5, 'measurement_seconds': 10},
}


def cases():
    return [dict(name=name, method=method, threads=threads, workload=workload) for
            name, method, threads, workload in (
                ('read_t1', 'oneThread', 1, 'READ_100'),
                ('write_t1', 'oneThread', 1, 'WRITE_100'),
                ('read_parallel', 'cpuThreads', 4, 'READ_100'),
                ('write_parallel', 'cpuThreads', 4, 'WRITE_100'),
                ('mix_parallel', 'cpuThreads', 4, 'READ_90_WRITE_10'))]


def documentation_only(paths):
    community = {'LICENSE', 'NOTICE', 'AUTHORS', 'CONTRIBUTING.md', 'SECURITY.md',
                 'CODE_OF_CONDUCT.md', 'README.md', 'README.zh-CN.md'}
    def allowed(path):
        p = PurePosixPath(path)
        if path in community:
            return True
        if path.startswith('docs/') and p.suffix.lower() in (
                '.md', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg'):
            return True
        return path.startswith('.github/ISSUE_TEMPLATE/') and p.suffix in ('.md', '.yml', '.yaml')
    return bool(paths) and all(allowed(path) for path in paths)


def selection(event, jdk, draft, paths, mode):
    if mode not in MODES:
        raise ValueError('unknown performance mode: ' + mode)
    if event == 'push':
        return False, 'main push'
    if event == 'pull_request':
        if draft:
            return False, 'draft PR'
        if documentation_only(paths):
            return False, 'documentation only'
        return str(jdk) == '21', 'PR JDK 21' if str(jdk) == '21' else 'non-measurement JDK'
    if event == 'workflow_dispatch':
        selected = mode == 'all-jdks' or (mode in ('jdk21', 'aa-jdk21') and str(jdk) == '21')
        return selected, 'manual ' + mode
    return False, 'unsupported event'


def instrumentation_changed(paths):
    return any(path.startswith(('scripts/', 'red-ohc-jmh/', 'tools/benchmark-runtime/',
                                '.github/workflows/')) for path in paths)


def batch_plan(case, batch):
    count = SAMPLING[batch]['pairs']
    for number in range(1, count + 1):
        order = ('BASE', 'NEW') if number % 2 else ('NEW', 'BASE')
        for label in order:
            yield case | {'batch': batch, 'round': number, 'label': label}


def command(java, jars, case, batch, result_file):
    timing = SAMPLING[batch]
    return [java, *CONTROLLER_ARGS, '-cp', os.pathsep.join(map(str, jars)),
            'org.openjdk.jmh.Main', '^' + re.escape(BENCHMARK + case['method']) + '$',
            '-jvm', java, '-f', '1', '-wi', str(timing['warmup_iterations']),
            '-w', str(timing['warmup_seconds']) + 's',
            '-i', str(timing['measurement_iterations']),
            '-r', str(timing['measurement_seconds']) + 's', '-t', str(case['threads']),
            '-p', 'keyBytes=32', '-p', 'valueBytes=5120', '-p', 'workload=' + case['workload'],
            '-p', 'distribution=UNIFORM', '-p', 'writeShape=MIXED',
            '-jvmArgs', ' '.join(FORK_ARGS), '-jvmArgsPrepend', '', '-jvmArgsAppend', '',
            '-foe', 'true', '-rf', 'json', '-rff', str(result_file)]


def measurement(results, log, case, batch, java, jdk):
    timing = SAMPLING[batch]
    throughput = validate_results(results)
    r = results[0]
    expected = {'benchmark': BENCHMARK + case['method'], 'mode': 'thrpt',
                'threads': case['threads'], 'forks': 1, 'jvm': java, 'jvmArgs': FORK_ARGS,
                'warmupIterations': timing['warmup_iterations'],
                'measurementIterations': timing['measurement_iterations'],
                'warmupTime': str(timing['warmup_seconds']) + ' s',
                'measurementTime': str(timing['measurement_seconds']) + ' s'}
    if any(r.get(k) != v for k, v in expected.items()):
        raise ValueError('effective fork configuration differs from the requested protocol')
    if re.match(r'\d+', r.get('jdkVersion', '')) is None or int(re.match(r'\d+', r['jdkVersion'])[0]) != jdk:
        raise ValueError('effective measurement JDK does not match')
    if r.get('params') != {'keyBytes': '32', 'valueBytes': '5120', 'workload': case['workload'],
                           'distribution': 'UNIFORM', 'writeShape': 'MIXED'}:
        raise ValueError('effective workload configuration differs')
    primary = r['primaryMetric']
    rates = r['secondaryMetrics']['completedOperations']['rawData'][0]
    values = primary['rawData']
    if (primary['scoreUnit'] != 'ops/s' or len(values) != 1 or len(values[0]) != 5
            or any(not math.isfinite(v) or v <= 0 for v in values[0])
            or not math.isclose(statistics.mean(values[0]), throughput, rel_tol=1e-9)):
        raise ValueError('invalid throughput iterations')
    for metric in r['secondaryMetrics'].values():
        # AuxCounters(Type.OPERATIONS) are rates. Never treat them as raw counts.
        if metric['scoreUnit'] != 'ops/s':
            raise ValueError('auxiliary operation counters must be rates in ops/s')
    if any(rate <= 0 for rate in rates):
        raise ValueError('no completed operations in a measurement iteration')
    matches = re.findall(r'measurement-window: wallNanos=(-?\d+), processCpuNanos=(-?\d+), '
                         r'actorCpuNanos=(-?\d+), processCpuKnown=(true|false), '
                         r'actorCpuKnown=(true|false)', log)
    if len(matches) != 5:
        raise ValueError('expected exactly five measurement CPU windows')
    windows = []
    for wall, cpu, actor, known, actor_known in matches:
        if int(wall) <= 0:
            raise ValueError('measurement wall duration must be positive')
        windows.append({'wall_seconds': int(wall) / 1e9,
                        'process_cpu_seconds': int(cpu) / 1e9 if known == 'true' and int(cpu) > 0 else None,
                        'actor_cpu_seconds': int(actor) / 1e9 if actor_known == 'true' else None})
    cpu_per_op = None
    if all(w['process_cpu_seconds'] is not None for w in windows):
        estimated_operations = sum(rate * w['wall_seconds'] for rate, w in zip(rates, windows))
        cpu_per_op = sum(w['process_cpu_seconds'] for w in windows) / estimated_operations
    values = values[0]
    cv = statistics.stdev(values) / statistics.mean(values)
    drift = abs(statistics.mean(values[-2:]) / statistics.mean(values[:2]) - 1.)
    quality = []
    if cv > .05 and not math.isclose(cv, .05, abs_tol=1e-12, rel_tol=0):
        quality.append('iteration throughput CV exceeds 5%')
    if drift > .05 and not math.isclose(drift, .05, abs_tol=1e-12, rel_tol=0):
        quality.append('first/last two iteration drift exceeds 5%')
    return {'throughput': throughput, 'cpu_per_op': cpu_per_op, 'quality': quality,
            'iteration_cv': cv, 'iteration_drift': drift, 'measurement_windows': windows,
            'operation_rates': r['secondaryMetrics'], 'throughput_iterations': values}


def _outside(ratio):
    # Allow floating point round-trip error at the inclusive decimal thresholds.
    return (ratio >= 1.10 or ratio <= .90
            or math.isclose(ratio, 1.10, abs_tol=1e-12, rel_tol=0)
            or math.isclose(ratio, .90, abs_tol=1e-12, rel_tol=0))


def _stat(pairs, metric, noisy):
    values = [(pair['BASE'][metric], pair['NEW'][metric]) for pair in pairs]
    if any(base is None or new is None for base, new in values):
        return {'verdict': 'N/A', 'ratio': None, 'interval': None, 'pairs': len(pairs), 'direction': None}
    if any(not math.isfinite(v) or v <= 0 for pair in values for v in pair):
        raise ValueError('invalid fork metric: ' + metric)
    logs = [math.log(new / base) for base, new in values]
    mean = statistics.mean(logs)
    # Import statistical tooling only after a batch, never during a measured fork.
    from scipy.stats import t
    half = float(t.ppf(.975, len(logs) - 1)) * statistics.stdev(logs) / math.sqrt(len(logs))
    ratio = math.exp(mean)
    low, high = math.exp(mean - half), math.exp(mean + half)
    crosses = (low < .90 < high or low < 1.10 < high) and not math.isclose(low, high, rel_tol=1e-12)
    verdict = 'INCONCLUSIVE' if noisy or crosses else 'CHANGE' if _outside(ratio) else 'STABLE'
    direction = 'improvement' if (ratio > 1.) == (metric == 'throughput') else 'regression'
    return {'ratio': ratio, 'interval': [low, high], 'pairs': len(logs),
            'base_geomean': math.exp(statistics.mean(math.log(base) for base, _ in values)),
            'new_geomean': math.exp(statistics.mean(math.log(new) for _, new in values)),
            'verdict': verdict, 'direction': direction if _outside(ratio) else None}


def analyze(records, expected_pairs):
    rounds = {}
    for r in records:
        key, label = r['round'], r['label']
        if label not in ('BASE', 'NEW') or label in rounds.setdefault(key, {}):
            raise ValueError('duplicate/invalid fork pair')
        rounds[key][label] = r
    if set(rounds) != set(range(1, expected_pairs + 1)) or any(set(p) != {'BASE', 'NEW'} for p in rounds.values()):
        raise ValueError('partial result batch')
    noisy = any(r['quality'] for r in records)
    stats = {metric: _stat(list(rounds.values()), metric, noisy)
             for metric in ('throughput', 'cpu_per_op')}
    return stats | {'needs_confirmation': any(s['verdict'] in ('CHANGE', 'INCONCLUSIVE') for s in stats.values()),
                    'quality': sorted({q for r in records for q in r['quality']})}


def final_verdict(screening, confirmation):
    if not screening['needs_confirmation']:
        return 'STABLE'
    if confirmation is None:
        raise ValueError('required confirmation batch is missing')
    for metric in ('throughput', 'cpu_per_op'):
        first, second = screening[metric], confirmation[metric]
        if first['verdict'] == 'INCONCLUSIVE' or second['verdict'] == 'INCONCLUSIVE':
            return 'INCONCLUSIVE'
        if first['verdict'] == 'CHANGE' and (second['verdict'] != 'CHANGE' or first['direction'] != second['direction']):
            return 'INCONCLUSIVE'
        if first['verdict'] in ('N/A', 'STABLE') and second['verdict'] == 'CHANGE':
            return 'INCONCLUSIVE'
    return 'WARNING'
