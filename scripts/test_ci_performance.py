"""Small deterministic protocol fixtures, never a local Java benchmark."""
import copy
import math
from pathlib import Path
import unittest

import ci_performance as perf


def result(values=None):
    values = values or [100.] * 5
    def metric(raw):
        return {'score': sum(raw) / 5, 'rawData': [raw], 'scoreUnit': 'ops/s'}
    counters = {name: metric([0.] * 5) for name in
                ('rejectedWrites', 'exceptionCount', 'incompleteOperations')}
    counters.update(attemptedOperations=metric(values.copy()),
                    completedOperations=metric(values.copy()),
                    attemptedWrites=metric([10.] * 5), acceptedWrites=metric([10.] * 5))
    return {'benchmark': 'com.red.ohc.jmh.OHCSerializedBenchmark.oneThread',
            'mode': 'thrpt', 'threads': 1, 'forks': 1, 'jdkVersion': '21.0.10',
            'jvm': '/jdk/bin/java', 'jvmArgs': perf.FORK_ARGS,
            'warmupIterations': 3, 'warmupTime': '5 s',
            'measurementIterations': 5, 'measurementTime': '8 s',
            'params': {'keyBytes': '32', 'valueBytes': '5120', 'workload': 'READ_100',
                       'distribution': 'UNIFORM', 'writeShape': 'MIXED'},
            'primaryMetric': metric(values), 'secondaryMetrics': counters}


def windows(cpu=2_000_000_000, wall=1_000_000_000, known=True):
    return ('measurement-window: wallNanos=%s, processCpuNanos=%s, '
            'actorCpuNanos=100000000, processCpuKnown=%s, actorCpuKnown=true\n' %
            (wall, cpu, str(known).lower())) * 5


def records(ratio=1., cpu_ratio=1., pairs=5):
    rows = []
    for number in range(1, pairs + 1):
        for label, factor, cpu in [('BASE', 1., .02), ('NEW', ratio, .02 * cpu_ratio)]:
            rows.append({'round': number, 'label': label, 'throughput': 100. * factor,
                         'cpu_per_op': cpu, 'quality': []})
    return rows


class PolicyTest(unittest.TestCase):
    def test_parallel_loads_use_the_runner_cpu_count(self):
        self.assertEqual([(c['name'], c['threads'], c['workload']) for c in perf.cases(4)],
                         [('read_t1', 1, 'READ_100'), ('write_t1', 1, 'WRITE_100'),
                          ('read_parallel', 4, 'READ_100'), ('write_parallel', 4, 'WRITE_100'),
                          ('mix_parallel', 4, 'READ_90_WRITE_10')])
        self.assertTrue(all(c['threads'] == 4 for c in perf.cases(4)[2:]))

    def test_cpu_count_is_an_explicit_input_without_reserving_or_capping_cores(self):
        import inspect
        self.assertIn('logical_cpus', inspect.signature(perf.cases).parameters)
        for cpus in (1, 2, 3, 4, 8, 64):
            self.assertEqual([c['threads'] for c in perf.cases(cpus)], [1, 1, cpus, cpus, cpus])

    def test_longer_warmup_and_measurement_are_used_in_both_batches(self):
        for batch, warmup in [('screening', '5s'), ('confirmation', '10s')]:
            args = perf.command('/jdk/bin/java', ['h.jar', 'core.jar'], perf.cases(4)[0],
                                batch, Path('result.json'))
            self.assertEqual(args[args.index('-wi') + 1], '3')
            self.assertEqual(args[args.index('-w') + 1], warmup)
            self.assertEqual(args[args.index('-i') + 1], '5')
            self.assertEqual(args[args.index('-r') + 1], '8s')

    def test_only_documents_images_and_community_content_can_skip(self):
        self.assertTrue(perf.documentation_only(['README.md', 'docs/a.md', 'docs/img/a.png',
                                                 'LICENSE', '.github/ISSUE_TEMPLATE/bug.yml']))
        for files in [[], ['docs/run.py'], ['pom.xml'], ['.github/workflows/ci.yml'],
                      ['README.md', 'red-ohc-core/src/X.java'], ['.github/CODEOWNERS']]:
            self.assertFalse(perf.documentation_only(files))

    def test_pr_and_dispatch_selection_keeps_main_correctness_jobs(self):
        self.assertEqual(perf.selection('push', '21', False, ['pom.xml'], 'off'),
                         (False, 'main push'))
        self.assertEqual(perf.selection('pull_request', '21', False, ['pom.xml'], 'off')[0], True)
        self.assertFalse(perf.selection('pull_request', '11', False, ['pom.xml'], 'off')[0])
        self.assertFalse(perf.selection('pull_request', '21', True, ['pom.xml'], 'off')[0])
        self.assertFalse(perf.selection('pull_request', '21', False, ['README.md'], 'off')[0])
        for jdk in ['11', '17', '21', '25']:
            self.assertTrue(perf.selection('workflow_dispatch', jdk, False, [], 'all-jdks')[0])
            self.assertEqual(perf.selection('workflow_dispatch', jdk, False, [], 'aa-jdk21')[0],
                             jdk == '21')
        self.assertFalse(perf.selection('workflow_dispatch', '21', False, [], 'off')[0])
        with self.assertRaises(ValueError):
            perf.selection('workflow_dispatch', '21', False, [], 'unexpected')

    def test_pairs_are_adjacent_and_alternate_in_each_independent_batch(self):
        rows = list(perf.batch_plan(perf.cases(4)[0], 'screening'))
        self.assertEqual(len(rows), 10)
        self.assertEqual([r['label'] for r in rows[:6]], ['BASE', 'NEW', 'NEW', 'BASE', 'BASE', 'NEW'])
        self.assertEqual(len(list(perf.batch_plan(perf.cases(4)[0], 'confirmation'))), 6)


class MeasurementTest(unittest.TestCase):
    def test_cpu_uses_each_completed_rate_times_corresponding_window(self):
        parsed = perf.measurement([result([100., 200., 300., 400., 500.])], windows(),
                                  perf.cases(4)[0], 'screening', '/jdk/bin/java', 21)
        self.assertAlmostEqual(parsed['cpu_per_op'], 10. / 1500.)
        self.assertEqual(parsed['throughput'], 300.)
        self.assertTrue(parsed['quality'])

    def test_missing_cpu_is_na_and_missing_result_is_a_real_error(self):
        parsed = perf.measurement([result()], windows(known=False), perf.cases(4)[0],
                                  'screening', '/jdk/bin/java', 21)
        self.assertIsNone(parsed['cpu_per_op'])
        for data, log in [([], windows()), ([result()], windows()[:-100])]:
            with self.assertRaises((ValueError, RuntimeError)):
                perf.measurement(data, log, perf.cases(4)[0], 'screening', '/jdk/bin/java', 21)

    def test_jvm_unknown_cpu_sentinel_is_minus_one_not_zero(self):
        log = windows(cpu=-1, known=False).replace('actorCpuNanos=100000000', 'actorCpuNanos=-1')
        log = log.replace('actorCpuKnown=true', 'actorCpuKnown=false')
        parsed = perf.measurement([result()], log, perf.cases(4)[0],
                                  'screening', '/jdk/bin/java', 21)
        self.assertIsNone(parsed['cpu_per_op'])
        self.assertIsNone(parsed['measurement_windows'][0]['actor_cpu_seconds'])

    def test_per_iteration_mismatch_rejection_and_nonfinite_values_fail(self):
        for name, raw in [('completedOperations', [90., 110., 100., 100., 100.]),
                          ('rejectedWrites', [0., 0., 1., 0., 0.]),
                          ('acceptedWrites', [float('nan')] * 5)]:
            bad = result()
            bad['secondaryMetrics'][name]['rawData'][0] = raw
            with self.subTest(name=name), self.assertRaises((ValueError, RuntimeError)):
                perf.measurement([bad], windows(), perf.cases(4)[0],
                                 'screening', '/jdk/bin/java', 21)

    def test_wrong_fork_annotation_override_and_partial_results_fail(self):
        for change in [{'jvmArgs': perf.FORK_ARGS + ['-Xmx1g']}, {'threads': 8},
                       {'warmupTime': '2 s'}, {'forks': 2}, {'jdkVersion': '11.0.30'}]:
            with self.subTest(change=change), self.assertRaises((ValueError, RuntimeError)):
                perf.measurement([result() | change], windows(), perf.cases(4)[0],
                                 'screening', '/jdk/bin/java', 21)

    def test_iteration_noise_and_drift_use_five_percent_strict_boundary(self):
        for raw, quality in [([95., 105., 100., 95., 105.], []),
                             ([100., 100., 102.5, 105., 105.], []),
                             ([94.99, 105.01, 100., 94.99, 105.01], ['CV']),
                             ([100., 100., 102.5, 105.01, 105.01], ['drift'])]:
            with self.subTest(raw=raw):
                observed = perf.measurement([result(raw)], windows(), perf.cases(4)[0],
                                            'screening', '/jdk/bin/java', 21)['quality']
                self.assertEqual(len(observed), len(quality))
                for expected in quality:
                    self.assertTrue(any(expected in message for message in observed))

    def test_controller_is_small_and_annotation_arguments_are_cleared(self):
        args = perf.command('/jdk/bin/java', ['h.jar', 'core.jar'], perf.cases(4)[1],
                            'screening', Path('result.json'))
        self.assertEqual(args[:3], ['/jdk/bin/java', '-Xms64m', '-Xmx256m'])
        self.assertEqual(args[args.index('-jvm') + 1], '/jdk/bin/java')
        self.assertEqual(args[args.index('-jvmArgs') + 1],
                         '-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC')
        self.assertEqual(args[args.index('-jvmArgsAppend') + 1], '')
        self.assertEqual(args[args.index('-jvmArgsPrepend') + 1], '')
        self.assertEqual(args[args.index('-t') + 1], '1')


class AnalysisTest(unittest.TestCase):
    def test_thresholds_are_inclusive_in_both_directions(self):
        for ratio, expected in [(1.0999, 'STABLE'), (1.10, 'CHANGE'), (1.1001, 'CHANGE'),
                                (.9001, 'STABLE'), (.90, 'CHANGE'), (.8999, 'CHANGE')]:
            with self.subTest(ratio=ratio):
                summary = perf.analyze(records(ratio), 5)
                self.assertEqual(summary['throughput']['verdict'], expected)
                self.assertEqual(summary['needs_confirmation'], expected != 'STABLE')

    def test_cpu_direction_is_inverse_to_throughput(self):
        summary = perf.analyze(records(cpu_ratio=1.1), 5)
        self.assertEqual(summary['cpu_per_op']['direction'], 'regression')
        summary = perf.analyze(records(cpu_ratio=.9), 5)
        self.assertEqual(summary['cpu_per_op']['direction'], 'improvement')
        self.assertEqual(perf.analyze(records(1.1), 5)['throughput']['direction'], 'improvement')

    def test_independent_fork_pair_interval_not_iteration_pooling(self):
        rows = records()
        for index, ratio in enumerate([.9, 1., 1.1, 1.2, 1.3]):
            rows[index * 2 + 1]['throughput'] *= ratio
        stat = perf.analyze(rows, 5)['throughput']
        # (0.9 * 1.0 * 1.1 * 1.2 * 1.3) ** (1/5).
        self.assertAlmostEqual(stat['ratio'], 1.0908171531, places=8)
        self.assertLess(stat['interval'][0], 1.)
        self.assertGreater(stat['interval'][1], 1.1)
        self.assertEqual(stat['verdict'], 'INCONCLUSIVE')
        self.assertEqual(stat['pairs'], 5)

    def test_zero_variance_cpu_na_and_noise_are_not_hidden(self):
        rows = records()
        for row in rows:
            row['cpu_per_op'] = None
        summary = perf.analyze(rows, 5)
        self.assertEqual(summary['throughput']['interval'], [1., 1.])
        self.assertEqual(summary['cpu_per_op']['verdict'], 'N/A')
        self.assertFalse(summary['needs_confirmation'])
        rows[0]['quality'] = ['iteration CV exceeds 5%']
        self.assertTrue(perf.analyze(rows, 5)['needs_confirmation'])

    def test_partial_and_duplicate_pairs_are_real_errors(self):
        for rows in [records()[:-1], records() + [records()[0]]]:
            with self.assertRaises(ValueError):
                perf.analyze(rows, 5)

    def test_confirmation_is_separate_and_unreproduced_signal_is_inconclusive(self):
        initial = perf.analyze(records(1.2), 5)
        confirmed = perf.analyze(records(1., pairs=3), 3)
        self.assertEqual(perf.final_verdict(initial, confirmed), 'INCONCLUSIVE')
        confirmed = perf.analyze(records(1.2, pairs=3), 3)
        self.assertEqual(perf.final_verdict(initial, confirmed), 'WARNING')
        self.assertEqual(perf.final_verdict(perf.analyze(records(), 5), None), 'STABLE')


if __name__ == '__main__':
    unittest.main()
