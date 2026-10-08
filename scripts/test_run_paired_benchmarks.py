"""Protocol tests only: no Java processes, native allocation or benchmarks."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from unittest.mock import Mock
import zipfile

import run_paired_benchmarks as paired


def measurement():
    def metric(values):
        return {'score': sum(values) / len(values), 'rawData': [values]}
    metrics = {name: metric([0.] * 5) for name in
               ('exceptionCount', 'incompleteOperations', 'rejectedWrites')}
    metrics.update(attemptedOperations=metric([100.] * 5),
                   completedOperations=metric([100.] * 5),
                   attemptedWrites=metric([10.] * 5),
                   acceptedWrites=metric([10.] * 5))
    return [{'primaryMetric': metric([100.] * 5), 'secondaryMetrics': metrics}]


class ResultsTest(unittest.TestCase):
    def test_valid_measurement(self):
        paired.validate_results(measurement())

    def test_per_iteration_mismatch_cannot_cancel_in_the_mean(self):
        results = measurement()
        results[0]['secondaryMetrics']['completedOperations']['rawData'][0][:2] = [90., 110.]
        with self.assertRaisesRegex(RuntimeError, 'reconcile'):
            paired.validate_results(results)

    def test_errors_rejections_and_non_finite_counters_are_rejected(self):
        for name, value in [('exceptionCount', 1.), ('rejectedWrites', 1.),
                            ('incompleteOperations', 1.), ('acceptedWrites', float('nan'))]:
            with self.subTest(name=name):
                results = measurement()
                results[0]['secondaryMetrics'][name]['rawData'][0][0] = value
                with self.assertRaises(RuntimeError):
                    paired.validate_results(results)

    def test_unaccepted_write_is_rejected(self):
        results = measurement()
        results[0]['secondaryMetrics']['acceptedWrites']['rawData'][0][0] = 9.
        with self.assertRaisesRegex(RuntimeError, 'reconcile'):
            paired.validate_results(results)

    def test_partial_fork_and_empty_results_are_rejected(self):
        partial = measurement()
        partial[0]['secondaryMetrics']['completedOperations']['rawData'][0].pop()
        for results in ([], partial):
            with self.assertRaises(RuntimeError):
                paired.validate_results(results)

    def test_cpu_uses_measurement_windows_instead_of_whole_process_time(self):
        line = ('measurement-window: wallNanos=1000000000, processCpuNanos=2000000000, '
                'actorCpuNanos=100000000, processCpuKnown=true, actorCpuKnown=true\n')
        metric = paired.measurement_cpu(line * 5, 100.)
        self.assertEqual(metric['measured_process_cpu_seconds'], 10.)
        self.assertEqual(metric['estimated_cpu_seconds_per_completed_operation'], .02)
        with self.assertRaises(RuntimeError):
            paired.measurement_cpu(line * 4, 100.)

    def test_unknown_cpu_is_not_reported_as_zero_cost(self):
        line = ('measurement-window: wallNanos=1000000000, processCpuNanos=0, '
                'actorCpuNanos=0, processCpuKnown=false, actorCpuKnown=false\n')
        metric = paired.measurement_cpu(line * 5, 100.)
        self.assertIsNone(metric['estimated_cpu_seconds_per_completed_operation'])


class ProtocolTest(unittest.TestCase):
    @unittest.skipUnless(os.name == 'posix', 'process groups require POSIX')
    def test_cleanup_escalates_for_a_fork_after_controller_exit(self):
        child = subprocess.Popen(
            [sys.executable, '-c',
             'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
             'print("ready", flush=True); time.sleep(30)'],
            stdout=subprocess.PIPE, text=True, start_new_session=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            controller = Mock(pid=child.pid)
            controller.wait.return_value = 0
            paired.stop_process(controller, grace_seconds=.1)
            self.assertNotEqual(child.wait(timeout=5), 0)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()

    def test_each_pair_is_adjacent_and_order_alternates(self):
        rows = list(paired.plan())
        self.assertEqual(len(rows), 640)
        for left, right in zip(rows[::2], rows[1::2]):
            identity = {key: value for key, value in left.items() if key != 'label'}
            self.assertEqual(identity, {key: value for key, value in right.items() if key != 'label'})
            self.assertEqual([left['label'], right['label']],
                             ['BASE', 'NEW'] if left['round'] % 2 else ['NEW', 'BASE'])
        self.assertEqual({row['jdk'] for row in rows}, {11, 17, 21, 25})

    def test_non_linux_execution_is_rejected(self):
        with patch.object(paired.platform, 'system', return_value='Darwin'):
            with self.assertRaisesRegex(RuntimeError, 'Linux x86-64'):
                paired.require_linux()

    def test_jmh_command_uses_selected_jdk_for_controller_and_fork(self):
        row = next(paired.plan())
        command = paired.command('/java11', {'first.jar': 'sha'}, row, Path('result.json'), 'args')
        self.assertEqual(command[0], '/java11')
        self.assertEqual(command[command.index('-jvm') + 1], '/java11')
        self.assertEqual(command[command.index('-p') + 1], 'keyBytes=32')

    def test_custom_heap_replaces_annotation_defaults(self):
        options = '-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC'
        command = paired.command('/java11', {'first.jar': 'sha'}, next(paired.plan()),
                                 Path('result.json'), options)
        self.assertEqual(command[command.index('-jvmArgs') + 1], options)
        self.assertIn('-jvmArgsAppend', command)
        self.assertEqual(command[command.index('-jvmArgsAppend') + 1], '')
        self.assertIn('-jvmArgsPrepend', command)
        self.assertEqual(command[command.index('-jvmArgsPrepend') + 1], '')

    def test_actual_fork_metadata_must_match_requested_settings(self):
        row = next(paired.plan())
        options = '-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC'
        result = {'benchmark': 'com.red.ohc.jmh.OHCSerializedBenchmark.oneThread',
                  'threads': 1, 'forks': 1, 'jdkVersion': '11.0.31', 'jvm': '/java11',
                  'jvmArgs': options.split(), 'primaryMetric': {'scoreUnit': 'ops/s'},
                  'params': {'keyBytes': '32', 'valueBytes': '5120', 'workload': 'READ_100',
                             'distribution': 'UNIFORM', 'writeShape': 'MIXED'}}
        paired.validate_fork_configuration(result, row, '/java11', options)
        for changed in ({'jvmArgs': options.split() + ['-Xmx1g']}, {'jdkVersion': '21.0.12'},
                        {'jvm': '/java21'}, {'threads': 8}, {'forks': 2}):
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(RuntimeError, 'configuration'):
                    paired.validate_fork_configuration(result | changed, row, '/java11', options)
        result['params']['writeShape'] = 'REPLACE_ONLY'
        with self.assertRaisesRegex(RuntimeError, 'parameters'):
            paired.validate_fork_configuration(result, row, '/java11', options)

    def test_tampered_prepared_artifact_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            jar = output / 'a.jar'
            jar.write_bytes(b'original')
            receipt = {'base': paired.BASE, 'new': 'new-sha', 'runner_head': 'head-sha',
                       'artifacts': {'BASE': {str(jar): paired.digest(jar)},
                                     'NEW': {str(jar): paired.digest(jar)}}}
            (output / 'build-receipt.json').write_text(json.dumps(receipt))
            with patch.object(paired, 'git', side_effect=['', 'head-sha']):
                paired.checked_receipt(output, 'new-sha')
            jar.write_bytes(b'changed')
            with patch.object(paired, 'git', side_effect=['', 'head-sha']):
                with self.assertRaisesRegex(RuntimeError, 'bytes changed'):
                    paired.checked_receipt(output, 'new-sha')

    def test_application_bytecode_must_be_java11(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'test.jar'
            for major in (55, 61):
                with zipfile.ZipFile(path, 'w') as jar:
                    jar.writestr('com/red/ohc/Sample.class', struct.pack('>IHH', 0xcafebabe, 0, major))
                if major == 55:
                    paired.check_java11_jar(path)
                else:
                    with self.assertRaisesRegex(RuntimeError, 'Java 11'):
                        paired.check_java11_jar(path)

    def test_incomplete_pair_is_not_marked_complete(self):
        row = next(paired.plan()) | {'throughput': 100., 'cpu_seconds': 2.,
                                     'estimated_cpu_seconds_per_completed_operation': .02}
        summary = paired.paired_summary([row])
        self.assertFalse(summary[0]['complete'])
        self.assertIsNone(summary[0]['median_throughput_ratio'])
        other = copy.deepcopy(row)
        other.update(label='NEW', throughput=110.)
        summary = paired.paired_summary([row, other])
        self.assertFalse(summary[0]['complete'])
        self.assertAlmostEqual(summary[0]['median_throughput_ratio'], 1.1)


if __name__ == '__main__':
    unittest.main()
