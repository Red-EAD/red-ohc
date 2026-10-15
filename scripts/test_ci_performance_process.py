"""Owned process cleanup and memory sampling with tiny Python children."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

import psutil

import ci_performance_process as monitor


class HostProcessTest(unittest.TestCase):
    def setUp(self):
        platform = patch.object(sys, 'platform', 'linux')
        platform.start()
        self.addCleanup(platform.stop)

    @staticmethod
    def process(pid, born, cpu, name='background'):
        return SimpleNamespace(info={'pid': pid, 'create_time': born, 'name': name,
                                     'cpu_times': SimpleNamespace(user=cpu, system=0.)})

    def test_background_cpu_excludes_owned_tree_and_observer_without_command_lines(self):
        clock = [0.]
        with patch.object(monitor.psutil, 'process_iter') as scan, \
                patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]):
            sampler = monitor.HostProcessSampler()
            scan.return_value = [self.process(10, 1., 20.), self.process(20, 2., 10., 'indexer'),
                                 self.process(os.getpid(), 3., 30., 'observer')]
            self.assertIsNone(sampler.sample(0., {(10, 1.)})['outside_cpu_cores'])
            scan.return_value = [self.process(10, 1., 60.), self.process(20, 2., 30., 'indexer'),
                                 self.process(os.getpid(), 3., 40., 'observer')]
            clock[0] = 10.
            measured = sampler.sample(10., {(10, 1.)})
            self.assertEqual(measured['outside_cpu_cores'], 2.)
            self.assertEqual(measured['top_processes'],
                             [{'pid': 20, 'created_at': 2., 'name': 'indexer', 'cpu_cores': 2., 'interval_seconds': 10.}])
            self.assertNotIn('cmdline', str(scan.call_args))
            self.assertGreaterEqual(measured['sampling_wall_seconds'], 0.)

    def test_windows_idle_pid_is_not_work_but_posix_kernel_pid_remains_visible(self):
        for platform_name, expected in [('nt', 1.), ('posix', 5.)]:
            clock = [0.]
            with self.subTest(platform=platform_name), \
                    patch.object(monitor.psutil, 'process_iter') as scan, \
                    patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]), \
                    patch.object(monitor.os, 'name', platform_name):
                sampler = monitor.HostProcessSampler()
                scan.return_value = [self.process(0, 0., 100., 'idle-or-kernel'),
                                     self.process(20, 1., 0., 'worker')]
                sampler.sample(0., set())
                clock[0] = 10.
                scan.return_value = [self.process(0, 0., 140., 'idle-or-kernel'),
                                     self.process(20, 1., 10., 'worker')]
                measured = sampler.sample(10., set())
                self.assertEqual(measured['outside_cpu_cores'], expected)
                self.assertEqual(any(p['pid'] == 0 for p in measured['top_processes']),
                                 platform_name == 'posix')

    def test_real_iterator_cache_is_refreshed_when_an_existing_pid_is_reused(self):
        state = {'born': 1., 'cpu': 100., 'now': 0.}
        class CachedProcess:
            def __init__(self, pid):
                self.pid = pid
                self.born = state['born']
            def as_dict(self, **kwargs):
                return {'pid': self.pid, 'create_time': self.born, 'name': 'worker',
                        'cpu_times': SimpleNamespace(user=state['cpu'], system=0.)}
        # Exercise the pinned library's real iterator/cache, with only OS reads replaced.
        monitor.psutil.process_iter.cache_clear()
        try:
            with patch.object(monitor.psutil, 'pids', return_value=[20]), \
                    patch.object(monitor.psutil, 'Process', CachedProcess), \
                    patch.object(monitor.time, 'monotonic', side_effect=lambda: state['now']):
                sampler = monitor.HostProcessSampler()
                sampler.sample(0., set())
                state.update(born=2., cpu=1000., now=10.)
                sample = sampler.sample(10., set())
                self.assertIsNone(sample['outside_cpu_cores'])
                self.assertEqual(sample['top_processes'], [])
        finally:
            monitor.psutil.process_iter.cache_clear()

    def test_cpu_denominator_uses_fresh_observation_time_after_slow_introspection(self):
        clock = [0.]
        with patch.object(monitor.psutil, 'process_iter') as scan, \
                patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]):
            sampler = monitor.HostProcessSampler()
            scan.return_value = [self.process(20, 2., 0.)]
            sampler.sample(0., set())
            clock[0] = 12.  # Caller retained elapsed=10 before a two-second OS query.
            scan.return_value = [self.process(20, 2., 24.)]
            result = sampler.sample(10., set())
            self.assertEqual(result['outside_cpu_cores'], 2.)
            self.assertEqual(result['top_processes'][0]['interval_seconds'], 12.)

    def test_streamed_counter_time_is_not_shifted_by_a_later_process_query(self):
        clock = [0.]
        def scan(**kwargs):
            yield self.process(20, 2., clock[0] * 2., 'observed first')
            if clock[0] > 0:
                clock[0] += 2.  # An unrelated later kernel query stalls.
            yield self.process(30, 3., 0., 'observed later')
        with patch.object(monitor.psutil, 'process_iter', side_effect=scan), \
                patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]):
            sampler = monitor.HostProcessSampler()
            sampler.sample(0., set())
            clock[0] = 10.
            sample = sampler.sample(10., set())
            self.assertEqual(sample['top_processes'][0]['interval_seconds'], 10.)
            self.assertEqual(sample['outside_cpu_cores'], 2.)

    def test_pid_reuse_new_and_unreadable_processes_never_invent_cpu_deltas(self):
        with patch.object(monitor.psutil, 'process_iter') as scan:
            sampler = monitor.HostProcessSampler()
            scan.return_value = [self.process(20, 1., 100.)]
            sampler.sample(0., set())
            unreadable = SimpleNamespace(info={'pid': 30, 'name': None, 'create_time': None, 'cpu_times': None})
            scan.return_value = [self.process(20, 2., 1000.), unreadable]
            sample = sampler.sample(10., set())
            self.assertIsNone(sample['outside_cpu_cores'])
            self.assertEqual(sample['unavailable_process_count'], 1)
            self.assertTrue(sample['coverage_is_partial'])
            self.assertTrue(sample['cpu_rates_are_estimates'])
            self.assertEqual(sample['top_processes'], [])
            scan.side_effect = psutil.AccessDenied(pid=1)
            sample = sampler.sample(20., set())
            self.assertIsNone(sample['outside_cpu_cores'])
            self.assertEqual(sample['error'], 'AccessDenied')

    def test_darwin_system_cpu_uses_native_read_only_counters_with_identity_validation(self):
        clock = [0.]
        process = self.process(20, 2., 0., 'system service')
        process.info['cpu_times'] = None  # task_for_pid is denied for system services.
        kernel = self.process(0, 0., 0., 'kernel_task')
        kernel.info['cpu_times'] = None
        with patch.object(sys, 'platform', 'darwin'), \
                patch.object(monitor.psutil, 'process_iter', return_value=[process, kernel]), \
                patch.object(monitor.psutil, 'Process', return_value=SimpleNamespace(create_time=lambda: 2.)), \
                patch.object(monitor.subprocess, 'check_output', side_effect=[
                    '20 1:00.00 system service\n', '20 1:20.00 system service\n']) as native, \
                patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]):
            sampler = monitor.HostProcessSampler()
            sampler.sample(0., set())
            clock[0] = 10.
            sample = sampler.sample(10., set())
            self.assertEqual(sample['outside_cpu_cores'], 2.)
            self.assertEqual(sample['top_processes'][0]['name'], 'system service')
            self.assertEqual(sample['unavailable_process_count'], 1)
            self.assertTrue(sample['coverage_is_partial'])
            self.assertEqual(native.call_args.args[0], ['/bin/ps', '-A', '-c', '-o', 'pid=,time=,comm='])
            self.assertLessEqual(native.call_args.kwargs['timeout'], 2.)

    def test_darwin_native_snapshot_rejects_pid_reuse_and_retains_optional_failure(self):
        process = self.process(20, 1., 0.)
        process.info['cpu_times'] = None
        with patch.object(sys, 'platform', 'darwin'), \
                patch.object(monitor.psutil, 'process_iter', return_value=[process]), \
                patch.object(monitor.psutil, 'Process', return_value=SimpleNamespace(create_time=lambda: 2.)), \
                patch.object(monitor.subprocess, 'check_output', return_value='20 1:00.00 worker\n') as native:
            sampler = monitor.HostProcessSampler()
            sampler.sample(0., set())
            sample = sampler.sample(10., set())
            self.assertIsNone(sample['outside_cpu_cores'])
            self.assertEqual(sample['unavailable_process_count'], 1)
            native.side_effect = subprocess.TimeoutExpired('ps', 2)
            sample = sampler.sample(20., set())
            self.assertIsNone(sample['outside_cpu_cores'])
            self.assertEqual(sample['error'], 'TimeoutExpired')

    def test_native_cpu_counter_units_and_invalid_values(self):
        output = '20 123:45.67 worker\n21 1:02:03.50 other service\n'
        with patch.object(monitor.subprocess, 'check_output', return_value=output):
            counters, _ = monitor.darwin_cpu_snapshot()
        self.assertEqual(counters, {20: 7425.67, 21: 3723.5})
        for invalid in ('20 NaN worker\n', '20 0:NaN worker\n', '20 -1:00 worker\n'):
            with self.subTest(invalid=invalid), \
                    patch.object(monitor.subprocess, 'check_output', return_value=invalid), \
                    self.assertRaises(ValueError):
                monitor.darwin_cpu_snapshot()


class TimeoutDiagnosticTest(unittest.TestCase):
    def test_only_live_owned_jvms_get_same_jdk_bounded_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            java = Path(directory) / 'java.exe'
            jcmd = Path(directory) / 'jcmd.exe'
            jcmd.touch()
            live = SimpleNamespace(is_running=lambda: True, cmdline=lambda: [str(java), '-version'])
            reused = SimpleNamespace(is_running=lambda: False,
                                     cmdline=lambda: self.fail('reused identity must not be inspected'))
            other = SimpleNamespace(is_running=lambda: True, cmdline=lambda: ['unrelated'])
            tree = SimpleNamespace(known={(10, 1.): live, (20, 2.): reused, (30, 3.): other})
            with patch.object(monitor, 'run_process', side_effect=RuntimeError('TIMEOUT: jcmd')) as launch:
                result = monitor.timeout_diagnostics(tree, Path(directory) / 'maven.log')
            self.assertEqual(len(result['targets']), 1)
            self.assertEqual(result['targets'][0]['pid'], 10)
            self.assertIn('TIMEOUT', result['targets'][0]['error'])
            self.assertEqual(launch.call_args.args[0], [str(jcmd), '10', 'Thread.print', '-l'])
            self.assertLessEqual(launch.call_args.args[3], 5.)
            self.assertFalse(launch.call_args.kwargs['diagnose_timeout'])

    def test_a_disappearing_non_jvm_does_not_rewrite_a_previous_diagnostic_result(self):
        with tempfile.TemporaryDirectory() as directory:
            java = Path(directory) / 'java'
            java.with_name('jcmd').touch()
            live = SimpleNamespace(is_running=lambda: True, cmdline=lambda: [str(java)])
            def vanished():
                raise psutil.NoSuchProcess(20)
            gone = SimpleNamespace(is_running=vanished)
            tree = SimpleNamespace(known={(20, 2.): gone, (10, 1.): live})
            with patch.object(monitor, 'run_process', return_value={}):
                result = monitor.timeout_diagnostics(tree, Path(directory) / 'maven.log')
            self.assertEqual(result['targets'][0]['error'], None)


class ProcessTest(unittest.TestCase):
    def test_deadline_is_rechecked_after_a_slow_sample_even_if_process_has_exited(self):
        clock = [0.]
        process = SimpleNamespace(pid=123456789, create_time=lambda: 1., poll=lambda: 0,
                                  returncode=0, close=lambda: None)
        target = ('ci_performance_windows.WindowsProcess' if os.name == 'nt'
                  else 'ci_performance_process.psutil.Popen')
        def sample(tree):
            clock[0] = 2.
            return 0
        with tempfile.TemporaryDirectory() as directory, patch(target, return_value=process), \
                patch.object(monitor.OwnedTree, 'sample', sample), \
                patch.object(monitor.OwnedTree, 'stop'), \
                patch.object(monitor.time, 'monotonic', side_effect=lambda: clock[0]), \
                self.assertRaisesRegex(RuntimeError, 'TIMEOUT'):
            monitor.run_process(['fake-java'], Path(directory), Path(directory) / 'slow.log', timeout=1.)

    def test_unreadable_command_is_unknown_metadata_without_aborting_owned_process(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(psutil.Process, 'cmdline', side_effect=psutil.AccessDenied(pid=1)):
            info = self.run_command('import time; print("ok"); time.sleep(.15)', directory)
            self.assertEqual(info['exit_code'], 0)
            self.assertGreater(info['sampled_peak_rss_bytes'], 0)
            self.assertTrue(info['processes'])
            for process in info['processes']:
                self.assertIsNone(process['command'])
                self.assertEqual(process['command_unavailable_reason'], 'access denied')
            self.assertIsNone(info['error'])
            self.assertIsNone(info['cleanup_error'])

    def test_primary_process_sampling_permission_errors_still_fail_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(psutil.Process, 'memory_info', side_effect=psutil.AccessDenied(pid=1)):
            with self.assertRaises(psutil.AccessDenied):
                self.run_command('import time; time.sleep(30)', directory)
            evidence = json.loads((Path(directory)/'child.process.json').read_text())
            self.assertIsNotNone(evidence['error'])
            self.assertIsNone(evidence['cleanup_error'])

    def test_expensive_sampling_is_one_hertz_and_observer_cpu_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            info = self.run_command('import time; time.sleep(1.2)', directory)
            self.assertEqual(info['sample_interval_seconds'], 1.)
            self.assertLessEqual(len(info['rss_samples']), 4)
            self.assertGreaterEqual(info['sampler_cpu_seconds'], 0.)
            self.assertEqual(info['background_sample_interval_seconds'], 10.)
            self.assertGreaterEqual(len(info['background_samples']), 2)
            self.assertTrue(all('sampling_wall_seconds' in s for s in info['background_samples']))
            self.assertLess(info['sampler_cpu_seconds'], info['wall_seconds'])

    def test_zero_duration_startup_failure_keeps_original_error_and_evidence(self):
        target = ('ci_performance_windows.WindowsProcess' if os.name == 'nt'
                  else 'ci_performance_process.psutil.Popen')
        with tempfile.TemporaryDirectory() as directory:
            with patch(target, side_effect=OSError('spawn failed')), \
                    patch.object(monitor.time, 'monotonic', return_value=1.):
                with self.assertRaisesRegex(OSError, 'spawn failed'):
                    self.run_command('pass', directory)
            evidence = json.loads((Path(directory) / 'child.process.json').read_text())
            self.assertEqual(evidence['wall_seconds'], 0)
            self.assertIsNone(evidence['sampled_normalized_cpu_percent'])
            self.assertEqual(evidence['error'], 'spawn failed')

    @unittest.skipUnless(os.name == 'nt', 'native Windows job contract')
    def test_job_assignment_failure_never_starts_the_controller(self):
        import win32job
        import win32process
        created = []
        original = win32process.CreateProcess
        def create(*args):
            handles = original(*args)
            created.append(handles[2])
            return handles
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'must-not-run'
            code = 'from pathlib import Path; import sys; Path(sys.argv[1]).touch()'
            with patch.object(win32process, 'CreateProcess', side_effect=create), \
                    patch.object(win32job, 'AssignProcessToJobObject', side_effect=RuntimeError('assignment failed')):
                with self.assertRaisesRegex(RuntimeError, 'assignment failed'):
                    monitor.run_process([sys.executable, '-c', code, str(marker)], Path(directory),
                                        Path(directory) / 'failed.log', timeout=3)
            self.assertFalse(marker.exists())
            self.assertEqual(len(created), 1)
            self.assertFalse(psutil.pid_exists(created[0]))

    def test_unobserved_child_is_stopped_after_its_controller_exits(self):
        # No explicit remember(child): reproduce the discovery gap on Windows.
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / 'unobserved.pid'
            code = ('import subprocess,sys,os; from pathlib import Path; '
                    'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"]); '
                    'Path(sys.argv[1]).write_text(str(p.pid)); os._exit(0)')
            child = None
            try:
                with patch.object(monitor.OwnedTree, 'sample', return_value=0):
                    monitor.run_process([sys.executable, '-c', code, str(pid_file)],
                                        Path(directory), Path(directory) / 'root.log', timeout=3)
                pid = int(pid_file.read_text())
                try:
                    child = psutil.Process(pid)
                except psutil.NoSuchProcess:
                    return
                deadline = time.monotonic() + 2
                while True:
                    try:
                        stopped = not child.is_running() or child.status() == psutil.STATUS_ZOMBIE
                    except psutil.NoSuchProcess:
                        stopped = True
                    if stopped or time.monotonic() >= deadline:
                        break
                    time.sleep(.01)
                self.assertTrue(stopped)
            finally:
                if child is not None:
                    try:
                        if child.is_running():
                            child.kill()
                        child.wait(timeout=2)
                    except psutil.NoSuchProcess:
                        pass
                    except psutil.TimeoutExpired:
                        pass  # A POSIX orphan zombie is reaped by the OS, not this test.

    def run_command(self, code, directory, **kwargs):
        return monitor.run_process([sys.executable, '-c', code], Path(directory),
                                   Path(directory) / 'child.log', timeout=3, **kwargs)

    def test_success_records_sampled_memory_cpu_and_wall(self):
        with tempfile.TemporaryDirectory() as directory:
            info = self.run_command('import time; print("ok"); time.sleep(.15)', directory)
            self.assertEqual(info['exit_code'], 0)
            self.assertGreater(info['sampled_peak_rss_bytes'], 0)
            self.assertGreater(info['wall_seconds'], .1)
            self.assertTrue(info['processes'])

    def test_host_cpu_and_load_samples_are_retained_for_load_diagnosis(self):
        from collections import namedtuple
        times = namedtuple('CpuTimes', 'user system idle iowait steal guest')
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(monitor.psutil, 'cpu_times', return_value=times(10, 2, 20, 3, 4, 5)), \
                patch.object(monitor.psutil, 'cpu_percent', return_value=75.), \
                patch.object(monitor.os, 'getloadavg', return_value=(2., 3., 4.), create=True):
            info = self.run_command('import time; time.sleep(.15)', directory)
            self.assertIn('host_samples', info)
            self.assertGreaterEqual(len(info['host_samples']), 2)
            for sample in info['host_samples']:
                self.assertEqual(sample['cpu_times'], dict(user=10, system=2, idle=20,
                                                         iowait=3, steal=4, guest=5))
                self.assertEqual(sample['load_average'], [2., 3., 4.])
            self.assertEqual(info['host_sample_interval_seconds'], 1.)
            self.assertIsNone(info['host_samples'][0]['cpu_busy_percent'])
            self.assertEqual(info['host_samples'][-1]['cpu_busy_percent'], 75.)

    def test_unavailable_host_diagnostics_are_null_and_do_not_abort(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(monitor.psutil, 'cpu_times', side_effect=NotImplementedError), \
                patch.object(monitor.psutil, 'cpu_percent', side_effect=NotImplementedError), \
                patch.object(monitor.os, 'getloadavg', side_effect=OSError, create=True):
            info = self.run_command('pass', directory)
            self.assertIn('host_samples', info)
            self.assertTrue(info['host_samples'])
            self.assertTrue(all(s['cpu_times'] is None and s['load_average'] is None
                                for s in info['host_samples']))

    def test_large_sampled_rss_is_recorded_without_rejecting_the_process(self):
        with tempfile.TemporaryDirectory() as directory:
            sampled_rss = 6 * 1024**3
            with patch.object(monitor.OwnedTree, 'sample', return_value=sampled_rss):
                try:
                    info = self.run_command('import time; time.sleep(.15)', directory)
                except RuntimeError as error:
                    self.fail('sampled RSS must not abort the process: ' + str(error))
            self.assertEqual(info['exit_code'], 0)
            self.assertEqual(info['sampled_peak_rss_bytes'], sampled_rss)
            evidence = json.loads((Path(directory) / 'child.process.json').read_text())
            self.assertIsNone(evidence['error'])
            self.assertEqual(evidence['rss_samples'][0]['rss_bytes'], sampled_rss)
            self.assertNotIn('rss_limit_bytes', evidence)

    def test_crash_and_timeout_fail_and_always_leave_partial_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            for code, timeout, message in [('raise SystemExit(7)', 2, 'PROCESS_ERROR'),
                                            ('import time; time.sleep(30)', .15, 'TIMEOUT')]:
                with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                    monitor.run_process([sys.executable, '-c', code], Path(directory),
                                        Path(directory) / 'run.log', timeout=timeout)
                self.assertTrue((Path(directory) / 'run.process.json').exists())

    def test_timeout_keeps_original_failure_when_thread_diagnostic_cannot_attach(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(monitor, 'timeout_diagnostics', create=True,
                             side_effect=RuntimeError('attach unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'TIMEOUT: performance process'):
                monitor.run_process([sys.executable, '-c', 'import time; time.sleep(30)'],
                                    Path(directory), Path(directory) / 'timed.log', timeout=.1)
            info = json.loads((Path(directory) / 'timed.process.json').read_text())
            self.assertIn('timeout_diagnostics', info)
            self.assertEqual(info['timeout_diagnostics']['error'], 'attach unavailable')
            self.assertIn('TIMEOUT', info['error'])
            self.assertIsNone(info['cleanup_error'])

    def test_known_child_is_killed_even_after_controller_has_exited(self):
        with tempfile.TemporaryDirectory() as directory:
            child = psutil.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
            root = psutil.Popen([sys.executable, '-c', 'pass'])
            try:
                owned = monitor.OwnedTree(root)
                owned.remember(child)
                root.wait(timeout=3)
                owned.stop(grace=.05)
                self.assertFalse(child.is_running())
            finally:
                if child.is_running():
                    child.kill()
                child.wait()

    def test_cancellation_cleans_up_owned_process(self):
        with tempfile.TemporaryDirectory() as directory:
            original = monitor.OwnedTree.sample
            seen = []
            def cancel(tree):
                seen.append(tree.root)
                original(tree)
                raise KeyboardInterrupt('cancelled')
            with patch.object(monitor.OwnedTree, 'sample', cancel), self.assertRaises(KeyboardInterrupt):
                self.run_command('import time; time.sleep(30)', directory)
            self.assertFalse(seen[0].is_running())


if __name__ == '__main__':
    unittest.main()
