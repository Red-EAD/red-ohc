"""Owned process cleanup with tiny Python children; resource limits are simulated."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import psutil

import ci_performance_process as monitor


class ProcessTest(unittest.TestCase):
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

    def test_rss_over_limit_is_an_error_without_allocating_gigabytes(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(monitor.OwnedTree, 'sample', return_value=4 * 1024**3 + 1):
                with self.assertRaisesRegex(RuntimeError, 'RESOURCE_ERROR'):
                    self.run_command('import time; time.sleep(30)', directory)
            evidence = json.loads((Path(directory) / 'child.process.json').read_text())
            self.assertIn('RESOURCE_ERROR', evidence['error'])
            self.assertIsNotNone(evidence['exit_code'])

    def test_crash_and_timeout_fail_and_always_leave_partial_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            for code, timeout, message in [('raise SystemExit(7)', 2, 'PROCESS_ERROR'),
                                            ('import time; time.sleep(30)', .15, 'TIMEOUT')]:
                with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                    monitor.run_process([sys.executable, '-c', code], Path(directory),
                                        Path(directory) / 'run.log', timeout=timeout)
                self.assertTrue((Path(directory) / 'run.process.json').exists())

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
