"""Contracts for the existing CI entry point; no benchmarks are executed."""
from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

import yaml


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml'
        self.workflow = yaml.load(source.read_text(), Loader=yaml.BaseLoader)

    @unittest.skipUnless(os.name == 'posix', 'POSIX runner entry-process signal contract')
    def test_cancellation_signal_reaches_the_python_entry_process(self):
        command = next(step['run'] for step in self.workflow['jobs']['verify']['steps']
                       if step.get('name') == 'Compare paired performance')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'scripts').mkdir()
            (root / 'scripts/run_ci_performance.py').write_text(
                'import signal,sys,time\nfrom pathlib import Path\n'
                'def interrupted(*args):\n Path("cleaned").touch()\n sys.exit(0)\n'
                'signal.signal(signal.SIGINT,interrupted)\nPath("ready").touch()\n'
                'while True: time.sleep(.1)\n')
            script = root / 'step.sh'
            script.write_text(command + '\n')
            env = os.environ | {'PERFORMANCE_PYTHON': sys.executable,
                                'RUNNER_TEMP': directory, 'JAVA_HOME': directory}
            shell = subprocess.Popen(['bash', str(script)], cwd=root, env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                                     start_new_session=True)
            try:
                deadline = time.monotonic() + 3
                while not (root / 'ready').exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue((root / 'ready').exists())
                shell.send_signal(signal.SIGINT)
                self.assertEqual(shell.wait(timeout=3), 0)
                self.assertTrue((root / 'cleaned').exists())
            finally:
                try:
                    os.killpg(shell.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                shell.wait()

    def test_ready_for_review_triggers_fresh_ci(self):
        event = self.workflow['on']['pull_request']
        # GitHub's implicit default omits ready_for_review.
        activities = event.get('types', []) if isinstance(event, dict) else []
        self.assertIn('ready_for_review', activities)
        self.assertTrue({'opened', 'synchronize', 'reopened'} <= set(activities))

    def test_longer_measurements_have_bounded_step_and_job_deadlines(self):
        verify = self.workflow['jobs']['verify']
        self.assertIn('150 || 30', verify['timeout-minutes'])
        step = next(s for s in verify['steps'] if s.get('name') == 'Compare paired performance')
        self.assertEqual(step['timeout-minutes'], '121')

    def test_runner_fixture_does_not_publish_fake_actions_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / 'real-summary.md'
            summary.write_text('real step summary\n')
            env = os.environ | {'GITHUB_STEP_SUMMARY': str(summary)}
            completed = subprocess.run(
                [sys.executable, '-m', 'unittest',
                 'test_run_ci_performance.RunnerTest.test_whole_runner_warnings_succeed_and_real_errors_propagate'],
                cwd=Path(__file__).resolve().parent, env=env,
                capture_output=True, text=True, timeout=15)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertFalse(any(line.startswith('::') for line in completed.stdout.splitlines()),
                             completed.stdout)
            self.assertEqual(summary.read_text(), 'real step summary\n')

    def test_all_existing_publication_checks_remain_present_on_main_push(self):
        jobs = self.workflow['jobs']
        self.assertEqual(set(jobs), {'verify', 'jcstress', 'example'})
        self.assertNotIn('if', jobs['verify'])
        self.assertNotIn('if', jobs['jcstress'])
        self.assertNotIn('if', jobs['example'])
        matrix = jobs['verify']['strategy']['matrix']
        self.assertEqual(set(matrix['os']), {'ubuntu-latest', 'macos-latest', 'windows-latest'})
        self.assertEqual(set(matrix['java']), {'11', '17', '21', '25'})
        self.assertEqual(self.workflow['on']['push']['branches'], ['main'])


if __name__ == '__main__':
    unittest.main()
