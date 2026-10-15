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
        command = next(step['run'] for step in self.workflow['jobs']['performance']['steps']
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
                                'RUNNER_TEMP': directory, 'JAVA_HOME': directory,
                                'PERFORMANCE_CASE': 'read_t1', 'WORKFLOW_DEADLINE': ''}
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
        job = self.workflow['jobs']['performance']
        step = next(s for s in job['steps'] if s.get('name') == 'Compare paired performance')
        self.assertEqual(step['timeout-minutes'], '26')
        self.assertIn('--workflow-deadline', step['run'])
        self.assertIn('--case', step['run'])
        self.assertEqual(self.workflow['jobs']['verify']['timeout-minutes'], '30')

    def test_long_scenarios_run_on_separate_runner_jobs(self):
        jobs = self.workflow['jobs']
        self.assertIn('performance', jobs)
        performance = jobs['performance']
        self.assertEqual(performance['needs'], ['plan', 'verify'])
        self.assertNotIn('if', performance, 'Skipped matrix jobs expose an unevaluated name on GitHub')
        self.assertEqual(performance['strategy']['matrix'], '${{ fromJSON(needs.plan.outputs.matrix) }}')
        self.assertEqual(performance['name'], '${{ matrix.name }}')
        self.assertEqual(performance['timeout-minutes'], '27')
        self.assertEqual(performance['strategy']['fail-fast'], 'false')

    def test_macos_performance_uses_intel_without_changing_arm_correctness(self):
        jobs = self.workflow['jobs']
        self.assertEqual(jobs['performance']['runs-on'],
                         "${{ matrix.os == 'macos-latest' && 'macos-26-intel' || matrix.os }}")
        self.assertEqual(jobs['verify']['runs-on'], '${{ matrix.os }}')
        matrix = jobs['verify']['strategy']['matrix']
        self.assertIn('macos-latest', matrix['os'])
        self.assertEqual(set(matrix['java']), {'11', '17', '21', '25'})

    def test_unmeasured_job_runs_only_its_explicit_summary_step(self):
        steps = self.workflow['jobs']['performance']['steps']
        for step in steps:
            if step.get('name') == 'Record unmeasured performance':
                self.assertIn("needs.plan.outputs.enabled != 'true'", step['if'])
                self.assertIn('NOT_MEASURED', step['run'])
            elif not step.get('uses', '').startswith('actions/checkout@'):
                self.assertIn("needs.plan.outputs.enabled == 'true'", step.get('if', ''))
        self.assertTrue(any(s.get('name') == 'Record unmeasured performance' for s in steps))

    def test_checks_have_explicit_names_and_a_per_platform_performance_gate(self):
        jobs = self.workflow['jobs']
        for name in ('verify', 'jcstress', 'example'):
            self.assertIn('name', jobs[name])
        self.assertIn('quality', jobs)
        self.assertEqual(jobs['quality']['needs'], ['plan', 'verify', 'performance'])
        self.assertIn('always()', jobs['quality']['if'])

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
        self.assertEqual(set(jobs), {'plan', 'verify', 'performance', 'quality', 'jcstress', 'example'})
        self.assertNotIn('if', jobs['verify'])
        self.assertNotIn('if', jobs['jcstress'])
        self.assertNotIn('if', jobs['example'])
        matrix = jobs['verify']['strategy']['matrix']
        self.assertEqual(set(matrix['os']), {'ubuntu-latest', 'macos-latest', 'windows-latest'})
        self.assertEqual(set(matrix['java']), {'11', '17', '21', '25'})
        self.assertEqual(self.workflow['on']['push']['branches'], ['main'])

    def test_snapshot_trigger_tracks_the_renamed_ci_workflow(self):
        source = Path(__file__).resolve().parents[1] / '.github/workflows/publish-snapshot.yml'
        snapshot = yaml.load(source.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(snapshot['on']['workflow_run']['workflows'], [self.workflow['name']])

    def test_display_names_match_the_required_check_contract(self):
        import ci_checks
        names = set()
        for job_id in ('verify', 'quality'):
            job = self.workflow['jobs'][job_id]
            matrix = job['strategy']['matrix']
            for included in matrix['include']:
                for java in matrix.get('java', ['21']):
                    names.add(job['name'].replace('${{ matrix.platform }}', included['platform'])
                              .replace('${{ matrix.java }}', java))
        names |= {self.workflow['jobs'][key]['name'] for key in ('jcstress', 'example')}
        self.assertTrue(ci_checks.REQUIRED_JOBS <= names)


if __name__ == '__main__':
    unittest.main()
