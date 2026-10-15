"""Required check naming and per-platform aggregation contracts."""
import importlib.util
from pathlib import Path
import unittest


class ChecksTest(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).with_name('ci_checks.py')
        self.assertTrue(path.is_file(), 'shared CI naming and quality gate are missing')
        spec = importlib.util.spec_from_file_location('ci_checks', path)
        self.checks = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.checks)

    def jobs(self, measured=True, jdks=('21',)):
        names = [self.checks.core_name('ubuntu-latest', '21')]
        if measured:
            names += [self.checks.performance_name('ubuntu-latest', jdk, case)
                      for jdk in jdks for case in self.checks.CASE_NAMES]
        return [dict(name=name, status='completed', conclusion='success') for name in names]

    def test_fourteen_named_checks_retain_all_platform_jdk_gates(self):
        required = self.checks.REQUIRED_JOBS
        self.assertEqual(len(required), 14)
        self.assertIn('Core & JMH / Linux / JDK 11', required)
        self.assertIn('Quality / Windows / JDK 21', required)
        self.assertIn('Quality / macOS / JDK 21', required)
        self.assertIn('Concurrency / JCStress', required)
        self.assertIn('Example / User cache', required)
        self.assertNotIn('Core & JMH / Linux / JDK 21', required)

    def test_measured_and_not_measured_gates_require_real_core_success(self):
        for measured in (False, True):
            jobs = self.jobs(measured)
            self.checks.check_quality_jobs(jobs, 'ubuntu-latest', measured, ['21'])
            jobs[0]['conclusion'] = 'failure'
            with self.assertRaisesRegex(ValueError, 'Core & JMH'):
                self.checks.check_quality_jobs(jobs, 'ubuntu-latest', measured, ['21'])

    def test_missing_failed_cancelled_skipped_and_duplicate_measurements_fail(self):
        for state in ('failure', 'cancelled', 'skipped', 'timed_out', None):
            jobs = self.jobs()
            jobs[-1]['conclusion'] = state
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, 'Performance'):
                self.checks.check_quality_jobs(jobs, 'ubuntu-latest', True, ['21'])
        for jobs in (self.jobs()[:-1], self.jobs() + [self.jobs()[-1]]):
            with self.assertRaises(ValueError):
                self.checks.check_quality_jobs(jobs, 'ubuntu-latest', True, ['21'])

    def test_each_platform_checks_its_own_jobs_and_all_selected_jdks(self):
        jobs = self.jobs(jdks=('11', '17', '21', '25'))
        self.checks.check_quality_jobs(jobs, 'ubuntu-latest', True, ['11', '17', '21', '25'])
        with self.assertRaises(ValueError):
            self.checks.check_quality_jobs(jobs, 'macos-latest', True, ['21'])
        jobs[-1]['conclusion'] = 'failure'
        with self.assertRaisesRegex(ValueError, 'JDK 25'):
            self.checks.check_quality_jobs(jobs, 'ubuntu-latest', True, ['11', '17', '21', '25'])

    def test_invalid_or_empty_jdk_selection_cannot_skip_measurements(self):
        for jdks in ([], ['99'], ['21', '21']):
            with self.subTest(jdks=jdks), self.assertRaises(ValueError):
                self.checks.check_quality_jobs(self.jobs(), 'ubuntu-latest', True, jdks)


if __name__ == '__main__':
    unittest.main()
