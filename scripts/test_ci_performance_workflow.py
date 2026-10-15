"""Contracts for the existing CI entry point; no benchmarks are executed."""
from pathlib import Path
import unittest

import yaml


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml'
        self.workflow = yaml.load(source.read_text(), Loader=yaml.BaseLoader)

    def test_ready_for_review_triggers_fresh_ci(self):
        event = self.workflow['on']['pull_request']
        # GitHub's implicit default omits ready_for_review.
        activities = event.get('types', []) if isinstance(event, dict) else []
        self.assertIn('ready_for_review', activities)
        self.assertTrue({'opened', 'synchronize', 'reopened'} <= set(activities))

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
