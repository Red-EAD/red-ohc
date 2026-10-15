"""Build provenance and orchestration tests; all Maven/JMH launches are replaced."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import run_ci_performance as runner
from test_ci_performance import records


class RunnerTest(unittest.TestCase):
    def test_maven_launcher_preserves_space_paths_and_reports_missing_tool(self):
        with patch.object(runner.shutil, 'which', return_value=None), self.assertRaisesRegex(ValueError, 'Maven'):
            runner.maven_executable()
        bash = shutil.which('bash')
        with tempfile.TemporaryDirectory(prefix='Maven home with spaces ') as directory:
            script = Path(directory) / 'mvn'
            script.write_text('printf "<%s>\\n" "$@"\n')
            binary = script.with_suffix('.cmd') if os.name == 'nt' else script
            def which(name):
                return bash if name == 'bash' else str(binary)
            with patch.object(runner.shutil, 'which', side_effect=which):
                prefix = runner.maven_executable()
            output = subprocess.check_output([*prefix, '-Dmaven.repo.local=cache with spaces', '-version'], text=True)
            self.assertEqual(output.splitlines(), ['<-Dmaven.repo.local=cache with spaces>', '<-version>'])

    def test_main_and_documentation_decisions_write_not_measured_without_launching(self):
        for event, payload, reason in [('push', {}, 'main push'),
                                       ('pull_request', {'pull_request': {'draft': False,
                                        'base': {'sha': 'a' * 40}, 'head': {'sha': 'b' * 40}}},
                                        'documentation only')]:
            with self.subTest(event=event), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'event.json').write_text(json.dumps(payload))
                env = {'GITHUB_EVENT_NAME': event, 'GITHUB_EVENT_PATH': str(root / 'event.json'),
                       'GITHUB_SHA': 'c' * 40, 'JAVA_HOME': '/runtime jdk',
                       'GITHUB_OUTPUT': str(root / 'outputs'), 'GITHUB_STEP_SUMMARY': str(root / 'summary')}
                def git(*args):
                    if args[0] == 'diff':
                        return 'README.md'
                    return args[1][:-9] if args[1].endswith('^{commit}') else 'c' * 40
                with patch.dict(os.environ, env), patch.object(runner, 'git', side_effect=git):
                    runner.select(SimpleNamespace(jdk='21', plan=root / 'plan.json'))
                plan = json.loads((root / 'plan.json').read_text())
                self.assertFalse(plan['enabled'])
                self.assertEqual(plan['reason'], reason)
                self.assertIn('NOT_MEASURED', (root / 'summary').read_text())
                self.assertEqual((root / 'outputs').read_text(), 'enabled=false\n')

    def test_sha_inputs_cannot_be_refs_options_or_partial_hashes(self):
        for value in ['', 'main', '1234567', '--upload-pack=bad', 'g' * 40]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.full_sha(value)
        self.assertEqual(runner.full_sha('a' * 40), 'a' * 40)

    def test_tampered_core_and_common_harness_are_rejected_before_launch(self):
        with tempfile.TemporaryDirectory(prefix='receipt with spaces ') as directory:
            core = Path(directory) / 'core.jar'
            core.write_bytes(b'compiled-core')
            harness = Path(directory) / 'harness.jar'
            harness.write_bytes(b'compiled-harness')
            receipt = {'artifacts': {'BASE': [runner.artifact(core)],
                                     'NEW': [runner.artifact(core)]},
                       'harness': runner.artifact(harness)}
            runner.check_artifacts(receipt)
            for changed in [core, harness]:
                old = changed.read_bytes()
                changed.write_bytes(b'wrong')
                with self.assertRaisesRegex(ValueError, 'artifact'):
                    runner.check_artifacts(receipt)
                changed.write_bytes(old)

    def test_aa_requires_identical_revisions(self):
        with self.assertRaisesRegex(ValueError, 'A/A'):
            runner.comparison_shas('workflow_dispatch', {'inputs': {
                'performance': 'aa-jdk21', 'base_sha': 'a' * 40, 'new_sha': 'b' * 40}}, {})
        self.assertEqual(runner.comparison_shas('pull_request', {'pull_request': {
            'base': {'sha': 'a' * 40}, 'head': {'sha': 'b' * 40}}},
            {'GITHUB_SHA': 'c' * 40}), ('a' * 40, 'c' * 40))

    def test_confirmation_is_once_only_and_warning_remains_success(self):
        screening = records(1.2)
        confirmation = records(1.2, pairs=3)
        batches = []
        def batch(case, stage):
            batches.append(stage)
            return screening if stage == 'screening' else confirmation
        summary = runner.compare_case({'name': 'read_t1'}, batch)
        self.assertEqual(batches, ['screening', 'confirmation'])
        self.assertEqual(summary['verdict'], 'WARNING')
        self.assertEqual(summary['screening']['throughput']['pairs'], 5)
        self.assertEqual(summary['confirmation']['throughput']['pairs'], 3)

    def test_incomplete_batch_cannot_be_rendered_as_success(self):
        with self.assertRaisesRegex(ValueError, 'partial'):
            runner.compare_case({'name': 'read_t1'}, lambda case, batch: records()[:-1])

    def test_whole_runner_warnings_succeed_and_real_errors_propagate(self):
        from test_ci_performance import result, windows
        import ci_performance_process
        for failure in [None, RuntimeError('PROCESS_ERROR: simulated')]:
            with self.subTest(error=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                plan = root / 'plan.json'
                plan.write_text(json.dumps({'enabled': True, 'runner_head': 'a' * 40,
                                            'base': 'a' * 40, 'new': 'b' * 40, 'jdk': 21,
                                            'java': '/jdk/bin/java', 'instrumentation_changed': True}))
                receipt = {'harness': {'path': 'h.jar'},
                           'artifacts': {'BASE': [{'path': 'base.jar'}], 'NEW': [{'path': 'new.jar'}]}}
                def launch(command, cwd, log, *args, **kwargs):
                    if failure:
                        raise failure
                    jmh = result([120.] * 5 if 'new.jar' in command[command.index('-cp') + 1] else [100.] * 5)
                    jmh['warmupTime'] = command[command.index('-w') + 1][:-1] + ' s'
                    jmh['measurementTime'] = command[command.index('-r') + 1][:-1] + ' s'
                    Path(command[command.index('-rff') + 1]).write_text(json.dumps([jmh]))
                    log.write_text(windows())
                    return {'sampled_peak_rss_bytes': 10, 'wall_seconds': 25.}
                def git(*args):
                    return 'a' * 40 if args[0] == 'rev-parse' else ''
                arguments = SimpleNamespace(plan=plan, output=root / 'output', compiler_home='/jdk11')
                case = {'name': 'read_t1', 'method': 'oneThread', 'threads': 1, 'workload': 'READ_100'}
                output = io.StringIO()
                with patch.object(runner, 'git', side_effect=git), patch.object(runner, 'prepare', return_value=receipt), \
                        patch.object(runner, 'check_artifacts'), patch.object(runner, 'cpu_description', return_value='Test CPU'), \
                        patch.object(runner.protocol, 'cases', return_value=[case]), \
                        patch.object(ci_performance_process, 'run_process', side_effect=launch), \
                        patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(root / 'step-summary.md')}), \
                        redirect_stdout(output):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, 'PROCESS_ERROR'):
                            runner.run(arguments)
                    else:
                        runner.run(arguments)
                status = json.loads((root / 'output/reports/status.json').read_text())
                self.assertEqual(status['status'], 'ERROR' if failure else 'COMPLETED')
                manifest = json.loads((root / 'output/reports/manifest.json').read_text())
                self.assertNotIn('rss_limit_bytes', manifest)
                self.assertEqual((root / 'step-summary.md').read_text(),
                                 (root / 'output/reports/summary.md').read_text() + '\n')
                if not failure:
                    self.assertEqual(status['completed_forks'], 16)
                    self.assertIn('WARNING', (root / 'output/reports/summary.md').read_text())
                    self.assertIn('::warning title=Performance 21::read_t1: WARNING', output.getvalue())


if __name__ == '__main__':
    unittest.main()
