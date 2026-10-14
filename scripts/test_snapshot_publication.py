import copy
import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name('snapshot_publication.py')
REPOSITORY = 'Red-EAD/red-ohc'
SHA = 'a' * 40
OTHER_SHA = 'b' * 40
CHECKS = ['verify ({}, {})'.format(system, java)
          for system in ('ubuntu-latest', 'macos-latest', 'windows-latest')
          for java in ('11', '17', '21', '25')] + ['jcstress', 'example']


class PublicationGateTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), 'The SNAPSHOT publication gate is not implemented')
        spec = importlib.util.spec_from_file_location('snapshot_publication', SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.run = {
            'id': 123, 'workflow_id': 456, 'event': 'push', 'head_branch': 'main',
            'head_sha': SHA, 'status': 'completed', 'conclusion': 'success',
            'repository': {'full_name': REPOSITORY},
            'head_repository': {'full_name': REPOSITORY},
        }
        self.event = {'repository': {'full_name': REPOSITORY}, 'workflow_run': copy.deepcopy(self.run)}
        self.jobs = [{'name': name, 'status': 'completed', 'conclusion': 'success'} for name in CHECKS]

    def decide(self, event_name='workflow_run', source_sha=SHA, main_sha=SHA):
        return self.module.publication_decision(source_sha, main_sha, event_name, self.event,
                                                self.run, self.jobs, 456, 'refs/heads/main')

    def test_accepts_successful_main_push_with_every_required_check(self):
        result = self.decide()
        self.assertTrue(result['publish'])
        self.assertEqual(SHA, result['sha'])
        self.assertEqual(123, result['ci_run_id'])

    def test_manual_publication_uses_the_same_ci_gate(self):
        self.assertTrue(self.decide('workflow_dispatch')['publish'])
        self.jobs[0]['conclusion'] = 'failure'
        self.assertFalse(self.decide('workflow_dispatch')['publish'])

    def test_new_main_prevents_old_snapshot_upload(self):
        self.assertFalse(self.decide(main_sha=OTHER_SHA)['publish'])

    def test_unverified_checkout_cannot_publish(self):
        self.assertFalse(self.decide(source_sha=OTHER_SHA, main_sha=OTHER_SHA)['publish'])

    def test_pull_request_and_fork_runs_cannot_publish(self):
        self.run['event'] = 'pull_request'
        self.assertFalse(self.decide()['publish'])
        self.run['event'] = 'push'
        self.run['head_repository']['full_name'] = 'attacker/red-ohc'
        self.assertFalse(self.decide()['publish'])

    def test_foreign_repository_event_cannot_publish(self):
        self.event['repository']['full_name'] = 'attacker/red-ohc'
        self.assertFalse(self.decide()['publish'])

    def test_non_main_branch_or_dispatch_ref_cannot_publish(self):
        self.run['head_branch'] = 'feature/test'
        self.assertFalse(self.decide()['publish'])
        self.run['head_branch'] = 'main'
        result = self.module.publication_decision(SHA, SHA, 'workflow_dispatch', self.event,
                                                  self.run, self.jobs, 456, 'refs/heads/feature/test')
        self.assertFalse(result['publish'])

    def test_failed_cancelled_or_running_ci_cannot_publish(self):
        for conclusion in ('failure', 'cancelled', 'timed_out', None):
            with self.subTest(conclusion=conclusion):
                self.run['conclusion'] = conclusion
                self.assertFalse(self.decide()['publish'])
        self.run['conclusion'] = 'success'
        self.run['status'] = 'in_progress'
        self.assertFalse(self.decide()['publish'])

    def test_missing_skipped_or_duplicate_required_job_cannot_publish(self):
        original = copy.deepcopy(self.jobs)
        self.jobs.pop()
        self.assertFalse(self.decide()['publish'])
        self.jobs = copy.deepcopy(original)
        self.jobs[-1]['conclusion'] = 'skipped'
        self.assertFalse(self.decide()['publish'])
        self.jobs = copy.deepcopy(original) + [copy.deepcopy(original[0])]
        self.assertFalse(self.decide()['publish'])

    def test_wrong_workflow_or_event_run_id_cannot_publish(self):
        self.run['workflow_id'] = 999
        self.assertFalse(self.decide()['publish'])
        self.run['workflow_id'] = 456
        self.event['workflow_run']['id'] = 999
        self.assertFalse(self.decide()['publish'])

    def test_sha_is_validated_before_becoming_shell_output(self):
        with self.assertRaises(ValueError):
            self.decide(source_sha='main; echo unsafe')

    def test_snapshot_pom_inherits_only_public_parent_coordinates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_project(root, '1.0.0-SNAPSHOT')
            self.assertEqual('1.0.0-SNAPSHOT', self.module.snapshot_version(root))
            (root / 'red-ohc-core/pom.xml').write_text(
                '<project><parent><groupId>com.red</groupId><artifactId>ohc</artifactId>'
                '<version>1.0.0-SNAPSHOT</version></parent><artifactId>red-ohc-core</artifactId></project>')
            with self.assertRaises(ValueError):
                self.module.snapshot_version(root)

    def test_stable_version_is_skipped(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_project(root, '1.0.0')
            self.assertIsNone(self.module.snapshot_version(root))

    def test_consumer_enables_only_official_snapshot_repository(self):
        pom = self.module.consumer_pom('1.0.0-SNAPSHOT')
        self.assertIn('https://central.sonatype.com/repository/maven-snapshots/', pom)
        self.assertIn('<snapshots><enabled>true</enabled></snapshots>', pom)
        self.assertIn('<releases><enabled>false</enabled></releases>', pom)
        self.assertNotIn('<servers>', pom)
        self.assertNotIn('CENTRAL_PASSWORD', pom)

    def test_versions_cannot_escape_artifact_paths(self):
        with self.assertRaises(ValueError):
            self.module.consumer_pom('../1.0.0-SNAPSHOT')

    def test_consumer_must_resolve_the_jar_that_was_built(self):
        self.assertTrue(hasattr(self.module, 'verify_consumer'), 'Consumer checksum validation is missing')
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_project(root, '1.0.0-SNAPSHOT')
            built = root / 'red-ohc-core/target/red-ohc-core-1.0.0-SNAPSHOT.jar'
            resolved = root / 'm2/io/github/red-ead/red-ohc-core/1.0.0-SNAPSHOT/red-ohc-core-1.0.0-SNAPSHOT.jar'
            for jar in (built, resolved):
                jar.parent.mkdir(parents=True, exist_ok=True)
                jar.write_bytes(b'expected JAR')
            self.module.verify_consumer(root, root / 'm2')
            resolved.write_bytes(b'a different snapshot')
            with self.assertRaises(ValueError):
                self.module.verify_consumer(root, root / 'm2')

    @staticmethod
    def write_project(root, version):
        (root / 'red-ohc-core').mkdir()
        (root / 'pom.xml').write_text(
            '<project><groupId>io.github.red-ead</groupId><artifactId>red-ohc</artifactId>'
            '<version>{}</version></project>'.format(version))
        (root / 'red-ohc-core/pom.xml').write_text(
            '<project><parent><groupId>io.github.red-ead</groupId><artifactId>red-ohc</artifactId>'
            '<version>{}</version></parent><artifactId>red-ohc-core</artifactId></project>'.format(version))


if __name__ == '__main__':
    unittest.main()
