import copy
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_snapshot_publication as snapshot_tests

CHECKS = snapshot_tests.CHECKS
REPOSITORY = snapshot_tests.REPOSITORY
SHA = snapshot_tests.SHA
OTHER_SHA = snapshot_tests.OTHER_SHA


SCRIPT = Path(__file__).with_name('release_publication.py')


class StableReleaseTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), 'The stable release gate is not implemented')
        spec = importlib.util.spec_from_file_location('release_publication', SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        snapshot_tests.PublicationGateTest.write_project(self.root, '1.0.0')
        self.run = {'id': 123, 'run_attempt': 1, 'workflow_id': 456, 'event': 'push', 'head_branch': 'main',
                    'head_sha': SHA, 'status': 'completed', 'conclusion': 'success',
                    'repository': {'full_name': REPOSITORY},
                    'head_repository': {'full_name': REPOSITORY}}
        self.jobs = [{'name': name, 'status': 'completed', 'conclusion': 'success'} for name in CHECKS]
        self.tag = {'object': {'type': 'commit', 'sha': SHA}}

    def github(self, path):
        if path == 'git/ref/tags/v1.0.0':
            return self.tag
        if path == 'git/tags/' + OTHER_SHA:
            return {'object': {'type': 'commit', 'sha': SHA}}
        if path == 'branches/main':
            return {'commit': {'sha': OTHER_SHA}}
        if path == 'compare/' + SHA + '...' + OTHER_SHA:
            return {'status': self.compare_status}
        if path == 'actions/workflows/ci.yml':
            return {'id': 456}
        if path.startswith('actions/workflows/ci.yml/runs?'):
            return {'workflow_runs': [self.run]}
        if path == 'actions/runs/123':
            return self.run
        if path.startswith('actions/runs/123/jobs?') or path.startswith('actions/runs/123/attempts/1/jobs?'):
            if getattr(self, 'change_attempt', False):
                self.run['run_attempt'] = 2
            return {'jobs': self.jobs}
        self.fail('Unexpected GitHub request: ' + path)

    def inspect(self, expected=SHA):
        self.compare_status = getattr(self, 'compare_status', 'ahead')
        with patch.object(self.module, 'github', self.github):
            return self.module.inspect_release(self.root, 'v1.0.0', expected)

    def test_stable_tag_on_main_with_exact_complete_ci_is_eligible(self):
        result = self.inspect()
        self.assertEqual(SHA, result['sha'])
        self.assertEqual('1.0.0', result['version'])
        self.assertEqual(123, result['ci_run_id'])

    def test_annotated_tag_resolves_to_its_commit(self):
        self.tag['object'] = {'type': 'tag', 'sha': OTHER_SHA}
        self.assertEqual(SHA, self.inspect()['sha'])

    def test_successful_ci_attempt_is_recorded_and_cannot_change_during_inspection(self):
        result = self.inspect()
        self.assertIn('ci_run_attempt', result, 'Release evidence must identify the successful CI attempt')
        self.assertEqual(1, result['ci_run_attempt'])
        self.change_attempt = True
        with self.assertRaises(ValueError):
            self.inspect()

    def test_moved_tag_cannot_use_preapproval_candidate(self):
        self.tag['object']['sha'] = OTHER_SHA
        with self.assertRaises(ValueError):
            self.inspect()

    def test_tag_not_on_main_cannot_publish(self):
        for status in ('behind', 'diverged'):
            self.compare_status = status
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.inspect()

    def test_pending_failed_or_cancelled_ci_cannot_publish(self):
        for status, conclusion in [('in_progress', None), ('completed', 'failure'),
                                   ('completed', 'cancelled')]:
            self.run.update(status=status, conclusion=conclusion)
            with self.subTest(conclusion=conclusion), self.assertRaises(ValueError):
                self.inspect()

    def test_missing_skipped_or_duplicate_check_cannot_publish(self):
        original = copy.deepcopy(self.jobs)
        for jobs in (original[:-1], original + [original[0]],
                     original[:-1] + [dict(original[-1], conclusion='skipped')]):
            self.jobs = jobs
            with self.assertRaises(ValueError):
                self.inspect()

    def test_unexpected_failed_ci_job_is_not_ignored(self):
        self.jobs.append({'name': 'extra check', 'status': 'completed', 'conclusion': 'failure'})
        with self.assertRaises(ValueError):
            self.inspect()

    def test_pr_fork_wrong_sha_and_wrong_workflow_cannot_publish(self):
        original = copy.deepcopy(self.run)
        for changes in ({'event': 'pull_request'}, {'head_sha': OTHER_SHA}, {'workflow_id': 999},
                        {'head_repository': {'full_name': 'attacker/red-ohc'}}):
            self.run = dict(original, **changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.inspect()

    def test_stable_tag_and_pom_must_match(self):
        with self.assertRaises(ValueError):
            self.module.release_version(self.root, 'v2.0.0')

    def test_snapshot_prerelease_and_unsafe_tag_are_rejected(self):
        for tag in ('v1.0.0-SNAPSHOT', 'v1.0.0-rc1', 'v01.0.0', '../v1.0.0', 'v1.0.0;echo bad'):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                self.module.release_version(self.root, tag)

    def test_core_parent_must_match_public_coordinates(self):
        path = self.root / 'red-ohc-core/pom.xml'
        path.write_text(path.read_text().replace('io.github.red-ead', 'com.red'))
        with self.assertRaises(ValueError):
            self.module.release_version(self.root, 'v1.0.0')

    def test_stable_consumer_uses_default_central_without_custom_repository(self):
        pom = self.module.consumer_pom('1.0.0')
        self.assertIn('<version>1.0.0</version>', pom)
        self.assertNotIn('<repositories>', pom)
        self.assertNotIn('<servers>', pom)

    def test_primary_signing_fingerprint_is_required(self):
        fingerprint = 'A' * 40
        status = '[GNUPG:] VALIDSIG {} 2026-10-14 1 0 4 0 1 10 00 {}'.format(fingerprint, fingerprint)
        self.module.require_signature(status, fingerprint)
        with self.assertRaises(ValueError):
            self.module.require_signature(status, 'B' * 40)
        with self.assertRaises(ValueError):
            self.module.require_signature('[GNUPG:] GOODSIG 123 somebody', fingerprint)
        with self.assertRaises(ValueError):
            self.module.require_signature(status + '\n[GNUPG:] EXPKEYSIG 123 somebody', fingerprint)

    def test_subkey_signature_is_not_accepted_as_primary_signature(self):
        status = '[GNUPG:] VALIDSIG {} 2026-10-14 1 0 4 0 1 10 00 {}'.format('B' * 40, 'A' * 40)
        with self.assertRaises(ValueError):
            self.module.require_signature(status, 'A' * 40)

    def test_bundle_verifies_signatures_and_excludes_benchmark(self):
        import package_release_bundle
        import zipfile
        for artifact, source in [('red-ohc', self.root), ('red-ohc-core', self.root / 'red-ohc-core')]:
            target = source / 'target'
            target.mkdir()
            suffixes = ['.pom'] if artifact == 'red-ohc' else ['.pom', '.jar', '-sources.jar', '-javadoc.jar']
            for suffix in suffixes:
                name = artifact + '-1.0.0' + suffix
                if suffix != '.pom':
                    (target / name).write_bytes(name.encode())
                (target / (name + '.asc')).write_bytes(b'unit-test signature placeholder')
        output = self.root / 'bundle.zip'
        with patch('sys.argv', ['package_release_bundle.py', '--root', str(self.root), '--tag',
                                'v1.0.0', '--fingerprint', 'A' * 40, '--output', str(output)]), \
                patch.object(package_release_bundle, 'verify_signature') as verify:
            package_release_bundle.main()
        self.assertEqual(5, verify.call_count)
        with zipfile.ZipFile(output) as bundle:
            self.assertEqual(30, len(bundle.namelist()))
            self.assertTrue(all(name.startswith('io/github/red-ead/red-ohc') for name in bundle.namelist()))
            self.assertFalse(any('jmh' in name for name in bundle.namelist()))

    def test_persisted_bundle_finishes_publication_without_a_rebuild(self):
        self.assertTrue(hasattr(self.module, 'verify_persisted_publication'),
                        'Post-publication recovery must use the preserved signed bundle')
        import zipfile
        cache = self.root / 'm2'
        bundle = self.root / 'release-bundle.zip'
        with zipfile.ZipFile(bundle, 'w') as archive:
            for name, path in self.module.artifact_files(self.root, '1.0.0').items():
                content = path.read_bytes() if path.name == 'pom.xml' else name.encode()
                archive.writestr(name, content)
                archive.writestr(name + '.asc', b'unit-test signature placeholder')
                resolved = cache / name
                resolved.parent.mkdir(parents=True, exist_ok=True)
                resolved.write_bytes(content)
        receipt = self.module.write_receipt(self.root, 'v1.0.0', SHA, 123, 1, 'A' * 40, bundle)
        with patch.object(self.module, 'import_public_key'), patch.object(self.module, 'verify_signature') as verify:
            result = self.module.verify_persisted_publication(self.root, 'v1.0.0', SHA, 123, 1,
                                                             'A' * 40, bundle, receipt, cache)
        self.assertEqual(5, verify.call_count)
        self.assertEqual(5, len(result['artifacts']))
        self.assertFalse((self.root / 'red-ohc-core/target').exists(), 'Recovery must not build artifacts')
        resolved.write_bytes(b'different javadoc')
        with patch.object(self.module, 'import_public_key'), patch.object(self.module, 'verify_signature'), \
                self.assertRaises(ValueError):
            self.module.verify_persisted_publication(self.root, 'v1.0.0', SHA, 123, 1,
                                                     'A' * 40, bundle, receipt, cache)
        receipt['source_sha'] = OTHER_SHA
        with self.assertRaises(ValueError):
            self.module.verify_persisted_publication(self.root, 'v1.0.0', SHA, 123, 1,
                                                     'A' * 40, bundle, receipt, cache)

    def test_public_github_release_with_exact_assets_is_idempotent(self):
        self.assertTrue(hasattr(self.module, 'finalize_github_release'),
                        'GitHub finalization must resume without republishing or overwriting assets')
        notes = self.root / 'notes.md'
        notes.write_text('Verified publication run 123')
        asset = self.root / 'release-bundle.zip'
        asset.write_bytes(b'bundle')
        import hashlib
        release = {'id': 99, 'tag_name': 'v1.0.0', 'body': notes.read_text(), 'draft': False,
                   'assets': [{'name': asset.name, 'digest': 'sha256:' + hashlib.sha256(asset.read_bytes()).hexdigest()}]}
        with patch.object(self.module, 'find_github_release', return_value=release), \
                patch.object(self.module.subprocess, 'run') as run:
            self.module.finalize_github_release('v1.0.0', notes, [asset])
        run.assert_not_called()

    def test_draft_github_release_resumes_only_missing_assets(self):
        self.assertTrue(hasattr(self.module, 'finalize_github_release'))
        notes = self.root / 'notes.md'
        notes.write_text('Verified publication run 123')
        asset = self.root / 'release-bundle.zip'
        asset.write_bytes(b'bundle')
        release = {'id': 99, 'tag_name': 'v1.0.0', 'body': notes.read_text(), 'draft': True, 'assets': []}
        with patch.object(self.module, 'find_github_release', return_value=release), \
                patch.object(self.module.subprocess, 'run') as run:
            self.module.finalize_github_release('v1.0.0', notes, [asset])
        self.assertEqual(['release', 'upload'], run.call_args_list[0].args[0][1:3])
        self.assertEqual(['release', 'edit'], run.call_args_list[1].args[0][1:3])
        self.assertFalse(any('--clobber' in call.args[0] for call in run.call_args_list))

    def test_github_finalization_does_not_overwrite_different_assets_or_notes(self):
        self.assertTrue(hasattr(self.module, 'finalize_github_release'))
        notes = self.root / 'notes.md'
        notes.write_text('Verified publication run 123')
        asset = self.root / 'release-bundle.zip'
        asset.write_bytes(b'bundle')
        release = {'id': 99, 'tag_name': 'v1.0.0', 'body': notes.read_text(), 'draft': False,
                   'assets': [{'name': asset.name, 'digest': 'sha256:' + '0' * 64}]}
        with patch.object(self.module, 'find_github_release', return_value=release), self.assertRaises(ValueError):
            self.module.finalize_github_release('v1.0.0', notes, [asset])
        release['body'] = 'An unrelated release'
        with patch.object(self.module, 'find_github_release', return_value=release), self.assertRaises(ValueError):
            self.module.finalize_github_release('v1.0.0', notes, [asset])

    def test_draft_lookup_uses_existing_github_cli_and_release_id(self):
        self.assertTrue(hasattr(self.module, 'find_github_release'),
                        'The published-release-by-tag endpoint cannot resume draft creation')
        release = {'id': 99, 'tag_name': 'v1.0.0', 'draft': True}
        import subprocess
        lookup = subprocess.CompletedProcess([], 0, '{"databaseId":99}', '')
        with patch.object(self.module.subprocess, 'run', return_value=lookup) as command, \
                patch.object(self.module, 'github', return_value=release) as query:
            self.assertEqual(release, self.module.find_github_release('v1.0.0'))
        self.assertEqual(['gh', 'release', 'view', 'v1.0.0', '--repo', REPOSITORY, '--json', 'databaseId'],
                         command.call_args.args[0])
        query.assert_called_once_with('releases/99')

    def test_new_release_is_drafted_before_asset_upload_and_publication(self):
        notes = self.root / 'notes.md'
        notes.write_text('Verified publication run 123')
        asset = self.root / 'release-bundle.zip'
        asset.write_bytes(b'bundle')
        release = {'id': 99, 'tag_name': 'v1.0.0', 'body': notes.read_text(), 'draft': True, 'assets': []}
        with patch.object(self.module, 'find_github_release', side_effect=[None, release]), \
                patch.object(self.module.subprocess, 'run') as run:
            self.module.finalize_github_release('v1.0.0', notes, [asset])
        self.assertEqual(['release', 'create'], run.call_args_list[0].args[0][1:3])
        self.assertIn('--draft', run.call_args_list[0].args[0])
        self.assertEqual(['release', 'upload'], run.call_args_list[1].args[0][1:3])
        self.assertEqual(['release', 'edit'], run.call_args_list[2].args[0][1:3])

    def test_lookup_only_treats_absent_release_as_absent(self):
        import subprocess
        absent = subprocess.CompletedProcess([], 1, '', 'release not found\n')
        with patch.object(self.module.subprocess, 'run', return_value=absent):
            self.assertIsNone(self.module.find_github_release('v1.0.0'))
        denied = subprocess.CompletedProcess([], 1, '', 'HTTP 403\n')
        with patch.object(self.module.subprocess, 'run', return_value=denied), \
                self.assertRaises(subprocess.CalledProcessError):
            self.module.find_github_release('v1.0.0')


if __name__ == '__main__':
    unittest.main()
