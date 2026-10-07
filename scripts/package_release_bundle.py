#!/usr/bin/env python3
"""Package already-signed parent/core artifacts locally. This script has no uploader."""
import argparse
import hashlib
import subprocess
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]
NS = '{http://maven.apache.org/POM/4.0.0}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    version = ET.parse(ROOT / 'pom.xml').getroot().findtext(NS + 'version')
    if not version or version.endswith('-SNAPSHOT'):
        parser.error('commit a stable release version and run -Prelease verify first')
    files = {}
    for module in ['', 'red-ohc-core']:
        artifact = module or 'red-ohc'
        source = ROOT / module
        prefix = artifact + '-' + version
        repository = 'io/github/red-ead/' + artifact + '/' + version + '/'
        pom = source / 'pom.xml'
        model = ET.parse(pom).getroot()
        actual = model.findtext(NS + 'version') or model.findtext(NS + 'parent/' + NS + 'version')
        if actual != version:
            parser.error('reactor version mismatch: ' + artifact)
        expected = {prefix + '.pom': pom,
                    prefix + '.pom.asc': source / 'target' / (prefix + '.pom.asc')}
        if module:
            for suffix in ['.jar', '-sources.jar', '-javadoc.jar']:
                expected[prefix + suffix] = source / 'target' / (prefix + suffix)
                expected[prefix + suffix + '.asc'] = source / 'target' / (prefix + suffix + '.asc')
        for name, path in expected.items():
            if name.endswith('.asc'):
                artifact_path = expected[name[:-4]]
                subprocess.run(['gpg', '--verify', str(path), str(artifact_path)], check=True)
            if not path.is_file():
                parser.error('missing real artifact/signature: ' + str(path))
            content = path.read_bytes()
            files[repository + name] = content
            if not name.endswith('.asc'):
                for algorithm in ['md5', 'sha1', 'sha256', 'sha512']:
                    files[repository + name + '.' + algorithm] = (
                        hashlib.new(algorithm, content).hexdigest() + '\n').encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, 'x', compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, content in sorted(files.items()):
            entry = zipfile.ZipInfo(name, (2026, 10, 7, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(entry, content)
    print('Local signed bundle:', args.output)
    print('SHA256:', hashlib.sha256(args.output.read_bytes()).hexdigest())


if __name__ == '__main__':
    main()
