#!/usr/bin/env python3
"""Prepare and compare immutable revisions on the current hosted runner."""
import argparse
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import tarfile
import time
import xml.etree.ElementTree as ET

import ci_performance as protocol
from ci_checks import CASE_NAMES, JDKS, NOT_MEASURED, PLATFORMS, check_quality_jobs, performance_name
from run_paired_benchmarks import check_java11_jar
from snapshot_publication import github

ROOT = Path(__file__).resolve().parents[1]
PHASE_TIMEOUT_SECONDS = 25 * 60
FORK_TIMEOUT_SECONDS = 240


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def full_sha(value):
    if not re.fullmatch(r'[0-9a-fA-F]{40}', value):
        raise ValueError('BASE/NEW must be full 40-character commit SHAs')
    return value.lower()


def comparison_shas(event, payload, env):
    if event == 'pull_request':
        return full_sha(payload['pull_request']['base']['sha']), full_sha(env['GITHUB_SHA'])
    inputs = payload.get('inputs', {})
    base, new = full_sha(inputs.get('base_sha', '')), full_sha(inputs.get('new_sha', ''))
    if inputs.get('performance') == 'aa-jdk21' and base != new:
        raise ValueError('A/A calibration requires identical BASE/NEW SHA inputs')
    return base, new


def append_summary(text):
    path = os.environ.get('GITHUB_STEP_SUMMARY')
    if path:
        with open(path, 'a', encoding='utf-8') as stream:
            stream.write(text + '\n')


def select(args):
    payload = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text(encoding='utf-8'))
    event = os.environ['GITHUB_EVENT_NAME']
    inputs = payload.get('inputs', {})
    mode = inputs.get('performance', 'off')
    draft = payload.get('pull_request', {}).get('draft', False)
    paths = []
    base = new = None
    eligible = event == 'workflow_dispatch' and mode != 'off' or event == 'pull_request' and not draft
    if eligible:
        base, new = comparison_shas(event, payload, os.environ)
        for sha in {base, new}:
            if git('rev-parse', sha + '^{commit}') != sha:
                raise ValueError('resolved commit differs from requested SHA')
        paths = git('diff', '--name-only', base, new).splitlines()
    enabled, reason = protocol.selection(event, args.jdk, draft, paths, mode)
    java = Path(os.environ['JAVA_HOME']) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
    runner_head = git('rev-parse', 'HEAD')
    tooling_paths = git('diff', '--name-only', base, runner_head).splitlines() if base else paths
    plan = {'enabled': enabled, 'reason': reason, 'base': base, 'new': new,
            'jdk': int(args.jdk), 'java': str(java.resolve()), 'mode': mode, 'event': event,
            'pr_head': payload.get('pull_request', {}).get('head', {}).get('sha'),
            'runner_head': runner_head, 'changed_paths': paths, 'tooling_changed_paths': tooling_paths,
            'instrumentation_changed': protocol.instrumentation_changed(paths + tooling_paths),
            'run_id': os.environ.get('GITHUB_RUN_ID'), 'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT')}
    write_json(args.plan, plan)
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as stream:
        stream.write('enabled=' + str(enabled).lower() + '\n')
    if not enabled:
        append_summary('### Performance — ' + os.environ.get('RUNNER_OS', platform.system()) +
                       ' / JDK ' + args.jdk + '\n\n`NOT_MEASURED`: ' + reason +
                       '. Correctness checks still run.\n')


def plan_workflow(args):
    select(args)
    plan = json.loads(args.plan.read_text(encoding='utf-8'))
    jdks = list(JDKS) if plan['mode'] == 'all-jdks' else ['21']
    matrix = [{'os': system, 'platform': display, 'java': java, 'case': case,
               'name': performance_name(system, java, case)}
              for system, display in PLATFORMS.items() for java in jdks for case in CASE_NAMES]
    if not plan['enabled']:
        matrix = [{'os': 'ubuntu-latest', 'name': NOT_MEASURED}]
    # Leave two minutes for evidence uploads and final gates in a PR's 30-minute budget.
    deadline = ''
    if plan['event'] == 'pull_request':
        run = github('actions/runs/{}/attempts/{}'.format(
            os.environ['GITHUB_RUN_ID'], os.environ['GITHUB_RUN_ATTEMPT']))
        deadline = datetime.fromisoformat(run['run_started_at'].replace('Z', '+00:00')).timestamp() + 28 * 60
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as stream:
        stream.write('jdks=' + json.dumps(jdks) + '\n')
        stream.write('matrix=' + json.dumps({'include': matrix}) + '\n')
        stream.write('deadline_epoch=' + str(deadline) + '\n')


def gate(args):
    if args.plan_result != 'success' or args.enabled not in ('true', 'false'):
        raise ValueError('The workflow plan must complete successfully')
    jobs, page = [], 1
    while True:
        batch = github('actions/runs/{}/attempts/{}/jobs?per_page=100&page={}'.format(
            os.environ['GITHUB_RUN_ID'], os.environ['GITHUB_RUN_ATTEMPT'], page))['jobs']
        jobs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    check_quality_jobs(jobs, args.os, args.enabled == 'true', json.loads(args.jdks))
    print('Required core and selected performance jobs succeeded for ' + PLATFORMS[args.os])


def artifact(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'bytes': path.stat().st_size}


def check_artifacts(receipt):
    for item in [receipt['harness'], *(jar for jars in receipt['artifacts'].values() for jar in jars)]:
        if artifact(item['path']) != item:
            raise ValueError('prepared artifact bytes changed: ' + item['path'])


def java_info(java, expected):
    text = subprocess.check_output([str(java), '-XshowSettings:properties', '-version'],
                                   stderr=subprocess.STDOUT, text=True,
                                   env=clean_environment(), timeout=30)
    version = re.search(r'java.specification.version\s*=\s*(\d+)', text)
    if version is None or int(version[1]) != expected:
        raise ValueError('Java executable does not match requested JDK ' + str(expected))
    return {'java': str(java), 'details': text, 'binary': artifact(java)}


def clean_environment():
    from ci_performance_process import clean_environment as clean
    return clean()


def cpu_description():
    system = platform.system()
    if system == 'Linux':
        for line in Path('/proc/cpuinfo').read_text(encoding='utf-8').splitlines():
            if line.startswith('model name'):
                return line.partition(':')[2].strip()
    command = (['sysctl', '-n', 'machdep.cpu.brand_string'] if system == 'Darwin' else
               ['powershell', '-NoProfile', '-Command', '(Get-CimInstance Win32_Processor).Name'])
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT,
                                       timeout=15).strip()
    except (OSError, subprocess.SubprocessError) as error:
        return 'N/A: ' + str(error)


def maven_executable():
    executable = shutil.which('mvn.cmd' if os.name == 'nt' else 'mvn')
    if executable is None:
        raise ValueError('Maven must be installed on PATH')
    # The same Maven distribution contains the POSIX launcher on Windows as well.
    # GitHub's bash handles spaces without cmd.exe batch-file quoting ambiguities.
    script = Path(executable).with_suffix('') if os.name == 'nt' else Path(executable)
    bash = shutil.which('bash')
    if bash is None or not script.is_file():
        raise ValueError('Maven POSIX launcher and bash must be available')
    return [bash, script.as_posix()]


def prepare(plan, output, compiler_home, deadline):
    from ci_performance_process import run_process
    compiler = Path(compiler_home).resolve() / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
    compiler_info = java_info(compiler, 11)
    runtime = java_info(plan['java'], plan['jdk'])
    maven = maven_executable()
    env = clean_environment()
    env.update(JAVA_HOME=str(Path(compiler_home).resolve()),
               PATH=str(compiler.parent) + os.pathsep + env.get('PATH', ''),
               MAVEN_OPTS='-Xms64m -Xmx512m')
    # Do not inherit caller-controlled Maven options or hidden JVM injection.
    env.pop('MAVEN_ARGS', None)
    version_log = output / 'logs' / 'maven-version.log'
    run_process([*maven, '-version'], ROOT, version_log, 30, deadline=deadline, env=env)
    receipt = {'base': plan['base'], 'new': plan['new'], 'compiler': compiler_info,
               'runtime': runtime, 'maven_command': maven,
               'maven_version': version_log.read_text(encoding='utf-8'), 'artifacts': {}}
    sources = {}
    for label, sha in [('BASE', plan['base']), ('NEW', plan['new'])]:
        checkout = output / 'build' / label
        checkout.mkdir(parents=True)
        archive = subprocess.check_output(['git', 'archive', sha], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            source.extractall(checkout, filter='data')
        sources[label] = checkout
    base_source = sources['BASE']
    ns = '{http://maven.apache.org/POM/4.0.0}'
    for label, checkout in sources.items():
        if label == 'NEW' and plan['mode'] == 'aa-jdk21':
            # Calibration executes the exact same prepared bytes on both sides.
            receipt['artifacts']['NEW'] = receipt['artifacts']['BASE']
            receipt['NEW_core_sha256'] = receipt['BASE_core_sha256']
            continue
        version = ET.parse(checkout / 'pom.xml').getroot().findtext(ns + 'version')
        if not version or not re.fullmatch(r'[A-Za-z0-9_.-]+', version):
            raise ValueError('missing or invalid project version')
        repository = output / 'm2' / label
        settings = base_source / 'config/maven/settings.xml'
        prefix = [*maven, '-B', '-s', str(settings), '-gs', str(settings),
                  '-Dmaven.repo.local=' + str(repository)]
        modules = ['-Pbenchmarks', '-pl', 'red-ohc-core,red-ohc-jmh', '-am'] if label == 'BASE' else ['-pl', 'red-ohc-core', '-am']
        run_process([*prefix, *modules, '-DskipTests', 'clean', 'install'], checkout,
                    output / 'logs' / (label + '-build.log'), 600, deadline=deadline, env=env)
        classpath = output / (label + '.classpath')
        # Always use BASE's resolver and harness; each core uses its own isolated repo.
        run_process([*prefix, '-f', str(base_source / 'tools/benchmark-runtime/pom.xml'),
                     '-Pred', '-Dred-ohc.version=' + version,
                     'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath',
                     '-Dmdep.outputFile=' + str(classpath)], checkout,
                    output / 'logs' / (label + '-dependencies.log'), 300, deadline=deadline, env=env)
        dependencies = [Path(path) for path in classpath.read_text(encoding='utf-8').strip().split(os.pathsep)]
        core = checkout / 'red-ohc-core/target' / ('red-ohc-core-' + version + '.jar')
        installed_core = repository / 'io/github/red-ead/red-ohc-core' / version / core.name
        if dependencies.count(installed_core) != 1 or artifact(core)['sha256'] != artifact(installed_core)['sha256']:
            raise ValueError('dependency resolution did not select the core built from source')
        jars = []
        for index, path in enumerate(dependencies):
            target = output / 'artifacts' / label / (str(index) + '-' + path.name)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            if path == installed_core:
                check_java11_jar(target)
            jars.append(artifact(target))
        receipt['artifacts'][label] = jars
        receipt[label + '_core_sha256'] = artifact(core)['sha256']
        if label == 'BASE':
            thin = checkout / 'red-ohc-jmh/target' / ('red-ohc-jmh-' + version + '.jar')
            target = output / 'artifacts/common-harness.jar'
            shutil.copyfile(thin, target)
            check_java11_jar(target)
            receipt['harness'] = artifact(target)
    check_artifacts(receipt)
    write_json(output / 'reports/build-receipt.json', receipt)
    return receipt


def compare_case(case, run_batch):
    screening = protocol.analyze(run_batch(case, 'screening'), protocol.SAMPLING['screening']['pairs'])
    confirmation = (protocol.analyze(run_batch(case, 'confirmation'), protocol.SAMPLING['confirmation']['pairs'])
                    if screening['needs_confirmation'] else None)
    return {'case': case, 'screening': screening, 'confirmation': confirmation,
            'verdict': protocol.final_verdict(screening, confirmation)}


def _metric_text(metric, cpu=False):
    if metric['ratio'] is None:
        return 'N/A'
    low, high = metric['interval']
    scale, unit = (1e9, 'ns/op') if cpu else (1, 'ops/s')
    absolute = '{:.3g} → {:.3g} {}; '.format(metric['base_geomean'] * scale,
                                            metric['new_geomean'] * scale, unit)
    return absolute + '{:+.2f}% [{:+.2f}%, {:+.2f}%] {}'.format(
        (metric['ratio'] - 1) * 100, (low - 1) * 100, (high - 1) * 100, metric['verdict'])


def summary_markdown(manifest, results, records, error):
    text = ['### Performance — {} / JDK {}'.format(manifest['system'], manifest['jdk']), '',
            'BASE `{base}` → NEW `{new}`; P={parallel_threads}.'.format(**manifest), '',
            '**Measurement tooling changed: {}.**'.format(manifest['instrumentation_changed']), '',
            '| Case | Batch | Throughput NEW/BASE (95% paired interval) | Estimated CPU/op NEW/BASE (95% interval) | Verdict |',
            '|---|---|---|---|---|']
    for row in results:
        for batch in ('screening', 'confirmation'):
            if row[batch] is not None:
                text.append('| {} | {} | {} | {} | {} |'.format(row['case']['name'], batch,
                            _metric_text(row[batch]['throughput']), _metric_text(row[batch]['cpu_per_op'], cpu=True), row['verdict']))
    text += ['', '| Fork | CV / drift | GC count / time (ms) | Measured compilation (ms) | Observer CPU / wall |',
             '|---|---|---|---|---|']
    for record in records:
        def score(name):
            value = record['profiler_metrics'].get(name, {}).get('score')
            return 'N/A' if value is None else '{:.3g}'.format(value)
        text.append('| {batch}/{round}/{label} | {cv:.1%} / {drift:.1%} | {gc} / {gc_ms} | {jit} | {observer:.2%} |'.format(
            **record, cv=record['iteration_cv'], drift=record['iteration_drift'],
            gc=score('gc.count'), gc_ms=score('gc.time'), jit=score('compiler.time.profiled'),
            observer=record['sampler_cpu_seconds'] / record['wall_seconds']))
    if error:
        text += ['', '**ERROR:** ' + str(error).replace('\n', ' ')[:500]]
    peak = max((r['sampled_peak_rss_bytes'] for r in records), default=0)
    text += ['', 'Sampled controller + fork peak RSS: {:.3f} GiB. {} completed forks.'.format(peak / 1024**3, len(records)),
             'RSS is sampled without a failure threshold. Missing CPU data is N/A.',
             'Process/RSS sampling is 1 Hz; observer CPU includes Python launch, monitoring and cleanup, across warmup and measurement. GC/JIT diagnostics retain per-iteration units separately from operation rates.',
             'Host CPU busy, cumulative CPU times (including available steal/iowait) and load samples are retained in each process JSON; host load includes this benchmark.',
             'CPU/op is estimated from measurement-window process CPU and corresponding completed-operation rates; it includes the actor.',
             'Warnings and INCONCLUSIVE results do not fail CI. Raw iteration rates, CPU windows, logs, environment and artifact hashes are retained for 90 days.',
             'Hosted-runner comparisons do not establish dedicated Linux/AMD/Intel performance acceptance.', '']
    return '\n'.join(text)


def run(args):
    from ci_performance_process import run_process
    plan = json.loads(args.plan.read_text(encoding='utf-8'))
    if not plan['enabled']:
        raise ValueError('performance measurement is not enabled in this plan')
    if git('rev-parse', 'HEAD') != plan['runner_head'] or git('status', '--porcelain'):
        raise ValueError('run from the exact clean committed CI checkout')
    output = args.output.resolve()
    if output == ROOT or ROOT in output.parents:
        raise ValueError('keep results and build caches outside the source checkout')
    output.mkdir(parents=True, exist_ok=False)
    (output / 'logs').mkdir()
    (output / 'reports').mkdir()
    budget = PHASE_TIMEOUT_SECONDS
    if args.workflow_deadline is not None:
        budget = min(budget, args.workflow_deadline - time.time())
    deadline = time.monotonic() + budget
    available_cases = protocol.cases(os.cpu_count() or 1)
    all_cases = [case for case in available_cases if case['name'] == args.case]
    if not all_cases:
        raise ValueError('Unknown performance case: ' + args.case)
    manifest = plan | {'system': platform.system(), 'platform': platform.platform(),
                       'machine': platform.machine(), 'processor': platform.processor(),
                       'cpu_model': cpu_description(),
                       'logical_cpus': os.cpu_count(), 'parallel_threads': available_cases[-1]['threads'],
                       'compiler_home': str(args.compiler_home), 'fork_args': protocol.FORK_ARGS,
                       'controller_args': protocol.CONTROLLER_ARGS, 'cases': all_cases,
                       'phase_timeout_seconds': PHASE_TIMEOUT_SECONDS,
                       'workflow_deadline_epoch': args.workflow_deadline,
                       'fork_timeout_seconds': FORK_TIMEOUT_SECONDS,
                       'sampling': protocol.SAMPLING,
                       'profilers': protocol.PROFILERS,
                       'image': {k: os.environ.get(k) for k in ('ImageOS', 'ImageVersion', 'RUNNER_OS', 'RUNNER_ARCH')},
                       'tool_sha256': {p.name: artifact(p)['sha256'] for p in
                                      [Path(__file__), ROOT / 'scripts/ci_performance.py', ROOT / 'scripts/ci_performance_process.py']}}
    # Capture actual platform descriptions without invoking privileged tooling.
    import psutil
    manifest['physical_memory_bytes'] = psutil.virtual_memory().total
    manifest['python'] = platform.python_version()
    manifest['python_packages'] = {}
    import importlib.metadata
    for package in ('psutil', 'numpy', 'scipy', 'PyYAML'):
        manifest['python_packages'][package] = importlib.metadata.version(package)
    if os.name == 'nt':
        manifest['python_packages']['pywin32'] = importlib.metadata.version('pywin32')
    write_json(output / 'reports/manifest.json', manifest)
    rows, results = [], []
    error = None
    def interrupted(signum, frame):
        raise KeyboardInterrupt('cancelled by signal ' + str(signum))
    old_signals = {kind: signal.signal(kind, interrupted) for kind in (signal.SIGINT, signal.SIGTERM)}
    try:
        if budget <= 0:
            raise RuntimeError('TIMEOUT: workflow performance budget is exhausted')
        receipt = prepare(plan, output, args.compiler_home, deadline)
        def run_batch(case, batch):
            records = []
            for row in protocol.batch_plan(case, batch):
                check_artifacts(receipt)
                name = '{name}-{batch}-{round}-{label}'.format(**row)
                result_file = output / 'logs' / (name + '.jmh.json')
                log = output / 'logs' / (name + '.log')
                jars = [receipt['harness']['path'], *[j['path'] for j in receipt['artifacts'][row['label']]]]
                command = protocol.command(plan['java'], jars, case, batch, result_file)
                print(name, flush=True)
                process = run_process(command, ROOT, log, FORK_TIMEOUT_SECONDS, deadline=deadline)
                measurement = protocol.measurement(json.loads(result_file.read_text(encoding='utf-8')),
                                                   log.read_text(encoding='utf-8'), case, batch,
                                                   plan['java'], plan['jdk'])
                record = row | measurement | {'sampled_peak_rss_bytes': process['sampled_peak_rss_bytes'],
                                             'sampler_cpu_seconds': process['sampler_cpu_seconds'],
                                             'wall_seconds': process['wall_seconds']}
                rows.append(record)
                records.append(record)
                write_json(output / 'reports/forks.json', rows)
            return records
        for case in all_cases:
            compared = compare_case(case, run_batch)
            results.append(compared)
            write_json(output / 'reports/comparisons.json', results)
            if compared['verdict'] != 'STABLE':
                print('::warning title=Performance {}::{}: {} (review both batches)'.format(
                    plan['jdk'], case['name'], compared['verdict']), flush=True)
    except BaseException as failure:
        error = failure
        raise
    finally:
        for kind, handler in old_signals.items():
            signal.signal(kind, handler)
        markdown = summary_markdown(manifest, results, rows, error)
        (output / 'reports/summary.md').write_text(markdown, encoding='utf-8')
        append_summary(markdown)
        write_json(output / 'reports/status.json', {'status': 'ERROR' if error else 'COMPLETED',
                                                   'error': str(error) if error else None,
                                                   'completed_forks': len(rows)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    choose = actions.add_parser('select')
    choose.add_argument('--jdk', choices=['11', '17', '21', '25'], required=True)
    choose.add_argument('--plan', type=Path, required=True)
    planning = actions.add_parser('plan')
    planning.add_argument('--jdk', choices=['21'], default='21')
    planning.add_argument('--plan', type=Path, required=True)
    execute = actions.add_parser('run')
    execute.add_argument('--plan', type=Path, required=True)
    execute.add_argument('--output', type=Path, required=True)
    execute.add_argument('--compiler-home', type=Path, required=True)
    execute.add_argument('--case', choices=CASE_NAMES, required=True)
    execute.add_argument('--workflow-deadline', type=float)
    aggregate = actions.add_parser('gate')
    aggregate.add_argument('--os', choices=PLATFORMS, required=True)
    aggregate.add_argument('--enabled', required=True)
    aggregate.add_argument('--jdks', required=True)
    aggregate.add_argument('--plan-result', required=True)
    args = parser.parse_args()
    {'select': select, 'plan': plan_workflow, 'run': run, 'gate': gate}[args.action](args)


if __name__ == '__main__':
    main()
