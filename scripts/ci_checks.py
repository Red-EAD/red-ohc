"""Names and aggregation rules shared by CI and publication gates."""
from collections import Counter

PLATFORMS = {'ubuntu-latest': 'Linux', 'macos-latest': 'macOS', 'windows-latest': 'Windows'}
JDKS = ('11', '17', '21', '25')
CASE_NAMES = ('read_t1', 'write_t1', 'read_parallel', 'write_parallel', 'mix_parallel')
NOT_MEASURED = 'Performance / NOT_MEASURED'


def core_name(system, java):
    return 'Core & JMH / {} / JDK {}'.format(PLATFORMS[system], java)


def performance_name(system, java, case):
    return 'Performance / {} / JDK {} / {}'.format(PLATFORMS[system], java, case)


def quality_name(system):
    return 'Quality / {} / JDK 21'.format(PLATFORMS[system])


REQUIRED_JOBS = {core_name(system, java) for system in PLATFORMS for java in JDKS if java != '21'}
REQUIRED_JOBS |= {quality_name(system) for system in PLATFORMS}
REQUIRED_JOBS |= {'Concurrency / JCStress', 'Example / User cache'}


def check_quality_jobs(jobs, system, measured, jdks):
    if not jdks or len(jdks) != len(set(jdks)) or not set(jdks) <= set(JDKS):
        raise ValueError('Invalid measurement JDK selection')
    required = {core_name(system, '21')}
    if measured:
        required |= {performance_name(system, java, case) for java in jdks for case in CASE_NAMES}
    else:
        required.add(NOT_MEASURED)
    counts = Counter(job.get('name') for job in jobs)
    successful = {job.get('name') for job in jobs
                  if job.get('status') == 'completed' and job.get('conclusion') == 'success'}
    failed = sorted(name for name in required if counts[name] != 1 or name not in successful)
    if failed:
        raise ValueError('Required jobs did not succeed exactly once: ' + '; '.join(failed))
