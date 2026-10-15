"""Cross-platform sampling and cleanup restricted to processes launched by this tool."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import psutil


class HostProcessSampler:
    """Low-frequency, read-only CPU attribution; never records command arguments."""
    def __init__(self):
        self.previous = {}
        self.elapsed = None

    def sample(self, elapsed, owned):
        started = time.monotonic()
        current, top = {}, []
        unavailable = 0
        matched = 0
        error = None
        interval = started - self.elapsed if self.elapsed is not None else None
        try:
            # process_iter and Process.create_time both cache; refresh before testing identities.
            psutil.process_iter.cache_clear()
            for process in psutil.process_iter(
                    attrs=['pid', 'name', 'create_time', 'cpu_times'], ad_value=None):
                info = process.info
                if os.name == 'nt' and info['pid'] == 0:
                    continue  # Windows PID 0 reports idle CPU, not competing work.
                cpu, born = info['cpu_times'], info['create_time']
                if cpu is None or born is None:
                    unavailable += 1
                    continue
                identity = (info['pid'], born)
                total = cpu.user + cpu.system
                observed = time.monotonic()
                current[identity] = (total, observed)
                if (identity in owned or info['pid'] == os.getpid()
                        or identity not in self.previous):
                    continue
                previous, before = self.previous[identity]
                duration = observed - before
                if duration > 0 and total >= previous:
                    matched += 1
                if duration > 0 and total > previous:
                    top.append({'pid': identity[0], 'created_at': born, 'name': info['name'],
                                'cpu_cores': (total - previous) / duration,
                                'interval_seconds': duration})
        except (OSError, NotImplementedError, psutil.Error) as failure:
            error = type(failure).__name__
        self.previous = current if error is None else {}
        self.elapsed = started
        return {'elapsed_seconds': elapsed, 'interval_seconds': interval,
                'outside_cpu_cores': (sum(p['cpu_cores'] for p in top)
                                      if matched and error is None else None),
                'top_processes': sorted(top, key=lambda p: p['cpu_cores'], reverse=True)[:5],
                'unavailable_process_count': unavailable, 'coverage_is_partial': True,
                'cpu_rates_are_estimates': True,
                'error': error, 'sampling_wall_seconds': time.monotonic() - started}


class OwnedTree:
    def __init__(self, root):
        self.root = root
        self.known = {}
        self.stats = {}
        self.remember(root)

    def remember(self, process):
        # psutil Process identity includes create time; terminate/kill check PID reuse.
        try:
            identity = (process.pid, process.create_time())
            self.known[identity] = process
        except psutil.NoSuchProcess:
            pass

    def sample(self):
        try:
            for child in self.root.children(recursive=True):
                self.remember(child)
        except psutil.NoSuchProcess:
            pass
        rss = 0
        for identity, process in list(self.known.items()):
            try:
                if not process.is_running():
                    continue
                with process.oneshot():
                    memory = process.memory_info().rss
                    cpu = process.cpu_times()
                    command_error = None
                    try:
                        command = process.cmdline()
                    except psutil.AccessDenied:
                        # macOS may deny KERN_PROCARGS2 for a transitioning child.
                        # Optional metadata must not abort an owned build or fork.
                        command = None
                        command_error = 'access denied'
                rss += memory
                previous = self.stats.get(identity, {})
                self.stats[identity] = {'pid': identity[0], 'created_at': identity[1],
                                        'command': command,
                                        'command_unavailable_reason': command_error,
                                        'sampled_peak_rss_bytes': max(memory, previous.get('sampled_peak_rss_bytes', 0)),
                                        'user_seconds': max(cpu.user, previous.get('user_seconds', 0)),
                                        'system_seconds': max(cpu.system, previous.get('system_seconds', 0))}
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
        return rss

    def stop(self, grace=2):
        # Discover once more while the controller still exists; retain known children
        # after it exits. No machine-wide process names or unrelated PIDs are targeted.
        try:
            for child in self.root.children(recursive=True):
                self.remember(child)
        except psutil.NoSuchProcess:
            pass
        live = []
        for process in reversed(list(self.known.values())):
            try:
                if process.is_running():
                    process.terminate()
                    live.append(process)
            except psutil.NoSuchProcess:
                pass
        _, remaining = psutil.wait_procs(live, timeout=grace)
        for process in remaining:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        # POSIX groups also cover a fork that died/reparented between samples.
        if os.name == 'posix':
            try:
                current = psutil.Process(self.root.pid)
                same_identity = current.create_time() == self.root.create_time()
            except psutil.NoSuchProcess:
                same_identity = True  # The old group can outlive its leader.
            if same_identity:
                try:
                    os.killpg(self.root.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        _, remaining = psutil.wait_procs(remaining, timeout=2)
        if remaining:
            raise RuntimeError('PROCESS_ERROR: owned children did not terminate')
        self.root.wait()


def clean_environment(env=None):
    result = dict(os.environ if env is None else env)
    for key in ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'):
        result.pop(key, None)
    return result


def host_sample(elapsed, initial=False):
    """Unprivileged guest-wide diagnostics; absent host visibility stays unknown."""
    sample = {'elapsed_seconds': elapsed, 'cpu_busy_percent': None,
              'cpu_times': None, 'load_average': None}
    try:
        busy = psutil.cpu_percent(interval=None)
        # The first non-blocking call primes psutil's CPU-delta baseline.
        sample['cpu_busy_percent'] = None if initial else busy
        sample['cpu_times'] = psutil.cpu_times()._asdict()
    except (OSError, NotImplementedError, psutil.Error):
        pass
    try:
        load = getattr(os, 'getloadavg', None)
        if load is not None:
            sample['load_average'] = list(load())
    except (OSError, NotImplementedError):
        pass
    return sample


def run_process(args, cwd, log_file, timeout, deadline=None, env=None):
    log_file = Path(log_file)
    started = time.monotonic()
    cpu_started = time.process_time()
    process = tree = None
    error = None
    samples = []
    host_samples = []
    background_samples = []
    background = HostProcessSampler()
    try:
        with log_file.open('w', encoding='utf-8') as log:
            if os.name == 'nt':
                from ci_performance_windows import WindowsProcess
                process = WindowsProcess(args, cwd, clean_environment(env), log)
            else:
                process = psutil.Popen(args, cwd=cwd, env=clean_environment(env),
                                       stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            tree = OwnedTree(process)
            while True:
                now = time.monotonic()
                elapsed = now - started
                finished = process.poll() is not None
                # Process-tree enumeration is expensive on some platforms. Keep
                # deadline/cancellation polling responsive without sampling at 10 Hz.
                if not samples or elapsed - samples[-1]['elapsed_seconds'] >= 1. or finished:
                    sample_started = time.monotonic()
                    rss = tree.sample()
                    samples.append({'elapsed_seconds': elapsed, 'rss_bytes': rss,
                                    'sampling_wall_seconds': time.monotonic() - sample_started})
                    sample_started = time.monotonic()
                    host = host_sample(elapsed, initial=not host_samples)
                    host['sampling_wall_seconds'] = time.monotonic() - sample_started
                    host_samples.append(host)
                if (not background_samples
                        or elapsed - background_samples[-1]['elapsed_seconds'] >= 10. or finished):
                    background_samples.append(background.sample(time.monotonic() - started, set(tree.known)))
                # Sampling can block in OS introspection. Never test a stale pre-sample clock.
                now = time.monotonic()
                if now - started > timeout or (deadline is not None and now > deadline):
                    raise RuntimeError('TIMEOUT: performance process exceeded its deadline')
                if finished:
                    if process.returncode:
                        raise RuntimeError('PROCESS_ERROR: exit code ' + str(process.returncode))
                    break
                time.sleep(.1)
    except BaseException as failure:
        error = failure
        raise
    finally:
        cleanup_error = None
        try:
            if tree is not None:
                tree.stop()
        except BaseException as failure:
            cleanup_error = failure
        finally:
            if process is not None and os.name == 'nt':
                try:
                    process.close()
                except BaseException as failure:
                    cleanup_error = cleanup_error or failure
        info = {'command': list(map(str, args)), 'exit_code': process.returncode if process else None,
                'wall_seconds': time.monotonic() - started,
                'sampler_cpu_seconds': time.process_time() - cpu_started,
                'sample_interval_seconds': 1., 'rss_samples': samples,
                'host_sample_interval_seconds': 1., 'host_samples': host_samples,
                'background_sample_interval_seconds': 10., 'background_samples': background_samples,
                'sampled_peak_rss_bytes': max((s['rss_bytes'] for s in samples), default=0),
                'processes': list(tree.stats.values()) if tree else [],
                'process_cpu_samples_are_lower_bounds': True,
                'error': str(error) if error is not None else None,
                'cleanup_error': str(cleanup_error) if cleanup_error is not None else None}
        info['sampled_user_seconds'] = sum(p['user_seconds'] for p in info['processes'])
        info['sampled_system_seconds'] = sum(p['system_seconds'] for p in info['processes'])
        info['logical_cpus'] = os.cpu_count()
        info['sampled_normalized_cpu_percent'] = (
            100 * (info['sampled_user_seconds'] + info['sampled_system_seconds'])
            / info['wall_seconds'] / (os.cpu_count() or 1)
            if info['wall_seconds'] > 0 else None)
        log_file.with_suffix('.process.json').write_text(json.dumps(info, indent=2) + '\n', encoding='utf-8')
        if cleanup_error is not None and error is None:
            raise cleanup_error
    return info
