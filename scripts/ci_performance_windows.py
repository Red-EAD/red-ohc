"""Race-free Windows process-tree ownership through a private kill-on-close job."""
import os
import subprocess
import time

import msvcrt
import psutil
import pywintypes
import win32con
import win32event
import win32file
import win32job
import win32process


class WindowsProcess:
    def __init__(self, args, cwd, env, log):
        self.returncode = None
        self.process_handle = None
        self.job = win32job.CreateJobObject(None, None)
        thread_handle = input_handle = None
        output_handle = msvcrt.get_osfhandle(log.fileno())
        inherited = os.get_handle_inheritable(output_handle)
        try:
            limits = win32job.QueryInformationJobObject(self.job, win32job.JobObjectExtendedLimitInformation)
            limits['BasicLimitInformation']['LimitFlags'] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            win32job.SetInformationJobObject(self.job, win32job.JobObjectExtendedLimitInformation, limits)
            security = pywintypes.SECURITY_ATTRIBUTES()
            security.bInheritHandle = True
            input_handle = win32file.CreateFile('NUL', win32con.GENERIC_READ,
                                               win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
                                               security, win32con.OPEN_EXISTING, 0, None)
            os.set_handle_inheritable(output_handle, True)
            startup = win32process.STARTUPINFO()
            startup.dwFlags = win32con.STARTF_USESTDHANDLES
            startup.hStdInput = input_handle
            startup.hStdOutput = startup.hStdError = output_handle
            self.process_handle, thread_handle, self.pid, _ = win32process.CreateProcess(
                None, subprocess.list2cmdline(list(map(str, args))), None, None, True,
                win32con.CREATE_SUSPENDED | win32con.CREATE_UNICODE_ENVIRONMENT,
                env, str(cwd), startup)
            self.observer = psutil.Process(self.pid)
            # A suspended primary thread cannot create a fork before job assignment.
            win32job.AssignProcessToJobObject(self.job, self.process_handle)
            if win32process.ResumeThread(thread_handle) != 1:
                raise RuntimeError('PROCESS_ERROR: unexpected primary-thread suspend count')
        except BaseException:
            if self.process_handle is not None:
                win32process.TerminateProcess(self.process_handle, 1)
                win32event.WaitForSingleObject(self.process_handle, 5000)
            self.close()
            raise
        finally:
            if thread_handle is not None:
                thread_handle.Close()
            if input_handle is not None:
                input_handle.Close()
            os.set_handle_inheritable(output_handle, inherited)

    def __getattr__(self, name):
        return getattr(self.observer, name)

    def poll(self):
        if win32event.WaitForSingleObject(self.process_handle, 0) == win32event.WAIT_OBJECT_0:
            self.returncode = win32process.GetExitCodeProcess(self.process_handle)
        return self.returncode

    def wait(self, timeout=None):
        milliseconds = win32event.INFINITE if timeout is None else max(0, int(timeout * 1000))
        if win32event.WaitForSingleObject(self.process_handle, milliseconds) == win32event.WAIT_TIMEOUT:
            raise psutil.TimeoutExpired(timeout, pid=self.pid)
        self.returncode = win32process.GetExitCodeProcess(self.process_handle)
        return self.returncode

    def close(self):
        try:
            if self.job is not None:
                try:
                    win32job.TerminateJobObject(self.job, 1)
                    deadline = time.monotonic() + 5
                    while win32job.QueryInformationJobObject(
                            self.job, win32job.JobObjectBasicAccountingInformation)['ActiveProcesses']:
                        if time.monotonic() > deadline:
                            raise RuntimeError('PROCESS_ERROR: Windows job did not become empty')
                        time.sleep(.01)
                finally:
                    self.job.Close()
                    self.job = None
        finally:
            if self.process_handle is not None:
                self.process_handle.Close()
                self.process_handle = None
