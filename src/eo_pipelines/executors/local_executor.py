# MIT License
#
# Copyright (c) 2022-2025 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import queue
import threading
import os
import copy
import uuid
import time
import logging

from .executor import Executor
from ..utils.process_runner import ProcessRunner


class TimeLocker(threading.Thread):
    time_lock = threading.Lock()

    def __init__(self, interval_s=5):
        threading.Thread.__init__(self, daemon=True)
        self.interval_s = interval_s
        TimeLocker.time_lock.acquire()
        self.start()

    def run(self):
        time.sleep(self.interval_s)
        TimeLocker.time_lock.release()


class ExecutorThread(threading.Thread):

    def __init__(self, executor, shell, conda_path, echo_stdout, temp_path, timeout, stagger_time):
        threading.Thread.__init__(self, daemon=True)
        self.executor = executor
        self.shell = shell
        self.conda_path = conda_path
        self.echo_stdout = echo_stdout
        self.temp_path = temp_path
        self.timeout = timeout
        self.stagger_time = stagger_time

    def run(self):
        while True:
            (stage_id, task_id, script, env, working_dir) = self.executor.get_task()
            self.executor.task_started(task_id)
            cmd = [self.shell, "-e", script]
            env_vars = copy.deepcopy(env)
            env_vars["CONDA_PATH"] = self.conda_path
            env_vars["TMPDIR"] = self.temp_path
            log_folder = os.path.join(working_dir, "task-logs")
            os.makedirs(log_folder, exist_ok=True)
            log_path = os.path.join(log_folder, task_id + ".log")
            with open(log_path, "a") as f:
                f.write("\n\n------------------------------------\n\n")
                cmd_s = " ".join(cmd)
                f.write(f"Running command: {cmd_s}\n")
                f.write(f"Working Directory: {working_dir}\n")
                f.write("Environment:\n")
                for (key, value) in env_vars.items():
                    f.write(f"\t{key}={value}\n")
                f.write("Script:\n")
                with open(script) as rf:
                    script_contents = rf.read()
                    for (key, value) in env_vars.items():
                        script_contents = script_contents.replace("$" + key, value)
                    f.write(script_contents)
                f.write("\n\n------------------------------------\n\n")

            pr = ProcessRunner(cmd, env_vars, stage_id, echo_stdout=self.echo_stdout, log_path=log_path,
                               working_dir=working_dir, timeout=self.timeout)
            if self.stagger_time > 0:
                tl = TimeLocker(self.stagger_time)  # ensure that we don't start processes at exactly the same time
            start_time = time.time()
            (ret, timed_out) = pr.run()
            end_time = time.time()
            if self.stagger_time > 0:
                tl.join()
            self.executor.set_task_result(task_id, ret, timed_out, end_time - start_time)


class LocalExecutor(Executor):

    def __init__(self, environment, stage_configuration={}):
        self.logger = logging.getLogger("LocalExecutor")
        self.shell = environment.get("shell", "/bin/bash")
        self.conda_path = environment.get("conda_path", "~/miniforge3/bin/conda")
        self.temp_path = environment.get("temp_path", "/tmp")
        if self.temp_path != "/tmp":
            os.makedirs(self.temp_path, exist_ok=True)
        self.nr_threads = stage_configuration.get("nr_threads", 1)
        self.timeout = stage_configuration.get("timeout", -1)
        self.echo_stdout = stage_configuration.get("echo_stdout", False)
        self.stagger_time = stage_configuration.get("stagger_time", 0)
        self.retry_count = stage_configuration.get("retry_count", 0)
        self.pending_queue = queue.Queue()
        self.executor_threads = []
        self.task_ids = []
        self.task_results = {}
        self.submitted_count = self.completed_count = 0
        self.task_descriptions = {}
        self.lock = threading.RLock()

        self.logger.info("Created local executor with %d threads, timeout=%d, stagger_time=%d"
                         % (self.nr_threads, self.timeout, self.stagger_time))
        for i in range(self.nr_threads):
            et = ExecutorThread(self, shell=self.shell, conda_path=self.conda_path, echo_stdout=self.echo_stdout,
                                temp_path=self.temp_path, timeout=self.timeout, stagger_time=self.stagger_time)
            et.start()
            self.executor_threads.append(et)

    def clear(self):
        self.task_ids = []
        self.task_results = {}
        self.task_descriptions = {}
        self.submitted_count = self.completed_count = 0

    def get_task(self):
        return self.pending_queue.get()

    def task_started(self, task_id):
        pass

    def queue_task(self, stage_id, script, env, working_dir, description="", try_nr=0, task_id=None):
        self.lock.acquire()
        try:
            if task_id is None:
                task_id = "task-" + uuid.uuid4().hex
                self.submitted_count += 1
                self.task_ids.append(task_id)
            self.pending_queue.put((stage_id, task_id, script, env, working_dir))
            self.task_descriptions[task_id] = (stage_id, script, env, working_dir, description, try_nr)
            return task_id
        finally:
            self.lock.release()

    def set_task_result(self, task_id, ret, timed_out, elapsed_secs):
        succeeded = (ret == 0) and not timed_out
        self.lock.acquire()
        try:
            (stage_id, script, env, working_dir, description, try_nr) = self.task_descriptions[task_id]
            retrying = False
            if not succeeded:
                if try_nr < self.retry_count:
                    try_nr += 1
                    self.queue_task(stage_id, script, env, working_dir, description, try_nr, task_id=task_id)
                    retrying = True
            if not retrying:
                self.task_results[task_id] = succeeded
                self.completed_count += 1
            pct_complete = int(100 * self.completed_count / self.submitted_count)
            if succeeded:
                self.logger.info("[%d%%] task %s (%s) succeeded (%d secs)"
                                 % (pct_complete, task_id, description, int(elapsed_secs)))
            else:
                if timed_out:
                    self.logger.warning("[%d%%] task %s (%s) failed (%d secs) - timed out"
                                        % (pct_complete, task_id, description, int(elapsed_secs)))
                else:
                    self.logger.warning("[%d%%] task %s (%s) failed (%d secs) - check task log"
                                        % (pct_complete, task_id, description, int(elapsed_secs)))
            if retrying:
                self.logger.warning("re-queuing failed task %s (retry %d/%d)"
                                    % (task_id, try_nr, self.retry_count))
        finally:
            self.lock.release()

    def get_task_result(self, task_id):
        self.lock.acquire()
        try:
            return self.task_results.get(task_id, None)
        finally:
            self.lock.release()

    def wait_for_tasks(self):
        while True:
            time.sleep(10)
            self.lock.acquire()
            try:
                if self.submitted_count == self.completed_count:
                    break
            finally:
                self.lock.release()
