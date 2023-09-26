# MIT License
#
# Copyright (c) 2022 National Center for Earth Observation (NCEO)
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

import copy
import time
import os
import logging

from .executor import Executor
from ..pipeline_utils import PipelineUtils

available = False
try:
    from pyjob import use
    use("slurm")

    import pyjob
    available = True
except:
    pass

default_jobopts = {
    'runtime': '04:00',
    'memlimit': '4192',
    'queue': 'short-serial',
    'name': 'testjob'
    }


class SlurmExecutor(Executor):

    def __init__(self, environment, stage_configuration={}):
        self.logger = logging.getLogger("SlurmExecutor")
        self.environment = environment
        self.stage_configuration = stage_configuration
        self.task_ids = []
        self.task_results = {}
        self.jobs = {}
        self.task_descriptions = {}
        self.logger.info("Created slurm executor")

    def queue_task(self, stage_id, script, env, working_dir, description=""):
        with open(script) as f:
            script_contents = f.read()
        script_contents = script_contents.replace("#!/bin/bash","")
        script_contents = script_contents.replace("eval \"$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)\"","")
        script_contents = PipelineUtils.sub_env_vars(script_contents, env)
        options = copy.deepcopy(default_jobopts)
        options["name"] = stage_id

        slurm_options = self.stage_configuration.get("slurm_options", {})

        for (k,v) in slurm_options.items():
            options[k] = v

        curdir = os.getcwd()
        try:
            os.chdir(working_dir)
            job = pyjob.Job('hostname', script=script_contents, options=options, env="/bin/bash")
            task_id = pyjob.cluster.submit(job)
            self.jobs[task_id] = (job, working_dir)
            self.task_ids.append(task_id)
            self.task_descriptions[task_id] = description
            return task_id
        finally:
            os.chdir(curdir)

    def clear(self):
        self.task_ids = []
        self.task_results = {}
        self.jobs = {}
        self.task_descriptions = {}

    def set_task_result(self, task_id, succeeded):
        self.task_results[task_id] = succeeded
        description = self.task_descriptions[task_id]
        if succeeded:
            self.logger.info("task %s (%s) succeeded" % (task_id, description))
        else:
            self.logger.warning(
                "task %s (%s) failed - check task log" % (task_id, description))

    def get_task_result(self, task_id):
        if task_id in self.task_results:
            return self.task_results[task_id]
        (job, working_dir) = self.jobs[task_id]
        fname = os.path.join(working_dir, job.stdoutname.format(jobid=task_id, ind='arr')) + '.err'
        print("get_task_result checking:"+fname)
        if os.path.exists(fname):
            pyjob.cluster.parse_log(fname,job)
            if job.result != 'UNKNOWN':
                succeeded = job.done
                self.task_results[task_id] = succeeded
                return succeeded
        return None

    def wait_for_tasks(self):
        # hey dontcha need some mutexes here buddy?
        waiting = True
        while waiting:
            time.sleep(30)
            waiting = False
            for task_id in self.task_ids:
                result = self.get_task_result(task_id)
                if result is not None:
                    self.task_results[task_id] = result
                else:
                    waiting = True # waiting for at least one task



