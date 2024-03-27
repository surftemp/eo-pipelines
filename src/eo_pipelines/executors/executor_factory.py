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

import enum
import os.path

from .local_executor import LocalExecutor
from .slurm_executor import SlurmExecutor, available as slurm_available
from ..pipeline_exceptions import PipelineExecutorException
from .tracking_database import TrackingDatabase

tracking_databases = {}

class ExecutorType(enum.Enum):

    Local = 0
    Slurm = 1

    valid_executor_type_names = ["local","slurm"]

    @staticmethod
    def parse_executor_type_name(name):
        if name == "local":
            return ExecutorType.Local
        elif name == "slurm":
            return ExecutorType.Slurm
        else:
            raise PipelineExecutorException("Invalid executor type, should be one of: "
                             + ",".join(ExecutorType.valid_executor_type_names))

    @staticmethod
    def get_executor_type_name(value):
        if value == ExecutorType.Local:
            return "local"
        elif value == ExecutorType.Slurm:
            return "slurm"
        else:
            return None

class ExecutorFactory:

    def __init__(self):
        pass

    def create_executor(self,executor_type,environment,executor_settings):
        tracking_path = environment.get("tracking_database_path",None)

        tracking_database = None
        if tracking_path:
            if not os.path.isabs(tracking_path):
                tracking_path = os.path.abspath(tracking_path)
            if tracking_path not in tracking_databases:
                tracking_databases[tracking_path] = TrackingDatabase(tracking_path, run_id=environment.get("run_id","?"))
            tracking_database = tracking_databases[tracking_path]

        if executor_type == ExecutorType.Local:
            return LocalExecutor(environment, executor_settings, tracking_database)
        elif executor_type == ExecutorType.Slurm:
            if not slurm_available:
                raise PipelineExecutorException("SLURM executor not available - check pyjob is installed")
            return SlurmExecutor(environment, executor_settings, tracking_database)
        else:
            raise PipelineExecutorException("Unknown executor_type")

