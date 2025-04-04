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

import enum

from .local_executor import LocalExecutor
from ..pipeline_exceptions import PipelineExecutorException


class ExecutorType(enum.Enum):
    Local = 0

    valid_executor_type_names = ["local"]

    @staticmethod
    def parse_executor_type_name(name):
        if name == "local":
            return ExecutorType.Local
        else:
            raise PipelineExecutorException("Invalid executor type, should be one of: "
                                            + ",".join(ExecutorType.valid_executor_type_names))

    @staticmethod
    def get_executor_type_name(value):
        if value == ExecutorType.Local:
            return "local"
        else:
            return None


class ExecutorFactory:

    def __init__(self):
        pass

    def create_executor(self, executor_type, environment, executor_settings):

        if executor_type == ExecutorType.Local:
            return LocalExecutor(environment, executor_settings)
        else:
            raise PipelineExecutorException("Unknown executor_type")
