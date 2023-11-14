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

import os
import logging
import os.path
import time

from eo_pipelines.executors.executor_factory import ExecutorType, ExecutorFactory

class PipelineStage:

    def __init__(self, stage_id, stage_type, configuration, spec, environment):
        self.__stage_id = stage_id
        self.__stage_type = stage_type
        self.__configuration = configuration or {}
        self.__spec = spec
        self.__environment = environment
        self.__execution_settings = self.__configuration.get("executor_settings",{})
        self.__executor_factory = None
        if "executor_type" in self.__execution_settings:
            executor_type_name = self.__execution_settings["executor_type"]
            self.__default_executor_type = ExecutorType.parse_executor_type_name(executor_type_name)
        else:
            self.__default_executor_type = ExecutorType.Local
        self.__working_directory = self.__configuration.get("working_directory",
                                         os.path.join(self.__environment.get("working_directory","/tmp"), self.__stage_id))
        os.makedirs(self.__working_directory, exist_ok=True)

        self.__logger = logging.getLogger(self.__stage_id)

        self.__logger.info("Created %s stage id=%s, dir=%s"%(self.__stage_type,self.__stage_id,self.__working_directory))
        self.__executor_factory = ExecutorFactory()

    def set_executor_factory(self, executor_factory):
        self.__executor_factory = executor_factory

    def get_configuration(self):
        return self.__configuration

    def get_spec(self):
        return self.__spec

    def get_environment(self):
        return self.__environment

    def create_executor(self, executor_type=None):
        if executor_type is None:
            executor_type = self.__default_executor_type
        executor_name = ExecutorType.get_executor_type_name(executor_type)
        executor_settings = self.__execution_settings.get(executor_name,{})
        return self.__executor_factory.create_executor(executor_type, self.get_environment(), executor_settings)

    def get_stage_id(self):
        return self.__stage_id

    def get_working_directory(self):
        return self.__working_directory

    def get_logger(self):
        return self.__logger

    def __repr__(self):
        return self.__stage_id+"/"+self.__stage_type

    def execute(self,inputs):
        start_time = time.time()
        self.__logger.info("Executing stage %s " % self.__stage_type)
        try:
            result = self.execute_stage(inputs)
        except Exception as ex:
            duration = int(time.time() - start_time)
            self.__logger.info("Failed stage %s with %s (%d seconds)" % (self.__stage_type, str(ex), duration))
            raise
        duration = int(time.time() - start_time)
        self.__logger.info("Executed %s stage (%d seconds)" % (self.__stage_type, duration))
        return result

    def execute_stage(self, inputs):
        raise NotImplementedError()

    def get_parameters(self):
        return {}


