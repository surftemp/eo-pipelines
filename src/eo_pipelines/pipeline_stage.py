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
import json
import os.path

from eo_pipelines.executors.executor_factory import ExecutorType, ExecutorFactory
from .utils.merge_dictionaries_recursive import merge_dictionaries_recursive

class PipelineStage:

    def __init__(self, stage_id, stage_type, configuration, spec, environment):
        self.__stage_id = stage_id
        self.__stage_type = stage_type
        self.__configuration = configuration
        self.__spec = spec
        self.__environment = environment
        self.__execution_settings = {}
        self.__executor_factory = None
        self.__default_executor_type = ExecutorType.Local
        self.__working_directory = self.__configuration.get("working_directory",
                                         os.path.join(self.__environment.get("working_directory","/tmp"), self.__stage_id))
        os.makedirs(self.__working_directory, exist_ok=True)
        self.__input_context_path = os.path.join(self.__working_directory, "input_context.json")
        self.__parameters_context_path = os.path.join(self.__working_directory, "parameters_context.json")
        self.__output_context_path = os.path.join(self.__working_directory, "output_context.json")
        self.__logger = logging.getLogger(self.__stage_id)

        self.__logger.info("Created %s stage id=%s, dir=%s"%(self.__stage_type,self.__stage_id,self.__working_directory))
        self.__executor_factory = ExecutorFactory()

    def set_execution_settings(self, execution_settings):
        self.__execution_settings = execution_settings

    def set_executor_factory(self, executor_factory):
        self.__executor_factory = executor_factory

    def set_default_executor_type(self, default_executor_type):
        self.__default_executor_type = ExecutorType.parse_executor_type_name(default_executor_type)

    def get_configuration(self):
        return self.__configuration

    def get_spec(self):
        return self.__spec

    def get_environment(self):
        return self.__environment

    def create_executor(self, executor_type=None, override_execution_settings=None):
        if executor_type is None:
            executor_type = self.__default_executor_type
        execution_settings = merge_dictionaries_recursive(override_execution_settings,self.__execution_settings)
        return self.__executor_factory.create_executor(executor_type, self.get_environment(), execution_settings)

    def get_input_types(self):
        return {}

    def get_output_types(self):
        return {}

    def set_inputs(self,inputs):
        # dictionary mapping input to "stage-id:output"
        self.inputs = inputs

    def get_inputs(self):
        return self.inputs

    def get_stage_id(self):
        return self.__stage_id

    def get_working_directory(self):
        return self.__working_directory

    def get_logger(self):
        return self.__logger

    def __repr__(self):
        return self.__stage_id+"/"+self.__stage_type

    def run(self,input_context):
        raise NotImplementedError()

    def get_parameters(self):
        return {}

    def can_skip(self, input_context):
        if os.path.exists(self.__input_context_path) \
                and os.path.exists(self.__output_context_path)\
                and os.path.exists(self.__parameters_context_path):

            with open(self.__input_context_path) as input_f:
                last_input_context = json.loads(input_f.read())
                if input_context != last_input_context:
                    return False

            with open(self.__parameters_context_path) as parameters_f:
                last_parameters = json.loads(parameters_f.read())
                if self.get_parameters() != last_parameters:
                    return False
            return True
        return False

    def write_context(self, input_context, output_context):
        with open(self.__input_context_path,"w") as input_f:
            input_f.write(json.dumps(input_context))
        with open(self.__parameters_context_path,"w") as input_f:
            input_f.write(json.dumps(self.get_parameters()))
        with open(self.__output_context_path,"w") as results_f:
            results_f.write(json.dumps(output_context))

    def get_output_context(self):
        if os.path.exists(self.__output_context_path):
            with open(self.__output_context_path) as results_f:
                return json.loads(results_f.read())
        return None

