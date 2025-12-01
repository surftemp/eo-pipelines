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

import os
import logging
import os.path
import time
import json

from eo_pipelines.executors.local_executor import LocalExecutor


class PipelineStage:
    CONCURRENT_EXECUTION_CHECK_AVG_INTERVAL_S = 60

    def __init__(self, node_services, stage_type):
        self.node_services = node_services
        self.stage_id = node_services.get_node_id()
        self.stage_type = stage_type
        self.configuration = None
        self.spec = None
        self.environment = None
        self.executor_settings = None
        self.executor_factory = None
        self.working_directory = None
        self.logger = logging.getLogger(self.stage_id)

    async def load(self):
        properties = await self.node_services.get_properties()
        self.configuration = properties.get("configuration", {})
        self.spec = self.node_services.get_configuration().get_spec()
        self.environment = self.node_services.get_configuration().get_environment()
        self.executor_settings = properties.get("executor_settings", {})
        self.working_directory = self.configuration.get("working_directory",
                os.path.join(self.environment.get("working_directory", os.getcwd()), self.stage_id))

        if not os.path.isabs(self.working_directory):
            self.working_directory = os.path.abspath(self.working_directory)

        os.makedirs(self.working_directory, exist_ok=True)

        self.logger.info(
            "Created %s stage id=%s, dir=%s" % (self.stage_type, self.stage_id, self.working_directory))


    def get_configuration(self):
        return self.configuration

    def get_spec(self):
        return self.spec

    def get_environment(self):
        return self.environment

    def create_executor(self):
        return LocalExecutor(self.get_environment(), self.executor_settings)

    def get_stage_id(self):
        return self.stage_id

    def get_working_directory(self):
        return self.working_directory

    def get_logger(self):
        return self.logger

    def __repr__(self):
        return self.stage_id + "/" + self.stage_type

    async def run(self, inputs):
        start_time = time.time()
        can_skip = self.executor_settings.get("can_skip", False)
        results_path = os.path.join(self.working_directory, "results.json")
        try:
            if not can_skip or not os.path.exists(results_path):
                self.logger.info("Executing stage %s " % self.stage_type)
                result = self.execute_stage(inputs)
                with open(results_path, "w") as of:
                    of.write(json.dumps(result))
                duration = int(time.time() - start_time)
                self.logger.info("Executed %s stage (%d seconds)" % (self.stage_type, duration))
            else:
                self.logger.info("Skipping stage %s execution (reusing previous results)" % self.stage_type)
                with open(results_path) as f:
                    result = json.loads(f.read())
        except Exception as ex:
            duration = int(time.time() - start_time)
            self.logger.info("Failed stage %s with %s (%d seconds)" % (self.stage_type, str(ex), duration))
            raise

        return result

    def execute_stage(self, inputs):
        # must be implemented in a sub-class
        raise NotImplementedError()

    def get_parameters(self):
        return {}
