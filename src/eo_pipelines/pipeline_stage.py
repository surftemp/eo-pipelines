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
import datetime
import os
import logging
import os.path
import random
import time
import json
import uuid

from eo_pipelines.executors.executor_factory import ExecutorType, ExecutorFactory

class PipelineStage:

    CONCURRENT_EXECUTION_CHECK_AVG_INTERVAL_S = 60

    def __init__(self, node_services, stage_type):
        self.__stage_id = node_services.get_node_id()
        self.__stage_type = stage_type
        self.__configuration = node_services.get_property("configuration",{})
        self.__spec = node_services.get_configuration().get_spec()
        self.__environment = node_services.get_configuration().get_environment()
        self.__executor_settings = node_services.get_property("executor_settings",{})
        self.__executor_factory = None
        self.__tracking_path = None
        if "executor_type" in self.__executor_settings:
            executor_type_name = self.__executor_settings["executor_type"]
            self.__default_executor_type = ExecutorType.parse_executor_type_name(executor_type_name)
        else:
            self.__default_executor_type = ExecutorType.Local
        self.__working_directory = self.__configuration.get("working_directory",
                                         os.path.join(self.__environment.get("working_directory","/tmp"), self.__stage_id))

        if not os.path.isabs(self.__working_directory):
            self.__working_directory = os.path.abspath(self.__working_directory)

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
        executor_settings = self.__executor_settings.get(executor_name,{})
        return self.__executor_factory.create_executor(executor_type, self.get_environment(), executor_settings)

    def get_stage_id(self):
        return self.__stage_id

    def get_working_directory(self):
        return self.__working_directory

    def get_logger(self):
        return self.__logger

    def __repr__(self):
        return self.__stage_id+"/"+self.__stage_type

    def check_concurrency_limit(self):
        concurrency_limit = self.__executor_settings.get("concurrent_execution_limit",None)
        if concurrency_limit is None:
            return # execute immediately

        tracking_folder = concurrency_limit.get("tracking_folder",None)
        limit = concurrency_limit.get("limit",1)
        tracking_filename = str(uuid.uuid4())+".id"
        if not tracking_folder:
            raise Exception("Unable to enforce concurrent_execution_limit, tracking_folder not specified")
        os.makedirs(tracking_folder,exist_ok=True)

        def wait_for_slot():
            while True:
                files = [f for f in os.listdir(tracking_folder) if f.endswith(".id")]
                if len(files) >= limit:
                    # wait here
                    time.sleep(random.random()+2*PipelineStage.CONCURRENT_EXECUTION_CHECK_AVG_INTERVAL_S)
                else:
                    break

        self.__tracking_path = os.path.join(tracking_folder,tracking_filename)

        dawdle_start_time = time.time()
        self.__logger.info("Limiting concurrency for stage %s " % self.__stage_type)

        while True:

            wait_for_slot()

            # claim the slot
            with open(self.__tracking_path,"w") as f:
                f.write(str(datetime.datetime.utcnow()))

            # wait a short while and recheck for collision
            time.sleep(10*random.random())

            # recheck for unlikely contention with another execution
            files = [f for f in os.listdir(tracking_folder) if f.endswith(".id")]
            if len(files) > limit:
                # back off and retry
                os.remove(self.__tracking_path)
            else:
                break

        dawdle_duration = int(time.time() - dawdle_start_time)
        self.__logger.info("Limit concurrency delay for stage %s complete (%d seconds)" % (self.__stage_type, dawdle_duration))

    def release_tracking(self):
        if self.__tracking_path:
            os.remove(self.__tracking_path)

    async def execute(self,inputs):
        start_time = time.time()
        can_skip = self.__executor_settings.get("can_skip", False)
        results_path = os.path.join(self.__working_directory,"results.json")
        try:
            if not can_skip or not os.path.exists(results_path):
                self.check_concurrency_limit()
                self.__logger.info("Executing stage %s " % self.__stage_type)
                result = self.execute_stage(inputs)
                with open(results_path,"w") as of:
                    of.write(json.dumps(result))
                duration = int(time.time() - start_time)
                self.__logger.info("Executed %s stage (%d seconds)" % (self.__stage_type, duration))
            else:
                self.__logger.info("Skipping stage %s execution (reusing previous results)" % self.__stage_type)
                with open(results_path) as f:
                    result = json.loads(f.read())
        except Exception as ex:
            duration = int(time.time() - start_time)
            self.__logger.info("Failed stage %s with %s (%d seconds)" % (self.__stage_type, str(ex), duration))
            raise
        finally:
            self.release_tracking()

        return result

    def execute_stage(self, inputs):
        raise NotImplementedError()

    def get_parameters(self):
        return {}


