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
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.executors.executor_factory import ExecutorType


class Group(PipelineStage):

    VERSION = "0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "group")
        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(), self.output_path)
        self.get_logger().info("eo_pipeline_stages.Group %s" % Group.VERSION)

    def get_parameters(self):
        all_datasets = self.get_spec().get_datasets()
        dataset_bands = {dataset: self.get_spec().get_bands_for_dataset(dataset) for dataset in all_datasets}
        return { "DATASET_BANDS": dataset_bands, "OUTPUT_PATH": self.output_path }

    def execute_stage(self, inputs):

        output_paths = {}
        for input_scenes in inputs["input"]:

            group_name = "___".join(sorted(input_scenes.keys()))
            group_path = os.path.abspath(os.path.join(self.output_path, group_name))
            output_paths[group_name] = group_path
            executor = self.create_executor(ExecutorType.Local)

            grouping_spec_file_path = os.path.join(self.get_working_directory(),"grouping_spec.json")
            grouping_spec = {
                "datasets": input_scenes,
                "bands": self.get_parameters()["DATASET_BANDS"]
            }
            with open(grouping_spec_file_path,"w") as f:
                f.write(json.dumps(grouping_spec))

            custom_env = {
                "GROUPING_SPEC_PATH": grouping_spec_file_path,
                "OUTPUT_PATH": group_path
            }
            script = os.path.join(os.path.split(__file__)[0], "usgs_group.sh")
            executor.queue_task(self.get_stage_id(),script,custom_env,self.get_working_directory())
            executor.wait_for_tasks()

        return {"output":output_paths}


