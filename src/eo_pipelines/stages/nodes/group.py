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
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.executors.executor_factory import ExecutorType

DEFAULT_TIME_WINDOW_SECONDS = 300

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
        rename = self.get_configuration().get("rename", {})
        overlap_using = self.get_configuration().get("overlap_using", {})
        group_by_attributes = self.get_configuration().get("group_by_attributes", [])
        time_window_seconds = self.get_configuration().get("time_window_seconds", DEFAULT_TIME_WINDOW_SECONDS)
        return {
            "DATASET_BANDS": dataset_bands,
            "OUTPUT_PATH": self.output_path,
            "RENAME": rename,
            "OVERLAP_USING": overlap_using,
            "GROUP_BY_ATTRIBUTES": group_by_attributes,
            "TIME_WINDOW_SECONDS": time_window_seconds
        }

    def execute_stage(self, inputs):

        parameters = self.get_parameters()
        output_paths = {}
        for input_scenes in inputs["input"]:
            group_name = "___".join(sorted(input_scenes.keys()))
            group_path = os.path.abspath(os.path.join(self.output_path, group_name))
            output_paths[group_name] = group_path
            executor = self.create_executor(ExecutorType.Local)

            grouping_spec_file_path = os.path.join(self.get_working_directory(), "grouping_spec.json")
            grouping_spec = {
                "datasets": input_scenes,
                "bands": parameters["DATASET_BANDS"],
                "rename": parameters["RENAME"],
                "group_by_attributes": parameters["GROUP_BY_ATTRIBUTES"],
                "time_window_seconds": parameters["TIME_WINDOW_SECONDS"],
                "overlap_using": parameters["OVERLAP_USING"]
            }
            with open(grouping_spec_file_path, "w") as f:
                f.write(json.dumps(grouping_spec))

            custom_env = {
                "GROUPING_SPEC_PATH": grouping_spec_file_path,
                "OUTPUT_PATH": group_path
            }
            script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "group.sh")
            executor.queue_task(self.get_stage_id(), script, custom_env, self.get_working_directory())
            executor.wait_for_tasks()

        return {"output": output_paths}
