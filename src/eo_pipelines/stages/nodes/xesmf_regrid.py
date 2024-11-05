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

import os.path

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int


class XESMFRegrid(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "xesmf_regrid")
        self.node_services = node_services

        self.output_path = self.get_configuration().get("output_path",None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(),self.output_path)

        self.cache_path = self.get_configuration().get("cache_path", None)
        if self.cache_path is None:
            self.cache_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.cache_path):
                self.cache_path = os.path.join(self.get_working_directory(),self.cache_path)

        self.get_logger().info("eo_pipeline_stages.XESMFRegrid %s" % XESMFRegrid.VERSION)

    def get_parameters(self):
        parameters = {
            "GRID_PATH": self.get_configuration().get("target_grid_path"),
            "MAX_DISTANCE": format_int(self.get_configuration().get("max_distance",100))
        }
        if self.cache_path:
            parameters["CACHE_PATH"] = "--cache-folder "+self.cache_path
        return parameters

    def execute_stage(self, inputs):

        os.makedirs(self.cache_path,exist_ok=True)
        executor = self.create_executor()

        output_scenes = {}

        error_fraction_threshold = self.get_configuration().get("error_fraction_threshold", 0.01)

        total_succeeded = 0
        total_failed = 0

        for input in inputs["input"]:

            for dataset in input:
                succeeded = 0
                failed = 0
                input_paths = []
                dataset_folder = input[dataset]
                for filename in os.listdir(dataset_folder):
                    if filename.endswith(".nc"):
                        input_paths.append(os.path.join(dataset_folder,filename))

                dataset_output_folder = os.path.abspath(os.path.join(self.output_path,dataset))
                os.makedirs(dataset_output_folder, exist_ok=True)
                output_scenes[dataset] = dataset_output_folder

                task_ids = []
                if input_paths:
                    for input_path in input_paths:
                        custom_env = self.get_parameters()
                        custom_env["INPUT_PATH"] = input_path
                        custom_env["OUTPUT_FOLDER"] = dataset_output_folder
                        script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "xesmf_regrid.sh")
                        task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                                  description=os.path.split(input_path)[-1])
                        task_ids.append(task_id)

                    executor.wait_for_tasks()
                    for task_id in task_ids:
                        if executor.get_task_result(task_id):
                            succeeded += 1
                        else:
                            failed += 1

                    error_fraction = failed / (succeeded + failed)
                    if error_fraction > error_fraction_threshold:
                        raise Exception(
                            f"Failed to regrid dataset {dataset}: error fraction {error_fraction} > threshold {error_fraction_threshold}")

                    total_succeeded += succeeded
                    total_failed += failed

        self.get_logger().info(f"Regrid scenes: succeeded:{total_succeeded}, failed:{total_failed}")

        return {"output":output_scenes}


