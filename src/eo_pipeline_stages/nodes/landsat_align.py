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


class LandsatAlign(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "landsat_align")
        self.node_services = node_services

        self.output_min_path = self.get_configuration().get("output_min_path", "")
        self.output_max_path = self.get_configuration().get("output_max_path", "")
        self.output_folder = self.get_configuration().get("output_folder", "")

        if self.output_min_path:
            if not os.path.isabs(self.output_min_path):
                self.output_min_path = os.path.join(self.get_working_directory(),self.output_min_path)

        if self.output_max_path:
            if not os.path.isabs(self.output_max_path):
                self.output_max_path = os.path.join(self.get_working_directory(),self.output_max_path)

        if self.output_folder:
            if not os.path.isabs(self.output_folder):
                self.output_folder = os.path.join(self.get_working_directory(),self.output_folder)

        self.get_logger().info("eo_pipeline_stages.LandsatAlign %s" % LandsatAlign.VERSION)

    def get_parameters(self):
        params = {}
        if self.output_folder:
            params["OUTPUT_FOLDER"] = "--output-folder "+self.output_folder

        if self.output_min_path:
            params["OUTPUT_MIN_PATH"] = "--output-min-path "+self.output_min_path

        if self.output_max_path:
            params["OUTPUT_MAX_PATH"] = "--output-max-path "+self.output_max_path

        return params

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_scenes = {}

        succeeded = 0
        failed = 0

        if self.output_min_path:
            folder = os.path.split(self.output_min_path)[0]
            os.makedirs(folder,exist_ok=True)
        if self.output_folder:
            os.makedirs(self.output_folder,exist_ok=True)

        for input in inputs["input"]:

            for dataset in input:

                custom_env = self.get_parameters()
                input_path =  input[dataset]
                custom_env["INPUT_FOLDER"] = input_path

                count = 0
                for fname in os.listdir(input_path):
                    if fname.endswith(".nc"):
                        count += 1

                if count < 2:
                    self.get_logger().warning(f"not enough input files for {dataset}")
                    continue

                script = os.path.join(os.path.split(__file__)[0], "landsat_align.sh")
                task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                              description=os.path.split(input_path)[-1])

                executor.wait_for_tasks()
                if executor.get_task_result(task_id):
                    succeeded += 1
                    if self.output_folder:
                        output_scenes[dataset] = self.output_folder
                else:
                    failed += 1

        summary = f"Landsat Align: succeeded:{succeeded}, failed:{failed}"
        if succeeded > 0:
            if failed > 0:
                self.get_logger().warn(summary)
            else:
                self.get_logger().info(summary)
        else:
            if failed > 0:
                self.get_logger().error(summary)
            else:
                self.get_logger().warn(summary)

        return {"output":output_scenes}


