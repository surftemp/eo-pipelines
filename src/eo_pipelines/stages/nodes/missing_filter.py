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

import os.path

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int


class MissingFilter(PipelineStage):
    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "missing_filter")
        self.node_services = node_services

        self.output_folder = self.get_configuration().get("output_folder", "filtered_scenes")

        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.get_working_directory(), self.output_folder)

        self.get_logger().info("MissingFilter %s" % MissingFilter.VERSION)

    def get_parameters(self):
        params = {}
        params["BAND"] = str(self.get_configuration().get("band"))
        params["MAX_MISSING_FRACTION"] = str(self.get_configuration().get("max_missing_fraction"))
        return params

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_scenes = {}

        succeeded = 0
        failed = 0

        if self.output_folder:
            os.makedirs(self.output_folder, exist_ok=True)

        for input in inputs["input"]:

            for dataset in input:

                custom_env = self.get_parameters()
                input_path = input[dataset]
                custom_env["INPUT_FOLDER"] = input_path
                output_folder = os.path.join(self.output_folder, dataset)
                custom_env["OUTPUT_FOLDER"] = output_folder

                count = 0
                for fname in os.listdir(input_path):
                    if fname.endswith(".nc"):
                        count += 1

                if count:

                    script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "missing_filter.sh")

                    task_id = executor.queue_task(self.get_stage_id(), script, custom_env, self.get_working_directory(),
                                                  description=dataset)

                    executor.wait_for_tasks()
                    if executor.get_task_result(task_id):
                        succeeded += 1
                        if self.output_folder:
                            output_scenes[dataset] = output_folder
                    else:
                        failed += 1

        return {"output": output_scenes}
