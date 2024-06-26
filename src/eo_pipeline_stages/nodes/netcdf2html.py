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

class Netcdf2Html(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "netcdf2html")
        self.node_services = node_services

        self.output_folder = self.get_configuration().get("output_folder", "html_output")

        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.get_working_directory(), self.output_folder)

        self.get_logger().info("eo_pipeline_stages.netcdf2html %s" % Netcdf2Html.VERSION)

    def get_parameters(self):
        parameters = {}
        title = self.get_configuration().get("title","")
        if title:
            parameters["TITLE"] = str(title)
        return parameters

    def build_options(self):
        options = ""
        for option in ["netcdf-download-filename","sample-count","sample-cases"]:
            option_value = self.get_configuration().get(option,"")
            if option_value:
                options += f' --{option} "{option_value}"'
        return options

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_scenes = {}

        succeeded = 0
        failed = 0

        layer_config_path = os.path.join(self.get_working_directory(),"layers.json")

        import json
        with open(layer_config_path,"w") as f:
            f.write(json.dumps(self.get_configuration().get("specification")))

        if self.output_folder:
            os.makedirs(self.output_folder,exist_ok=True)

        options = self.build_options()

        for input in inputs["input"]:

            for dataset in input:

                input_folder = input[dataset]

                output_folder = os.path.join(self.output_folder,dataset)

                input_paths = []
                for fname in os.listdir(input_folder):
                    if fname.endswith(".nc"):
                        input_paths.append(os.path.join(input_folder,fname))

                for input_path in input_paths:

                    filename_root = os.path.splitext(os.path.split(input_path)[1])[0]

                    output_path = os.path.join(output_folder,filename_root)
                    os.makedirs(output_path,exist_ok=True)

                    script = os.path.join(os.path.split(__file__)[0], "netcdf2html.sh")

                    custom_env = {
                        "INPUT_PATH": input_path,
                        "OUTPUT_PATH": output_path,
                        "LAYER_CONFIG_PATH": layer_config_path,
                        "OPTIONS": options
                    }

                    custom_env.update(self.get_parameters())

                    if "TITLE" not in custom_env:
                        custom_env["TITLE"] = filename_root

                    task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                                  description=dataset)

                    executor.wait_for_tasks()
                    if executor.get_task_result(task_id):
                        succeeded += 1
                        if self.output_folder:
                            output_scenes[dataset] = output_folder
                    else:
                        failed += 1

        summary = f"netcdf2html: succeeded:{succeeded}, failed:{failed}"
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
        if failed > 0:
            raise Exception("Failed")
        return {}


