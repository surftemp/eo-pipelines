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


class CustomProcessor(PipelineStage):
    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "custom_processor")
        self.node_services = node_services

    async def load(self):
        await super().load()
        self.output_folder = self.get_configuration().get("output_path", "altered_scenes")
        self.script_path = self.get_configuration().get("script_path", "")

        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.get_working_directory(), self.output_folder)

        self.get_logger().info("eo_pipeline_stages.CustomProcessor %s" % CustomProcessor.VERSION)

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_scenes = {}

        succeeded = 0
        failed = 0

        if self.output_folder:
            os.makedirs(self.output_folder, exist_ok=True)

        config_path = os.path.join(self.get_working_directory(), "config.json")

        import json
        with open(config_path, "w") as f:
            f.write(json.dumps(self.get_configuration().get("specification", {})))

        input = inputs.get("input",{})

        for dataset in input:

            output_folder = os.path.join(self.output_folder, dataset)
            os.makedirs(output_folder, exist_ok=True)
            input_folder = input[dataset]

            custom_env = {
                "INPUT_FOLDER": input_folder,
                "OUTPUT_FOLDER": output_folder
            }

            task_id = executor.queue_task(self.get_stage_id(), self.script_path, custom_env,
                                          self.get_working_directory(),
                                          description=dataset)

            executor.wait_for_tasks()

            if executor.get_task_result(task_id):
                succeeded += 1
            else:
                failed += 1

            output_scenes[dataset] = output_folder

        summary = f"custom_processor: succeeded:{succeeded}, failed:{failed}"
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

        return {"output": output_scenes}
