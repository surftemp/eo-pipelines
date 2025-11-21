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
import shutil

from eo_pipelines.pipeline_stage import PipelineStage


class ReadDatasetDirectory(PipelineStage):
    VERSION = "0.0.2"

    DEFAULT_RESOLUTION = 50

    def __init__(self, node_services):
        super().__init__(node_services, "read_dataset_directory")
        self.node_services = node_services

    async def load(self):
        await super().load()
        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(), self.output_path)

        self.get_logger().info("eo_pipeline_stages.ReadDatasetDirectory %s" % ReadDatasetDirectory.VERSION)

    def execute_stage(self, inputs):

        dataset_name = self.get_configuration().get("dataset_name", None)
        dataset_directory = self.get_configuration().get("dataset_directory", None)

        if os.path.isfile(dataset_directory):
            filename = os.path.split(dataset_directory)[-1]
            shutil.copyfile(dataset_directory, os.path.join(self.output_path, filename))
            return {"output": {dataset_name: self.output_path}}
        else:
            return {"output": {dataset_name: dataset_directory}}
