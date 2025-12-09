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
import csv
import json
import datetime

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int, format_date, format_float

class CreateGrid(PipelineStage):

    VERSION = "0.0.6"

    def __init__(self, node_services):
        super().__init__(node_services, "create_grid")
        self.node_services = node_services

    async def load(self):
        await super().load()
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.get_logger().info("eo_pipeline_stages.CreateGrid %s" % CreateGrid.VERSION)
        self.resolution = self.get_configuration().get("resolution", None)

    def get_parameters(self):
        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "RESOLUTION": format_float(self.resolution)
        }

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_grid_path = os.path.join(self.output_path, "grid.nc")

        custom_env = self.get_parameters()
        custom_env["OUTPUT_PATH"] = output_grid_path

        script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "create_grid.sh")
        task_id = executor.queue_task(self.get_stage_id(), script, custom_env,
                                                   self.get_working_directory())
        executor.wait_for_tasks()
        if not executor.get_task_result(task_id):
            self.get_logger().error("create_grid.sh failed")

        return {"output": output_grid_path}
