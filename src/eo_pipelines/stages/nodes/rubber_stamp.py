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

from eo_pipelines import VERSION
from eo_pipelines.pipeline_stage import PipelineStage
import xarray as xr
from eo_pipelines.utils.runtime_parameters import RuntimeParameters

class RubberStamp(PipelineStage):

    VERSION = "0.0.5"

    def __init__(self, node_services):
        super().__init__(node_services, "rubberstamp")
        self.node_services = node_services

    async def load(self):
        await super().load()

        self.output_folder = self.get_configuration().get("output_folder", "rubberstamped")

        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.get_working_directory(), self.output_folder)

        self.get_logger().info("eo_pipeline_stages.RubberStamp %s" % RubberStamp.VERSION)

    def execute_stage(self, inputs):

        output_scenes = {}

        succeeded = 0
        failed = 0

        if self.output_folder:
            os.makedirs(self.output_folder, exist_ok=True)

        input = inputs.get("input",{})

        yaml_path = RuntimeParameters.get_parameter("YAML_PATH")
        with open(yaml_path,"r") as f:
            yaml_content = f.read()

        for dataset in input:

            output_folder = os.path.join(self.output_folder, dataset)
            os.makedirs(output_folder, exist_ok=True)
            input_folder = input[dataset]

            for fname in os.listdir(input_folder):
                if fname.endswith(".nc"):
                    try:
                        input_path = os.path.join(input_folder, fname)
                        output_path = os.path.join(output_folder, fname)
                        ds = xr.open_dataset(input_path)

                        da = xr.DataArray(0)
                        da.attrs["pipeline"] = yaml_content
                        da.attrs["eo_pipelines_version"] = VERSION
                        for name, value in RuntimeParameters.get_parameters().items():
                            da.attrs[name] = value
                        ds["eo_pipelines_metadata"] = da

                        ds.to_netcdf(output_path)
                        ds.close()
                        succeeded += 1
                    except Exception as ex:
                        self.logger.exception("failed to process %s" % input_path)
                        failed += 1

            output_scenes[dataset] = output_folder

        summary = f"rubberstamp: succeeded:{succeeded}, failed:{failed}"
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
