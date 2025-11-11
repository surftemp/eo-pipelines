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
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.executors.executor_factory import ExecutorType
from eo_pipelines.pipeline_stage_utils import format_int, format_float


class LandsatImport(PipelineStage):
    VERSION = "0.0.2"

    DEFAULT_RESOLUTION = 50

    def __init__(self, node_services):
        super().__init__(node_services, "landsat_import")
        self.node_services = node_services

        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(), self.output_path)

        self.get_logger().info("eo_pipeline_stages.LandsatImport %s" % LandsatImport.VERSION)

    def get_parameters(self):
        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max())
        }

    def execute_stage(self, inputs):

        executor = self.create_executor(ExecutorType.Local)

        output_scenes = {}

        inject_metadata = self.get_configuration().get("inject_metadata", None)
        inject_metadata_cmd = ""
        if inject_metadata is not None:
            inject_metadata_cmd = "--inject-metadata"
            for (key, value) in inject_metadata.items():
                inject_metadata_cmd += f" {key}=\"{value}\""

        error_fraction_threshold = self.get_configuration().get("error_fraction_threshold", 0.03)
        export_optical_as = self.get_configuration().get("export_optical_as","")
        export_int16 = self.get_configuration().get("export_int16",{})

        total_succeeded = 0
        total_failed = 0

        for input in inputs["input"]:

            for dataset in input:
                succeeded = 0
                failed = 0

                bands = self.get_spec().get_bands_for_dataset(dataset)
                export_cmds = []
                if export_optical_as:
                    export_cmds.append(f"--export-optical-as {export_optical_as}")
                for band in bands:
                    if band in export_int16:
                        scale = export_int16[band].get("scale",1)
                        offset = export_int16[band].get("offset",0)
                        export_cmds.append(f"--export-int16 {band} {scale} {offset}")

                export_cmd = " ".join(export_cmds)
                metadata_paths = []
                dataset_folder = input[dataset]
                for filename in os.listdir(dataset_folder):
                    if filename.endswith("xml"):
                        metadata_paths.append(os.path.join(dataset_folder, filename))

                dataset_output_folder = os.path.join(self.output_path, dataset)
                os.makedirs(dataset_output_folder, exist_ok=True)
                output_scenes[dataset] = dataset_output_folder
                task_ids = []
                if metadata_paths:
                    for metadata_path in metadata_paths:
                        metadata_id = os.path.splitext(os.path.split(metadata_path)[1])[0]
                        custom_env = self.get_parameters()
                        custom_env["BANDS"] = " ".join(bands)
                        custom_env["SCENE_PATH"] = metadata_path
                        custom_env["OUTPUT_PATH"] = dataset_output_folder
                        custom_env["INJECT_METADATA"] = inject_metadata_cmd
                        custom_env["EXPORT_CMD"] = export_cmd

                        script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "landsat_import.sh")
                        task_id = executor.queue_task(self.get_stage_id(), script, custom_env,
                                                      self.get_working_directory(),
                                                      description=metadata_id)
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
                            f"Failed to import dataset {dataset}: error fraction {error_fraction} > threshold {error_fraction_threshold}")

                    total_succeeded += succeeded
                    total_failed += failed

        self.get_logger().info(f"landsat import scenes: succeeded:{total_succeeded}, failed:{total_failed}")

        return {"output": output_scenes}
