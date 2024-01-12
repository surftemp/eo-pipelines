import os.path
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.executors.executor_factory import ExecutorType
from eo_pipelines.pipeline_stage_utils import format_int, format_float


class LandsatImport(PipelineStage):

    VERSION = "0.0.2"

    DEFAULT_RESOLUTION = 50

    def __init__(self, node_services):
        super().__init__(node_services,"landsat_import")
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

        succeeded = 0
        failed = 0

        inject_metadata = self.get_configuration().get("inject_metadata", None)
        inject_metadata_cmd = ""
        if inject_metadata is not None:
            inject_metadata_cmd = "--inject-metadata"
            for (key,value) in inject_metadata.items():
                inject_metadata_cmd += f" {key}=\"{value}\""

        for input in inputs["input"]:

            for dataset in input:
                metadata_paths = []
                dataset_folder = input[dataset]
                for filename in os.listdir(dataset_folder):
                    if filename.endswith("xml"):
                        metadata_paths.append(os.path.join(dataset_folder,filename))

                dataset_output_folder = os.path.join(self.output_path,dataset)
                os.makedirs(dataset_output_folder, exist_ok=True)
                output_scenes[dataset] = dataset_output_folder
                task_ids = []
                for metadata_path in metadata_paths:

                    metadata_id = os.path.splitext(os.path.split(metadata_path)[1])[0]
                    custom_env = self.get_parameters()
                    custom_env["BANDS"] = ",".join(self.get_spec().get_bands_for_dataset(dataset))
                    custom_env["SCENE_PATH"] = metadata_path
                    custom_env["OUTPUT_PATH"] = dataset_output_folder
                    custom_env["INJECT_METADATA"] = inject_metadata_cmd

                    script = os.path.join(os.path.split(__file__)[0], "landsat_import.sh")
                    task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                              description=metadata_id)
                    task_ids.append(task_id)


                executor.wait_for_tasks()
                for task_id in task_ids:
                    if executor.get_task_result(task_id):
                        succeeded += 1
                    else:
                        failed += 1

        summary = f"landsat import scenes: succeeded:{succeeded}, failed:{failed}"
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


