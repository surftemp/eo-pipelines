
import os

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

class Elevation(PipelineStage):

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "elevation", cfg, spec, environment)
        self.output_path = self.get_configuration()\
            .get("output_path", os.path.join(self.get_working_directory(),"outputs"))
        self.dem_path = self.get_configuration() \
            .get("dem_path",None)
        if not self.dem_path:
            raise PipelineStageException(stage_id,"dem_path not specified in configuration")
        self.get_logger().info("eo_pipeline_stages.Elevation %s" % VERSION)

    def get_parameters(self):
        return { "OUTPUT_PATH": self.output_path, "DEM_PATH": self.dem_path }

    def get_input_types(self):
        return {"input": "netcdf4_yx"}

    def get_output_types(self):
        return {"output": "netcdf4_yx"}

    def run(self, input_context):

        input_context = input_context["input"]

        if "scene_folders" not in input_context:
            raise PipelineStageException("group","No scenes available to compute elevation for")

        executor = self.create_executor(ExecutorType.Local)

        scene_folders = input_context["scene_folders"]
        output_context = {"scene_folders": {}}
        os.makedirs(self.output_path,exist_ok=True)

        for dataset in scene_folders:
            output_folder = os.path.join(self.output_path,dataset)
            os.makedirs(output_folder, exist_ok=True)

            custom_env = {
                "INPUT_PATH": scene_folders[dataset],
                "OUTPUT_PATH": output_folder,
                "DEM_PATH": self.dem_path
            }
            script = os.path.join(os.path.split(__file__)[0],"elevation.sh")
            task_id = executor.queue_task(self.get_stage_id(),script,custom_env,self.get_working_directory())
            executor.wait_for_tasks()

            if executor.get_task_result(task_id):
                output_context["scene_folders"][dataset] = output_folder
            else:
                return None

        return {"output":output_context}

