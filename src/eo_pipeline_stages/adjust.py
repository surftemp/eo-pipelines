
import os

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

class Adjust(PipelineStage):

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "adjust", cfg, spec, environment)
        self.output_path = self.get_configuration()\
            .get("output_path", os.path.join(self.get_working_directory(),"outputs"))
        self.get_logger().info("eo_pipeline_stages.Adjust %s" % VERSION)

    def get_parameters(self):
        return { "OUTPUT_PATH": self.output_path }

    def get_input_types(self):
        return {"input": "netcdf4_yx"}

    def get_output_types(self):
        return {"output": "netcdf4_yx"}

    def run(self, input_context):

        input_context = input_context["input"]

        if "scene_folders" not in input_context:
            raise PipelineStageException("group","No scenes available to compute elevation for")

        executor = ExecutorFactory.create_executor(ExecutorType.Local,self.get_environment())

        scene_folders = input_context["scene_folders"]
        output_context = {"scene_folders": {}}

        for dataset in scene_folders:
            output_folder = os.path.join(self.output_path,dataset)

            custom_env = {
                "INPUT_PATH": scene_folders[dataset],
                "OUTPUT_PATH": output_folder,
                "ADJUST_OPTIONS": "--simplify-coords"
            }
            script = os.path.join(os.path.split(__file__)[0],"adjust.sh")
            task_id = executor.queue_task(self.get_stage_id(),script,custom_env,self.get_working_directory())
            executor.wait_for_tasks()

            if executor.get_task_result(task_id):
                output_context["scene_folders"][dataset] = output_folder
            else:
                return None

        return {"output":output_context}

