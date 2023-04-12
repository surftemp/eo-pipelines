
import os

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

class TimeStacker(PipelineStage):

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "time_stacker", cfg, spec, environment)
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.output_name = self.get_configuration().get("output_name", "stacked.nc")
        if len(os.path.split(self.output_path)) == 1:
            self.output_path = os.path.join(self.get_working_directory(),self.output_path)
        self.get_logger().info("eo_pipeline_stages.TimeStacker %s" % VERSION)

    def get_parameters(self):
        return {"OUTPUT_PATH":self.output_path}

    def get_input_types(self):
        return {"input": "netcdf4_yx"}

    def get_output_types(self):
        return {"output": "netcdf4_yxt"}

    def run(self, input_context):

        input_context = input_context["input"]

        if "scene_folders" not in input_context:
            raise PipelineStageException("group","No scenes output from previous stage")

        executor = self.create_executor(ExecutorType.Local)

        output_scene_folders = {}
        task_ids = []
        for (dataset,folder) in input_context["scene_folders"].items():
            output_folder = os.path.join(self.output_path,dataset)
            os.makedirs(output_folder,exist_ok=True)
            path = os.path.join(output_folder, self.output_name)
            custom_env = {
                "INPUT_FOLDER": folder,
                "OUTPUT_PATH": path,
                "BASHENV":"~/.bashrc"
            }
            script = os.path.join(os.path.split(__file__)[0], "time_stacker.sh")
            if not os.path.exists(script):
                raise PipelineStageException("time_stacker", "script %s not found" % script)

            task_id = executor.queue_task(self.get_stage_id(), script, custom_env, self.get_working_directory())
            task_ids.append(task_id)
            output_scene_folders[dataset] = path
        executor.wait_for_tasks()

        success = 0
        failed = 0
        for task_id in task_ids:
            if executor.get_task_result(task_id):
                success += 1
            else:
                failed += 1

        if success > 0:
            output_context = {"stacked_scene_folders": output_scene_folders, "scene_folders":input_context["scene_folders"]}
            return {"output":output_context}
        else:
            return None
