
import os
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

class Group(PipelineStage):

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "group", cfg, spec, environment)
        self.output_path = self.get_configuration()\
            .get("output_path", os.path.join(self.get_working_directory(),"outputs"))
        self.get_logger().info("eo_pipeline_stages.Group %s" % VERSION)

    def get_parameters(self):
        all_datasets = self.get_spec().get_datasets()
        dataset_bands = {dataset: self.get_spec().get_bands_for_dataset(dataset) for dataset in all_datasets}
        return { "DATASET_BANDS": dataset_bands, "OUTPUT_PATH": self.output_path }

    def get_input_types(self):
        return {"input": "netcdf4_yx"}

    def get_output_types(self):
        return {"output": "netcdf4_yx"}

    def run(self, input_context):

        input_context = input_context["input"]

        if "scene_folders" not in input_context:
            raise PipelineStageException("group","No scenes available to group")

        executor = self.create_executor(ExecutorType.Local)

        grouping_spec_file_path = os.path.join(self.get_working_directory(),"grouping_spec.json")
        grouping_spec = {
            "datasets": input_context["scene_folders"],
            "bands": self.get_parameters()["DATASET_BANDS"]
        }
        with open(grouping_spec_file_path,"w") as f:
            f.write(json.dumps(grouping_spec))

        custom_env = {
            "GROUPING_SPEC_PATH": grouping_spec_file_path,
            "OUTPUT_PATH": self.output_path
        }
        script = os.path.join(os.path.split(__file__)[0],"group.sh")
        task_id = executor.queue_task(self.get_stage_id(),script,custom_env,self.get_working_directory())
        executor.wait_for_tasks()

        if executor.get_task_result(task_id):
            output_context = {"scene_folders": {"combined":self.output_path}}
            return {"output":output_context}
        else:
            return None

