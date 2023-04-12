
import os.path
import random
from yaml import dump
from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

class SliderTool(PipelineStage):

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "slider_tool", cfg, spec, environment)
        self.slider_tool_config = self.get_configuration()
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.yaml_path = os.path.join(self.output_path, "slider.yaml")

        os.makedirs(self.output_path, exist_ok=True)
        with open(self.yaml_path,"w") as f:
            f.write(dump(self.slider_tool_config))
        self.sample_seed = self.get_configuration().get("sample_seed", 1.0)
        self.sample_fraction = self.get_configuration().get("sample_fraction",1.0)
        self.get_logger().info("eo_pipeline_stages.SliderTool %s" % VERSION)

    def get_input_types(self):
        return {"input": "netcdf4_yx"}

    def get_output_types(self):
        return {"output": "slider_tool_metadata"}

    def get_parameters(self):
        return {
            "OUTPUT_PATH": self.output_path,
            "SAMPLE_SEED": self.sample_seed,
            "SAMPLE_FRACTION": self.sample_fraction
        }

    def run(self, input_context):

        input_context = input_context["input"]

        if "scene_folders" not in input_context:
            raise PipelineStageException("slider_tool", "No fetched scenes to plot")

        executor = ExecutorFactory.create_executor(ExecutorType.Local, self.get_environment())

        failed = success = 0
        rng = random.Random(self.sample_seed)
        task_ids = []
        for (dataset,folder) in input_context["scene_folders"].items():

            for filename in os.listdir(folder):
                if not filename.endswith(".nc"):
                    continue

                if rng.random() > self.sample_fraction:
                    continue

                input_filepath = os.path.join(folder,filename)
                output_filename = os.path.splitext(filename)[0]+".html"
                output_filepath = os.path.join(self.output_path,output_filename)
                custom_env = {
                    "YAML_PATH": self.yaml_path,
                    "INPUT_PATH": input_filepath,
                    "OUTPUT_PATH": output_filepath,
                }

                script = os.path.join(os.path.split(__file__)[0], "slider_tool.sh")
                task_id = executor.queue_task(self.get_stage_id(),script,
                                              custom_env, self.get_working_directory())
                task_ids.append(task_id)

        executor.wait_for_tasks()
        for task_id in task_ids:
            if executor.get_task_result(task_id):
                success += 1
            else:
                failed += 1

        self.get_logger().info("Plotted %d scenes (%d failed)" % (success, failed))
        if success > 0:
            output_context = {"plot_folder": self.output_path, "scene_folders": input_context["scene_folders"] }
            return {"output":output_context}
        else:
            None

