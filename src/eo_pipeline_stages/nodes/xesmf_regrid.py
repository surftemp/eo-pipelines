import os.path

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int


class XESMFRegrid(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "xesmf_regrid")
        self.node_services = node_services

        self.output_path = self.get_configuration().get("output_path",None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(),self.output_path)

        self.cache_path = self.get_configuration().get("cache_path", None)
        if self.cache_path is None:
            self.cache_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.cache_path):
                self.cache_path = os.path.join(self.get_working_directory(),self.cache_path)

        self.get_logger().info("eo_pipeline_stages.XESMFRegrid %s" % XESMFRegrid.VERSION)

    def get_parameters(self):
        return {
            "GRID_PATH": self.get_configuration().get("target_grid_path"),
            "CACHE_PATH": "--cache-folder "+self.cache_path,
            "MAX_DISTANCE": format_int(self.get_configuration().get("max_distance",100))
        }

    def execute_stage(self, inputs):

        os.makedirs(self.cache_path,exist_ok=True)
        executor = self.create_executor()

        output_scenes = {}

        succeeded = 0
        failed = 0

        for input in inputs["input"]:

            for dataset in input:
                input_paths = []
                dataset_folder = input[dataset]
                for filename in os.listdir(dataset_folder):
                    if filename.endswith(".nc"):
                        input_paths.append(os.path.join(dataset_folder,filename))

                dataset_output_folder = os.path.abspath(os.path.join(self.output_path,dataset))
                os.makedirs(dataset_output_folder, exist_ok=True)
                output_scenes[dataset] = dataset_output_folder

                task_ids = []
                for input_path in input_paths:
                    custom_env = self.get_parameters()
                    custom_env["INPUT_PATH"] = input_path
                    custom_env["OUTPUT_FOLDER"] = dataset_output_folder
                    script = os.path.join(os.path.split(__file__)[0], "xesmf_regrid.sh")
                    task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                              description=os.path.split(input_path)[-1])
                    task_ids.append(task_id)

                executor.wait_for_tasks()
                for task_id in task_ids:
                    if executor.get_task_result(task_id):
                        succeeded += 1
                    else:
                        failed += 1

        summary = f"Regrid scenes: succeeded:{succeeded}, failed:{failed}"
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


