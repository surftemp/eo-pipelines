import os.path

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int

class TimeStacker(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "time_stacker")
        self.node_services = node_services

        self.output_folder = self.get_configuration().get("output_folder", "stacked_output")

        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.get_working_directory(),self.output_folder)

        self.get_logger().info("eo_pipeline_stages.TimeStacker %s" % TimeStacker.VERSION)

    def get_parameters(self):
        return {}

    def execute_stage(self, inputs):

        executor = self.create_executor()
        output_filename = self.get_configuration().get("output_filename","stacked.nc")

        output_scenes = {}

        succeeded = 0
        failed = 0

        os.makedirs(self.output_folder,exist_ok=True)

        for input in inputs["input"]:

            for dataset in input:

                custom_env = self.get_parameters()
                input_path = input[dataset]
                custom_env["INPUT_FOLDER"] = input_path

                dataset_output_folder = os.path.join(self.output_folder,dataset)
                os.makedirs(dataset_output_folder, exist_ok=True)
                output_path = os.path.join(dataset_output_folder,output_filename)

                custom_env["OUTPUT_PATH"] = output_path

                count = 0
                for fname in os.listdir(input_path):
                    if fname.endswith(".nc"):
                        count += 1

                if count:

                    script = os.path.join(os.path.split(__file__)[0], "time_stacker.sh")
                    task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                                  description=os.path.split(input_path)[-1])

                    executor.wait_for_tasks()
                    if executor.get_task_result(task_id):
                        succeeded += 1
                        if self.output_folder:
                            output_scenes[dataset] = dataset_output_folder
                    else:
                        failed += 1

        return {"output":output_scenes}


