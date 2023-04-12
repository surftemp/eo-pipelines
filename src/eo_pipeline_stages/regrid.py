import os.path

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_exceptions import PipelineStageException
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from eo_pipelines.pipeline_stage_utils import format_int, format_date, format_float
from . import VERSION

class Regrid(PipelineStage):

    DEFAULT_RESOLUTION = 50

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "regrid", cfg, spec, environment)
        self.resolution = self.get_configuration().get("resolution_m", Regrid.DEFAULT_RESOLUTION)
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.include_angles = self.get_configuration().get("include_angles", False)
        self.export_oli_as = self.get_configuration().get("export_oli_as", "corrected_reflectance")
        self.get_logger().info("eo_pipeline_stages.Regrid %s" % VERSION)

    def get_input_types(self):
        return { "input":"usgs_imagery" }

    def get_output_types(self):
        return { "output":"netcdf4_yx" }

    def get_parameters(self):
        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "BANDS": self.get_spec().get_all_bands(),
            "RESOLUTION": format_int(self.resolution),
            "INCLUDE_ANGLES": "--include-angles" if self.include_angles else "",
            "EXPORT_OLI_AS": self.export_oli_as
        }

    def run(self, input_context):

        input_context = input_context["input"]

        if "fetched_scenes" not in input_context:
            raise PipelineStageException("regrid", "No fetched scenes from a previous stage to regrid")

        cfg = self.get_configuration()
        executor = cfg.get("executor","local")
        if executor == "local":
            executor = ExecutorFactory.create_executor(ExecutorType.Local, self.get_environment(), self.get_configuration())
        else:
            executor = ExecutorFactory.create_executor(ExecutorType.Slurm, self.get_environment(), self.get_configuration())

        fetched_scenes = input_context["fetched_scenes"]
        regridded_scene_folders = {}
        task_ids = []
        for (dataset,scenes) in fetched_scenes.items():
            regrid_datasets = self.get_spec().get_datasets()

            if dataset not in regrid_datasets:
                continue
            regrid_output_path = os.path.join(self.output_path,dataset)
            os.makedirs(regrid_output_path,exist_ok=True)
            regridded_scene_folders[dataset] = regrid_output_path
            self.get_logger().info("Regridding %d scenes in dataset %s"%(len(scenes),dataset))
            for scene in scenes:

                custom_env = {
                    "SCENE_PATH": scene,
                    "OUTPUT_PATH": regrid_output_path,
                }
                for (k,v) in self.get_parameters().items():
                    custom_env[k] = v

                # override the BANDS with just those that are requested for this dataset
                custom_env["BANDS"] = ",".join(self.get_spec().get_bands_for_dataset(dataset))

                script = os.path.join(os.path.split(__file__)[0], "regrid.sh")
                task_id = executor.queue_task(self.get_stage_id(),script, custom_env, self.get_working_directory(),
                                              description=dataset+"/"+scene)
                task_ids.append(task_id)

        succeeded = 0
        failed = 0
        executor.wait_for_tasks()
        for task_id in task_ids:
            if executor.get_task_result(task_id):
                succeeded += 1
            else:
                failed += 1

        self.get_logger().info("Regridded %d scenes (%d failed)" % (succeeded, failed))

        if succeeded > 0:
            output_context = {"scene_folders": regridded_scene_folders}
            return {"output":output_context}
        else:
            return None

