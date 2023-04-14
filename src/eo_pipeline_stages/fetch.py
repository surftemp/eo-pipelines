
import os
import csv

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int, format_date, format_float
from eo_pipelines.executors.executor_factory import ExecutorFactory, ExecutorType
from . import VERSION

# for supported datasets, map from channel name to a unique file suffix
suffix_map = {
    "LANDSAT_OT_C2_L1": {
        "1" : "B1.TIF",
        "2" : "B2.TIF",
        "3" : "B3.TIF",
        "4" : "B4.TIF",
        "5" : "B5.TIF",
        "6" : "B6.TIF",
        "7" : "B7.TIF",
        "8" : "B8.TIF",
        "9" : "B9.TIF",
        "10" : "B10.TIF",
        "11" : "B11.TIF",
        "QA" : "BQA.TIF",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "VAA": "VAA.TIF", # collection 2 only
        "VZA": "VZA.TIF", # collection 2 only
        "SAA": "SAA.TIF", # collection 2 only
        "SZA": "SZA.TIF"  # collection 2 only
    },

    "LANDSAT_OT_C2_L2": {
        "1" : "SR_B1.TIF",
        "2" : "SR_B2.TIF",
        "3" : "SR_B3.TIF",
        "4" : "SR_B4.TIF",
        "5" : "SR_B5.TIF",
        "6" : "SR_B6.TIF",
        "7" : "SR_B7.TIF",
        "ST": "ST_B10.TIF",
        "ST_QA": "ST_QA.TIF",
        "EMIS": "ST_EMIS.TIF",
        "EMSD": "ST_EMSD.TIF",
        "TRAD": "ST_TRAD.TIF",
        "URAD": "ST_URAD.TIF",
        "DRAD": "ST_DRAD.TIF",
        "ATRAN": "ST_ATRAN.TIF",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "QA_AEROSOL": "SR_QA_AEROSOL.TIF",
        "QA_RADSAT": "QA_RADSAT.TIF"
    }
}

class Fetch(PipelineStage):

    fetch_stage_count = 0

    def __init__(self, stage_id, cfg, spec, environment):
        super().__init__(stage_id, "fetch", cfg, spec, environment)
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.get_logger().info("eo_pipeline_stages.Fetch %s" % VERSION)

    def get_output_types(self):
        return { "output":"usgs_imagery" }

    def get_parameters(self):
        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "START_DATE": format_date(self.get_spec().get_start_date()),
            "END_DATE": format_date(self.get_spec().get_end_date()),
            "MAX_CLOUD_COVER_PCT": format_int(int(self.get_spec().get_max_cloud_cover_fraction() * 100)),
            "OUTPUT_PATH": self.output_path
        }

    def run(self,input_context):

        # input_context is not used

        usgs_username = os.getenv("USGS_USERNAME")
        usgs_password = os.getenv("USGS_PASSWORD")

        if not usgs_password or not usgs_username:
            raise Exception("Please set environment variables USGS_USERNAME and USGS_PASSWORD")

        fetched_scenes = {}
        failed_scenes = {}
        parameters = self.get_parameters()

        total_fetched = 0
        total_failed = 0

        # if any of the datasets are ecostress, make sure that ECOSTRESS_ECO1BGEO is also included
        datasets = self.get_spec().datasets[:]
        for dataset in datasets:
            if dataset.startswith("ECOSTRESS") and "ECOSTRESS_ECO1BGEO" not in datasets:
                datasets.append("ECOSTRESS_ECO1BGEO")

        for dataset in datasets:

            executor = self.create_executor(ExecutorType.Local)

            fetched_scenes[dataset] = []
            failed_scenes[dataset] = []

            # for each dataset
            scenes_csv_path = os.path.join(self.get_working_directory(),"%s_scenes.csv"%(dataset))

            custom_env = {
                    "USGS_USERNAME": usgs_username,
                    "USGS_PASSWORD": usgs_password,
                    "USGS_DATADIR": self.output_path,
                    "SCENES_CSV_PATH": scenes_csv_path,
                    "DATASET": dataset
            }

            for key in parameters:
                custom_env[key] = parameters[key]

            # for each dataset, get a list of scenes (written to CSV) and download all scenes

            list_script = os.path.join(os.path.split(__file__)[0],"fetch_list.sh")
            list_task_id = executor.queue_task(self.get_stage_id(),list_script,custom_env,self.get_working_directory())
            executor.wait_for_tasks()
            if not executor.get_task_result(list_task_id):
                self.get_logger().error(
                    "Failed to get list of scenes for dataset: %s" % dataset)
                continue
            executor.clear()

            # work out which files we can safely prune from the downloaded data
            # and prepare a list of their suffixes
            prune_suffixes = []
            required_bands = self.get_spec().get_bands_for_dataset(dataset)
            suffix_mapping = suffix_map.get(dataset, {})
            for band_name in suffix_mapping:
                if band_name not in required_bands:
                    prune_suffixes.append(suffix_mapping[band_name])

            self.get_logger().info(
                "Configured pruning of downloaded files with suffixes: %s" % ",".join(prune_suffixes))

            download_script = os.path.join(os.path.split(__file__)[0], "fetch_download.sh")
            scene_count = 0
            with open(scenes_csv_path) as scenes_f:
                reader = csv.reader(scenes_f)
                for line in reader:
                    catalog = line[0].strip()
                    dataset = line[1].strip()
                    scene = line[2].strip()
                    custom_env = {
                        "USGS_USERNAME": usgs_username,
                        "USGS_PASSWORD": usgs_password,
                        "USGS_DATADIR": self.output_path,
                        "CATALOG": catalog,
                        "DATASET": dataset,
                        "SCENE": scene
                    }
                    if len(prune_suffixes) > 0:
                        custom_env["PRUNE_SUFFIXES"] = '--prune-suffixes="'+",".join(prune_suffixes)+'"'
                    scene_count += 1
                    executor.queue_task(self.get_stage_id(), download_script, custom_env, self.get_working_directory(), description=dataset+"/"+scene)

            self.get_logger().info("Attempting download of %d scenes for dataset %s" % (scene_count,dataset))
            executor.wait_for_tasks()
            executor.clear()

            # check that the scenes have downloaded OK
            with open(scenes_csv_path) as scenes_f:
                reader = csv.reader(scenes_f)
                for line in reader:
                    catalog = line[0].strip()
                    dataset = line[1].strip()
                    scene = line[2].strip()
                    if not catalog or not dataset or not scene:
                        continue
                    scene_path = os.path.join(self.output_path,catalog,dataset,scene)
                    if os.path.exists(scene_path) and os.path.isdir(scene_path):
                        fetched_scenes[dataset].append(scene_path)
                    else:
                        failed_scenes[dataset].append(scene_path)

            fetched_count = len(fetched_scenes[dataset])
            failed_count = len(failed_scenes[dataset])
            self.get_logger().info("Fetch summary for dataset %s, fetched=%d, failed=%d"%(dataset, fetched_count, failed_count))

            total_fetched += fetched_count
            total_failed += failed_count

        if total_fetched:
            output_context = {"fetched_scenes":fetched_scenes,"failed_scenes":failed_scenes}
            return {"output":output_context}
        else:
            return None
