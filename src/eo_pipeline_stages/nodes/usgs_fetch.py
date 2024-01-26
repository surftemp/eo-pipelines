# MIT License
#
# Copyright (c) 2022 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import csv
import json

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int, format_date, format_float
from eo_pipelines.executors.executor_factory import ExecutorType


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

    VERSION = "0.0.2"
    fetch_stage_count = 0

    # by default run 4 download sub-processes, making sure they don't start
    # within 5 seconds of each other (
    default_execution_settings = {
        "local": {
            "nr_threads": 4,
            "stagger_time": 5
        }
    }

    def __init__(self, node_services):
        super().__init__(node_services,"fetch")
        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(),self.output_path)

        self.download_path = self.get_configuration().get("download_path", None)
        if self.download_path is None:
            self.download_path = self.output_path
        else:
            if not os.path.isabs(self.download_path):
                self.download_path = os.path.join(self.get_working_directory(), self.download_path)

        self.get_logger().info("eo_pipeline_stages.Fetch %s" % Fetch.VERSION)

    def execute_stage(self,inputs):

        usgs_username = os.getenv("USGS_USERNAME")
        usgs_password = os.getenv("USGS_PASSWORD")

        limit = self.get_configuration().get("limit",None)

        file_cache_index = self.get_configuration().get("file_cache_index","")
        if file_cache_index != "":
            file_cache_index = "--file-cache-index "+file_cache_index

        if not usgs_password or not usgs_username:
            raise Exception("Please set environment variables USGS_USERNAME and USGS_PASSWORD")

        output_folders = {}

        executor = self.create_executor(ExecutorType.Local)

        parameters = self.get_parameters()

        # compare parameters with last run, if exists
        last_parameters = None
        parameters_path = os.path.join(self.get_working_directory(), "parameters.json")
        if os.path.exists(parameters_path):
            with open(parameters_path) as f:
                last_parameters = json.loads(f.read())

        if last_parameters is None or parameters != last_parameters:
            if last_parameters:
                os.unlink(parameters_path)

            for input in inputs["input"]:
                fetched = 0
                for dataset in input:
                    entity_ids = input[dataset]
                    scenes_csv_path = os.path.join(self.get_working_directory(), "%s_scenes.csv" % (dataset))
                    with open(scenes_csv_path,"w") as f:
                        f.write(dataset+"\n")
                        for entity_id in entity_ids:
                            f.write(entity_id+"\n")

                    required_bands = self.get_spec().get_bands_for_dataset(dataset)

                    suffix_mapping = suffix_map.get(dataset, {})
                    suffixes = [".XML"]
                    for band_name in suffix_mapping:
                        if len(required_bands)==0 or band_name in required_bands:
                            suffixes.append(suffix_mapping[band_name])

                    dataset_output_folder = os.path.join(self.output_path,dataset)
                    os.makedirs(dataset_output_folder, exist_ok=True)

                    dataset_download_folder = os.path.join(self.download_path, dataset)
                    os.makedirs(dataset_download_folder, exist_ok=True)

                    custom_env = {
                            "USGS_USERNAME": usgs_username,
                            "USGS_PASSWORD": usgs_password,
                            "USGS_DATADIR": self.output_path,
                            "SCENES_CSV_PATH": scenes_csv_path,
                            "SUFFIXES": " ".join(suffixes),
                            "OUTPUT_FOLDER": dataset_output_folder,
                            "DOWNLOAD_FOLDER": dataset_download_folder,
                            "FILE_CACHE_INDEX": file_cache_index
                    }

                    fetch_script = os.path.join(os.path.split(__file__)[0], "usgs_fetch.sh")
                    fetch_task_id = executor.queue_task(self.get_stage_id(),fetch_script,custom_env,self.get_working_directory())
                    executor.wait_for_tasks()
                    if not executor.get_task_result(fetch_task_id):
                        self.get_logger().error("Failed to fetch of scenes for dataset: %s" % dataset)
                    else:
                        fetched += 1
                    output_folders[dataset] = dataset_output_folder
                    if limit is not None and fetched >= limit:
                        break

        return {"output":output_folders}

