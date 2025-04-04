# MIT License
#
# Copyright (c) 2022-2025 National Center for Earth Observation (NCEO)
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
        "B1": "B1.TIF",
        "B2": "B2.TIF",
        "B3": "B3.TIF",
        "B4": "B4.TIF",
        "B5": "B5.TIF",
        "B6": "B6.TIF",
        "B7": "B7.TIF",
        "B8": "B8.TIF",
        "B9": "B9.TIF",
        "B10": "B10.TIF",
        "B11": "B11.TIF",
        "QA": "BQA.TIF",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "VAA": "VAA.TIF",
        "VZA": "VZA.TIF",
        "SAA": "SAA.TIF",
        "SZA": "SZA.TIF"
    },

    "LANDSAT_OT_C2_L2": {
        "B1": "SR_B1.TIF",
        "B2": "SR_B2.TIF",
        "B3": "SR_B3.TIF",
        "B4": "SR_B4.TIF",
        "B5": "SR_B5.TIF",
        "B6": "SR_B6.TIF",
        "B7": "SR_B7.TIF",
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
    },

    "LANDSAT_ETM_C2_L1": {
        "B1": "B1.TIF",
        "B2": "B2.TIF",
        "B3": "B3.TIF",
        "B4": "B4.TIF",
        "B5": "B5.TIF",
        "B6_1": "B6_VCID_1.TIF",
        "B6_2": "B6_VCID_2.TIF",
        "B7": "B7.TIF",
        "B8": "B8.TIF",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "VAA": "VAA.TIF",
        "VZA": "VZA.TIF",
        "SAA": "SAA.TIF",
        "SZA": "SZA.TIF"
    },

    "LANDSAT_ETM_C2_L2": {
        "B1": "SR_B1.TIF",
        "B2": "SR_B2.TIF",
        "B3": "SR_B3.TIF",
        "B4": "SR_B4.TIF",
        "B5": "SR_B5.TIF",
        "B7": "SR_B7.TIF",
        "ST": "ST_B6.TIF",
        "ST_QA": "ST_QA.TIF",
        "EMIS": "ST_EMIS.TIF",
        "EMSD": "ST_EMSD.TIF",
        "TRAD": "ST_TRAD.TIF",
        "URAD": "ST_URAD.TIF",
        "DRAD": "ST_DRAD.TIF",
        "ATRAN": "ST_ATRAN.TIF",
        "QA_PIXEL": "QA_PIXEL.TIF",
        "QA_RADSAT": "QA_RADSAT.TIF"
    }
}

# for supported datasets, map from channel name to a unique file suffix
excldue_suffix_map = {

    "LANDSAT_OT_C2_L1": {
    },

    "LANDSAT_OT_C2_L2": {
    },

    "LANDSAT_ETM_C2_L1": {
        "B1": "_GM_B1.TIF",
        "B2": "_GM_B2.TIF",
        "B3": "_GM_B3.TIF",
        "B4": "_GM_B4.TIF",
        "B5": "_GM_B5.TIF",
        "B6_1": "_GM_B6_VCID_1.TIF",
        "B6_2": "_GM_B6_VCID_2.TIF",
        "B7": "_GM_B7.TIF",
        "B8": "_GM_B8.TIF"
    },

    "LANDSAT_ETM_C2_L2": {
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
        super().__init__(node_services, "fetch")
        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(), self.output_path)

        self.download_path = self.get_configuration().get("download_path", None)
        if self.download_path is None:
            self.download_path = self.output_path
        else:
            if not os.path.isabs(self.download_path):
                self.download_path = os.path.join(self.get_working_directory(), self.download_path)

        self.get_logger().info("eo_pipeline_stages.Fetch %s" % Fetch.VERSION)

    def execute_stage(self, inputs):

        usgs_username = os.getenv("USGS_USERNAME")
        usgs_token = os.getenv("USGS_TOKEN")

        limit = self.get_configuration().get("limit", None)

        no_download = self.get_configuration().get("no_download", False)

        file_cache_index = self.get_configuration().get("file_cache_index", "")
        if file_cache_index != "":
            file_cache_index = "--file-cache-index " + file_cache_index

        if not no_download and (not usgs_token or not usgs_username):
            raise Exception("Please set environment variables USGS_USERNAME and USGS_TOKEN")

        error_fraction_threshold = self.get_configuration().get("error_fraction_threshold", 0.01)

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
                for dataset in input:
                    entity_ids = input[dataset]
                    if len(entity_ids) > 0:
                        scenes_csv_path = os.path.join(self.get_working_directory(), "%s_scenes.csv" % (dataset))
                        with open(scenes_csv_path, "w") as f:
                            f.write(dataset + "\n")
                            for entity_id in entity_ids:
                                f.write(entity_id + "\n")

                        required_bands = self.get_spec().get_bands_for_dataset(dataset)

                        suffix_mapping = suffix_map.get(dataset, {})

                        suffixes = [".XML"]

                        for band_name in suffix_mapping:
                            if len(required_bands) == 0 or band_name in required_bands:
                                suffixes.append(suffix_mapping[band_name])

                        # for each suffix, add _GM_+suffix to exclude L7 gap mask files
                        exclude_suffixes = []
                        exclude_suffix_mapping = excldue_suffix_map.get(dataset, {})
                        for band_name in exclude_suffix_mapping:
                            if len(required_bands) == 0 or band_name in required_bands:
                                exclude_suffixes.append(exclude_suffix_mapping[band_name])

                        dataset_output_folder = os.path.join(self.output_path, dataset)
                        os.makedirs(dataset_output_folder, exist_ok=True)

                        dataset_download_folder = os.path.join(self.download_path, dataset)
                        os.makedirs(dataset_download_folder, exist_ok=True)

                        download_summary_path = os.path.join(self.output_path, dataset + ".csv")

                        custom_env = {
                            "USGS_USERNAME": usgs_username,
                            "USGS_TOKEN": usgs_token,
                            "USGS_DATADIR": self.output_path,
                            "SCENES_CSV_PATH": scenes_csv_path,
                            "SUFFIXES": " ".join(suffixes),
                            "EXCLUDE_SUFFIXES": " -x " + " ".join(exclude_suffixes) if len(exclude_suffixes) else "",
                            "OUTPUT_FOLDER": dataset_output_folder,
                            "DOWNLOAD_FOLDER": dataset_download_folder,
                            "FILE_CACHE_INDEX": file_cache_index,
                            "NO_DOWNLOAD": "--no-download" if no_download else "",
                            "DOWNLOAD_SUMMARY_PATH": download_summary_path,
                            "LIMIT": f"--limit {limit}" if limit else ""
                        }

                        fetch_script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "usgs_fetch.sh")
                        fetch_task_id = executor.queue_task(self.get_stage_id(), fetch_script, custom_env,
                                                            self.get_working_directory())
                        executor.wait_for_tasks()
                        if not executor.get_task_result(fetch_task_id):
                            self.get_logger().error("Failed to fetch of scenes for dataset: %s" % dataset)

                        with open(download_summary_path) as f:
                            rdr = csv.reader(f)
                            entity_ids = set()
                            failed_entity_ids = set()
                            for line in rdr:
                                [entity_id, filename] = line
                                entity_ids.add(entity_id)
                                if not os.path.isfile(os.path.join(dataset_output_folder, filename)):
                                    failed_entity_ids.add(entity_id)

                            if len(entity_ids):
                                error_fraction = len(failed_entity_ids) / len(entity_ids)
                                if error_fraction > error_fraction_threshold:
                                    raise Exception(
                                        f"Failed to download dataset {dataset}: error fraction {error_fraction} > threshold {error_fraction_threshold}")

                        output_folders[dataset] = dataset_output_folder

        return {"output": output_folders}
