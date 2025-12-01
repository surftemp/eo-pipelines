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
import datetime

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int, format_date, format_float

class USGS_Search(PipelineStage):
    VERSION = "0.0.5"

    # by default run 4 download sub-processes, making sure they don't start
    # within 5 seconds of each other (
    default_execution_settings = {
        "local": {
            "nr_threads": 4,
            "stagger_time": 5
        }
    }

    def __init__(self, node_services):
        super().__init__(node_services, "search")
        self.node_services = node_services

    async def load(self):
        await super().load()
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.get_logger().info("eo_pipeline_stages.Search %s" % USGS_Search.VERSION)

    def get_parameters(self):
        night = self.get_configuration().get("night", False)
        night_filter = "--night-only" if night else ""
        row_filter = self.get_configuration().get("row", "")
        if row_filter:
            row_filter = "--row " + str(row_filter)
        path_filter = self.get_configuration().get("path", "")
        if path_filter:
            path_filter = "--path " + str(path_filter)
        month_filter = self.get_configuration().get("months", [])
        if len(month_filter) > 0:
            month_filter = "--months " + " ".join(map(lambda m: str(m), month_filter))
        else:
            month_filter = ""

        start_date = self.get_spec().get_start_date()
        end_date = self.get_spec().get_end_date()

        # allow the specification start and end date to be overridden in this stage
        if "start_date" in self.get_configuration():
            start_date = datetime.datetime.strptime(self.get_configuration()["start_date"], "%Y-%m-%d").date()
        if "end_date" in self.get_configuration():
            end_date = datetime.datetime.strptime(self.get_configuration()["end_date"], "%Y-%m-%d").date()

        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "START_DATE": format_date(start_date),
            "END_DATE": format_date(end_date),
            "MAX_CLOUD_COVER_PCT": format_int(int(self.get_spec().get_max_cloud_cover_fraction() * 100)),
            "OUTPUT_PATH": self.output_path,
            "MONTH_FILTER": month_filter,
            "ROW_FILTER": row_filter,
            "PATH_FILTER": path_filter,
            "NIGHT_FILTER": night_filter
        }

    def get_filter_parameters(self):
        min_overlap_fraction = self.get_configuration().get("min_overlap_fraction", 0.5)
        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "MIN_OVERLAP_FRACTION": format_float(min_overlap_fraction)
        }

    def execute_stage(self, inputs):

        usgs_username = os.getenv("USGS_USERNAME")
        usgs_token = os.getenv("USGS_TOKEN")

        if not usgs_token or not usgs_username:
            raise Exception("Please set environment variables USGS_USERNAME and USGS_TOKEN")

        parameters = self.get_parameters()

        # compare parameters with last run, if exists
        last_parameters = None
        parameters_path = os.path.join(self.get_working_directory(), "parameters.json")
        if os.path.exists(parameters_path):
            with open(parameters_path) as f:
                last_parameters = json.loads(f.read())

        if "datasets" in self.get_configuration():
            # if a list of datasets is specified in the parameters for this stage, use that list...
            datasets = self.get_configuration()["datasets"]
        else:
            # otherwise use the specification
            datasets = self.get_spec().datasets

        # if any of the datasets are ecostress, make sure that ECOSTRESS_ECO1BGEO is also included
        for dataset in datasets:
            if dataset.startswith("ECOSTRESS") and "ECOSTRESS_ECO1BGEO" not in datasets:
                datasets.append("ECOSTRESS_ECO1BGEO")

        # no previous run or parameters have changed... fetch the scene lists from USGS
        if last_parameters is None or parameters != last_parameters:
            if last_parameters:
                os.unlink(parameters_path)

            # for each dataset, get a list of scenes
            for dataset in datasets:

                executor = self.create_executor()

                # for each dataset
                scenes_csv_path = os.path.join(self.get_working_directory(), "%s_scenes.csv" % (dataset))

                custom_env = {
                    "USGS_USERNAME": usgs_username,
                    "USGS_TOKEN": usgs_token,
                    "USGS_DATADIR": self.output_path,
                    "SCENES_CSV_PATH": scenes_csv_path,
                    "DATASET": dataset
                }

                for key in parameters:
                    custom_env[key] = parameters[key]

                list_script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "usgs_search.sh")
                list_task_id = executor.queue_task(self.get_stage_id(), list_script, custom_env,
                                                   self.get_working_directory())
                executor.wait_for_tasks()
                if not executor.get_task_result(list_task_id):
                    self.get_logger().error(
                        "Failed to get list of scenes for dataset: %s" % dataset)
                    continue

                min_overlap_filter = self.get_configuration().get("min_overlap_fraction", None)
                if min_overlap_filter is not None:
                    filter_env = self.get_filter_parameters()
                    filter_env["SCENES_CSV_PATH"] = scenes_csv_path
                    filter_script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "usgs_filter.sh")
                    filter_task_id = executor.queue_task(self.get_stage_id(), filter_script, filter_env,
                                                         self.get_working_directory())
                    executor.wait_for_tasks()
                    if not executor.get_task_result(filter_task_id):
                        self.get_logger().error(
                            "Failed to get list of scenes for dataset: %s" % dataset)
                        continue

                executor.clear()

        scene_list = {}
        errors = 0
        for dataset in datasets:
            try:
                scenes = []
                scenes_csv_path = os.path.join(self.get_working_directory(), "%s_scenes.csv" % (dataset))
                found = 0
                with open(scenes_csv_path) as scenes_f:
                    rdr = csv.reader(scenes_f)
                    for line in rdr:
                        scenes.append(line[2])
                        found += 1
                self.get_logger().info(f"Found {found} scenes for dataset: {dataset}")
                scene_list[dataset] = scenes
            except Exception as ex:
                self.get_logger().exception(f"Error fetching scenes for {dataset}")
                errors += 1

        # scenes are the scene identifier in form LC8083231YYYYDDDLGN00 where YYYY is they year and DDD is the day of year
        # date-time matchup: when we have multiple datasets, use only the scenes where we have data from all datasets for a given year/day of year
        if len(datasets) > 1:
            def parse_scene_id(scene_id):
                year = int(scene_id[9:13])
                doy = int(scene_id[13:16])
                return (year, doy)

            year_day = {}  # lookup (year,doy) => set(dataset_name)
            # build the lookup
            for dataset in datasets:
                for scene_id in scene_list[dataset]:
                    key = parse_scene_id(scene_id)
                    if key not in year_day:
                        year_day[key] = set()
                    year_day[key].add(dataset)
            # perform the screening, only include scenes where there is data on the same day in all datasets
            for dataset in datasets:
                filtered_scenes = []
                for scene_id in scene_list[dataset]:
                    key = parse_scene_id(scene_id)
                    if len(year_day[key]) == len(datasets):
                        filtered_scenes.append(scene_id)
                removed = len(scene_list[dataset]) - len(filtered_scenes)
                self.get_logger().info(f"Datetime matchup: filtered {removed} scenes from dataset {dataset}")
                scene_list[dataset] = filtered_scenes

        if errors == 0:
            with open(parameters_path, "w") as f:
                f.write(json.dumps(parameters))

        return {"output": scene_list}
