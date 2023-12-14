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


class USGS_Search(PipelineStage):

    VERSION = "0.0.2"

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
        self.output_path = self.get_configuration().get("output_path", self.get_working_directory())
        self.get_logger().info("eo_pipeline_stages.Fetch %s" % USGS_Search.VERSION)

    def get_parameters(self):
        row_filter = self.get_configuration().get("row","")
        if row_filter:
            row_filter = "--row "+str(row_filter)
        path_filter = self.get_configuration().get("path","")
        if path_filter:
            path_filter = "--path "+str(path_filter)

        return {
            "LAT_MIN": format_float(self.get_spec().get_lat_min()),
            "LAT_MAX": format_float(self.get_spec().get_lat_max()),
            "LON_MIN": format_float(self.get_spec().get_lon_min()),
            "LON_MAX": format_float(self.get_spec().get_lon_max()),
            "START_DATE": format_date(self.get_spec().get_start_date()),
            "END_DATE": format_date(self.get_spec().get_end_date()),
            "MAX_CLOUD_COVER_PCT": format_int(int(self.get_spec().get_max_cloud_cover_fraction() * 100)),
            "OUTPUT_PATH": self.output_path,
            "ROW_FILTER": row_filter,
            "PATH_FILTER": path_filter
        }


    def execute_stage(self,inputs):

        usgs_username = os.getenv("USGS_USERNAME")
        usgs_password = os.getenv("USGS_PASSWORD")

        if not usgs_password or not usgs_username:
            raise Exception("Please set environment variables USGS_USERNAME and USGS_PASSWORD")

        parameters = self.get_parameters()

        # compare parameters with last run, if exists
        last_parameters = None
        parameters_path = os.path.join(self.get_working_directory(),"parameters.json")
        if os.path.exists(parameters_path):
            with open(parameters_path) as f:
                last_parameters = json.loads(f.read())

        datasets = self.get_spec().datasets[:]

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

                executor = self.create_executor(ExecutorType.Local)

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

                list_script = os.path.join(os.path.split(__file__)[0], "usgs_search.sh")
                list_task_id = executor.queue_task(self.get_stage_id(),list_script,custom_env,self.get_working_directory())
                executor.wait_for_tasks()
                if not executor.get_task_result(list_task_id):
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
                with open(scenes_csv_path) as scenes_f:
                    rdr = csv.reader(scenes_f)
                    for line in rdr:
                        scenes.append(line[2])
                scene_list[dataset] = scenes
            except:
                errors += 1

        if errors == 0:
            with open(parameters_path,"w") as f:
                f.write(json.dumps(parameters))

        return {"output":scene_list}

