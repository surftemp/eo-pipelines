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


import datetime
import os
import tempfile
import json
from typing import Literal

from hyrrokkin.engine_launchers.python_engine_launcher import PythonEngineLauncher
from hyrrokkin.api.topology import Topology
from hyrrokkin.api.topology_listener_base import TopologyListenerBase
from hyrrokkin.utils.yaml_importer import import_from_yaml
from eo_pipelines.utils.runtime_parameters import RuntimeParameters

schema_path = "eo_pipelines.stages"

class EOPipelineListener(TopologyListenerBase):

    def __init__(self, eo_pipeline_runner):
        self.eo_pipeline_runner = eo_pipeline_runner

    def execution_event(self, timestamp: float | None, node_id: str,
                        state: Literal["pending","running","completed","failed"],
                        error: str | None, is_manual: bool) -> None:
        self.eo_pipeline_runner.track_execution(timestamp, node_id, state, error)

class EOPipelineRunner:

    STATUS_FILENAME = "eo-pipeline-status.json"

    def __init__(self):

        self.status = {}

        self.status["stage_start_times"] = {}
        self.status["stage_end_times"] = {}
        self.status["stage_durations"] = {}
        self.status["executed_stages"] = []
        self.status["stage_failures"] = {}
        self.yaml_path = None
        self.stage_start_times = {}

    def fixup(self, yaml_path):
        # migrate old-format YAML files (renaming configuration to configurations and grouping package properties under
        # under a properties key) to work correctly with newer versions of hyrrokkin
        from yaml import load, FullLoader, dump
        with open(yaml_path, "r") as from_file:
            cfg = load(from_file, Loader=FullLoader)
        if "configuration" in cfg:
            cfg["configurations"] = cfg["configuration"]
            del cfg["configuration"]
            for package_id in cfg["configurations"]:
                package_properties = {}
                keys = list(cfg["configurations"][package_id].keys())
                for key in keys:
                    package_properties[key] = cfg["configurations"][package_id][key]
                    del cfg["configurations"][package_id][key]
                cfg["configurations"][package_id]["properties"] = package_properties
            with open(yaml_path,"w") as to_file:
                dump(cfg, to_file, default_flow_style=False, sort_keys=False)

    def dump_timestamp(self, ts):
        return datetime.datetime.fromtimestamp(ts).isoformat()

    def save_status(self):
        with open(EOPipelineRunner.STATUS_FILENAME, "w") as of:
            of.write(json.dumps(self.status, indent=4))

    def track_execution(self, timestamp, node_id, state, exn):
        if state == "executing":
            self.stage_start_times[node_id] = timestamp
            self.status["stage_start_times"][node_id] = self.dump_timestamp(timestamp)
        elif state == "executed" or state == "failed":
            self.status["stage_end_times"][node_id] = self.dump_timestamp(timestamp)
            if state == "executed":
                self.status["executed_stages"].append(node_id)
            else:
                self.status["stage_failures"][node_id] = str(exn)
            start_time = self.stage_start_times.get(node_id, None)
            if start_time:
                duration = timestamp - start_time
                self.status["stage_durations"][node_id] = duration

        self.save_status()

    def run(self, yaml_path, only_stages=None):
        RuntimeParameters.set_parameter("YAML_PATH", os.path.abspath(yaml_path))

        with tempfile.TemporaryDirectory() as tmpdirname:

            t = Topology(tmpdirname, [schema_path],
                         engine_launcher=PythonEngineLauncher(verbose=False, in_process=True,
                                                              persistence="memory"), import_from_path=yaml_path)

            t.attach_listener(EOPipelineListener(self))

            stage_ids = list(t.get_nodes().keys())
            if only_stages:
                for stage_id in stage_ids:
                    if stage_id not in only_stages:
                        t.remove_node(stage_id)
                stage_ids = list(t.get_nodes().keys())

            self.status["running"] = datetime.datetime.now().isoformat()
            self.status["stage_ids"] = stage_ids
            self.save_status()

            ok = t.run()

            if ok:
                self.status["succeeded"] = datetime.datetime.now().isoformat()
                if "failed" in self.status:
                    del self.status["failed"]
            else:
                self.status["failed"] = datetime.datetime.now().isoformat()
                if "succeeded" in self.status:
                    del self.status["succeeded"]

            self.save_status()

            return ok
