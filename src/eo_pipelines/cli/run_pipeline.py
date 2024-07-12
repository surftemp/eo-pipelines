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

import argparse
import datetime
import logging
import os
import sys

from hyrrokkin.api.topology import Topology
from hyrrokkin.utils.yaml_importer import import_from_yaml
import tempfile
import json

schema_path = "eo_pipeline_stages"

class EOPipelineRunner:

    STATUS_FILENAME = "eo-pipeline-status.json"

    def __init__(self):
        pass

    def run(self, yaml_path):
        with tempfile.TemporaryDirectory() as tmpdirname:
            t = Topology(tmpdirname,[schema_path])
            with open(yaml_path) as f:
                import_from_yaml(t, f)
            stage_ids = t.get_node_ids()
            status = {}
            if os.path.exists(EOPipelineRunner.STATUS_FILENAME):
                try:
                    with open(EOPipelineRunner.STATUS_FILENAME) as f:
                        status = json.loads(f.read())
                except Exception as ex:
                    pass
            status["running"] = datetime.datetime.now().isoformat()
            status["stage_ids"] = stage_ids
            with open(EOPipelineRunner.STATUS_FILENAME,"w") as of:
                of.write(json.dumps(status, indent=4))
            ok = t.run()
            if ok:
                status["succeeded"] = datetime.datetime.now().isoformat()
            else:
                status["failed"] =  datetime.datetime.now().isoformat()
            with open(EOPipelineRunner.STATUS_FILENAME, "w") as of:
                of.write(json.dumps(status, indent=4))
            return ok

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path")
    args = parser.parse_args()
    runner = EOPipelineRunner()
    if not runner.run(args.yaml_path):
        sys.exit(-1)


if __name__ == '__main__':
    main()
