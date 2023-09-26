
import argparse
import logging

from hyrrokkin.api.topology import Topology
from hyrrokkin.utils.yaml_importer import import_from_yaml


schema_path = "eo_pipeline_stages"

class EOPipelineRunner():

    def __init__(self):
        pass

    def run(self, yaml_path):
        t = Topology([schema_path])
        with open(yaml_path) as f:
            import_from_yaml(t, f)
        t.run()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path")
    args = parser.parse_args()
    runner = EOPipelineRunner()
    runner.run(args.yaml_path)