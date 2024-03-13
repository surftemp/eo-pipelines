
from eo_pipelines.pipeline_spec import PipelineSpec
import uuid

class Configuration:

    def __init__(self, configuration_services):
        self.configuration_services = configuration_services
        self.spec = PipelineSpec(self.configuration_services.get_property("spec"))
        self.environment = self.configuration_services.get_property("environment")
        # allocate a unique id for the run if no run_id value is provided
        if "run_id" not in self.environment:
            self.environment["run_id"] = str(uuid.uuid4())

    def get_spec(self):
        return self.spec

    def get_environment(self):
        return self.environment