
from eo_pipelines.pipeline_spec import PipelineSpec


class Configuration:

    def __init__(self, configuration_services):
        self.configuration_services = configuration_services
        self.spec = PipelineSpec(self.configuration_services.get_property("spec"))
        self.environment = self.configuration_services.get_property("environment")

    def get_spec(self):
        return self.spec

    def get_environment(self):
        return self.environment