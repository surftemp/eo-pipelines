from eo_pipelines.pipeline_spec import PipelineSpec
import uuid

from .. import VERSION as EO_PIPELINES_VERSION
from .nodes.read_dataset_directory import ReadDatasetDirectory
from .nodes.usgs_search import USGS_Search
from .nodes.usgs_fetch import USGS_Fetch
from .nodes.landsat_import import LandsatImport
from .nodes.xesmf_regrid import XESMFRegrid
from .nodes.regrid import Regrid
from .nodes.group import Group
from .nodes.time_stacker import TimeStacker
from .nodes.missing_filter import MissingFilter
from .nodes.add_masks import AddMasks
from .nodes.add_spatial import AddSpatial
from .nodes.custom_processor import CustomProcessor
from .nodes.generate_html import GenerateHtml
from .nodes.metadata_tweaker import MetadataTweaker
from .nodes.rubber_stamp import RubberStamp

class Configuration:

    YAML_PATH = ""

    def __init__(self, configuration_services):
        self.configuration_services = configuration_services
        self.spec = None
        self.environment = None

    async def load(self):
        properties = await self.configuration_services.get_properties()
        self.spec = PipelineSpec(properties.get("spec", {}))
        self.environment = properties.get("environment", {})
        # allocate a unique id for the run if no run_id value is provided
        if "run_id" not in self.environment:
            self.environment["run_id"] = str(uuid.uuid4())

    def get_spec(self):
        return self.spec

    def get_environment(self):
        return self.environment

    async def create_node(self, node_type_id, node_services):
        match node_type_id:
            case "read_dataset_directory": return ReadDatasetDirectory(node_services)
            case "usgs_search": return USGS_Search(node_services)
            case "usgs_fetch": return USGS_Fetch(node_services)
            case "landsat_import": return LandsatImport(node_services)
            case "xesmf_regrid": return XESMFRegrid(node_services)
            case "regrid": return Regrid(node_services)
            case "group": return Group(node_services)
            case "time_stacker":  return TimeStacker(node_services)
            case "missing_filter": return MissingFilter(node_services)
            case "add_masks": return AddMasks(node_services)
            case "add_spatial": return AddSpatial(node_services)
            case "custom_processor": return CustomProcessor(node_services)
            case "generate_html": return GenerateHtml(node_services)
            case "metadata_tweaker": return MetadataTweaker(node_services)
            case "rubber_stamp": return RubberStamp(node_services)
            case _: return None
