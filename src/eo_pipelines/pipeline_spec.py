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

import re

from .pipeline_exceptions import PipelineSetupException

class PipelineSpec:

    lat_string_regexp = r"(\d+)\.(\d+)\.(\d+)(N|n|S|s)"
    lon_string_regexp = r"(\d+)\.(\d+)\.(\d+)(W|w|E|e)"

    @staticmethod
    def convert_lat_or_lon(value,is_lat):
        if isinstance(value,int):
            return float(value)
        if isinstance(value,float):
            return value
        if isinstance(value,str):
            matches = re.match(
                PipelineSpec.lat_string_regexp if is_lat else PipelineSpec.lon_string_regexp,
                value)
            if matches:
                degrees = matches.group(1)
                minutes = matches.group(2)
                seconds = matches.group(3)
                nsew = matches.group(4)
                value = float(degrees)+float(minutes)/60+float(seconds)/3600
                if is_lat and (nsew == "S" or nsew == "s"):
                    value = -value
                if not is_lat and (nsew == "W" or nsew == "w"):
                    value = -value
                return value
        unit = "latitude" if is_lat else "longitude"
        raise Exception(f"Unable to read {value} as a {unit}")


    def __init__(self, spec):
        print(spec)
        try:
            self.lat_min = PipelineSpec.convert_lat_or_lon(spec["lat_min"],is_lat=True)
            self.lat_max = PipelineSpec.convert_lat_or_lon(spec["lat_max"],is_lat=True)
            self.lon_min = PipelineSpec.convert_lat_or_lon(spec["lon_min"],is_lat=False)
            self.lon_max = PipelineSpec.convert_lat_or_lon(spec["lon_max"],is_lat=False)

            self.max_cloud_cover_fraction = float(spec.get("max_cloud_cover_fraction",1))

            # already converted to datetime.date if present
            self.start_date = spec.get("start_date",None)
            self.end_date = spec.get("end_date",None)

            self.bands_for_dataset = {}
            self.datasets = []
            if "datasets" in spec:
               self.datasets = list(spec["datasets"].keys())
               for (dataset, dataset_details) in spec["datasets"].items():
                   self.bands_for_dataset[dataset] = list(map(lambda b: str(b), dataset_details["bands"]))

        except Exception as ex:
            raise PipelineSetupException(str(ex))

    def get_lat_min(self):
        return self.lat_min

    def get_lat_max(self):
        return self.lat_max

    def get_lon_min(self):
        return self.lon_min

    def get_lon_max(self):
        return self.lon_max

    def get_start_date(self):
        return self.start_date

    def get_end_date(self):
        return self.end_date

    def get_max_cloud_cover_fraction(self):
        return self.max_cloud_cover_fraction

    def get_datasets(self):
        return self.datasets

    def get_all_bands(self):
        all_bands = set()
        for dataset in self.datasets:
            for band in self.bands_for_dataset.get(dataset,[]):
                all_bands.add(band)
        return sorted(list(all_bands))

    def get_bands_for_dataset(self,dataset):
        return self.bands_for_dataset.get(dataset,[])

    def __repr__(self):
        s = "Spec\n"
        s += f"\tLat: {self.lat_min} - {self.lat_max}\n"
        s += f"\tLon: {self.lon_min} - {self.lon_max}\n"
        s += f"\tDate: {self.start_date} - {self.end_date}\n"
        return s

if __name__ == '__main__':
    print(PipelineSpec.convert_lat_or_lon("12.30.0E",is_lat=False))