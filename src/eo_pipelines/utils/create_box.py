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

import math
import numpy as np
import xarray as xr
import argparse

m_per_degree = 111000  # approx m per degree of latitude


class Box:

    def __init__(self, min_lat, min_lon, max_lat, max_lon, resolution):
        self.resolution = resolution
        self.min_lat = min_lat
        self.min_lon = min_lon
        self.max_lat = max_lat
        self.max_lon = max_lon

    def get_resolution(self):
        mid_lat = (self.min_lat + self.max_lat) / 2
        resolution_degrees_y = self.resolution / m_per_degree  # height of each target pixel in degrees
        # work out width of each target pixel in degrees for the mid-latitude
        # width will increase at higher latitudes
        resolution_degrees_x = resolution_degrees_y / math.cos(math.radians(mid_lat))
        return (resolution_degrees_y, resolution_degrees_x)

    def export_netcdf(self, to_path, metadata={}):
        (resolution_degrees_y, resolution_degrees_x) = self.get_resolution()
        lats = np.flip(np.arange(self.min_lat, self.max_lat, resolution_degrees_y))
        lons = np.arange(self.min_lon, self.max_lon, resolution_degrees_x)
        shape = len(lats), len(lons)

        lats2d = np.broadcast_to(lats[None].T, shape)
        lons2d = np.broadcast_to(lons, shape)

        target_ds = xr.Dataset(attrs=metadata)
        target_ds["lat"] = xr.DataArray(lats2d, dims=("nj", "ni"),
                                        attrs={"units": "degrees_north", "standard_name": "latitude"})
        target_ds["lon"] = xr.DataArray(lons2d, dims=("nj", "ni"),
                                        attrs={"units": "degrees_east", "standard_name": "longitude"})

        target_ds.to_netcdf(to_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("output_path", help="path to write output file")
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", required=True)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", required=True)
    parser.add_argument("--resolution", type=float, help="resolution in metres", required=True)

    args = parser.parse_args()
    box = Box(args.lat_min, args.lon_min, args.lat_max, args.lon_max, args.resolution)
    box.export_netcdf(args.output_path)
