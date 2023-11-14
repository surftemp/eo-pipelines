#!/usr/bin/env python
# -*- coding: utf-8 -*-

#     EOCIS high resolution data processing for the British Isles
#
#     Copyright (C) 2023  EOCIS and National Centre for Earth Observation (NCEO)
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

import xarray as xr
import numpy as np

class MyRegridder:

    def __init__(self, input_lat, input_lon, resolution):

        self.lat = input_lat
        self.lat = input_lon
        self.min_lat = input_lat.min().data
        self.max_lat = input_lat.max().data
        self.min_lon = input_lon.min().data
        self.max_lon = input_lon.max().data
        self.resolution = resolution

        print(f"Extent: Lat: {self.min_lat} - {self.max_lat}, Lon: {self.min_lon} - {self.max_lon}")

        self.new_lats = np.linspace(self.min_lat,self.max_lat,int((self.max_lat-self.min_lat)/resolution)+1)
        self.new_lons = np.linspace(self.min_lon, self.max_lon, int((self.max_lon - self.min_lon)/resolution)+1)


    def regrid(self,from_dataset, variable):
        nj = len(self.new_lats)
        ni = len(self.new_lons)

        old_lats = from_dataset.lat.data
        old_lons = from_dataset.lon.data

        # work out the indices on the target grid
        indices_nj = np.int32(np.round((old_lats - self.min_lat)/self.resolution))
        indices_ni = np.int32(np.round((old_lons - self.min_lon)/self.resolution))

        # set indices to (nj,ni) for points that lie outside the target grid
        indices_nj = np.where(indices_nj < 0, nj, indices_nj)
        indices_nj = np.where(indices_nj >= nj, nj, indices_nj)
        indices_ni = np.where(indices_ni < 0, ni, indices_ni)
        indices_ni = np.where(indices_ni >= ni, ni, indices_ni)

        target_grid = xr.Dataset()
        target_grid["lat"] = xr.DataArray(self.new_lats,dims=("lat",))
        target_grid["lon"] = xr.DataArray(self.new_lons, dims=("lon",))

        target_data = np.zeros((nj+1,ni+1))
        target_data[:] = np.nan
        target_data[indices_nj,indices_ni] = from_dataset[variable].data

        target_data = target_data[0:nj, 0:ni]

        da = xr.DataArray(target_data,dims=("lat","lon"))

        print("Interpolating...")

        means = da.rolling(lat=3,lon=3,center=True, min_periods=4).mean()
        da = da.where(~np.isnan(da),means)

        target_grid[variable] = da

        return target_grid

def regrid(variable,input_path, output_path,resolution=None):
    ds = xr.open_dataset(input_path)
    regridder = MyRegridder(ds.lat, ds.lon, resolution)
    ds_regridded = regridder.regrid(ds,variable)
    ds_regridded.to_netcdf(output_path, encoding={variable:{"chunksizes":[500,500],"dtype":"float32","zlib":True,"complevel":5}})


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", help="input data file to be regridded", default="/home/dev/data/maxst.nc", required=True)
    parser.add_argument("--output-path", help="output data file", required=True)
    parser.add_argument("--resolution", type=float, help="output resolution (degrees)", default=0.001)
    parser.add_argument("--variable", help="variable", default="ST")

    args = parser.parse_args()

    regrid(args.variable, args.input_path, args.output_path, args.resolution)

