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
import pyproj
import os
import numpy as np
import sys
import time

available = False
try:
    from pyjob import use
    use("slurm")

    import pyjob
    available = True
except:
    pass

slurm_defaults = {
    'runtime': '04:00',
    'memlimit': '4192',
    'queue': 'short-serial',
    'name': 'regrid'
}

from copy_metadata import copy_metadata_ds

class MyRegridder:

    def __init__(self, target_grid):
        self.target_grid = target_grid
        # increase the target grid by a scale factor so that each source pixel should
        # map to a unique location in the expanded grid (the grid will later be coarsened
        # using mean, max etc) to the correct resolution
        self.scale_factor = 4

    def regrid(self,from_dataset):
        target_shape = self.target_grid.lat.data.shape
        nj = target_shape[0]
        ni = target_shape[1]
        lats = from_dataset.lat.data
        lons = from_dataset.lon.data

        transformer = pyproj.Transformer.from_crs(4326, 27700)
        eastings, northings = transformer.transform(lats, lons)

        max_northing = np.nanmax(self.target_grid.y.data)+50
        min_easting = np.nanmin(self.target_grid.x.data)-50

        # work out the indices on the target grid
        indices_nj = np.int32(self.scale_factor*np.round((max_northing - northings)/100))
        indices_ni = np.int32(self.scale_factor*np.round((eastings - min_easting)/100))

        # set indices to (nj,ni) for points that lie outside the target grid
        indices_nj = np.where(indices_nj < 0, self.scale_factor*nj, indices_nj)
        indices_nj = np.where(indices_nj >= self.scale_factor*nj, self.scale_factor*nj, indices_nj)
        indices_ni = np.where(indices_ni < 0, self.scale_factor*ni, indices_ni)
        indices_ni = np.where(indices_ni >= self.scale_factor*ni, self.scale_factor*ni, indices_ni)

        for v in from_dataset.variables:
            if v in ["lat","lon","time"]:
                continue
            target_data = np.zeros((nj*self.scale_factor+1,ni*self.scale_factor+1))
            target_data[:] = np.nan
            target_data[indices_nj,indices_ni] = from_dataset[v].data
            self.target_grid[v] = xr.DataArray(target_data[0:self.scale_factor*nj,0:self.scale_factor*ni],dims=("y","x")).coarsen(x=self.scale_factor,y=self.scale_factor).mean()

        return self.target_grid

def regrid(input_path, grid_path, output_path, limit=None):

    os.makedirs(output_path,exist_ok=True)
    if os.path.isdir(input_path):
        input_file_paths = list(map(lambda f: os.path.join(input_path,f),os.listdir(input_path)))
    else:
        input_file_paths = [input_path]

    idx = 1
    processed = 0
    total = len(input_file_paths)
    for input_file_path in input_file_paths:
        input_file_name = os.path.split(input_file_path)[1]
        output_file_path = os.path.join(output_path, input_file_name)
        if os.path.exists(output_file_path):
            print(f"skipping: {input_file_name} {idx}/{total} output already exists?")
            idx += 1
            continue
        else:
            print(f"processing: {input_file_name} {idx}/{total}")

        ds = xr.open_dataset(input_file_path)
        grid = xr.open_dataset(grid_path)

        regridder = MyRegridder(grid)
        ds_regridded = regridder.regrid(ds)
        if ds_regridded:
            ds_regridded.to_netcdf(output_file_path)

        processed += 1
        if limit is not None and processed >= limit:
            print(f"stopping after processing {processed} scenes")
            break
        idx += 1

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data_path", help="input data file to be regridded or folder containing files to be regridded",
                        default="/home/dev/20211122110514-NCEO-L1C-Landsat9-v2.0-fv01.0.nc")
    parser.add_argument("--target_grid_path", help="path to file defining grid lat/lon ontop which data is to be regridded",
                        default="/home/dev/severn.nc")
    parser.add_argument("--output_folder", help="path to the output folder into which regridded files are be written",
                        default=".")
    parser.add_argument("--limit", type=int, help="process only this many scenes (for testing)")
    args = parser.parse_args()

    regrid(args.input_data_path, args.target_grid_path, args.output_folder, args.limit)

