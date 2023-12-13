#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

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
import os
import logging

class RegridLatLon:

    def __init__(self, input_path, grid_path, output_path):
        self.input_path = input_path
        self.grid_path = grid_path
        self.output_path = output_path
        self.logger = logging.getLogger("RegridLatLon")

    def run(self, limit=None):

        os.makedirs(self.output_path,exist_ok=True)
        if os.path.isdir(self.input_path):
            input_file_paths = list(map(lambda f: os.path.join(self.input_path,f),os.listdir(self.input_path)))
        else:
            input_file_paths = [self.input_path]

        idx = 1
        processed = 0
        total = len(input_file_paths)

        grid = xr.open_dataset(self.grid_path)

        # work out the indices on the target grid
        lat0 = float(grid.lat[0])
        latN = float(grid.lat[-1])
        lon0 = float(grid.lon[0])
        lonN = float(grid.lon[-1])
        target_height = grid.lat.shape[0]
        target_width = grid.lon.shape[0]

        for input_file_path in input_file_paths:

            input_file_name = os.path.split(input_file_path)[1]
            output_file_path = os.path.join(self.output_path, input_file_name)
            if os.path.exists(output_file_path):
                self.logger.warning(f"skipping: {input_file_name} {idx}/{total} output already exists?")
                idx += 1
                continue
            else:
                self.logger.info(f"processing: {input_file_name} {idx}/{total}")

            ds = xr.open_dataset(input_file_path)

            indices_nj = np.int32(np.round(target_height * (ds.lat - lat0)/(latN-lat0)))
            indices_ni = np.int32(np.round(target_width * (ds.lon - lon0)/(lonN-lon0)))

            # set indices to (target_height,target_width) for points that lie outside the target grid
            indices_nj = np.where(indices_nj < 0, target_height, indices_nj)
            indices_nj = np.where(indices_nj >= target_height, target_height, indices_nj)
            indices_ni = np.where(indices_ni < 0, target_width, indices_ni)
            indices_ni = np.where(indices_ni >= target_width, target_width, indices_ni)

            output_ds = xr.Dataset()
            output_ds["time"] = ds["time"]
            output_ds["lat"] = grid["lat"]
            output_ds["lon"] = grid["lon"]

            encodings = {}
            encodings["lat"] = {"zlib": True, "complevel": 5}
            encodings["lat"] = {"zlib": True, "complevel": 5}
            for v in ds.variables:
                if v != "lat" and v != "lon" and v != "time":
                    if len(ds[v].shape) == 3:
                        target_data = np.zeros((1,target_height + 1, target_width + 1))
                        target_data[:, :] = np.nan
                        target_data[0,indices_nj,indices_ni] = ds[v].data
                        output_ds[v] = xr.DataArray(target_data[:,:-1,:-1], dims=("time","lat","lon"))

            output_ds.to_netcdf(output_file_path)

            processed += 1
            if limit is not None and processed >= limit:
                print(f"stopping after processing {processed} scenes")
                break
            idx += 1

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path",
                        help="input data file to be regridded or folder containing files to be regridded")
    parser.add_argument("target_grid_path",
                        help="path to file defining grid with 1D lat/lon onto which data is to be regridded")
    parser.add_argument("output_folder", help="path to the output folder into which regridded files are be written")
    parser.add_argument("--limit", type=int, help="process only this many scenes (for testing)", default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    regridder = RegridLatLon(input_path=args.input_data_path, grid_path=args.target_grid_path, output_path=args.output_folder)
    regridder.run(limit=args.limit)


if __name__ == '__main__':
    main()
