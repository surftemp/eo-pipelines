#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

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

NUM_RETRIES = 5
RETRY_DELAY = 60

class RegridLatLon:

    def __init__(self, input_path, grid_path, variables, output_folder=None, output_aggregation_path=None,
                 output_aggregation_chunksize=1800, output_aggregation_function="max",
                 coarsen=None, interpolate_box=None, interpolate_min=None):
        self.input_path = input_path
        self.grid_path = grid_path
        self.variables = variables
        self.output_folder = output_folder
        self.output_aggregation_path = output_aggregation_path
        self.output_aggregation_chunksize = output_aggregation_chunksize
        self.output_aggregation_function = output_aggregation_function
        self.coarsen = coarsen
        self.interpolate_box = interpolate_box
        self.interpolate_min = interpolate_min
        self.logger = logging.getLogger("RegridLatLon")

    def run(self, limit=None):

        if self.output_folder:
            os.makedirs(self.output_folder, exist_ok=True)

        if os.path.isdir(self.input_path):
            input_file_paths = list(map(lambda f: os.path.join(self.input_path,f),os.listdir(self.input_path)))
        else:
            input_file_paths = [self.input_path]

        idx = 1
        processed = 0
        total = len(input_file_paths)

        grid = xr.open_dataset(self.grid_path)
        if self.coarsen:
            grid = grid[{'nj': slice(None, None, self.coarsen), 'ni': slice(None, None, self.coarsen)}]
        self.logger.info(grid)

        # work out the indices on the target grid
        lat0 = float(grid.lat[0])
        latN = float(grid.lat[-1])
        lon0 = float(grid.lon[0])
        lonN = float(grid.lon[-1])
        target_height = grid.lat.shape[0]
        target_width = grid.lon.shape[0]

        accumulated = {}

        for input_file_path in input_file_paths:
            complete = False
            for retry in range(0,NUM_RETRIES):
                try:
                    input_file_name = os.path.split(input_file_path)[1]

                    output_file_path = None
                    if self.output_folder:
                        output_file_path = os.path.join(self.output_folder, input_file_name)

                    self.logger.info(f"processing: {input_file_name} {idx}/{total}")

                    ds = xr.open_dataset(input_file_path)

                    if len(ds.lat.shape) == 1:
                        shape = (len(ds.lat),len(ds.lon))
                        lats2d = np.broadcast_to(ds.lat.data[None].T, shape)
                        lons2d = np.broadcast_to(ds.lon.data, shape)
                    else:
                        lats2d = ds.lat.data
                        lons2d = ds.lon.data

                    indices_nj = np.int32(np.round(target_height * (lats2d - lat0)/(latN-lat0)))
                    indices_ni = np.int32(np.round(target_width * (lons2d - lon0)/(lonN-lon0)))

                    # set indices to (target_height,target_width) for points that lie outside the target grid
                    indices_nj = np.where(indices_nj < 0, target_height, indices_nj)
                    indices_nj = np.where(indices_nj >= target_height, target_height, indices_nj)
                    indices_ni = np.where(indices_ni < 0, target_width, indices_ni)
                    indices_ni = np.where(indices_ni >= target_width, target_width, indices_ni)

                    output_ds = None
                    if output_file_path:
                        output_ds = xr.Dataset()
                        output_ds["lat"] = grid["lat"]
                        output_ds["lon"] = grid["lon"]

                        encodings = {}
                        encodings["lat"] = {"zlib": True, "complevel": 5}
                        encodings["lon"] = {"zlib": True, "complevel": 5}

                    for v in self.variables:
                        da = ds[v].squeeze()
                        if len(da.dims) == 2:
                            self.logger.info(f"\treading: {v}")
                            target_data = np.zeros((target_height + 1, target_width + 1))
                            target_data[:, :] = np.nan
                            target_data[indices_nj,indices_ni] = ds[v].data
                            if output_ds is not None:
                                output_ds[v] = xr.DataArray(target_data[:-1,:-1], dims=("lat","lon"))
                                encodings[v] = {"zlib": True, "complevel": 5}
                            if self.output_aggregation_path:
                                if v not in accumulated:
                                    a = np.zeros((target_height,target_width))
                                    a[:,:] = np.nan
                                    accumulated[v] = a
                                if self.output_aggregation_function == "max":
                                    accumulated[v] = np.fmin(target_data[:-1,:-1], accumulated[v])
                                elif self.output_aggregation_function == "min":
                                    accumulated[v] = np.fmax(target_data[:-1, :-1], accumulated[v])
                                else:
                                    raise Exception(f"Invalid aggregation function {self.output_aggregation_function}")
                        else:
                            self.logger.error(f"variable {v} does not have exactly two non-unit dimensions")
                    if output_file_path:
                        self.logger.info(f"writing: {output_file_path}")
                        output_ds.to_netcdf(output_file_path,encoding=encodings)
                    complete = True
                    break

                except Exception as ex:
                    self.logger.warning(f"Error processing: {input_file_name} : {str(ex)}")
                    time.sleep(RETRY_DELAY)

            if not complete:
                self.logger.error(f"Unable to process: {input_file_name}")

            processed += 1
            if limit is not None and processed >= limit:
                self.logger.info(f"stopping after processing {processed} scenes")
                break
            idx += 1

        if self.output_aggregation_path:
            self.logger.info(f"writing: {self.output_aggregation_path}")
            for retry in range(0, NUM_RETRIES):
                # try:
                    encodings = {}
                    for v in accumulated:
                        da = xr.DataArray(data=accumulated[v],dims=("nj","ni"))
                        # interpolate using rolling mean
                        if self.interpolate_box is not None:
                            means = da.rolling(lat=self.interpolate_box, lon=self.interpolate_box, center=True, min_periods=self.interpolate_min).mean()
                            da = da.where(~np.isnan(da), means)
                        grid[v] = da
                        encodings[v] = {"zlib": True, "complevel": 5, "dtype": "float32",
                                        "chunksizes": [self.output_aggregation_chunksize, self.output_aggregation_chunksize]}

                    grid.set_coords(["lat", "lon"])
                    print(grid)
                    grid.to_netcdf(self.output_aggregation_path, encoding=encodings)
                    break
                # except Exception as ex:
                #    self.logger.error(f"Error writing: {self.output_aggregation_path} : {str(ex)}")
                #    time.sleep(RETRY_DELAY)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path",
                        help="input data file to be regridded or folder containing files to be regridded")
    parser.add_argument("target_grid_path",
                        help="path to file defining grid with 1D lat/lon onto which data is to be regridded")
    parser.add_argument("--output-folder", help="path to the output folder into which regridded files are be written",
                        default=None)
    parser.add_argument("--variables", nargs="+", help="Specify variables to process")
    parser.add_argument("--output-aggregation-path", help="Aggregate values and output to this path", default=None)
    parser.add_argument("--output-aggregation-chunksize", type=int, help="Aggregate values and output to this path", default=1000)
    parser.add_argument("--output-aggregation-function", type=str, help="Accumulation function to apply, should be min or max", default="min")
    parser.add_argument("--interpolate", type=int, help="perform missing value interpolation with this size of box")
    parser.add_argument("--interpolate-min", type=int, help="perform missing value interpolation with at least this number of items in the box")
    parser.add_argument("--limit", type=int, help="process only this many scenes (for testing)", default=None)
    parser.add_argument("--coarsen", type=int, help="coarsen the target grid (for testing)", default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    regridder = RegridLatLon(input_path=args.input_data_path, grid_path=args.target_grid_path,
                             variables=args.variables,
                             output_folder=args.output_folder,
                             output_aggregation_path=args.output_aggregation_path,
                             output_aggregation_chunksize=args.output_aggregation_chunksize,
                             output_aggregation_function=args.output_aggregation_function,
                             interpolate_box=args.interpolate, interpolate_min=args.interpolate_min,
                             coarsen=args.coarsen)
    regridder.run(limit=args.limit)


if __name__ == '__main__':
    main()
