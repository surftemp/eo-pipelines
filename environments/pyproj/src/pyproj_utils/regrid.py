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
import os
import logging
import sys
import pyproj
import time

NUM_RETRIES = 5
RETRY_DELAY = 60

class Regrid:

    def __init__(self, input_path, grid_path, variables,
                 source_x, source_y, source_crs, target_x, target_y, target_crs,
                 output_path=None,
                 mode="max",
                 coarsen=None, stride=4):
        self.input_path = input_path
        self.grid_path = grid_path
        self.variables = variables
        self.source_x = source_x
        self.source_y = source_y
        self.source_crs = source_crs
        self.target_x = target_x
        self.target_y = target_y
        self.target_crs = target_crs
        self.output_path = output_path
        self.mode = mode
        self.coarsen = coarsen
        self.stride = stride
        self.logger = logging.getLogger("SuperRegrid")


    def run(self, limit=None):
        output_folder = os.path.split(self.output_path)[0]
        if output_folder and not os.path.exists(output_folder):
            os.makedirs(output_folder)

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
        target_y0 = float(grid[self.target_y][0])
        target_yN = float(grid[self.target_y][-1])
        target_x0 = float(grid[self.target_x][0])
        target_xN = float(grid[self.target_x][-1])

        if len(grid[self.target_x].shape) != 1:
            self.logger.error("grid x variable should have only 1 dimension")
            sys.exit(-1)

        if len(grid[self.target_y].shape) != 1:
            self.logger.error("grid y variable should have only 1 dimension")
            sys.exit(-1)

        target_height = grid[self.target_y].shape[0]
        target_width = grid[self.target_x].shape[0]

        x_dim = grid[self.target_x].dims[0]
        y_dim = grid[self.target_y].dims[0]

        accumulated = {}
        counts = {} # mean only
        closest_sq_distances = {} # nearest only

        for v in self.variables:
            if self.mode == "mean":
                counts[v] = np.zeros((target_height, target_width))
                accumulated[v] = np.zeros((target_height, target_width)) # will hold the sum
            else:
                if self.mode == "nearest":
                    closest_sq_distances[v] = np.zeros((target_height,target_width))
                    closest_sq_distances[v][:,:] = np.nan
                a = np.zeros((target_height, target_width))
                a[:, :] = np.nan
                accumulated[v] = a

        for input_file_path in input_file_paths:
            complete = False
            for retry in range(0,NUM_RETRIES):
                try:
                    input_file_name = os.path.split(input_file_path)[1]

                    self.logger.info(f"processing: {input_file_name} {idx}/{total}")

                    ds = xr.open_dataset(input_file_path)

                    if len(ds.lat.shape) == 1:
                        shape = (len(ds.lat),len(ds.lon))
                        x2d = np.broadcast_to(ds[self.source_x].data[None].T, shape)
                        y2d = np.broadcast_to(ds[self.source_y], shape)
                    else:
                        x2d = ds[self.source_x].data
                        y2d = ds[self.source_y].data

                    if self.source_crs != self.target_crs:
                        transformer = pyproj.Transformer.from_crs(self.source_crs, self.target_crs)
                        x2d, y2d = transformer.transform(y2d, x2d)

                    indices_nj = np.int32(np.round(target_height * (y2d - target_y0)/(target_yN-target_y0)))
                    indices_ni = np.int32(np.round(target_width * (x2d - target_x0)/(target_xN-target_x0)))

                    # set indices to (target_height,target_width) for points that lie outside the target grid
                    indices_nj = np.where(indices_nj < 0, target_height, indices_nj)
                    indices_nj = np.where(indices_nj >= target_height, target_height, indices_nj)
                    indices_ni = np.where(indices_ni < 0, target_width, indices_ni)
                    indices_ni = np.where(indices_ni >= target_width, target_width, indices_ni)

                    indices_nj = xr.DataArray(indices_nj, dims=(y_dim, x_dim))
                    indices_ni = xr.DataArray(indices_ni, dims=(y_dim, x_dim))

                    sq_distances = {}
                    if self.mode == "nearest":
                        self.logger.info(f"\tCalculating distances")
                        for xs in range(0, self.stride):
                            for ys in range(0, self.stride):
                                slice_idx = ys * self.stride + xs
                                self.logger.info(f"\t\tCalculating distances for slice {slice_idx + 1}/{self.stride ** 2}")
                                s = {}
                                s[x_dim] = slice(xs, None, self.stride)
                                s[y_dim] = slice(ys, None, self.stride)
                                iy = indices_nj.isel(**s).data
                                ix = indices_ni.isel(**s).data
                                source_coords_x = ds[self.source_x].isel(**s).data
                                source_coords_y = ds[self.source_y].isel(**s).data
                                target_coords_x = grid[self.target_x][iy,ix].data
                                target_coords_y = grid[self.target_y][iy, ix].data
                                sq_distances[(ys,xs)] = np.power(source_coords_y-target_coords_y,2)+np.power(source_coords_x-target_coords_x,2)

                    for v in self.variables:
                        da = ds[v].squeeze()
                        if len(da.dims) == 2:
                            self.logger.info(f"\tReading: {v}")
                            target_data = np.zeros((target_height + 1, target_width + 1))
                            target_data[:, :] = np.nan
                            for xs in range(0,self.stride):
                                for ys in range(0,self.stride):
                                    slice_idx = ys*self.stride+xs
                                    self.logger.info(f"\t\tProcessing slice {slice_idx+1}/{self.stride**2}")
                                    s = {}
                                    s[x_dim] = slice(xs,None,self.stride)
                                    s[y_dim] = slice(ys,None,self.stride)
                                    iy = indices_nj.isel(**s).data
                                    ix = indices_ni.isel(**s).data
                                    target_data[iy,ix] = da.isel(**s).data

                                    if self.mode == "max":
                                        accumulated[v] = np.fmin(target_data[:-1,:-1], accumulated[v])
                                    elif self.mode == "min":
                                        accumulated[v] = np.fmax(target_data[:-1, :-1], accumulated[v])
                                    elif self.mode == "mean":
                                        counts[v] = counts[v] + np.where(np.isnan(target_data),0,1)
                                        accumulated[v] = accumulated[v] + np.where(np.isnan(target_data),0,target_data)
                                    elif self.mode == "nearest":
                                        sq_d = sq_distances[(ys,xs)]
                                        accumulated[v] = np.where()
                                    else:
                                        raise Exception(f"Invalid mode {self.mode}")
                        else:
                            self.logger.error(f"variable {v} does not have exactly two non-unit dimensions")
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

        if self.mode == "mean":
            for v in accumulated:
                accumulated[v] = np.where(counts[v] > 0, accumulated[v]/counts[v],np.nan)

        if self.output_path:
            self.logger.info(f"writing: {self.output_path}")
            for retry in range(0, NUM_RETRIES):
                try:
                    encodings = {}
                    for v in accumulated:
                        da = xr.DataArray(data=accumulated[v],dims=(y_dim,x_dim))
                        grid[v] = da
                        encodings[v] = {"zlib": True, "complevel": 5, "dtype": "float32"}

                    grid.set_coords([self.target_y, self.target_x])
                    grid.to_netcdf(self.output_path, encoding=encodings)
                    break
                except Exception as ex:
                    self.logger.error(f"Error writing: {self.output_path} : {str(ex)}")
                    time.sleep(RETRY_DELAY)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path",
                        help="input data file to be regridded or folder containing files to be regridded")
    parser.add_argument("target_grid_path",
                        help="path to file defining grid with 1D lat/lon onto which data is to be regridded")

    parser.add_argument("--target-y", type=str, help="target grid y dimension variable nane", default="lat")
    parser.add_argument("--target-x", type=str, help="target grid x dimension variable nane", default="lon")
    parser.add_argument("--target-crs", type=int, help="target CRS", default=4326)

    parser.add_argument("--source-y", type=str, help="input y dimension variable nane", default="lat")
    parser.add_argument("--source-x", type=str, help="input x dimension variable nane", default="lon")
    parser.add_argument("--source-crs", type=int, help="source CRS", default=4326)


    parser.add_argument("--variables", nargs="+", help="Specify variables to process")
    parser.add_argument("--output-path", help="Aggregate values and output to this path", default=None)
    parser.add_argument("--mode", type=str, help="Accumulation function to apply, should be mean, min or max", default="min")
    parser.add_argument("--limit", type=int, help="process only this many scenes (for testing)", default=None)
    parser.add_argument("--coarsen", type=int, help="coarsen the target grid (for testing)", default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    regridder = Regrid(input_path=args.input_data_path, grid_path=args.target_grid_path,
                             variables=args.variables,
                             source_x=args.source_x,
                             source_y=args.source_y,
                             source_crs=args.source_crs,
                             target_x=args.target_x,
                             target_y=args.target_y,
                             target_crs=args.target_crs,
                             output_path=args.output_path,
                             mode=args.mode,
                             coarsen=args.coarsen)
    regridder.run(limit=args.limit)


if __name__ == '__main__':
    main()
