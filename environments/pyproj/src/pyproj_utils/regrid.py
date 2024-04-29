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

RETRY_DELAY = 60

class Regrid:

    def __init__(self, input_ds, grid_ds, variables,
                 source_x, source_y, source_crs, target_x, target_y, target_crs,
                 output_path, coarsen=None, stride=4, nr_retries=0):
        self.input_ds = input_ds
        self.grid = grid_ds
        self.variables = [self.decode_variable_mode(v) for v in variables]
        self.source_x = source_x
        self.source_y = source_y
        self.source_crs = source_crs
        self.target_x = target_x
        self.target_y = target_y
        self.target_crs = target_crs
        self.output_path = output_path

        self.coarsen = coarsen
        self.stride = stride
        self.nr_retries = nr_retries

        self.target_height = None
        self.target_width = None
        self.target_x_dim = None
        self.target_y_dim = None

        self.dtypes = {}

        self.accumulated_mins = {}
        self.accumulated_maxes = {}
        self.accumulated_means = {}
        self.accumulated_nearest = {}

        self.dataset_attrs = {}
        self.variable_attrs = {}

        self.counts = {}  # mean only
        self.closest_sq_distances = None  # nearest only

        self.logger = logging.getLogger("Regrid")

        self.compute_distances = False
        for (v,mode) in self.variables:
            if mode == "nearest":
                if self.target_crs != 27700:
                    raise ValueError("Nearest neighbour currently only supported if the target CRS is 27700")
                self.compute_distances = True

    def decode_variable_mode(self, variable):
        splits = variable.split(":")
        if len(splits) == 2:
            variable_name = splits[0]
            mode = splits[1]
            if mode not in ["min","max","mean","nearest"]:
                raise ValueError(f"Invalid mode {mode} for variable {variable}")
        elif len(splits) == 1:
            variable_name = variable
            mode = "nearest"
        else:
            raise ValueError(f"Invalid variable directive: {variable} should be NAME or NAME:MODE")
        return variable_name, mode

    def run(self, limit=None):
        output_folder = os.path.split(self.output_path)[0]
        if output_folder and not os.path.exists(output_folder):
            os.makedirs(output_folder)

        if isinstance(self.input_ds,str):
            if os.path.isdir(self.input_ds):
                input_file_paths = list(map(lambda f: os.path.join(self.input_ds,f),os.listdir(self.input_ds)))
            else:
                input_file_paths = [self.input_ds]
            output_individual_files = os.path.exists(self.output_path) and os.path.isdir(self.output_path)
        else:
            input_file_paths = [""]
            output_individual_files = False

        idx = 1
        processed = 0
        total = len(input_file_paths)

        if self.coarsen:
            self.grid = self.grid[{'nj': slice(None, None, self.coarsen), 'ni': slice(None, None, self.coarsen)}]
        self.logger.info(self.grid)

        # work out the indices on the target grid
        target_y0 = float(self.grid[self.target_y][0])
        target_yN = float(self.grid[self.target_y][-1])
        target_x0 = float(self.grid[self.target_x][0])
        target_xN = float(self.grid[self.target_x][-1])

        if len(self.grid[self.target_x].shape) != 1:
            self.logger.error("target grid x variable should have only 1 dimension")
            sys.exit(-1)

        if len(self.grid[self.target_y].shape) != 1:
            self.logger.error("target grid y variable should have only 1 dimension")
            sys.exit(-1)

        self.target_height = self.grid[self.target_y].shape[0]
        self.target_width = self.grid[self.target_x].shape[0]

        self.target_x_dim = self.grid[self.target_x].dims[0]
        self.target_y_dim = self.grid[self.target_y].dims[0]

        self.reset()

        for input_file_path in input_file_paths:
            complete = False
            for retry in range(0,self.nr_retries+1):
                try:
                    input_file_name = os.path.split(input_file_path)[1] if input_file_path else "<input dataset>"

                    self.logger.info(f"processing: {input_file_name} {idx}/{total}")

                    ds = xr.open_dataset(input_file_path) if input_file_path else self.input_ds

                    time_da = None
                    if "time" in ds:
                        time_da = ds["time"]

                    self.dataset_attrs = ds.attrs

                    if len(ds[self.source_y].shape) == 1:
                        shape = (len(ds[self.source_y]),len(ds[self.source_x]))
                        source_y_dim = ds[self.source_y].dims[0]
                        source_x_dim = ds[self.source_x].dims[0]
                        y2d = xr.DataArray(np.broadcast_to(ds[self.source_y].data[None].T, shape),dims=(source_y_dim,source_x_dim))
                        x2d = xr.DataArray(np.broadcast_to(ds[self.source_x], shape),dims=(source_y_dim,source_x_dim))
                    else:
                        x2d = ds[self.source_x]
                        y2d = ds[self.source_y]
                        source_y_dim = ds[self.source_x].dims[0]
                        source_x_dim = ds[self.source_x].dims[1]

                    target_shape = (len(self.grid[self.target_y]), len(self.grid[self.target_x]))
                    target_y2d = np.broadcast_to(self.grid[self.target_y].data[None].T, target_shape)
                    target_x2d = np.broadcast_to(self.grid[self.target_x], target_shape)

                    if self.source_crs != self.target_crs:
                        transformer = pyproj.Transformer.from_crs(self.source_crs, self.target_crs)
                        x2d, y2d = transformer.transform(y2d.data, x2d.data)
                        y2d = xr.DataArray(y2d,dims=(source_y_dim, source_x_dim))
                        x2d = xr.DataArray(x2d,dims=(source_y_dim, source_x_dim))

                    indices_nj = np.int32(np.round((self.target_height-1) * (y2d.data - target_y0)/(target_yN-target_y0)))
                    indices_ni = np.int32(np.round((self.target_width-1) * (x2d.data - target_x0)/(target_xN-target_x0)))

                    print("indices_ni="+str(indices_ni))
                    print("indices_nj="+str(indices_nj))

                    # set indices to (target_height,target_width) for points that lie outside the target grid
                    indices_nj = np.where(indices_nj < 0, self.target_height, indices_nj)
                    indices_nj = np.where(indices_nj >= self.target_height, self.target_height, indices_nj)
                    indices_ni = np.where(indices_ni < 0, self.target_width, indices_ni)
                    indices_ni = np.where(indices_ni >= self.target_width, self.target_width, indices_ni)

                    indices_nj = xr.DataArray(indices_nj, dims=(source_y_dim, source_x_dim))
                    indices_ni = xr.DataArray(indices_ni, dims=(source_y_dim, source_x_dim))

                    indices_by_slice = {}

                    for xs in range(0, self.stride):
                        for ys in range(0, self.stride):
                            s = {}
                            s[source_x_dim] = slice(xs, None, self.stride)
                            s[source_y_dim] = slice(ys, None, self.stride)
                            iy = indices_nj.isel(**s).data
                            ix = indices_ni.isel(**s).data
                            indices_by_slice[(ys,xs)] = (s,iy,ix)

                    for (v,mode) in self.variables:

                        self.dtypes[v] = ds[v].dtype

                        da = ds[v].squeeze()

                        if v not in self.variable_attrs:
                            self.variable_attrs[v] = da.attrs
                        if len(da.dims) == 2:
                            self.logger.info(f"\tCalculating {mode} for {v}")

                            for xs in range(0,self.stride):
                                for ys in range(0,self.stride):
                                    target_data = np.zeros((self.target_height + 1, self.target_width + 1))
                                    target_data[:, :] = np.nan
                                    slice_idx = xs*self.stride+ys
                                    self.logger.info(f"\t\tProcessing slice {slice_idx+1}/{self.stride**2}")
                                    s, iy, ix = indices_by_slice[(ys,xs)]
                                    target_data[iy,ix] = da.isel(**s).data
                                    valid_target_data = target_data[:-1, :-1]
                                    if mode == "min":
                                        self.accumulated_mins[v] = np.fmin(valid_target_data, self.accumulated_mins[v])
                                    elif mode == "max":
                                        self.accumulated_maxes[v] = np.fmax(valid_target_data, self.accumulated_maxes[v])
                                    elif mode == "mean":
                                        self.counts[v] = self.counts[v] + np.where(np.isnan(valid_target_data),0,1)
                                        self.accumulated_means[v] = self.accumulated_means[v] \
                                                        + np.where(np.isnan(valid_target_data),0,valid_target_data)
                                    elif mode == "nearest":
                                        source_coords_x = np.zeros((self.target_height + 1, self.target_width + 1))
                                        source_coords_x[:, :] = np.nan
                                        source_coords_y = np.zeros((self.target_height + 1, self.target_width + 1))
                                        source_coords_y[:, :] = np.nan
                                        source_coords_x[iy,ix] = x2d.isel(**s).data
                                        source_coords_y[iy,ix] = y2d.isel(**s).data
                                        sq_dist = np.power(source_coords_y[:-1,:-1] - target_y2d, 2) \
                                                       + np.power(source_coords_x[:-1,:-1] - target_x2d, 2)
                                        self.accumulated_nearest[v] = np.where(sq_dist < self.closest_sq_distances, valid_target_data, self.accumulated_nearest[v])
                                        self.closest_sq_distances = np.where(np.isnan(sq_dist), self.closest_sq_distances,
                                                                           np.where(sq_dist < self.closest_sq_distances, sq_dist, self.closest_sq_distances))

                        else:
                            self.logger.error(f"variable {v} in {input_file_path} does not have exactly two non-unit dimensions, ignoring")
                    complete = True
                    break

                except Exception as ex:
                    self.logger.warning(f"Error processing: {input_file_name} : {str(ex)}")
                    time.sleep(RETRY_DELAY)

            if not complete:
                self.logger.error(f"Unable to process: {input_file_name}")

            if output_individual_files:
                output_ds, encodings = self.prepare_output_dataset(time_da=time_da)
                output_path = os.path.join(self.output_path, input_file_name)
                for retry in range(0, self.nr_retries + 1):
                    try:
                        self.logger.info(f"writing: {output_path}")
                        output_ds.to_netcdf(output_path, encoding=encodings)
                        break
                    except Exception as ex:
                        self.logger.error(f"Error writing: {output_path} : {str(ex)}")
                        time.sleep(RETRY_DELAY)
                self.reset()

            processed += 1
            if limit is not None and processed >= limit:
                self.logger.info(f"stopping after processing {processed} scenes")
                break
            idx += 1

        if not output_individual_files:
            output_ds, encodings = self.prepare_output_dataset()
            if self.output_path:
                for retry in range(0, self.nr_retries + 1):
                    try:
                        self.logger.info(f"writing: {self.output_path}")
                        output_ds.to_netcdf(self.output_path, encoding=encodings)
                        break
                    except Exception as ex:
                        self.logger.error(f"Error writing: {self.output_path} : {str(ex)}")
                        time.sleep(RETRY_DELAY)
            else:
                return output_ds, encodings


    def prepare_output_dataset(self, time_da=None):
        for (v, mode) in self.variables:
            if mode == "mean":
                self.accumulated_means[v] = np.where(self.counts[v] > 0, self.accumulated_means[v]/self.counts[v], np.nan)

        output_ds = self.grid.copy()
        if time_da is not None:
            output_ds["time"] = time_da
            dims = ("time", self.target_y_dim, self.target_x_dim)
        else:
            dims = (self.target_y_dim, self.target_x_dim)
        encodings = {}

        for (v,mode) in self.variables:
            accumulated = None
            if mode == "mean":
                accumulated = self.accumulated_means[v]
            elif mode == "max":
                accumulated = self.accumulated_maxes[v]
            elif mode == "min":
                accumulated = self.accumulated_mins[v]
            elif mode == "nearest":
                accumulated = self.accumulated_nearest[v]

            output_variable = v+"_"+mode
            if time_da is not None:
                accumulated = np.expand_dims(accumulated,axis=0)

            da = xr.DataArray(data=accumulated,dims=dims,attrs=self.variable_attrs.get(v,None))
            output_ds[output_variable] = da
            encodings[output_variable] = {"zlib": True, "complevel": 5, "dtype": str(self.dtypes[v])}

            if mode == "mean":
                # also output the counts
                output_variable = v+"_counts"
                arr = self.counts[v]
                if time_da is not None:
                    arr = np.expand_dims(arr,axis=0)
                da = xr.DataArray(data=arr, dims=dims)
                output_ds[output_variable] = da
                encodings[output_variable] = {"zlib": True, "complevel": 5, "dtype": "int32"}

        for (name,value) in self.dataset_attrs.items():
            output_ds.attrs[name] = value
        if self.compute_distances:
            distances = np.sqrt(np.where(self.closest_sq_distances < np.finfo(np.float32).max, self.closest_sq_distances, np.nan))
            if time_da is not None:
               distances = np.expand_dims(distances,axis=0)
            da = xr.DataArray(data=distances, dims=dims, attrs={ "units": "m"})
            output_ds["nearest_distance"] = da
            encodings["nearest_distance"] = {"zlib": True, "complevel": 5, "dtype": "float32"}

        output_ds.set_coords([self.target_y, self.target_x])
        return output_ds, encodings



    def reset(self):
        self.dataset_attrs = {}
        self.variable_attrs = {}
        self.accumulated_mins = {}
        self.accumulated_maxes = {}
        self.accumulated_means = {}
        self.accumulated_nearest = {}

        self.counts = {}  # mean only
        self.closest_sq_distances = None

        if self.compute_distances:
            self.closest_sq_distances = np.zeros((self.target_height,self.target_width),dtype="float32")
            self.closest_sq_distances[:,:] = np.finfo(np.float32).max

        for (v,mode) in self.variables:
            if mode == "mean":
                self.counts[v] = np.zeros((self.target_height, self.target_width))
                self.accumulated_means[v] = np.zeros((self.target_height, self.target_width)) # will hold the sum initially
            elif mode == "min":
                a = np.zeros((self.target_height, self.target_width))
                a[:, :] = np.nan
                self.accumulated_mins[v] = a  # will hold the mins
            elif mode == "max":
                a = np.zeros((self.target_height, self.target_width))
                a[:, :] = np.nan
                self.accumulated_maxes[v] = a  # will hold the maxes
            elif mode == "nearest":
                a = np.zeros((self.target_height, self.target_width))
                a[:, :] = np.nan
                self.accumulated_nearest[v] = a

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path",
                        help="input data file to be regridded or folder containing files to be regridded")
    parser.add_argument("target_grid_path",
                        help="path to file defining grid with 1D lat/lon onto which data is to be regridded")
    parser.add_argument("output_path",
                        help="path to file to hold regridded data")

    parser.add_argument("--target-y", type=str, help="target grid y dimension variable nane", default="lat")
    parser.add_argument("--target-x", type=str, help="target grid x dimension variable nane", default="lon")
    parser.add_argument("--target-crs", type=int, help="target CRS", default=4326)

    parser.add_argument("--source-y", type=str, help="input y dimension variable nane", default="lat")
    parser.add_argument("--source-x", type=str, help="input x dimension variable nane", default="lon")
    parser.add_argument("--source-crs", type=int, help="source CRS", default=4326)

    parser.add_argument("--variables", nargs="+", help="Specify variables to process, for nearest use NAME, for other modes (min,max,mean) use NAME:MODE")

    parser.add_argument("--limit", type=int, help="process only this many scenes (for testing)", default=None)
    parser.add_argument("--stride", type=int, help="set the stride", default=4)
    parser.add_argument("--coarsen", type=int, help="coarsen the target grid (for testing)", default=None)
    parser.add_argument("--nr-retries", type=int, help="use this many re-attempts to process each input file", default=0)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    grid_ds = xr.open_dataset(args.target_grid_path)

    regridder = Regrid(input_ds=args.input_data_path, grid_ds=grid_ds,
                             variables=args.variables,
                             source_x=args.source_x,
                             source_y=args.source_y,
                             source_crs=args.source_crs,
                             target_x=args.target_x,
                             target_y=args.target_y,
                             target_crs=args.target_crs,
                             output_path=args.output_path,
                             coarsen=args.coarsen,
                             nr_retries=args.nr_retries,
                             stride=args.stride)
    regridder.run(limit=args.limit)


if __name__ == '__main__':
    main()
