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

"""Utility programming for assembling together multiple smaller input scenes onto a larger grid, where all data is on the same grid"""

import xarray as xr
import numpy as np
import logging
import os.path
import random


class Assembler:

    def __init__(self, input_folders, target_grid, output_path, limit, sample):
        self.input_folders = input_folders
        self.target_grid = target_grid
        self.output_path = output_path
        self.limit = limit
        self.sample = sample
        self.logger = logging.getLogger("Assembler")

    def make_slices(self, target_dim_values, source_dim_values):
        target_min = min(target_dim_values)
        target_max = max(target_dim_values)
        source_min = min(source_dim_values)
        source_max = max(source_dim_values)
        if target_min > source_max or target_max < source_min:
            # no overlap
            return None, None

        smallest = max(target_min,source_min)
        largest = min(target_max,source_max)

        target_min_ix = target_dim_values.index(smallest)
        target_max_ix = target_dim_values.index(largest)
        target_step = 1 if target_max_ix > target_min_ix else -1
        target_max_ix += target_step
        if target_max_ix < 0:
            target_max_ix = None
        source_min_ix = source_dim_values.index(smallest)
        source_max_ix = source_dim_values.index(largest)
        source_step = 1 if source_max_ix > source_min_ix else -1
        source_max_ix += source_step
        if source_max_ix < 0:
            source_max_ix = None

        return slice(target_min_ix,target_max_ix,target_step), slice(source_min_ix,source_max_ix,source_step)

    def run(self):

        # work out a list of all input filenames
        filenames = []
        for input_folder in self.input_folders:
            filenames += [(input_folder, name) for name in os.listdir(input_folder) if name.endswith(".nc")]

        filenames = list(map(lambda t: os.path.join(*t), sorted(filenames, key=lambda t: t[1])))
        if len(filenames) == 0:
            raise RuntimeError("No files found to append")

        # reduce filenames using sample and/or limit if specified
        if self.sample is not None:
            filenames = list(filter(lambda t: random.random() < self.sample, filenames))

        if self.limit is not None:
            filenames = filenames[:self.limit]

        print(f"found {len(filenames)} filenames")

        # get a list of target variables
        first_file = filenames[0]
        first_ds = xr.open_dataset(first_file)
        target_variables = []
        for variable in first_ds.variables:
            v = first_ds.variables[variable]
            if "x" in v.dims and "y" in v.dims:
                # TODO if time is present, remove it from the dimensions and shape
                if variable == "lat" or variable == "lon":
                    continue
                target_variables.append((variable, v.dims, v.dtype, v.attrs, v.shape))
                print(variable)
        first_ds.close()

        # create the output file if it does not already exist

        arrays = {} # hold the target numpy arrays for each variable

        output_ds = xr.open_dataset(self.target_grid,mode="r")
        # add in empty target variables
        target_shape = output_ds.lat.shape
        for (name,dims,dtype,attrs,_) in target_variables:
            data = np.zeros(shape=target_shape,dtype=dtype)
            if not np.issubdtype(dtype, np.integer):
                data[...] = np.nan
            da = xr.DataArray(data,name=name,attrs=attrs,dims=dims)
            output_ds[name] = da
            arrays[name] = da.data

        for filename in filenames:
            print(f"processing {filename}")
            input_ds = xr.open_dataset(filename,mode="r")

            # get the slices for the overlap in both x and y dimensions.
            target_slice_x, source_slice_x = self.make_slices(output_ds.x.data.tolist(),input_ds.x.data.tolist())
            target_slice_y, source_slice_y = self.make_slices(output_ds.y.data.tolist(), input_ds.y.data.tolist())

            if target_slice_y is None or target_slice_y is None or source_slice_x is None or source_slice_y is None:
                self.logger.warning(f"No overlap for file {filename} with target grid, ignoring")
                continue

            for (name,dims,dtype,_,_) in target_variables:
                # update a slice of the target numpy array with the overlapping source array slice
                a = arrays[name]
                target_lookup = []
                source_lookup = []
                for d in dims:
                    if d == "x":
                        source_lookup.append(source_slice_x)
                        target_lookup.append(target_slice_x)
                    elif d == "y":
                        source_lookup.append(source_slice_y)
                        target_lookup.append(target_slice_y)
                    else:
                        source_lookup.append(slice(0, None))
                        target_lookup.append(slice(0, None))

                orig = a[tuple(target_lookup)]
                if not np.issubdtype(dtype, np.integer):
                    a[tuple(target_lookup)] = np.where(np.isnan(orig),input_ds[name].data[tuple(source_lookup)],orig)
                else:
                    a[tuple(target_lookup)] = np.where(orig == 0, input_ds[name].data[tuple(source_lookup)],orig)

        for name in arrays:
            output_ds[name].data = arrays[name]
        output_ds.to_netcdf(self.output_path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output-path", help="output path to store the stacked data", required=True)

    parser.add_argument(
        "--target-grid-path", help="path to the grid to use, if output path does not already exist", required=True)

    parser.add_argument(
        "--input-folders",
        help="paths to the input folder(s) containing files to stack (note: filenames must sort in ascending time order)",
        nargs="+", required=True)

    parser.add_argument(
        "--limit", metavar="LIMIT", help="limit processed scenes by count", default=None, type=int)

    parser.add_argument(
        "--sample", metavar="LIMIT", help="limit processed scenes by fraction", default=None, type=float)

    args = parser.parse_args()

    processor = Assembler(args.input_folders, args.target_grid_path, args.output_path, args.limit, args.sample)
    processor.run()
