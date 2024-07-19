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

"""Utility for stacking together individual scenes acquired at different times"""

import time
import xarray as xr
import netCDF4
import logging
import os.path
import random


class TimeStacker:

    def __init__(self, input_folders, output_path, limit, sample, batch_size, remove_attributes):
        self.input_folders = input_folders
        self.output_path = output_path
        self.limit = limit
        self.sample = sample
        self.batch_size = batch_size
        self.remove_attributes = remove_attributes
        self.logger = logging.getLogger("TimeStacker")
        self.retry_count = 3
        self.retry_delay_s = 10

        self.retry_append_count = 5
        self.retry_append_delay_s = 30

    def run(self):

        # work out a list of all filenames
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

        # scan the first file and work out which variables will be appended (ie those with time dimensions)
        append_variables = []  # list of (name,dimensions)
        pos = 0  # start position in filenames for appending
        with xr.open_dataset(filenames[0]) as first_ds:
            # if no output file exists, use the first file, setting the time dimension to unlimited
            if not os.path.exists(self.output_path):
                if self.remove_attributes:
                    for remove_attribute in self.remove_attributes:
                        if remove_attribute in first_ds.attrs:
                            del first_ds.attrs[remove_attribute]
                first_ds.to_netcdf(self.output_path, unlimited_dims=["time"])
                pos = 1
            # get a list of all variables which include the time dimension
            for variable in first_ds.variables:
                v = first_ds.variables[variable]
                if "time" in v.dims:
                    append_variables.append((variable, v.dims))

        # go through the filenames
        while pos < len(filenames):
            # append a batch of files to the output
            start_time = time.time()
            append_count = 0
            # open the output file for append, with retries
            for i in range(self.retry_append_count):
                try:
                    data = netCDF4.Dataset(self.output_path, "a")
                except:
                    self.logger.exception(f"error opening {self.output_path} for append, retry {i + 1}/{self.retry_count}")
                    time.sleep(self.retry_append_delay_s)
                    continue
                break

            # work out length of the time dimension
            count = len(data.variables["time"])
            while pos < len(filenames) and append_count < self.batch_size:
                # append a file to the output
                for i in range(self.retry_count):
                    try:
                        filename = filenames[pos]
                        add_data = netCDF4.Dataset(filename)
                        if len(add_data.variables["time"]) != 1:
                            raise RuntimeError(f"Can only append files with time dimension of size 1: {filename}")
                        for (variable, dims) in append_variables:
                            # append each variable
                            lh_lookup = []
                            rh_lookup = []
                            for d in dims:
                                if d == "time":
                                    lh_lookup.append(count)
                                    rh_lookup.append(0)
                                else:
                                    lh_lookup.append(slice(0, None))
                                    rh_lookup.append(slice(0, None))

                            data.variables[variable][tuple(lh_lookup)] = add_data.variables[variable][tuple(rh_lookup)]
                    except Exception:
                        self.logger.exception(f"error adding {filename} retry {i+1}/{self.retry_count}")
                        time.sleep(self.retry_delay_s)
                        continue
                    self.logger.info(f"added {filename}")
                    count += 1
                    append_count += 1
                    break
                pos += 1

            if append_count > 0:
                data.close()
                duration = int(time.time() - start_time)
                print(f"appended {append_count} files in {duration} seconds")


def main():
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-path", help="output path to store the stacked data", required=True)

    parser.add_argument(
        "--input-folders",
        help="paths to the input folder(s) containing files to stack (note: filenames MUST sort in ascending time order)",
        nargs="+", required=True)

    parser.add_argument(
        "--remove-attributes",
        help="specify one or more attributes to remove, if the output file does not exist (it will be created from the first input file)",
        nargs="+")

    parser.add_argument(
        "--limit", metavar="LIMIT", help="limit processed scenes by count", default=None, type=int)

    parser.add_argument(
        "--sample", metavar="LIMIT", help="limit processed scenes by random fraction", default=None, type=float)

    parser.add_argument(
        "--batch-size", type=int, metavar="BATCHSIZE", help="number of scenes to append before writing to disk",
        default=20)

    args = parser.parse_args()

    processor = TimeStacker(args.input_folders, args.output_path, args.limit, args.sample, args.batch_size,
                            args.remove_attributes)

    processor.run()

if __name__ == '__main__':
    main()