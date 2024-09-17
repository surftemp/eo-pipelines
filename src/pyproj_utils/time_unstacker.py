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

"""Utility for breaking apart a dataset file into multiple individual files, one of each time value"""

import xarray as xr
import logging
import os.path
import datetime


class TimeUnStacker:

    def __init__(self, input_path, output_pattern, output_folder, time_variable_name, remove_time):
        self.input_path = input_path
        self.output_pattern = output_pattern
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
        self.time_variable_name = time_variable_name
        self.remove_time = remove_time
        self.logger = logging.getLogger("TimeUnStacker")

    def run(self):
        with xr.open_dataset(self.input_path) as ds:
            for idx in range(ds.time.shape[0]):
                d = datetime.datetime.strptime(str(ds.time[idx].data).split("T")[0], "%Y-%m-%d").date()
                output_filename = d.strftime(self.output_pattern)
                output_path = os.path.join(self.output_folder, output_filename)
                self.logger.info(f"Writing {output_path}")
                if not self.remove_time:
                    out_ds = ds.isel(**{self.time_variable_name:range(idx,idx+1)})
                else:
                    out_ds = ds.isel(**{self.time_variable_name: idx})
                out_ds.to_netcdf(output_path)

def main():
    logging.basicConfig(level=logging.INFO)
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "input_path",
        help="paths to the input file to unstack (note: filenames MUST sort in ascending time order)")

    parser.add_argument(
        "output_folder", help="output path to store the unstacked data")

    parser.add_argument(
        "--output-pattern", help="output path to store the unstacked data", default=None)

    parser.add_argument(
        "--time-variable-name", help="name of the time variable", default="time")

    parser.add_argument(
        "--remove-time", help="remove the time dimension from the output", action="store_true")

    args = parser.parse_args()

    output_pattern = args.output_pattern
    if output_pattern is None:
        output_pattern = "%Y%m%d-"+os.path.split(args.input_path)[-1]

    processor = TimeUnStacker(args.input_path, output_pattern, args.output_folder, args.time_variable_name, args.remove_time)
    processor.run()

if __name__ == '__main__':
    main()