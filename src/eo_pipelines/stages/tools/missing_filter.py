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

"""Utility for filtering scenes with more than a certain fraction of missing values in a band"""

import shutil
import xarray as xr
import logging
import os.path


class MissingFilter:

    def __init__(self, input_folder, output_folder, band, max_missing_fraction):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.band = band
        self.max_missing_fraction = max_missing_fraction
        self.logger = logging.getLogger("MissingFilter")

    def run(self):

        # work out a list of all filenames
        filenames = [name for name in os.listdir(self.input_folder) if name.endswith(".nc")]

        if len(filenames) == 0:
            self.logger.warning("No files found to append")

        os.makedirs(self.output_folder, exist_ok=True)

        included_count = 0
        for filename in filenames:
            input_path = os.path.join(self.input_folder, filename)
            output_path = os.path.join(self.output_folder, filename)

            with xr.open_dataset(input_path) as ds:
                if self.band not in ds.variables:
                    self.logger.warning(f"Band {self.band} not found in input dataset {input_path}, skipping")
                    continue

                da = ds[self.band]
                missing_fraction = 1 - float(da.count()) / da.size
                if missing_fraction > self.max_missing_fraction:
                    continue  # filter this file out

                shutil.copyfile(input_path, output_path)
                included_count += 1
        included_pct = int(100 * included_count / len(filenames))

        self.logger.info(f"Included {included_count} scenes ({included_pct} %)")


def main():
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-folder", help="output folder to store filtered scenes", required=True)

    parser.add_argument(
        "--input-folder",
        help="input folder containing scenes to be filtered",
        required=True)

    parser.add_argument(
        "--band", metavar="BAND", help="limit processed scenes by count", default=None)

    parser.add_argument(
        "--max-missing-fraction", metavar="FRACTION",
        help="filter out scenes with more than this fraction of missing values", default=None, type=float)

    args = parser.parse_args()

    processor = MissingFilter(args.input_folder, args.output_folder, args.band, args.max_missing_fraction)
    processor.run()


if __name__ == '__main__':
    main()
