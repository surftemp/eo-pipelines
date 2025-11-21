# MIT License
#
# Copyright (c) 2022-2025 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# merge_datasets.py - A script for merging variables from an auxilary dataset into another dataset, with the following caveats:
#
# (1) The target variable from the main dataset must be selected so that merged variables can be assigned a compatible shape and dimensions.
# use --target-variable to select this.
#
# (2) The auxilary dataset variables may have different coordinate and dimension names and dimension orderings compared to the target variable
# in the main dataset but must have the same number of elements.  Use --aux-variable VAR1 VAR2 ... to select the variables to merge.
#
# (3) A map of coordinates should be provided to guide the process, flipping the dimensions if necessary.  For example, if both datasets use
# lat and lon as (effectively if not explicitly) coordinates, but are named lat/lon in the main dataset and latitude/longitude in the
# auxilary dataset, use command line options: --coordinate-map lat latitude --coordinate-map lon longitude
#
# (4) If no coordinate map is provided, a warning will be printed - because an assumption must be made that the auxilary and target variables are
# in the same orientation, without any checking of coordinates

import argparse
import xarray as xr
import numpy as np
import os
import sys
import logging

from itertools import chain, combinations

parser = argparse.ArgumentParser()
parser.add_argument("main_path", help="path of main dataset to merge variables into")
parser.add_argument("aux_path", help="path of auxilary dataset to merge variables from")
parser.add_argument("output_folder",help="path to a folder to write the merged dataset to")
parser.add_argument("--target-variable", required=True, help="name of a variable in the main input dataset providing the shape and dimensions that merged variables should adhere to")
parser.add_argument("--aux-variable", nargs="+", required=True, help="name of one or more variables to merge into the main dataset from the auxilary dataset")
parser.add_argument("--coordinate-map", nargs=2, action="append", required=True, help="Provide pairs of coordinate names from the main and auxilary dataset to check for compatible merging.  These are used to check the orientation of the auxilary dataset dimensions with respect to the main dataset")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

args = parser.parse_args()
main_ds = xr.open_dataset(args.main_path)
logging.info(f"Reading main dataset from {args.main_path}")
target_da = main_ds[args.target_variable]
target_shape = target_da.shape
target_dims = target_da.dims

logging.info(f"Reading auxilary dataset from {args.aux_path}")
aux_ds = xr.open_dataset(args.aux_path)

def powerset(iterable):
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

coordinate_map = {}

for (main_coord,aux_coord) in args.coordinate_map:
    coordinate_map[main_coord] = aux_coord

if len(coordinate_map) > 0:
    logging.info("Aligning coordinates...")
    # work out which dimension flips are needed to make the coordinates match between main and auxilary data
    flips = None
    for coord,mapped_coord in coordinate_map.items():
        # get the coordinates to compare
        a = main_ds[coord]
        b = aux_ds[mapped_coord]
        flip_dims = None

        # get every combination of dimension in this coordinate in the aux dataset (including no dimensions)
        # test flipping along each combination of dimensions to check that the coordinates are identical
        flip_combinations = list(powerset(aux_ds[mapped_coord].dims))
        logging.info(f"Checking coordinate {coord}/{mapped_coord} with the following combinations of dimension flips: {flip_combinations}")
        for flip_combination in flip_combinations:

            flipped = b
            for dname in flip_combination:
                flipped = flipped.isel(**{dname:slice(None,None,-1)})

            if np.array_equal(a.data, flipped.data):
                # these flips seem to work
                flip_dims = set()
                for dname in flip_combination:
                    flip_dims.add(dname)
                break

        if flips is None:
            flips = flip_dims
        if flip_dims is None or flip_dims != flips:
            logger.error("Unable to merge datasets, could not align coordinates")
            sys.exit(-1)
        flips = flip_dims
else:
    # no coordinate map provided, we cannot determine which flips are needed
    logger.warning("No coordinate map provided, fallback to simple reshape/dimension renaming without coordinate checks, check the results carefully.")
    flips = set()

if len(flips) > 0:
    print(f"Flipping dimensions on variables to be merged: {flips}")

# apply the flips to each of the variables to merge in
encodings = {}
for aux_var in args.aux_variable:
    logger.info(f"Merging auxilary variable {aux_var} to main dataset")
    aux_da = aux_ds[aux_var]
    if len(flips):
        aux_da = aux_da.isel(**{f:slice(None,None,-1) for f in flips})
    main_ds[aux_var] = xr.DataArray(aux_da.data.reshape(target_shape), dims=target_dims, attrs=aux_ds.attrs)
    encodings[aux_var] = {'zlib': True, 'complevel': 5}

# make a directory to hold the outputs if necessary
if not os.path.exists(args.output_folder):
    logger.info(f"Creating output folder {args.output_folder}")
    os.makedirs(args.output_folder)

# write the merged data out
output_path = os.path.join(args.output_folder, os.path.split(args.main_path)[-1])
logger.info(f"Writing merged dataset to {output_path}")
main_ds.to_netcdf(output_path, encoding=encodings)



