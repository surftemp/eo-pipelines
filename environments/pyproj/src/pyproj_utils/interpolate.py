#     Perform simple interpolation of missing values using means
#
#     Copyright (C) 2024 National Centre for Earth Observation (NCEO)
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
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="Path to input file")
parser.add_argument("output_file", help="Path to output file")

parser.add_argument("--interpolate-size", type=int, help="perform missing value interpolation with this size of box", default=3)
parser.add_argument("--interpolate-min", type=int, help="perform missing value interpolation with at least this number of items in the box", default=4)
parser.add_argument("--variables", nargs="+", help="names of variables to interpolate", required=True)
parser.add_argument("--dimensions", nargs="+", help="names of dimensions to interpolate along, specify if not interpolating along all non-unit dimensions", default=[])

args = parser.parse_args()

ds = xr.open_dataset(args.input_file)
encodings = {}
for variable in ds.variables:
    encodings[variable] = {k:v for k,v in ds[variable].encoding.items() if k in ['_FillValue', 'chunksizes', 'dtype', 'complevel', 'compression', 'zlib']}

for variable in args.variables:
    orig_da = ds[variable]
    interpolation_dimensions = args.dimensions
    if len(interpolation_dimensions) == 0:
        interpolation_dimensions = [orig_da.dims[idx] for idx in range(len(orig_da.dims)) if orig_da.shape[idx] > 1]
    if len(interpolation_dimensions) == 0:
        print(f"No dimensions for interpolation, skipping interpolation for {variable}")
    else:
        interpolation_spec = { name:args.interpolate_size for name in interpolation_dimensions}
        print(f"Calculating means to use in interpolation")
        means = orig_da.rolling(**interpolation_spec, center=True, min_periods=args.interpolate_min).mean()
        print(f"Interpolating {variable} along dimensions {','.join(interpolation_dimensions)}")
        ds[variable] = orig_da.where(~np.isnan(orig_da), means)

print(f"Writing results to {args.output_file}")
ds.to_netcdf(args.output_file, encoding=encodings)